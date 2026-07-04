"""Synthetic data generator for the Perishables Intelligence Platform.

The point of this module is not just to emit rows — it's to emit rows that
*contain the problem the platform solves*. A day-by-day inventory simulation
with FIFO batch aging produces natural spoilage (over-ordered, slow-selling,
aging stock) and natural stockouts (under-ordered stock that runs dry against
real demand). Downstream models then have something real to detect.

Grain of the emitted facts:
  * fact_sales             -> store x product x day   (only days a sale occurred)
  * fact_inventory_snapshot-> store x product x day   (every day, end-of-day state)

Business logic (e.g. "days remaining shelf life", the risk scores) is deliberately
NOT computed here. This module produces raw-ish operational data; the warehouse
layer joins it to the shelf-life dimension and derives the analytics. That
separation is the whole point of a medallion architecture.
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from config import (
    CATEGORY_SPECS,
    DOW_DEMAND_MULTIPLIER,
    ORDER_BIAS,
    PRODUCT_NAME_PARTS,
    RISK_PROFILE_WEIGHTS,
    SCD2_REVISION_SHARE,
    STORE_FORMATS,
    STORE_LOCATIONS,
)

CATEGORY_BY_NAME = {c.category: c for c in CATEGORY_SPECS}


# --------------------------------------------------------------------------- #
# Dimensions
# --------------------------------------------------------------------------- #
def generate_suppliers(n_suppliers: int, rng: np.random.Generator) -> pd.DataFrame:
    rows = []
    for i in range(1, n_suppliers + 1):
        rows.append(
            {
                "supplier_id": f"SUP{i:03d}",
                "supplier_name": f"Supplier {i:03d}",
                # Replenishment lead time drives how often a store can reorder.
                "lead_time_days": int(rng.integers(1, 6)),
                "reliability": round(float(rng.uniform(0.85, 0.99)), 3),
            }
        )
    return pd.DataFrame(rows)


def generate_stores(n_stores: int, rng: np.random.Generator) -> pd.DataFrame:
    rows = []
    for i in range(1, n_stores + 1):
        city, state, region = STORE_LOCATIONS[(i - 1) % len(STORE_LOCATIONS)]
        rows.append(
            {
                "store_id": f"S{i:03d}",
                "store_name": f"{city} #{i:03d}",
                "store_format": STORE_FORMATS[int(rng.integers(0, len(STORE_FORMATS)))],
                "city": city,
                "state": state,
                "region": region,
                # Size scales a store's baseline demand up or down.
                "size_factor": round(float(rng.uniform(0.6, 1.4)), 2),
                "open_date": (date(2015, 1, 1) + timedelta(days=int(rng.integers(0, 3000)))).isoformat(),
            }
        )
    return pd.DataFrame(rows)


def generate_products(
    n_skus: int, suppliers: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    supplier_ids = suppliers["supplier_id"].to_numpy()
    rows = []
    for i in range(1, n_skus + 1):
        spec = CATEGORY_SPECS[int(rng.integers(0, len(CATEGORY_SPECS)))]
        adjectives, nouns = PRODUCT_NAME_PARTS[spec.category]
        name = f"{rng.choice(adjectives)} {rng.choice(nouns)}"
        price = round(float(rng.uniform(*spec.unit_price)), 2)
        rows.append(
            {
                "product_id": f"P{i:05d}",
                "product_name": name,
                "category": spec.category,
                "supplier_id": str(rng.choice(supplier_ids)),
                "unit_price": price,
                "unit_cost": round(price * (1 - spec.gross_margin), 2),
                # Popularity nudges this SKU's demand above/below its category norm.
                "popularity": round(float(rng.uniform(0.5, 1.5)), 2),
            }
        )
    return pd.DataFrame(rows)


def generate_shelf_life_scd2(
    products: pd.DataFrame, start_date: date, rng: np.random.Generator
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Build a Type-2 slowly-changing shelf-life dimension.

    Most SKUs have a single current record. A share (``SCD2_REVISION_SHARE``)
    were revised before the simulation window opened, leaving an expired prior
    record and a current one. Returns the dimension plus a ``product_id ->
    current shelf_life_days`` lookup the simulation uses.
    """
    rows = []
    current_shelf_life: dict[str, int] = {}
    for _, p in products.iterrows():
        spec = CATEGORY_BY_NAME[p["category"]]
        lo, hi = spec.shelf_life_days
        current = int(rng.integers(lo, hi + 1))
        current_shelf_life[p["product_id"]] = current

        if rng.random() < SCD2_REVISION_SHARE and hi - lo >= 1:
            # A prior spec that was superseded before the window opened.
            prior = max(lo, current + int(rng.choice([-2, -1, 1, 2])))
            revised_on = start_date - timedelta(days=int(rng.integers(30, 180)))
            rows.append(
                {
                    "product_id": p["product_id"],
                    "shelf_life_days": prior,
                    "effective_from": (revised_on - timedelta(days=int(rng.integers(180, 720)))).isoformat(),
                    "effective_to": revised_on.isoformat(),
                    "is_current": False,
                }
            )
            rows.append(
                {
                    "product_id": p["product_id"],
                    "shelf_life_days": current,
                    "effective_from": revised_on.isoformat(),
                    "effective_to": None,
                    "is_current": True,
                }
            )
        else:
            rows.append(
                {
                    "product_id": p["product_id"],
                    "shelf_life_days": current,
                    "effective_from": (start_date - timedelta(days=int(rng.integers(200, 900)))).isoformat(),
                    "effective_to": None,
                    "is_current": True,
                }
            )
    return pd.DataFrame(rows), current_shelf_life


def generate_date_dim(start_date: date, days: int) -> pd.DataFrame:
    rows = []
    for t in range(days):
        d = start_date + timedelta(days=t)
        rows.append(
            {
                "date": d.isoformat(),
                "year": d.year,
                "month": d.month,
                "day": d.day,
                "day_of_week": d.strftime("%A"),
                "is_weekend": d.weekday() >= 5,
            }
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# The simulation
# --------------------------------------------------------------------------- #
def _simulate_combo(
    *,
    store: pd.Series,
    product: pd.Series,
    shelf_life: int,
    lead_time: int,
    risk_profile: str,
    days: int,
    start_date: date,
    rng: np.random.Generator,
) -> tuple[list[dict], list[dict]]:
    """Run one store x SKU through ``days`` of ordering, selling and spoiling.

    Uses a FIFO list of stock batches so that aging — and therefore spoilage —
    is tracked correctly: the oldest units are sold first, and any batch that
    outlives the shelf life is written off.
    """
    spec = CATEGORY_BY_NAME[product["category"]]
    base_demand = (
        float(np.mean(spec.base_daily_demand))
        * store["size_factor"]
        * product["popularity"]
    )
    base_demand = max(0.5, base_demand)

    # Reorder cadence tracks supplier lead time, but perishable items are
    # restocked at least as often as they'd spoil — you take seafood deliveries
    # daily, not every five days. Capping cadence at the shelf life is what
    # keeps short-life SKUs from sitting structurally empty between trucks.
    cadence = max(1, min(lead_time + int(rng.integers(0, 3)), shelf_life))
    order_offset = int(rng.integers(0, cadence))

    # Periodic *order-up-to* (base-stock) policy: on each delivery day, top the
    # shelf back up to a target level rather than blindly reordering a fixed
    # amount. This is how real replenishment works and — crucially — it's
    # self-correcting, so inventory can't ratchet to absurd levels. The
    # risk profile biases the *target*: over-orderers aim too high (spoilage),
    # under-orderers aim too low (stockouts).
    # A sane buyer never stocks more than can sell before it expires, so
    # coverage is capped at the shelf life. Removing this cap is what makes
    # short-shelf-life categories (seafood, prepared foods) hemorrhage spoilage.
    coverage_days = min(cadence + 1, max(1, shelf_life))
    target_level = max(
        1, int(round(base_demand * coverage_days * ORDER_BIAS[risk_profile]))
    )

    # Seed with an opening batch near the target level at a random partial age.
    opening_qty = max(1, int(round(target_level * rng.uniform(0.4, 0.9))))
    batches: list[dict] = [{"day": -int(rng.integers(0, max(1, shelf_life))), "qty": opening_qty}]

    sales_rows: list[dict] = []
    inv_rows: list[dict] = []

    for t in range(days):
        d = start_date + timedelta(days=t)

        # 1. Expire anything past its shelf life (FIFO write-off).
        spoiled = 0
        survivors = []
        for b in batches:
            if (t - b["day"]) >= shelf_life:
                spoiled += b["qty"]
            else:
                survivors.append(b)
        batches = survivors

        # 2. On a delivery day, order up to the target level (never negative).
        received = 0
        if t % cadence == order_offset:
            on_hand_now = sum(b["qty"] for b in batches)
            received = max(0, target_level - on_hand_now)
            if received > 0:
                batches.append({"day": t, "qty": received})

        # 3. Realise demand (Poisson around the day-of-week-adjusted rate).
        lam = base_demand * DOW_DEMAND_MULTIPLIER[d.weekday()]
        demand = int(rng.poisson(lam))
        on_hand = sum(b["qty"] for b in batches)
        sold = min(demand, on_hand)
        unmet = demand - sold

        # 4. Consume FIFO (oldest batch first).
        remaining = sold
        for b in sorted(batches, key=lambda x: x["day"]):
            if remaining <= 0:
                break
            take = min(b["qty"], remaining)
            b["qty"] -= take
            remaining -= take
        batches = [b for b in batches if b["qty"] > 0]

        on_hand_end = sum(b["qty"] for b in batches)
        oldest_age = max((t - b["day"] for b in batches), default=0)

        # A sales fact exists only when something actually sold (like reality).
        if sold > 0:
            sales_rows.append(
                {
                    "store_id": store["store_id"],
                    "product_id": product["product_id"],
                    "sale_date": d.isoformat(),
                    "units_sold": sold,
                    "unit_price": product["unit_price"],
                    "revenue": round(sold * product["unit_price"], 2),
                }
            )

        # Inventory is snapshotted every day for every SKU.
        inv_rows.append(
            {
                "store_id": store["store_id"],
                "product_id": product["product_id"],
                "snapshot_date": d.isoformat(),
                "on_hand_qty": on_hand_end,
                "received_qty": received,
                "oldest_batch_age_days": int(oldest_age),
                "spoiled_qty": spoiled,      # ground-truth shrink for validation
                "unmet_demand": unmet,       # ground-truth lost sales for validation
            }
        )

    return sales_rows, inv_rows


def simulate_operations(
    *,
    stores: pd.DataFrame,
    products: pd.DataFrame,
    current_shelf_life: dict[str, int],
    suppliers: pd.DataFrame,
    days: int,
    start_date: date,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Simulate every store x SKU combination and stack the results."""
    lead_time_by_supplier = dict(
        zip(suppliers["supplier_id"], suppliers["lead_time_days"])
    )
    profiles = list(RISK_PROFILE_WEIGHTS)
    profile_p = np.array(list(RISK_PROFILE_WEIGHTS.values()))
    profile_p = profile_p / profile_p.sum()

    all_sales: list[dict] = []
    all_inv: list[dict] = []

    for _, store in stores.iterrows():
        for _, product in products.iterrows():
            profile = str(rng.choice(profiles, p=profile_p))
            lead_time = int(lead_time_by_supplier[product["supplier_id"]])
            s_rows, i_rows = _simulate_combo(
                store=store,
                product=product,
                shelf_life=current_shelf_life[product["product_id"]],
                lead_time=lead_time,
                risk_profile=profile,
                days=days,
                start_date=start_date,
                rng=rng,
            )
            all_sales.extend(s_rows)
            all_inv.extend(i_rows)

    fact_sales = pd.DataFrame(all_sales)
    fact_inventory = pd.DataFrame(all_inv)
    return fact_sales, fact_inventory


def generate_all(
    *,
    n_stores: int,
    n_skus: int,
    days: int,
    n_suppliers: int,
    start_date: date,
    seed: int,
) -> dict[str, pd.DataFrame]:
    """Build the full dataset and return it as a dict of named DataFrames."""
    rng = np.random.default_rng(seed)

    suppliers = generate_suppliers(n_suppliers, rng)
    stores = generate_stores(n_stores, rng)
    products = generate_products(n_skus, suppliers, rng)
    shelf_life_dim, current_shelf_life = generate_shelf_life_scd2(products, start_date, rng)
    date_dim = generate_date_dim(start_date, days)

    fact_sales, fact_inventory = simulate_operations(
        stores=stores,
        products=products,
        current_shelf_life=current_shelf_life,
        suppliers=suppliers,
        days=days,
        start_date=start_date,
        rng=rng,
    )

    return {
        "dim_supplier": suppliers,
        "dim_store": stores,
        "dim_product": products,
        "dim_shelf_life": shelf_life_dim,
        "dim_date": date_dim,
        "fact_sales": fact_sales,
        "fact_inventory_snapshot": fact_inventory,
    }