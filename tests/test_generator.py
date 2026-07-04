"""Invariant tests for the synthetic data generator.

These are the data-contract checks that graduate into the pipeline's CI quality
gate: if the raw data can't satisfy them, nothing downstream should trust it.
A small dataset is generated once per module and shared across tests.
"""
from __future__ import annotations

from datetime import date

import pytest

from generate import generate_all

SEED = 123
START = date(2025, 1, 1)


@pytest.fixture(scope="module")
def tables() -> dict:
    return generate_all(
        n_stores=6, n_skus=80, days=30, n_suppliers=8, start_date=START, seed=SEED
    )


# --------------------------------------------------------------------------- #
# Structural integrity
# --------------------------------------------------------------------------- #
def test_all_expected_tables_present(tables):
    expected = {
        "dim_supplier", "dim_store", "dim_product", "dim_shelf_life",
        "dim_date", "fact_sales", "fact_inventory_snapshot",
    }
    assert expected == set(tables)


def test_referential_integrity(tables):
    """Every fact key must resolve to a dimension row."""
    stores = set(tables["dim_store"]["store_id"])
    products = set(tables["dim_product"]["product_id"])
    for fact_name in ("fact_sales", "fact_inventory_snapshot"):
        fact = tables[fact_name]
        assert set(fact["store_id"]) <= stores, f"{fact_name} has orphan store_id"
        assert set(fact["product_id"]) <= products, f"{fact_name} has orphan product_id"


def test_inventory_snapshot_is_dense(tables):
    """Inventory is snapshotted for every store x SKU x day — no gaps."""
    inv = tables["fact_inventory_snapshot"]
    n_expected = 6 * 80 * 30
    assert len(inv) == n_expected
    assert not inv.duplicated(["store_id", "product_id", "snapshot_date"]).any()


# --------------------------------------------------------------------------- #
# Value-level data contracts
# --------------------------------------------------------------------------- #
def test_no_negative_quantities(tables):
    inv = tables["fact_inventory_snapshot"]
    for col in ("on_hand_qty", "received_qty", "spoiled_qty", "unmet_demand", "oldest_batch_age_days"):
        assert (inv[col] >= 0).all(), f"{col} has negative values"


def test_sales_only_recorded_when_something_sold(tables):
    assert (tables["fact_sales"]["units_sold"] > 0).all()


def test_revenue_matches_units_times_price(tables):
    s = tables["fact_sales"]
    recomputed = (s["units_sold"] * s["unit_price"]).round(2)
    assert (recomputed - s["revenue"]).abs().max() < 0.01


def test_stock_never_outlives_shelf_life(tables):
    """On-hand stock must always be younger than its current shelf life —
    anything older should already have been written off as spoilage."""
    inv = tables["fact_inventory_snapshot"]
    sl = tables["dim_shelf_life"].query("is_current")[["product_id", "shelf_life_days"]]
    on_shelf = inv[inv["on_hand_qty"] > 0].merge(sl, on="product_id")
    assert (on_shelf["oldest_batch_age_days"] < on_shelf["shelf_life_days"]).all()


# --------------------------------------------------------------------------- #
# The dataset must actually contain the problems the platform detects
# --------------------------------------------------------------------------- #
def test_dataset_contains_both_failure_modes(tables):
    inv = tables["fact_inventory_snapshot"]
    assert (inv["on_hand_qty"] == 0).any(), "no stockouts present"
    assert (inv["spoiled_qty"] > 0).any(), "no spoilage present"


def test_spoilage_concentrates_in_short_shelf_life(tables):
    """Face-validity check: the most perishable category should spoil at a
    higher rate than the least perishable one."""
    inv = tables["fact_inventory_snapshot"]
    prod = tables["dim_product"][["product_id", "category"]]
    sl = tables["dim_shelf_life"].query("is_current")[["product_id", "shelf_life_days"]]
    m = inv.merge(prod, on="product_id").merge(sl, on="product_id")
    by_cat = m.groupby("category").agg(
        spoiled=("spoiled_qty", "sum"),
        received=("received_qty", "sum"),
        shelf=("shelf_life_days", "mean"),
    )
    by_cat["rate"] = by_cat["spoiled"] / by_cat["received"].clip(lower=1)
    shortest = by_cat["shelf"].idxmin()
    longest = by_cat["shelf"].idxmax()
    assert by_cat.loc[shortest, "rate"] > by_cat.loc[longest, "rate"]


# --------------------------------------------------------------------------- #
# Reproducibility
# --------------------------------------------------------------------------- #
def test_same_seed_is_deterministic():
    a = generate_all(n_stores=4, n_skus=40, days=20, n_suppliers=5, start_date=START, seed=SEED)
    b = generate_all(n_stores=4, n_skus=40, days=20, n_suppliers=5, start_date=START, seed=SEED)
    for name in a:
        assert a[name].equals(b[name]), f"{name} not reproducible under a fixed seed"


def test_scd2_has_current_record_per_product(tables):
    """Every product must have exactly one current shelf-life record."""
    sl = tables["dim_shelf_life"]
    current_counts = sl[sl["is_current"]].groupby("product_id").size()
    assert (current_counts == 1).all()