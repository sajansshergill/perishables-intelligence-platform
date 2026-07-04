"""Seed the synthetic perishables dataset.

Usage (matches the project README):
    python data/generators/seed.py --stores 20 --skus 500 --days 30

Writes each table to Parquet (facts) and CSV (dimensions, for easy eyeballing)
under the output directory, then prints a summary that confirms the data
actually contains the spoilage and stockout situations the platform detects.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from generate import generate_all


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic perishables data.")
    p.add_argument("--stores", type=int, default=20, help="number of stores")
    p.add_argument("--skus", type=int, default=500, help="number of products (SKUs)")
    p.add_argument("--days", type=int, default=30, help="length of the simulation window")
    p.add_argument("--suppliers", type=int, default=15, help="number of suppliers")
    p.add_argument("--seed", type=int, default=42, help="random seed for reproducibility")
    p.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today().replace(day=1),
        help="first day of the window (YYYY-MM-DD)",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "data" / "generated",
        help="output directory",
    )
    return p.parse_args()


def write_tables(tables: dict[str, pd.DataFrame], out: Path) -> None:
    dims_dir = out / "dims"
    facts_dir = out / "facts"
    dims_dir.mkdir(parents=True, exist_ok=True)
    facts_dir.mkdir(parents=True, exist_ok=True)

    for name, df in tables.items():
        if name.startswith("fact_"):
            df.to_parquet(facts_dir / f"{name}.parquet", index=False)
        else:
            # Dimensions are small — write both so they're trivial to inspect.
            df.to_parquet(dims_dir / f"{name}.parquet", index=False)
            df.to_csv(dims_dir / f"{name}.csv", index=False)


def print_summary(tables: dict[str, pd.DataFrame], out: Path) -> None:
    inv = tables["fact_inventory_snapshot"]
    sales = tables["fact_sales"]

    line = "─" * 64
    print(f"\n{line}\n  PERISHABLES DATASET — GENERATION SUMMARY\n{line}")
    print(f"  output: {out}")
    print(f"  window: {inv['snapshot_date'].min()} → {inv['snapshot_date'].max()}\n")

    print("  ROW COUNTS")
    for name, df in tables.items():
        print(f"    {name:<28} {len(df):>10,}")

    # Prove the data contains the two failure modes worth detecting.
    stockout_days = int((inv["on_hand_qty"] == 0).sum())
    lost_sales_days = int((inv["unmet_demand"] > 0).sum())
    spoilage_days = int((inv["spoiled_qty"] > 0).sum())
    spoiled_units = int(inv["spoiled_qty"].sum())
    total_revenue = float(sales["revenue"].sum())

    print("\n  SIGNAL CHECK  (the problems the platform exists to catch)")
    print(f"    snapshot rows at zero on-hand    {stockout_days:>10,}")
    print(f"    snapshot rows with unmet demand  {lost_sales_days:>10,}")
    print(f"    snapshot rows with spoilage      {spoilage_days:>10,}")
    print(f"    total units spoiled              {spoiled_units:>10,}")
    print(f"    total sales revenue              ${total_revenue:>12,.2f}")

    # A couple of illustrative rows: aging stock still sitting on the shelf.
    aging = inv[(inv["on_hand_qty"] > 0) & (inv["oldest_batch_age_days"] >= 3)]
    if not aging.empty:
        print("\n  SAMPLE: aging stock still on shelf (spoilage-risk candidates)")
        cols = ["store_id", "product_id", "snapshot_date", "on_hand_qty", "oldest_batch_age_days"]
        print(aging.sort_values("oldest_batch_age_days", ascending=False).head(5)[cols].to_string(index=False))
    print(f"{line}\n")


def main() -> None:
    args = parse_args()
    print(
        f"Generating: {args.stores} stores × {args.skus} SKUs × {args.days} days "
        f"(seed={args.seed}) …"
    )
    tables = generate_all(
        n_stores=args.stores,
        n_skus=args.skus,
        days=args.days,
        n_suppliers=args.suppliers,
        start_date=args.start_date,
        seed=args.seed,
    )
    write_tables(tables, args.out)
    print_summary(tables, args.out)


if __name__ == "__main__":
    main()