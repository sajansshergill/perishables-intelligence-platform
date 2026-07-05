"""Tests for the dashboard data layer.

Builds a tiny gold table + dimensions in an in-memory DuckDB so the enrichment
SQL and the money-at-risk maths are exercised without needing a full dbt build.
"""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest

import dashboard_data as dd


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")

    risk = pd.DataFrame(
        [
            # store, product, flag, on_hand, days_left, velocity, lead, spoil, stock
            ("S001", "P00001", "STOCKOUT", 0,  2, 10.0, 3, 0.0, 1.0),
            ("S001", "P00002", "SPOILAGE", 100, 1, 5.0, 2, 0.90, 0.0),
            ("S002", "P00003", "OK",       40, 6, 8.0, 4, 0.10, 0.20),
        ],
        columns=[
            "store_id", "product_id", "risk_flag", "on_hand_qty",
            "days_remaining_shelf_life", "trailing_7d_sell_through",
            "replenishment_lead_days", "spoilage_risk_score", "stockout_risk_score",
        ],
    )
    risk["snapshot_date"] = "2026-07-30"
    risk["projected_demand_before_expiry"] = 0.0

    products = pd.DataFrame(
        [
            ("P00001", "Wild-Caught Salmon", "Seafood", 12.0, 8.0),
            ("P00002", "Organic Strawberries", "Produce", 5.0, 3.0),
            ("P00003", "Whole Milk", "Dairy", 4.0, 2.5),
        ],
        columns=["product_id", "product_name", "category", "unit_price", "unit_cost"],
    )
    stores = pd.DataFrame(
        [
            ("S001", "Austin #001", "South"),
            ("S002", "Seattle #002", "West"),
        ],
        columns=["store_id", "store_name", "region"],
    )

    c.register("risk_df", risk)
    c.register("prod_df", products)
    c.register("store_df", stores)
    c.execute("create table perishables_risk as select * from risk_df")
    c.execute("create table dim_product as select * from prod_df")
    c.execute("create table dim_store as select * from store_df")
    return c


def test_enrich_attaches_dims_and_money(con):
    df = dd.load_enriched(con)
    assert len(df) == 3
    assert {"category", "region", "spoilage_cost_at_risk", "stockout_lost_revenue_per_day"} <= set(df.columns)

    salmon = df[df["product_id"] == "P00001"].iloc[0]
    # Stockout lost revenue ≈ velocity * price = 10 * 12.
    assert salmon["stockout_lost_revenue_per_day"] == pytest.approx(120.0)

    berries = df[df["product_id"] == "P00002"].iloc[0]
    # Spoilage cost ≈ spoil_score * on_hand * unit_cost = 0.9 * 100 * 3.
    assert berries["spoilage_cost_at_risk"] == pytest.approx(270.0)


def test_ok_rows_have_no_stockout_revenue(con):
    df = dd.load_enriched(con)
    milk = df[df["product_id"] == "P00003"].iloc[0]
    assert milk["stockout_lost_revenue_per_day"] == 0.0


def test_kpis(con):
    df = dd.load_enriched(con)
    k = dd.compute_kpis(df)
    assert k["sku_days"] == 3
    assert k["stockout_count"] == 1
    assert k["spoilage_count"] == 1
    assert k["ok_pct"] == pytest.approx(33.3, abs=0.1)
    assert k["stockout_lost_revenue_per_day"] == pytest.approx(120.0)
    assert k["spoilage_cost_at_risk"] == pytest.approx(270.0)


def test_flags_by_dimension(con):
    df = dd.load_enriched(con)
    by_region = dd.flags_by_dimension(df, "region")
    assert {"OK", "STOCKOUT", "SPOILAGE"} <= set(by_region.columns)
    south = by_region[by_region["region"] == "South"].iloc[0]
    assert south["STOCKOUT"] + south["SPOILAGE"] == 2  # both risky rows are in Austin


def test_top_risks_ranks_by_money(con):
    df = dd.load_enriched(con)
    stockouts = dd.top_risks(df, "STOCKOUT", n=5)
    assert list(stockouts.columns)[-1] == "stockout_lost_revenue_per_day"
    assert stockouts.iloc[0]["stockout_lost_revenue_per_day"] == pytest.approx(120.0)

    spoilage = dd.top_risks(df, "SPOILAGE", n=5)
    assert spoilage.iloc[0]["spoilage_cost_at_risk"] == pytest.approx(270.0)


def test_available_dates(con):
    assert dd.available_dates(con) == ["2026-07-30"]