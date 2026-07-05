"""Data access and business metrics for the Streamlit dashboard."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


DEFAULT_DB = Path(__file__).resolve().parents[2] / "warehouse" / "dbt" / "perishables.duckdb"
RISK_COLUMNS = [
    "snapshot_date",
    "store_id",
    "store_name",
    "region",
    "product_id",
    "product_name",
    "category",
    "risk_flag",
    "on_hand_qty",
    "days_remaining_shelf_life",
    "trailing_7d_sell_through",
    "projected_demand_before_expiry",
    "replenishment_lead_days",
    "spoilage_risk_score",
    "stockout_risk_score",
    "unit_price",
    "unit_cost",
    "stockout_lost_revenue_per_day",
    "spoilage_cost_at_risk",
]


def connect(path: str | os.PathLike[str] | None = None) -> duckdb.DuckDBPyConnection:
    """Open the local DuckDB warehouse built by dbt."""
    db_path = Path(path or os.environ.get("PERISHABLES_DUCKDB", DEFAULT_DB))
    return duckdb.connect(str(db_path), read_only=True)


def available_dates(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Return available gold-table dates, newest first."""
    rows = con.execute(
        """
        select distinct cast(snapshot_date as varchar) as snapshot_date
        from perishables_risk
        order by snapshot_date desc
        """
    ).fetchall()
    return [row[0] for row in rows]


def load_enriched(
    con: duckdb.DuckDBPyConnection, snapshot_date: str | None = None
) -> pd.DataFrame:
    """Load risk rows joined to product/store dimensions and dollar metrics."""
    where = ""
    params: list[Any] = []
    if snapshot_date is not None:
        where = "where r.snapshot_date = cast(? as date)"
        params.append(snapshot_date)

    df = con.execute(
        f"""
        select
            r.snapshot_date,
            r.store_id,
            s.store_name,
            s.region,
            r.product_id,
            p.product_name,
            p.category,
            r.risk_flag,
            r.on_hand_qty,
            r.days_remaining_shelf_life,
            r.trailing_7d_sell_through,
            r.projected_demand_before_expiry,
            r.replenishment_lead_days,
            r.spoilage_risk_score,
            r.stockout_risk_score,
            p.unit_price,
            p.unit_cost,
            case
                when r.risk_flag = 'STOCKOUT'
                then r.trailing_7d_sell_through * p.unit_price
                else 0.0
            end as stockout_lost_revenue_per_day,
            case
                when r.risk_flag = 'SPOILAGE'
                then r.spoilage_risk_score * r.on_hand_qty * p.unit_cost
                else 0.0
            end as spoilage_cost_at_risk
        from perishables_risk r
        left join dim_product p using (product_id)
        left join dim_store s using (store_id)
        {where}
        order by r.snapshot_date desc, r.risk_flag desc, r.store_id, r.product_id
        """,
        params,
    ).fetchdf()

    return df[RISK_COLUMNS]


def compute_kpis(df: pd.DataFrame) -> dict[str, float | int]:
    """Summarize the rows currently in view."""
    total = int(len(df))
    flags = df["risk_flag"].value_counts()
    ok = int(flags.get("OK", 0))
    return {
        "sku_days": total,
        "ok_pct": (ok / total * 100) if total else 0.0,
        "stockout_count": int(flags.get("STOCKOUT", 0)),
        "spoilage_count": int(flags.get("SPOILAGE", 0)),
        "stockout_lost_revenue_per_day": float(df["stockout_lost_revenue_per_day"].sum()),
        "spoilage_cost_at_risk": float(df["spoilage_cost_at_risk"].sum()),
    }


def flags_by_dimension(df: pd.DataFrame, dimension: str) -> pd.DataFrame:
    """Count risk flags by a categorical dashboard dimension."""
    allowed = {"region", "category", "store_name", "product_name"}
    if dimension not in allowed:
        raise ValueError(f"unsupported dimension: {dimension}")

    out = (
        df.pivot_table(index=dimension, columns="risk_flag", values="product_id", aggfunc="count", fill_value=0)
        .reset_index()
        .rename_axis(None, axis=1)
    )
    for flag in ("OK", "STOCKOUT", "SPOILAGE"):
        if flag not in out.columns:
            out[flag] = 0
    return out[[dimension, "OK", "STOCKOUT", "SPOILAGE"]]


def top_risks(df: pd.DataFrame, flag: str, n: int = 25) -> pd.DataFrame:
    """Return a prioritized action list for the requested risk flag."""
    if flag == "STOCKOUT":
        metric = "stockout_lost_revenue_per_day"
    elif flag == "SPOILAGE":
        metric = "spoilage_cost_at_risk"
    else:
        raise ValueError(f"unsupported risk flag: {flag}")

    cols = [
        "store_id",
        "store_name",
        "region",
        "product_id",
        "product_name",
        "category",
        "on_hand_qty",
        "days_remaining_shelf_life",
        "trailing_7d_sell_through",
        "spoilage_risk_score",
        "stockout_risk_score",
        metric,
    ]
    return df[df["risk_flag"] == flag].sort_values(metric, ascending=False).head(n)[cols]


def daily_trend(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Count risk flags by day for the dashboard trend chart."""
    return con.execute(
        """
        select cast(snapshot_date as varchar) as snapshot_date, risk_flag, count(*) as n
        from perishables_risk
        group by 1, 2
        order by 1, 2
        """
    ).fetchdf()
