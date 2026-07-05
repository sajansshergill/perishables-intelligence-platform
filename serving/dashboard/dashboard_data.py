"""Data access and business metrics for the Streamlit dashboard."""
from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
GENERATOR_DIR = ROOT / "data" / "generators"
DEFAULT_DB = Path(tempfile.gettempdir()) / "perishables_demo.duckdb"
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
    """Open the dashboard DuckDB database, bootstrapping demo data if needed."""
    db_path = Path(path or os.environ.get("PERISHABLES_DUCKDB", DEFAULT_DB))
    if not db_path.exists() and os.environ.get("PERISHABLES_BOOTSTRAP", "1") != "0":
        bootstrap_warehouse(db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"DuckDB warehouse not found at {db_path}. Enable PERISHABLES_BOOTSTRAP "
            "or set PERISHABLES_DUCKDB to an existing database."
        )
    return duckdb.connect(str(db_path), read_only=True)


def bootstrap_warehouse(db_path: Path) -> None:
    """Build a small demo warehouse without relying on committed artifacts."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    generate_all = _load_generate_all()

    start_date = datetime.strptime(
        os.environ.get("PERISHABLES_DEMO_START_DATE", "2026-07-01"),
        "%Y-%m-%d",
    ).date()
    tables = generate_all(
        n_stores=int(os.environ.get("PERISHABLES_DEMO_STORES", "8")),
        n_skus=int(os.environ.get("PERISHABLES_DEMO_SKUS", "120")),
        days=int(os.environ.get("PERISHABLES_DEMO_DAYS", "21")),
        n_suppliers=int(os.environ.get("PERISHABLES_DEMO_SUPPLIERS", "15")),
        start_date=start_date,
        seed=int(os.environ.get("PERISHABLES_DEMO_SEED", "42")),
    )

    con = duckdb.connect(str(db_path))
    try:
        _load_generated_tables(con, tables)
        _build_dashboard_tables(con)
    finally:
        con.close()


def _load_generate_all():
    """Load the generator module from its script location."""
    if str(GENERATOR_DIR) not in sys.path:
        sys.path.insert(0, str(GENERATOR_DIR))

    spec = importlib.util.spec_from_file_location(
        "perishables_generate", GENERATOR_DIR / "generate.py"
    )
    if spec is None or spec.loader is None:
        raise ImportError("Could not load data generator")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.generate_all


def _load_generated_tables(
    con: duckdb.DuckDBPyConnection, tables: dict[str, pd.DataFrame]
) -> None:
    for name, df in tables.items():
        con.register(f"{name}_df", df)
        con.execute(f"create or replace table {name} as select * from {name}_df")
        con.unregister(f"{name}_df")


def _build_dashboard_tables(con: duckdb.DuckDBPyConnection) -> None:
    window_days = int(os.environ.get("PERISHABLES_SELL_THROUGH_WINDOW_DAYS", "7"))
    threshold = float(os.environ.get("PERISHABLES_SPOILAGE_FLAG_THRESHOLD", "0.70"))

    con.execute(
        """
        alter table dim_product rename to raw_dim_product
        """
    )
    con.execute(
        """
        create or replace table dim_product as
        select
            p.product_id,
            p.product_name,
            p.category,
            p.supplier_id,
            s.supplier_name,
            p.unit_price,
            p.unit_cost,
            round(p.unit_price - p.unit_cost, 2) as unit_margin,
            p.popularity
        from raw_dim_product p
        left join dim_supplier s using (supplier_id)
        """
    )
    con.execute(
        f"""
        create or replace table int_inventory_sell_through as
        with dense as (
            select
                inv.store_id,
                inv.product_id,
                cast(inv.snapshot_date as date) as snapshot_date,
                inv.on_hand_qty,
                inv.received_qty,
                inv.oldest_batch_age_days,
                inv.spoiled_qty,
                inv.unmet_demand,
                coalesce(s.units_sold, 0) as units_sold
            from fact_inventory_snapshot inv
            left join fact_sales s
                on inv.store_id = s.store_id
               and inv.product_id = s.product_id
               and cast(inv.snapshot_date as date) = cast(s.sale_date as date)
        )
        select
            *,
            avg(units_sold) over (
                partition by store_id, product_id
                order by snapshot_date
                rows between {window_days - 1} preceding and current row
            ) as trailing_avg_daily_sell_through
        from dense
        """
    )
    con.execute(
        f"""
        create or replace table perishables_risk as
        with enriched as (
            select
                v.store_id,
                v.product_id,
                v.snapshot_date,
                v.on_hand_qty,
                v.oldest_batch_age_days,
                v.trailing_avg_daily_sell_through as sell_through,
                sl.shelf_life_days,
                sup.lead_time_days,
                greatest(sl.shelf_life_days - v.oldest_batch_age_days, 0)
                    as days_remaining_shelf_life
            from int_inventory_sell_through v
            join dim_product p on v.product_id = p.product_id
            join dim_supplier sup on p.supplier_id = sup.supplier_id
            join dim_shelf_life sl on v.product_id = sl.product_id and sl.is_current
        ),
        scored as (
            select
                *,
                case
                    when on_hand_qty <= 0 then 0.0
                    else least(1.0, greatest(0.0,
                        (on_hand_qty - coalesce(sell_through, 0)
                            * greatest(days_remaining_shelf_life, 0))
                        / nullif(on_hand_qty, 0)
                    ))
                end as spoilage_risk_score,
                case
                    when coalesce(sell_through, 0) <= 0 then 0.0
                    when lead_time_days <= 0 then 0.0
                    else least(1.0, greatest(0.0,
                        (lead_time_days - (on_hand_qty / nullif(sell_through, 0)))
                        / lead_time_days
                    ))
                end as stockout_risk_score
            from enriched
        )
        select
            store_id,
            product_id,
            snapshot_date,
            on_hand_qty,
            days_remaining_shelf_life,
            round(sell_through, 2) as trailing_7d_sell_through,
            round(sell_through * days_remaining_shelf_life, 1)
                as projected_demand_before_expiry,
            lead_time_days as replenishment_lead_days,
            round(spoilage_risk_score, 3) as spoilage_risk_score,
            round(stockout_risk_score, 3) as stockout_risk_score,
            case
                when on_hand_qty = 0 and sell_through > 0 then 'STOCKOUT'
                when round(spoilage_risk_score, 3) >= {threshold} then 'SPOILAGE'
                else 'OK'
            end as risk_flag,
            current_timestamp as computed_at
        from scored
        """
    )


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
