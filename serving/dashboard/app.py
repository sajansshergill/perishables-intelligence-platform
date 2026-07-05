"""Perishables Risk — the morning dashboard.

A category manager opens this to see, for a chosen day, where money is leaking:
which store x SKUs are stocked out (losing sales) and which are about to spoil
(pending write-offs), ranked by dollars at stake, sliceable by region and
category.

Run it after building the warehouse:
    export PERISHABLES_DUCKDB=warehouse/dbt/perishables.duckdb
    streamlit run serving/dashboard/app.py

All querying and maths live in dashboard_data.py (unit-tested); this file is
the rendering shell.
"""
from __future__ import annotations

import os
import sys

# Make the sibling data module importable regardless of launcher.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st

import dashboard_data as dd

st.set_page_config(page_title="Perishables Risk", page_icon="🥬", layout="wide")


@st.cache_resource
def get_connection():
    return dd.connect()


@st.cache_data
def load(snapshot_date: str):
    return dd.load_enriched(get_connection(), snapshot_date)


@st.cache_data
def load_trend():
    return dd.daily_trend(get_connection())


def money(x: float) -> str:
    return f"${x:,.0f}"


def main() -> None:
    st.title("🥬 Perishables Freshness & Replenishment Intelligence")
    st.caption(
        "Daily spoilage and stockout risk per store × SKU, ranked by dollars at stake."
    )

    con = get_connection()
    dates = dd.available_dates(con)
    if not dates:
        st.error("No data in `perishables_risk`. Build the warehouse first: `dbt build`.")
        return

    # ---- Sidebar filters ----------------------------------------------------
    with st.sidebar:
        st.header("Filters")
        snapshot_date = st.selectbox("Snapshot date", dates, index=0)
        df_all = load(snapshot_date)
        regions = sorted(df_all["region"].unique())
        categories = sorted(df_all["category"].unique())
        sel_regions = st.multiselect("Region", regions, default=regions)
        sel_categories = st.multiselect("Category", categories, default=categories)

    df = df_all[
        df_all["region"].isin(sel_regions) & df_all["category"].isin(sel_categories)
    ]
    if df.empty:
        st.warning("No rows match the current filters.")
        return

    kpis = dd.compute_kpis(df)

    # ---- KPI cards ----------------------------------------------------------
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("SKU-days in view", f"{kpis['sku_days']:,}")
    c2.metric("Healthy (OK)", f"{kpis['ok_pct']:.1f}%")
    c3.metric("Stockouts", f"{kpis['stockout_count']:,}",
              help="Empty shelf with live demand — losing sales now.")
    c4.metric("Lost revenue / day", money(kpis["stockout_lost_revenue_per_day"]),
              delta="-at risk", delta_color="inverse")
    c5.metric("Spoilage at risk", money(kpis["spoilage_cost_at_risk"]),
              delta="-write-off", delta_color="inverse")

    st.divider()

    # ---- Distribution + trend ----------------------------------------------
    left, right = st.columns(2)
    with left:
        st.subheader("Risk by category")
        by_cat = dd.flags_by_dimension(df, "category").set_index("category")
        st.bar_chart(by_cat[["STOCKOUT", "SPOILAGE", "OK"]])
    with right:
        st.subheader("Flag trend over time")
        trend = load_trend()
        pivot = trend.pivot(index="snapshot_date", columns="risk_flag", values="n").fillna(0)
        keep = [c for c in ("STOCKOUT", "SPOILAGE") if c in pivot.columns]
        st.line_chart(pivot[keep])

    st.divider()

    # ---- Prioritised action lists ------------------------------------------
    st.subheader(f"Action list — {snapshot_date}")
    tab_stockout, tab_spoilage = st.tabs(
        [f"🔴 Stockouts ({kpis['stockout_count']})",
         f"🟠 Spoilage ({kpis['spoilage_count']})"]
    )
    with tab_stockout:
        st.caption("Empty shelves with active demand, ranked by daily revenue lost.")
        st.dataframe(
            dd.top_risks(df, "STOCKOUT", n=25),
            width='stretch', hide_index=True,
        )
    with tab_spoilage:
        st.caption("Surplus stock projected to expire unsold, ranked by write-off cost.")
        st.dataframe(
            dd.top_risks(df, "SPOILAGE", n=25),
            width='stretch', hide_index=True,
        )


if __name__ == "__main__":
    main()