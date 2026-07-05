"""Unit tests for Glue batch transforms.

The actual Glue jobs import awsglue lazily, so their pure transforms can be
tested with a local SparkSession.
"""
from __future__ import annotations

import shutil

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from glue_shelf_life_load import transform_shelf_life  # noqa: E402
from glue_supplier_load import transform_suppliers  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    if shutil.which("java") is None:
        pytest.skip("Spark transform tests require a Java runtime")

    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("perishables-glue-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"SparkSession could not start: {exc}")
    yield session
    session.stop()


def test_supplier_transform_cleans_validates_and_dedupes(spark):
    raw = spark.createDataFrame(
        [
            (" sup001 ", "  Fresh Farms ", "2", "0.95", "2026-07-01T08:00:00"),
            ("SUP001", "Fresh Farms New", "3", "0.97", "2026-07-02T08:00:00"),
            ("bad", "Bad Id", "2", "0.90", "2026-07-02T08:00:00"),
            ("SUP002", "Bad Reliability", "1", "1.50", "2026-07-02T08:00:00"),
            ("SUP003", "Bad Lead", "-1", "0.80", "2026-07-02T08:00:00"),
        ],
        ["supplier_id", "supplier_name", "lead_time_days", "reliability", "extract_ts"],
    )

    rows = transform_suppliers(raw, "2026-07-05").collect()

    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["supplier_id"] == "SUP001"
    assert row["supplier_name"] == "Fresh Farms New"
    assert row["lead_time_days"] == 3
    assert row["reliability"] == pytest.approx(0.97)
    assert row["load_date"] == "2026-07-05"
    assert row["ingested_at"] is not None


def test_shelf_life_transform_builds_scd2_history(spark):
    raw = spark.createDataFrame(
        [
            (" p00001 ", "5", "2026-01-01"),
            ("P00001", "7", "2026-06-01"),
            ("P00001", "4", "2026-06-01"),  # duplicate day loses to larger value
            ("P00002", "0", "2026-01-01"),
            ("bad", "3", "2026-01-01"),
        ],
        ["product_id", "shelf_life_days", "effective_from"],
    )

    rows = [
        r.asDict()
        for r in transform_shelf_life(raw).orderBy("product_id", "effective_from").collect()
    ]

    assert len(rows) == 2
    assert rows[0]["product_id"] == "P00001"
    assert rows[0]["shelf_life_days"] == 5
    assert rows[0]["effective_to"].isoformat() == "2026-06-01"
    assert rows[0]["is_current"] is False
    assert rows[1]["shelf_life_days"] == 7
    assert rows[1]["effective_to"] is None
    assert rows[1]["is_current"] is True