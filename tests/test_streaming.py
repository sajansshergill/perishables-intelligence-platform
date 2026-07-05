"""Tests for the streaming ingestion path.

Two layers:
  * Unit tests on the pure validate/enrich contract in schema.py.
  * A full end-to-end integration test that stands up Kinesis, Firehose and S3
    in-process with moto, pushes events through the producer, runs them through
    the Lambda handler, and asserts the good records land in S3 bronze while the
    malformed ones are rejected — no Docker, no AWS account.
"""
from __future__ import annotations

import base64
import json

import pytest

import schema
from schema import ValidationError, enrich, validate, validate_and_enrich


# --------------------------------------------------------------------------- #
# Pure contract tests
# --------------------------------------------------------------------------- #
def _valid_sale() -> dict:
    return {
        "event_type": "pos_sale",
        "event_id": "e1",
        "event_ts": "2026-07-01T10:00:00+00:00",
        "store_id": "S001",
        "product_id": "P00001",
        "units": 3,
        "unit_price": 4.99,
    }


def _valid_inventory() -> dict:
    return {
        "event_type": "inventory_update",
        "event_id": "e2",
        "event_ts": "2026-07-01T23:00:00Z",
        "store_id": "S010",
        "product_id": "P00042",
        "on_hand_qty": 40,
        "received_qty": 0,
    }


def test_valid_events_pass():
    validate(_valid_sale())
    validate(_valid_inventory())


@pytest.mark.parametrize(
    "mutate, field",
    [
        (lambda e: e.pop("product_id"), "missing product_id"),
        (lambda e: e.update(store_id="store-1"), "bad store_id"),
        (lambda e: e.update(product_id="42"), "bad product_id"),
        (lambda e: e.update(event_type="refund"), "unknown type"),
        (lambda e: e.update(event_ts="yesterday"), "bad timestamp"),
        (lambda e: e.update(units=0), "non-positive units"),
        (lambda e: e.update(units=-2), "negative units"),
        (lambda e: e.update(unit_price=-1.0), "negative price"),
    ],
)
def test_invalid_sales_are_rejected(mutate, field):
    event = _valid_sale()
    mutate(event)
    with pytest.raises(ValidationError):
        validate(event)


def test_negative_inventory_rejected():
    event = _valid_inventory()
    event["on_hand_qty"] = -5
    with pytest.raises(ValidationError):
        validate(event)


def test_enrich_adds_partition_and_revenue():
    e = enrich(_valid_sale())
    assert e["event_date"] == "2026-07-01"
    assert e["revenue"] == round(3 * 4.99, 2)
    assert "ingest_ts" in e


def test_enrich_handles_zulu_timestamp():
    e = enrich(_valid_inventory())
    assert e["event_date"] == "2026-07-01"
    assert "revenue" not in e  # inventory events carry no revenue


def test_partition_path():
    e = validate_and_enrich(_valid_sale())
    assert schema.partition_path(e) == "bronze/stream/pos_sale/dt=2026-07-01/"


# --------------------------------------------------------------------------- #
# End-to-end: producer -> Kinesis -> Lambda -> Firehose -> S3  (all mocked)
# --------------------------------------------------------------------------- #
moto = pytest.importorskip("moto")
from moto import mock_aws  # noqa: E402

STREAM = "perishables-events"
FIREHOSE = "perishables-firehose"
BUCKET = "perishables-lake"


@pytest.fixture
def aws_env(monkeypatch):
    # moto needs credentials/region present; ensure no LocalStack endpoint leaks in.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("FIREHOSE_STREAM", FIREHOSE)


def _kinesis_event_from_shard(kinesis) -> dict:
    """Read everything on the stream and shape it as a Kinesis→Lambda event."""
    shard_id = kinesis.describe_stream(StreamName=STREAM)["StreamDescription"]["Shards"][0]["ShardId"]
    it = kinesis.get_shard_iterator(
        StreamName=STREAM, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]
    got = kinesis.get_records(ShardIterator=it, Limit=1000)["Records"]
    return {
        "Records": [
            {"kinesis": {"data": base64.b64encode(r["Data"]).decode("utf-8")}}
            for r in got
        ]
    }


@mock_aws
def test_stream_end_to_end(aws_env):
    import boto3

    import enrich_lambda
    import producer

    region = "us-east-1"
    kinesis = boto3.client("kinesis", region_name=region)
    s3 = boto3.client("s3", region_name=region)
    firehose = boto3.client("firehose", region_name=region)

    # 1. Stand up the infrastructure.
    kinesis.create_stream(StreamName=STREAM, ShardCount=1)
    kinesis.get_waiter("stream_exists").wait(StreamName=STREAM)
    s3.create_bucket(Bucket=BUCKET)
    firehose.create_delivery_stream(
        DeliveryStreamName=FIREHOSE,
        DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "RoleARN": "arn:aws:iam::123456789012:role/firehose-role",
            "BucketARN": f"arn:aws:s3:::{BUCKET}",
            "Prefix": "bronze/stream/",
            "CompressionFormat": "UNCOMPRESSED",
        },
    )

    # 2. Produce a known mix: N valid events + a few deliberately malformed.
    import numpy as np

    rng = np.random.default_rng(7)
    stores, product_ids, prices = producer.load_universe(pytest_data_dir())
    valid = [producer.make_event(rng, stores, product_ids, prices) for _ in range(30)]
    bad = [producer.corrupt(producer.make_event(rng, stores, product_ids, prices), rng) for _ in range(5)]
    # Keep only corruptions that actually break validation (some randomness).
    bad = [b for b in bad if _is_invalid(b)]
    all_events = valid + bad
    producer.emit_kinesis(all_events, STREAM)

    # 3. Drive the Lambda with what's on the shard.
    lambda_event = _kinesis_event_from_shard(kinesis)
    summary = enrich_lambda.handler(lambda_event)

    assert summary["received"] == len(all_events)
    assert summary["rejected"] == len(bad)
    assert summary["delivered"] == len(valid)

    # 4. The good records must have landed in S3 bronze, enriched.
    objs = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    assert objs, "Firehose delivered nothing to S3"
    body = s3.get_object(Bucket=BUCKET, Key=objs[0]["Key"])["Body"].read().decode()
    first = json.loads(body.splitlines()[0])
    assert "event_date" in first and "ingest_ts" in first


def _is_invalid(event: dict) -> bool:
    try:
        validate(event)
        return False
    except ValidationError:
        return True


def pytest_data_dir():
    import pathlib

    return pathlib.Path(__file__).resolve().parents[1] / "data" / "generated"