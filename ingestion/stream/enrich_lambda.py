"""Lambda: the quality boundary between the raw stream and the lake.

Triggered by Kinesis. For each record it decodes the payload, runs the shared
validate/enrich logic, and:

  * valid records   -> forwarded to Firehose, which buffers them to S3 bronze.
  * invalid records -> rejected: logged and (in production) sent to a dead-letter
                       stream. They never reach bronze, so the lake stays clean.

The handler is thin AWS plumbing; all the judgement lives in schema.py, which is
unit-tested directly. Firehose is reached through the same endpoint-aware client
factory, so this runs unmodified against LocalStack or real AWS.
"""
from __future__ import annotations

import base64
import json
import logging
import os

from aws import client
from schema import ValidationError, validate_and_enrich

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FIREHOSE_STREAM = os.environ.get("FIREHOSE_STREAM", "perishables-firehose")


def _decode(record: dict) -> dict:
    """Decode one Kinesis record's base64 payload into a JSON object."""
    raw = base64.b64decode(record["kinesis"]["data"])
    return json.loads(raw)


def process_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split a batch into (enriched_good, rejected). Pure except for logging."""
    good: list[dict] = []
    rejected: list[dict] = []
    for record in records:
        try:
            event = _decode(record)
            good.append(validate_and_enrich(event))
        except (ValidationError, ValueError, KeyError) as exc:
            rejected.append({"reason": str(exc), "raw": record.get("kinesis", {}).get("data")})
    return good, rejected


def _deliver(good: list[dict]) -> int:
    """Send enriched records to Firehose in batches of up to 500."""
    if not good:
        return 0
    firehose = client("firehose")
    delivered = 0
    for i in range(0, len(good), 500):
        chunk = good[i : i + 500]
        firehose.put_record_batch(
            DeliveryStreamName=FIREHOSE_STREAM,
            Records=[{"Data": (json.dumps(r) + "\n").encode("utf-8")} for r in chunk],
        )
        delivered += len(chunk)
    return delivered


def handler(event: dict, context=None) -> dict:
    """Kinesis → Lambda entry point. Returns a per-invocation summary."""
    records = event.get("Records", [])
    good, rejected = process_records(records)
    delivered = _deliver(good)

    if rejected:
        logger.warning("rejected %d/%d records", len(rejected), len(records))
        for r in rejected[:10]:  # cap the log noise
            logger.warning("reject reason: %s", r["reason"])

    summary = {"received": len(records), "delivered": delivered, "rejected": len(rejected)}
    logger.info("summary: %s", summary)
    return summary