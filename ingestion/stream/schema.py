"""Event contracts and the validate/enrich logic for the streaming path.

Everything here is pure — no AWS, no I/O — so it can be exercised directly in
unit tests and reused identically by the producer (to shape events) and the
Lambda (to police them). Two event types flow through the stream:

  * pos_sale         — a point-of-sale line: units of a SKU sold at a store.
  * inventory_update — an end-of-day (or event-driven) shelf position.

The Lambda rejects anything that fails validation rather than letting it
corrupt the bronze layer; enrichment adds the fields the lake partitions and
joins on.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timezone

STORE_RE = re.compile(r"^S\d{3}$")
PRODUCT_RE = re.compile(r"^P\d{5}$")

EVENT_TYPES = {"pos_sale", "inventory_update"}

# Required keys per event type (beyond the common envelope).
REQUIRED_FIELDS = {
    "common": ("event_type", "event_id", "event_ts", "store_id", "product_id"),
    "pos_sale": ("units", "unit_price"),
    "inventory_update": ("on_hand_qty", "received_qty"),
}


class ValidationError(Exception):
    """Raised when an event violates its contract. The message is safe to log."""


def _require(event: dict, keys: tuple[str, ...]) -> None:
    missing = [k for k in keys if k not in event or event[k] is None]
    if missing:
        raise ValidationError(f"missing fields: {', '.join(missing)}")


def _parse_ts(value) -> datetime:
    if not isinstance(value, str):
        raise ValidationError("event_ts must be an ISO-8601 string")
    try:
        # Accept a trailing 'Z' as UTC.
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError(f"event_ts is not valid ISO-8601: {value}") from exc


def validate(event: dict) -> None:
    """Raise ValidationError if the event violates its contract; else return."""
    if not isinstance(event, dict):
        raise ValidationError("event is not a JSON object")

    _require(event, REQUIRED_FIELDS["common"])

    etype = event["event_type"]
    if etype not in EVENT_TYPES:
        raise ValidationError(f"unknown event_type: {etype!r}")

    if not STORE_RE.match(str(event["store_id"])):
        raise ValidationError(f"malformed store_id: {event['store_id']!r}")
    if not PRODUCT_RE.match(str(event["product_id"])):
        raise ValidationError(f"malformed product_id: {event['product_id']!r}")

    _parse_ts(event["event_ts"])
    _require(event, REQUIRED_FIELDS[etype])

    if etype == "pos_sale":
        if not isinstance(event["units"], int) or event["units"] <= 0:
            raise ValidationError("units must be a positive integer")
        if not _is_number(event["unit_price"]) or event["unit_price"] <= 0:
            raise ValidationError("unit_price must be a positive number")
    else:  # inventory_update
        for field in ("on_hand_qty", "received_qty"):
            if not isinstance(event[field], int) or event[field] < 0:
                raise ValidationError(f"{field} must be a non-negative integer")


def enrich(event: dict) -> dict:
    """Return a copy of a *valid* event with the fields the lake needs added.

    Adds the event calendar date (partition key), an ingestion timestamp, and —
    for sales — derived revenue, so downstream never recomputes it inconsistently.
    """
    ts = _parse_ts(event["event_ts"])
    enriched = dict(event)
    enriched["event_date"] = ts.date().isoformat()
    enriched["ingest_ts"] = datetime.now(timezone.utc).isoformat()
    if event["event_type"] == "pos_sale":
        enriched["revenue"] = round(event["units"] * event["unit_price"], 2)
    return enriched


def validate_and_enrich(event: dict) -> dict:
    """Convenience: validate then enrich. Raises ValidationError on bad input."""
    validate(event)
    return enrich(event)


def partition_path(enriched: dict, prefix: str = "bronze/stream") -> str:
    """S3 key prefix for a record: Hive-style date partition the lake prunes on."""
    etype = enriched["event_type"]
    return f"{prefix}/{etype}/dt={enriched['event_date']}/"


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)