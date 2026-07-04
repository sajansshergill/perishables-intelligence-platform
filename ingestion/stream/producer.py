"""Streaming producer for the perishables platform.

Emits a live mix of pos_sale and inventory_update events drawn from the same
store/product universe the batch generator uses, so the streaming and batch
paths describe one coherent world. It can target:

  * kinesis  — real Kinesis Data Streams (or LocalStack via AWS_ENDPOINT_URL)
  * stdout   — JSON lines to the console (no AWS needed)
  * file     — append JSON lines to a file

A small, deliberate fraction of malformed events can be injected (--error-rate)
so the downstream validate/enrich Lambda has something to reject — proving the
quality boundary actually holds.

Examples
--------
    # No AWS required — watch events fly by:
    python ingestion/stream/producer.py --target stdout --rate 20 --count 100

    # Against LocalStack:
    AWS_ENDPOINT_URL=http://localhost:4566 \
        python ingestion/stream/producer.py --target kinesis \
        --stream-name perishables-events --rate 50 --duration 30
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from aws import client

# --------------------------------------------------------------------------- #
# The event universe: real store/product ids + prices if the batch data exists,
# otherwise a small synthetic fallback so the producer runs standalone.
# --------------------------------------------------------------------------- #
def load_universe(data_dir: Path):
    stores_path = data_dir / "dims" / "dim_store.parquet"
    products_path = data_dir / "dims" / "dim_product.parquet"
    if stores_path.exists() and products_path.exists():
        stores = pd.read_parquet(stores_path)["store_id"].tolist()
        products = pd.read_parquet(products_path)[["product_id", "unit_price"]]
        prices = dict(zip(products["product_id"], products["unit_price"]))
        return stores, list(prices), prices

    # Fallback universe (keeps the producer runnable with no generated data).
    stores = [f"S{i:03d}" for i in range(1, 6)]
    product_ids = [f"P{i:05d}" for i in range(1, 51)]
    prices = {pid: round(float(2 + (hash(pid) % 1500) / 100), 2) for pid in product_ids}
    return stores, product_ids, prices


def make_event(rng, stores, product_ids, prices) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    store_id = str(rng.choice(stores))
    product_id = str(rng.choice(product_ids))
    common = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now,
        "store_id": store_id,
        "product_id": product_id,
    }
    if rng.random() < 0.7:  # POS events are the more frequent signal
        return {
            **common,
            "event_type": "pos_sale",
            "units": int(rng.integers(1, 6)),
            "unit_price": prices[product_id],
        }
    return {
        **common,
        "event_type": "inventory_update",
        "on_hand_qty": int(rng.integers(0, 120)),
        "received_qty": int(rng.integers(0, 40)),
    }


def corrupt(event: dict, rng) -> dict:
    """Introduce one realistic defect so the Lambda has something to reject."""
    kind = rng.integers(0, 4)
    bad = dict(event)
    if kind == 0:
        bad.pop("product_id", None)              # missing required field
    elif kind == 1:
        bad["store_id"] = "store-42"             # malformed id
    elif kind == 2 and bad["event_type"] == "pos_sale":
        bad["units"] = -3                        # negative units
    else:
        bad["event_ts"] = "yesterday"            # unparseable timestamp
    return bad


# --------------------------------------------------------------------------- #
# Sinks
# --------------------------------------------------------------------------- #
def emit_kinesis(records: list[dict], stream_name: str) -> None:
    kinesis = client("kinesis")
    # PutRecords caps at 500; partition by store so a store's events stay ordered.
    for i in range(0, len(records), 500):
        chunk = records[i : i + 500]
        kinesis.put_records(
            StreamName=stream_name,
            Records=[
                {"Data": json.dumps(r).encode("utf-8"), "PartitionKey": r["store_id"]}
                for r in chunk
            ],
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Emit perishables events to a stream.")
    p.add_argument("--target", choices=["kinesis", "stdout", "file"], default="stdout")
    p.add_argument("--stream-name", default="perishables-events")
    p.add_argument("--rate", type=float, default=10.0, help="events per second")
    p.add_argument("--count", type=int, default=None, help="stop after N events")
    p.add_argument("--duration", type=float, default=None, help="stop after S seconds")
    p.add_argument("--error-rate", type=float, default=0.05, help="fraction malformed")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--out-file", type=Path, default=Path("stream_events.jsonl"))
    p.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "data" / "generated",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    rng = np.random.default_rng(args.seed)
    stores, product_ids, prices = load_universe(args.data_dir)

    if args.count is None and args.duration is None:
        args.count = 100  # sensible default so it terminates

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    start = time.time()
    sent = 0
    file_handle = args.out_file.open("w") if args.target == "file" else None
    batch: list[dict] = []

    try:
        while True:
            if args.count is not None and sent >= args.count:
                break
            if args.duration is not None and (time.time() - start) >= args.duration:
                break

            event = make_event(rng, stores, product_ids, prices)
            if rng.random() < args.error_rate:
                event = corrupt(event, rng)

            if args.target == "stdout":
                print(json.dumps(event))
            elif args.target == "file":
                file_handle.write(json.dumps(event) + "\n")
            else:
                batch.append(event)
                if len(batch) >= 500:
                    emit_kinesis(batch, args.stream_name)
                    batch.clear()

            sent += 1
            if interval:
                time.sleep(interval)

        if args.target == "kinesis" and batch:
            emit_kinesis(batch, args.stream_name)
    finally:
        if file_handle:
            file_handle.close()

    print(f"produced {sent} events → {args.target}", file=sys.stderr)


if __name__ == "__main__":
    main()