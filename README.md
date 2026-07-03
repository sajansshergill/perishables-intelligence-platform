# Perishables Freshness & Replenishment Intelligence Platform

> An AWS-native, batch **and** streaming data platform that unifies POS transactions, inventory positions, and shelf-life signals into automated analytic deliverables that flag **spoilage risk** and **stockout risk** per store × SKU.

Built to mirror the data-engineering problem at the heart of large-scale grocery operations: replacing opaque third-party inventory intelligence with a warehouse-owned, cost-effective system that gives category managers a single source of truth for perishables.

<p align="left">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.11-blue" />
  <img alt="AWS" src="https://img.shields.io/badge/AWS-Kinesis｜Glue｜Redshift｜S3｜Lambda-orange" />
  <img alt="dbt" src="https://img.shields.io/badge/dbt-Redshift-red" />
  <img alt="Airflow" src="https://img.shields.io/badge/Orchestration-Airflow-brightgreen" />
  <img alt="CI" src="https://img.shields.io/badge/CI-GitHub_Actions-black" />
  <img alt="tests" src="https://img.shields.io/badge/data_quality-Great_Expectations-purple" />
</p>

---

## The business problem this solves

Grocery margins live and die on **perishables**. Two failure modes leak money every day:

- **Overstock → spoilage.** Product expires on the shelf. Direct write-off.
- **Understock → out-of-stock.** Empty shelf. Lost sales *and* a customer who shops elsewhere.

The gap between *what's physically on the shelf right now* and *what's about to sell* is where the loss happens. Historically this intelligence is bought from a third-party vendor — expensive, a black box, and hard to extend.

**This platform is the AWS-native replacement.** It fuses real-time shelf state (streaming) with daily supplier/warehouse context (batch) into a governed warehouse model, then surfaces ranked risk deliverables the business can act on before product spoils or shelves empty.

---

## What it does

1. **Ingests** POS + inventory events in near-real-time and daily supplier/shelf-life reference data.
2. **Conforms** both into a dimensional model in Redshift (bronze → silver → gold on S3).
3. **Computes** two decision signals per store × SKU:
   - `spoilage_risk_score` — on-hand quantity vs. remaining shelf life vs. recent sell-through.
   - `stockout_risk_score` — projected demand vs. on-hand vs. replenishment lead time.
4. **Delivers** a `perishables_risk` gold table + dashboard ranking the highest-risk store/SKU combinations for the day.

---

## Architecture

```
                          ┌─────────────────────────────────────────────┐
                          │                   S3 DATA LAKE               │
   STREAMING PATH         │        bronze  →  silver  →  gold            │
 ┌──────────────┐         │                                             │
 │ POS /        │  events │   ┌──────────┐   ┌──────────┐   ┌─────────┐ │
 │ inventory    ├────────►│   │  Kinesis │──►│  Lambda  │──►│ Firehose│─┼──► s3://.../bronze/stream/
 │ event stream │         │   │  Streams │   │ validate │   │  → S3   │ │
 └──────────────┘         │   └──────────┘   │ enrich   │   └─────────┘ │
                          │                  └──────────┘               │
   BATCH PATH             │                                             │
 ┌──────────────┐  daily  │   ┌──────────┐        ┌──────────────────┐  │
 │ supplier /   ├────────►│   │  AWS Glue│───────►│  Redshift         │  │
 │ warehouse    │  drops  │   │ (PySpark)│  load  │  dimensional model│  │
 │ + shelf-life │         │   └──────────┘        │  + dbt transforms │  │
 └──────────────┘         │                       └────────┬─────────┘  │
                          └────────────────────────────────┼────────────┘
                                                            │
                                        ┌───────────────────▼───────────────────┐
                                        │   gold.perishables_risk                │
                                        │   → QuickSight / Streamlit dashboard    │
                                        └────────────────────────────────────────┘

        Orchestration: Airflow  ·  Transforms: dbt  ·  Quality gate: Great Expectations  ·  Packaged: Docker
```

**Design choices tied to the problem:**

| Decision | Why |
|---|---|
| **Kinesis** as the primary stream | Native to the AWS/Amazon stack; MSK/Kafka noted as a drop-in where an org already runs it. |
| **Bronze / silver / gold** on S3 | Cheap, replayable landing; conforming happens downstream, not at ingest. |
| **Redshift dimensional model** | Analytic deliverables need conformed dimensions, not raw events. Dist/sort keys tune cost + performance. |
| **dbt for gold transforms** | Version-controlled, testable business logic — the risk scores are auditable, not buried in a script. |
| **Firehose buffering** | Batches small events into efficient S3 objects → lower cost, fewer small-file problems. |

---

## Data model

Star schema in Redshift. Facts are grain-explicit; a slowly-changing dimension tracks shelf-life reference data as it's revised.

```
dim_store ───┐
dim_product ─┼──< fact_sales               (grain: store × product × transaction time)
dim_supplier ┤
dim_date ────┴──< fact_inventory_snapshot  (grain: store × product × snapshot time)

dim_product_shelf_life  (SCD Type 2 — shelf-life windows change over time)
```

**Gold deliverable**

```sql
-- gold.perishables_risk (one row per store × SKU × day)
store_id, product_id,
on_hand_qty, days_remaining_shelf_life, trailing_7d_sell_through,
projected_demand, replenishment_lead_days,
spoilage_risk_score,   -- 0..1, higher = more likely to spoil
stockout_risk_score,   -- 0..1, higher = more likely to stock out
risk_flag,             -- {SPOILAGE, STOCKOUT, OK}
computed_at
```

---

## Tech stack

| Layer | Tooling |
|---|---|
| Streaming ingest | Amazon Kinesis Data Streams, Lambda, Kinesis Firehose |
| Batch ingest / ETL | AWS Glue (PySpark), S3 |
| Warehouse | Amazon Redshift |
| Transforms | dbt |
| Orchestration | Apache Airflow (Step Functions noted as AWS-native alternative) |
| Data quality | Great Expectations (CI-gated) |
| Serving | QuickSight / Streamlit |
| Packaging & CI | Docker, GitHub Actions |
| Local emulation | LocalStack (Kinesis/S3/Lambda), Postgres-as-Redshift for local dev |

---

## Project structure

```
perishables-intelligence-platform/
├── infra/                    # IaC — Kinesis, Firehose, Glue, Redshift, IAM roles
│   └── terraform/
├── ingestion/
│   ├── stream/               # event producer (synthetic POS/inventory) + Lambda handler
│   └── batch/                # Glue PySpark jobs for supplier/shelf-life drops
├── warehouse/
│   └── dbt/                  # staging → marts, incl. perishables_risk model + tests
├── quality/
│   └── great_expectations/   # expectation suites run as a CI gate
├── orchestration/
│   └── airflow/dags/         # batch load DAG + risk-scoring DAG
├── serving/
│   └── dashboard/            # Streamlit app over gold tables
├── data/
│   └── generators/           # synthetic data generator (stores, SKUs, shelf-life)
├── tests/
├── docker-compose.yml        # LocalStack + Airflow + Postgres for local runs
├── .github/workflows/ci.yml
└── README.md
```

---

## Quickstart (local)

Runs the full pipeline against LocalStack + a Postgres-backed Redshift emulation — no AWS account required.

```bash
# 1. Clone and configure
git clone https://github.com/sajansshergill/perishables-intelligence-platform.git
cd perishables-intelligence-platform
cp .env.example .env

# 2. Bring up LocalStack, Airflow, and the warehouse
docker-compose up -d

# 3. Seed synthetic reference + historical data
python data/generators/seed.py --stores 20 --skus 500 --days 30

# 4. Start the streaming producer (simulated POS + inventory events)
python ingestion/stream/producer.py --rate 50   # events/sec

# 5. Trigger the batch load + risk-scoring DAGs
airflow dags trigger batch_supplier_load
airflow dags trigger perishables_risk_scoring

# 6. Open the deliverable
streamlit run serving/dashboard/app.py
```

---

## Data quality & CI

Every merge runs:

- **dbt tests** — not-null, uniqueness, referential integrity, accepted-range checks on risk scores.
- **Great Expectations suites** — freshness of inventory snapshots, no negative on-hand quantities, shelf-life windows within plausible bounds.
- **Unit tests** — Lambda enrichment logic and the risk-score functions.

A failing quality gate blocks the pipeline from promoting silver → gold, so bad data never reaches the deliverable.

```
[ci] dbt build ............... ✓  42 models, 118 tests passed
[ci] great_expectations ...... ✓  6 suites passed
[ci] pytest .................. ✓  31 passed
```

---

## Cost & performance notes

- **Firehose buffering** (size/interval tuned) collapses many small stream records into efficient S3 objects.
- **Redshift dist/sort keys** chosen on `store_id` / `product_id` / `computed_at` to keep the daily risk scan cheap.
- **Partitioned S3** (`dt=YYYY-MM-DD`) so Glue and Redshift Spectrum prune to the day being processed.
- **Incremental dbt models** on the fact tables — only new snapshots are reprocessed.

---

## Roadmap

- [ ] Swap the heuristic risk scores for a lightweight demand-forecast layer (feature store → model → back into gold).
- [ ] Add supplier-level lead-time variability to sharpen the stockout signal.
- [ ] Redshift Spectrum external tables to query bronze/silver directly for ad-hoc investigation.
- [ ] Alerting: push `risk_flag = STOCKOUT` rows to a replenishment queue.

---

## Note on scope

This is a **portfolio project** built with synthetic data to demonstrate end-to-end data-engineering capability — streaming + batch ingest, dimensional modeling, warehouse transforms, quality gating, and analytic delivery on an AWS-native stack. It stands as evidence of the approach and tooling, not a claim of production scale.

---

## Author

**Sajan Singh Shergill** — Data Engineer
[LinkedIn](https://linkedin.com/in/sajanshergill) · [Portfolio](https://sajansshergill.github.io) · sajansshergill@gmail.com
