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

---

## Project structure

Legend:  ✅ implemented and tested  ·  🚧 scaffolded next

```
perishables-intelligence-platform/
│
├── README.md
├── requirements.txt                         ✅ runtime + test deps
├── .gitignore                               ✅ (generated data is reproducible, not committed)
├── conftest.py                              ✅ puts the generator on the test path
├── docker-compose.yml                       ✅ LocalStack (Kinesis/Firehose/S3/Lambda)
│
├── data/
│   └── generators/
│       ├── config.py                        ✅ category/shelf-life/store specs + ordering knobs
│       ├── generate.py                      ✅ dimensions + FIFO inventory-aging simulation
│       └── seed.py                          ✅ CLI entry point → parquet/csv + summary report
│
├── ingestion/
│   ├── stream/
│   │   ├── schema.py                         ✅ event contracts + validate/enrich (pure, tested)
│   │   ├── aws.py                            ✅ endpoint-aware client factory (LocalStack ↔ AWS)
│   │   ├── producer.py                       ✅ emits POS/inventory events → Kinesis/stdout/file
│   │   └── enrich_lambda.py                  ✅ Kinesis→Lambda: validate, enrich, → Firehose
│   └── batch/
│       ├── glue_supplier_load.py             🚧 PySpark: supplier/warehouse drops → bronze
│       └── glue_shelf_life_load.py           🚧 PySpark: shelf-life reference → bronze
│
├── scripts/
│   └── localstack_setup.sh                   ✅ provisions stream/bucket/firehose/lambda locally
│
├── infra/
│   └── terraform/                            🚧 production IaC (LocalStack covers local dev)
│       ├── main.tf · s3.tf · kinesis.tf · firehose.tf
│       ├── glue.tf · redshift.tf · iam.tf · variables.tf
│
├── warehouse/
│   ├── ddl/
│   │   └── redshift_schema.sql               ✅ raw DDL with dist/sort keys (Redshift target)
│   └── dbt/
│       ├── dbt_project.yml                    ✅
│       ├── profiles.yml                       ✅ DuckDB local + Redshift target (documented)
│       ├── models/
│       │   ├── staging/
│       │   │   ├── _sources.yml               ✅ parquet sources (→ Redshift raw in prod)
│       │   │   ├── stg_sales.sql              ✅
│       │   │   ├── stg_inventory_snapshot.sql ✅
│       │   │   ├── stg_stores.sql             ✅
│       │   │   ├── stg_products.sql           ✅
│       │   │   ├── stg_suppliers.sql          ✅
│       │   │   ├── stg_shelf_life.sql         ✅
│       │   │   └── stg_date.sql               ✅
│       │   ├── intermediate/
│       │   │   └── int_inventory_sell_through.sql ✅ dense spine + trailing sell-through
│       │   └── marts/
│       │       ├── _marts.yml                 ✅ dbt tests (not-null, unique, relationships, values)
│       │       ├── dim_store.sql              ✅
│       │       ├── dim_product.sql            ✅
│       │       ├── dim_supplier.sql           ✅
│       │       ├── dim_shelf_life.sql         ✅ SCD Type 2
│       │       ├── dim_date.sql               ✅
│       │       ├── fact_sales.sql             ✅
│       │       ├── fact_inventory_snapshot.sql ✅
│       │       └── perishables_risk.sql       ✅ ⭐ the gold deliverable
│       ├── macros/
│       │   └── risk_scores.sql                ✅ reusable spoilage/stockout scoring logic
│       └── tests/
│           ├── assert_perishables_risk_scores_in_range.sql  ✅
│           ├── assert_perishables_risk_unique_grain.sql     ✅
│           └── assert_flag_matches_scores.sql               ✅
│
├── quality/
│   └── great_expectations/
│       ├── great_expectations.yml             🚧
│       └── expectations/
│           ├── inventory_snapshot_suite.json  🚧 freshness, no negative on-hand
│           └── perishables_risk_suite.json    🚧 score bounds, flag domain
│
├── orchestration/
│   └── airflow/
│       └── dags/
│           ├── batch_supplier_load.py         🚧 land → stage → test → promote
│           └── perishables_risk_scoring.py    🚧 build gold + run quality gate
│
├── serving/
│   └── dashboard/
│       └── app.py                             🚧 Streamlit over gold.perishables_risk
│
├── tests/
│   ├── test_generator.py                      ✅ 11 data-quality invariants (all passing)
│   └── test_streaming.py                      ✅ 14 tests: contract + moto end-to-end
│
└── .github/
    └── workflows/
        └── ci.yml                             🚧 pytest + dbt tests + Great Expectations gate
```

---

## Quickstart

### Runs today — generate the dataset, then build the warehouse

The generator and the dbt warehouse both run locally with no AWS dependency —
dbt targets DuckDB reading the generated Parquet directly.

```bash
# 1. Install
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Generate (defaults: 20 stores × 500 SKUs × 30 days, seeded & reproducible)
python data/generators/seed.py

#    …or size it yourself
python data/generators/seed.py --stores 10 --skus 200 --days 30 --seed 42

# 3. Run the generator's data-quality invariants
pytest -v

# 4. Build the warehouse: staging → marts → gold, plus all dbt tests
cd warehouse/dbt
export PERISHABLES_DATA_DIR=../../data/generated
dbt build            # 8 view models, 8 table models, 19 tests

# 5. Inspect the gold deliverable
duckdb perishables.duckdb "select risk_flag, count(*) from perishables_risk group by 1"
```

Output lands in `data/generated/` — dimensions as CSV **and** Parquet (easy to eyeball), facts as Parquet:

```
data/generated/
├── dims/   dim_store · dim_product · dim_supplier · dim_shelf_life · dim_date
└── facts/  fact_sales · fact_inventory_snapshot
```

### Runs today — the streaming path

The producer runs with zero AWS dependency in `stdout`/`file` mode, and the full
Kinesis → Lambda → Firehose → S3 path runs locally on LocalStack.

```bash
# No AWS at all — watch validated events stream by:
python ingestion/stream/producer.py --target stdout --rate 20 --count 100

# Full local path on LocalStack:
docker compose up -d                    # boots LocalStack + provisions stream/bucket/firehose/lambda
AWS_ENDPOINT_URL=http://localhost:4566 \
  python ingestion/stream/producer.py --target kinesis --rate 50 --duration 30
# → the Lambda validates + enriches each record and Firehose lands it in
#   s3://perishables-lake/bronze/stream/<event_type>/dt=YYYY-MM-DD/
```

The whole path is covered by `tests/test_streaming.py`, which stands up Kinesis,
Firehose and S3 **in-process with moto** and asserts good records reach S3 while
malformed ones are rejected — so `pytest` proves the streaming path without Docker.

### Planned — orchestration & serving

```bash
airflow dags trigger perishables_risk_scoring   # build gold + quality gate
streamlit run serving/dashboard/app.py          # open the deliverable
```

---

## The dataset (what the generator produces)

A day-by-day simulation with **FIFO batch aging**: oldest units sell first, and any batch that outlives its shelf life is written off. Spoilage and stockouts *emerge* from ordering behaviour rather than being sprinkled in, so downstream models have a real signal to detect. Every store × SKU is assigned an ordering personality (balanced / over-orderer / under-orderer) via a self-correcting order-up-to policy.

**Emitted tables**

| Table | Grain | Notable columns |
|---|---|---|
| `dim_store` | store | region, format, `size_factor` |
| `dim_product` | SKU | category, supplier, price/cost, `popularity` |
| `dim_supplier` | supplier | `lead_time_days`, reliability |
| `dim_shelf_life` | SKU × version | `shelf_life_days`, `effective_from/to`, `is_current` (SCD2) |
| `dim_date` | day | calendar attributes |
| `fact_sales` | store × SKU × day | `units_sold`, `revenue` (rows only when a sale occurred) |
| `fact_inventory_snapshot` | store × SKU × day | `on_hand_qty`, `received_qty`, `oldest_batch_age_days`, `spoiled_qty`, `unmet_demand` |

Business logic (`days_remaining_shelf_life`, the risk scores) is deliberately **not** computed here — that belongs in the warehouse layer.

**Representative signal** at the default scale (300K inventory rows):

```
spoilage rate ............ ~9%   of units that reached the shelf
                                 (concentrated in Seafood & Prepared Foods —
                                  ~2-day shelf life; Dairy at 15 days barely spoils)
zero on-hand ............. ~17%  of snapshots  (driven by the under-orderer cohort)
```

That category signature — most perishable categories spoil most — is asserted as a test, so the data can't silently drift into looking fake.

### The gold deliverable

`perishables_risk` — one row per store × SKU × day, carrying both continuous
scores and a single acute **risk_flag**. A representative run flags roughly:

```
OK ......... 79%
STOCKOUT ... 17%   empty shelf with live demand — losing sales now
SPOILAGE ....4%   majority of on-hand stock projected to expire unsold
```

The two scores are gradients (`stockout_risk_score` is lead-time-relative
reorder urgency; `spoilage_risk_score` is the fraction of on-hand unlikely to
sell before expiry); the flag marks only the acute, act-today state — so a
category manager gets a short prioritised list, not 50%-of-SKUs alert fatigue.

---

## Data quality & CI

`tests/test_generator.py` — 11 invariants, currently the project's quality contract:

```
test_all_expected_tables_present
test_referential_integrity                 every fact key resolves to a dimension
test_inventory_snapshot_is_dense           store × SKU × day, no gaps or dupes
test_no_negative_quantities
test_sales_only_recorded_when_something_sold
test_revenue_matches_units_times_price
test_stock_never_outlives_shelf_life       aging past shelf life must be written off
test_dataset_contains_both_failure_modes   spoilage AND stockouts are present
test_spoilage_concentrates_in_short_shelf_life   face-validity guard
test_same_seed_is_deterministic            reproducible under a fixed seed
test_scd2_has_current_record_per_product   exactly one current shelf-life row per SKU
```

These graduate into the CI gate (`.github/workflows/ci.yml`): once dbt and Great Expectations land, a failing check blocks promotion silver → gold, so bad data never reaches the deliverable.

---

## Cost & performance notes

- **Firehose buffering** (size/interval tuned) collapses many small stream records into efficient S3 objects.
- **Redshift dist/sort keys** on `store_id` / `product_id` / `computed_at` keep the daily risk scan cheap.
- **Partitioned S3** (`dt=YYYY-MM-DD`) lets Glue and Redshift Spectrum prune to the day being processed.
- **Incremental dbt models** on the fact tables reprocess only new snapshots.

---

## Roadmap

- [x] Synthetic data generator with FIFO aging + data-quality tests
- [x] Redshift DDL + dbt staging/marts, incl. the `perishables_risk` gold model
- [x] Kinesis → Lambda → Firehose streaming path (LocalStack + moto-tested)
- [ ] Glue batch loaders + Airflow orchestration
- [ ] Great Expectations gate wired into CI
- [ ] Streamlit dashboard over the gold table
- [ ] Swap heuristic risk scores for a lightweight demand-forecast layer

---

## Note on scope

This is a **portfolio project** built with synthetic data to demonstrate end-to-end data-engineering capability — streaming + batch ingest, dimensional modeling, warehouse transforms, quality gating, and analytic delivery on an AWS-native stack. It stands as evidence of the approach and tooling, not a claim of production scale.

---

## Author

**Sajan Singh Shergill** — Data Engineer
[LinkedIn](https://linkedin.com/in/sajanshergill) · [Portfolio](https://sajansshergill.github.io) · sajansshergill@gmail.com