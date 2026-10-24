# Retail Data Platform

Config-driven Databricks Lakehouse that integrates three operational sources of a
retail company and serves a governed Kimball star schema for BI and AI.

**Business problem:** web-shop orders (Cosmos DB), store sales (S3) and customers
(SQL Server CRM) live in different systems, so teams report different revenue
numbers. This platform gives one trusted, tested, governed source of truth.

## Architecture

```
SQL Server (CRM) ─┐ JDBC
Cosmos DB (web)  ─┼ PyMongo ──> RAW VOLUME ──> BRONZE ──> SILVER ──────────> GOLD ──────────> DQ ──> GOVERNANCE ──> BI / AI
Amazon S3 (POS)  ─┘ parquet     load_date=     Delta +    clean, flatten,     Kimball star     audit   tags, comments,
                                YYYY-MM-DD     audit      quarantine,         + unknown        table   masked view,
                                (as-is)        columns    SCD1 / SCD2         members                  grants
```

| Layer | Question | Objects |
| --- | --- | --- |
| Raw volume | What did the source send? | `/Volumes/<catalog>/bronze/raw_data/<source>/load_date=YYYY-MM-DD` |
| Bronze | What did we receive? | `sqlserver_customers`, `cosmosdb_sales_orders`, `s3_sales` (partitioned by `load_date`) |
| Silver | Can we trust it? | `customers` (SCD1), `customers_history` (SCD2), `sales_orders`, `order_products`, `promotions`, `clicked_items`, `sales`, `*_quarantine` |
| Gold | How does the business use it? | `dim_customer`, `dim_product`, `dim_date`, `dim_order`, `fact_sales`, `fact_pos_sales`, `vw_sales_all_channels`, `vw_dim_customer_secure` |
| Audit | Did it work? | `pipeline_run_log`, `dq_results` |

### Gold star schema (Kimball)

| Kimball question | Answer |
| --- | --- |
| Business process | Sales (online and in store) |
| Grain | `fact_sales`: one row per order line (`order_number` + `product_id`); `fact_pos_sales`: one row per customer + product + date |
| Facts | `qty`, `unit_price`, `discount`, `promo_quantity`, `total_amount = qty × unit_price × (1 − discount)` |
| Dimensions | `dim_customer`, `dim_product` (conformed across both channels), `dim_date`, `dim_order` |

```
                 dim_customer
                      |
  dim_product ── fact_sales ── dim_date          vw_sales_all_channels =
                      |                           fact_sales ∪ fact_pos_sales
                  dim_order
  dim_product ── fact_pos_sales ── dim_date
                      |
                 dim_customer
```

A fact row whose customer, product or date is not in its dimension points to the
**Unknown member** (`-1` / `'UNKNOWN'`), so no sale is lost and every foreign key is valid.

## Repository

```
retail-data-platform/
├── common_utils/            reusable, project-agnostic framework (the HOW)
│   ├── logging.py           get_logger()
│   ├── config.py            load_config() with {catalog} placeholders, get_param()
│   ├── notebook.py          standard notebook parameters
│   ├── ingestor.py          read_jdbc(), read_cosmosdb_json(), read_s3_parquet(), write_raw()
│   ├── bronze.py            read_raw(), bronze_ingestor() (idempotent per load_date), read_bronze_for_date()
│   ├── generic_functions.py rename/cast/drop/select/deduplicate, write_delta(), assertions
│   ├── scdfunctions.py      scd_type_1(), scd_type_2()
│   ├── data_quality.py      9 check types, audit table, quarantine split
│   └── audit.py             RunLogger -> audit.pipeline_run_log
├── transformations/         project business rules, unit-tested (silver_*.py, gold.py)
├── src/<layer>/notebooks/   thin notebooks that orchestrate: read config -> call functions -> write
├── src/<layer>/config/      JSON config per notebook (the WHAT)
├── resources/               Databricks jobs: daily pipeline + weekly maintenance
├── databricks.yml           Asset Bundle: dev / prod targets
├── tests/                   pytest: unit (local Spark), SCD (Delta), Databricks regression/smoke
├── .github/workflows/       CI/CD: test -> validate -> deploy dev / prod
└── docs/                    blueprint
```

Rules: `common_utils` never names a catalog, table or path. Config = WHAT, functions = HOW,
notebook = orchestration, secret scope = credentials.

## Setup (once)

1. **Secret scope** (Databricks CLI):

```bash
databricks secrets create-scope retail-platform-dev
databricks secrets put-secret retail-platform-dev sqlserver-username
databricks secrets put-secret retail-platform-dev sqlserver-password
databricks secrets put-secret retail-platform-dev cosmos-connection-string
databricks secrets put-secret retail-platform-dev aws-access-key-id
databricks secrets put-secret retail-platform-dev aws-secret-access-key
```

2. Put your workspace URL in `databricks.yml`.
3. Run `src/setup/notebooks/00_setup` once (catalog, schemas, volume, run log), or let the job do it.

## Run

Every notebook has widgets `path`, `catalog`, `run_date`, `secret_scope`, `job_run_id` with
working defaults, so **Run all** works interactively. In the job, job parameters fill them.

| # | Notebook | What it does |
| --- | --- | --- |
| 0 | `setup/00_setup` | UC objects |
| 1 | `ingestion/sql_server_ingestion`, `cosmosdb_ingestion`, `s3_ingestion` | source -> raw volume |
| 2 | `bronze/ds2b` | raw -> Bronze Delta + audit columns |
| 3 | `silver/b2s_customers`, `b2s_sales_orders`, `b2s_sales` | clean, flatten, quarantine, SCD |
| 4 | `gold/dim_date`, `dim_customer`, `dim_product`, `dim_order` | dimensions |
| 5 | `gold/fact_sales`, `fact_pos_sales`, `vw_sales_all_channels` | facts + unified view |
| 6 | `dq/run_data_quality` | checks -> `audit.dq_results`, fails on errors |
| 7 | `governance/apply_governance` | comments, tags, masked view, grants |
| weekly | `maintenance/optimize_tables` | OPTIMIZE + VACUUM |

### Deploy and orchestrate

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run retail_data_platform -t dev
```

Daily job `retail-data-platform-<target>`: 17 tasks, 05:00 Europe/London (paused in dev),
ingestion retried twice, email on failure or when a run passes 1 hour, one run at a time.
Weekly job `retail-data-platform-maintenance-<target>`: Sunday 07:00.

## Production properties

| Property | How |
| --- | --- |
| Idempotent | Raw: overwrite per `load_date`. Bronze: `replaceWhere load_date`. Silver: MERGE. Gold: full rebuild. |
| Traceable | Bronze `file_path`, `load_date`, `source_system`, `last_update_ts`; `audit.pipeline_run_log` per task |
| History | `silver.customers_history` SCD2 with `effective_from`, `effective_to`, `is_current` |
| Bad data | Row rules send failures to `silver.<table>_quarantine` with the rule names |
| Quality gate | 14 DQ configs incl. FK integrity, formula, reconciliation; job fails on `error` checks |
| Security | Credentials only in secret scope; consumers get SELECT on Gold only; names masked unless in `pii_readers` |
| Schema drift | Bronze writes with `mergeSchema`; reads use string schemas + `try_cast` (ANSI-safe) |
| Environments | Bundle targets `dev` / `prod` with their own catalog and secret scope |

## Tests

```bash
pip install -r requirements-dev.txt
pytest tests
```

| File | Type |
| --- | --- |
| `test_common_utils.py` | unit: config, rename, safe_cast (ANSI), dedup, quarantine |
| `test_silver_transformations.py` | unit: customers, sales orders (JSON flatten), store sales on messy samples |
| `test_gold.py` | unit: every dimension and fact, unknown members, all DQ configs pass |
| `test_scd.py` | Delta: SCD1 and SCD2 incl. idempotent re-run |
| `test_databricks_tables.py` | regression + smoke on the real tables (Databricks only) |

On Databricks, run from a notebook in the repo root:
`import pytest; pytest.main(["-v", "-p", "no:cacheprovider", "tests"])`.
