# Data Product Blueprint — Retail Data Platform

> Filled from `DATA-PRODUCT-BLUEPRINT.md`. "unknown" means not yet confirmed, not guessed.

## 1. Business scenario 🟥

Owner: Kazi Rahaman · Status: agreed

* A retail company sells online (web shop) and in physical stores. Online orders live in Cosmos DB,
  store/POS sales land in Amazon S3 as Parquet, and customer master data lives in the Azure SQL CRM.
  Teams report directly from different systems with different logic, so the same KPI
  (e.g. August revenue) gives different numbers.
* Trigger: revenue mismatch between reports.
* Success: one governed Gold star schema that every BI and AI consumer reads from.
* Out of scope (this version): streaming / real-time, CDC from the sources.

## 2. Stakeholders and ownership (RACI) 🟥

Owner: Kazi Rahaman · Status: draft

| Role | Name | R/A/C/I | Decides on |
| --- | --- | --- | --- |
| Sponsor | Course trainer (Bhevendra) | A | scope, sign-off |
| Data engineering | Kazi Rahaman | R | design, implementation |
| Source owners | unknown | C | access, schema changes |
| Consumers (BI, AI) | unknown | I/C | acceptance questions |

## 3. Business questions and KPIs 🟧

Owner: Kazi Rahaman · Status: draft

| # | Business question | KPI | Grain | Dimensions | Priority |
| --- | --- | --- | --- | --- | --- |
| Q1 | How much revenue did we make per day / month? | `SUM(total_amount)` | order line | date | must |
| Q2 | Which products sell most? | `SUM(qty)`, `SUM(total_amount)` | order line | product, category | must |
| Q3 | Which customers / loyalty segments buy most? | `SUM(total_amount)`, `COUNT(DISTINCT order_number)` | order line | customer, segment | must |
| Q4 | How much do promotions cost us? | `SUM(qty * unit_price * discount)` | order line | product, date | should |

`total_amount = qty * unit_price * (1 - discount)`, rounded to 2 decimals, currency USD.

## 4. Business definitions and glossary 🟧

Owner: Kazi Rahaman · Status: draft

| Term | Definition | Source of truth |
| --- | --- | --- |
| Customer | A person identified by `customer_id` in the CRM | SQL Server `retail.customers` |
| Order | One Cosmos DB document, identified by `order_number` | Cosmos DB `retail.sales_orders` |
| Order line | One element of `ordered_products` in an order | Cosmos DB |
| Product | Identified by product `id` in `ordered_products`; category from S3 sales | Cosmos DB + S3 |
| Discount | `promo_disc` in `promo_info`, a fraction 0–1 | Cosmos DB |

## 5. Source system details 🟥

Owner: Kazi Rahaman · Status: agreed

| Item | SQL Server | Cosmos DB | Amazon S3 |
| --- | --- | --- | --- |
| Technology | Azure SQL | Cosmos DB (MongoDB API) | S3 object storage |
| Object | `retail.customers` | db `retail`, collection `sales_orders` | `bucket-rivadata/retail/sales.parquet` |
| Access | Spark JDBC (`SQLServerDriver`) | PyMongo | Spark parquet over `s3a://` |
| Credentials | `sqlserver-username`, `sqlserver-password` | `cosmos-connection-string` | `aws-access-key-id`, `aws-secret-access-key` |
| Extract type | full | full | full |
| Business key | `customer_id` | `order_number` | `customer_id` + `product_id` + `order_date` |
| Shape | flat table | nested JSON: `ordered_products[]`, `promo_info[]`, `clicked_items[][]` | flat + `product` JSON string |
| Known issues | name is "LAST, FIRST"; postcode has `.0`; duplicate `customer_id` | epoch `order_datetime`; numbers stored as strings | `product` stored as JSON text |
| Volume, retention | unknown | unknown | unknown |

## 6. Data access, security and secrets 🟥

Owner: Kazi Rahaman · Status: agreed

* Dev runs as the developer user; prod is designed to run as a service principal (`run_as` in `databricks.yml`).
* All credentials live in Databricks secret scope `retail-platform-dev` (prod: `retail-platform-prod`).
  Config files hold only the key names, never values.
* Job permission: `CAN_VIEW` for group `users`. Consumers should get `SELECT` on `gold` only.

## 7. Confidentiality classification 🟥

Owner: Kazi Rahaman · Status: draft

| Dataset / column | Classification | Handling |
| --- | --- | --- |
| `silver.customers` name, address, `tax_id`, lat/lon | PII | Not carried to Gold except names |
| `gold.dim_customer` first/last/full name | PII | Gold access restricted to analytics groups |
| Orders, products, sales | Internal | — |

## 8. Data refresh, latency and SLAs 🟧

Owner: Kazi Rahaman · Status: agreed

| Item | Value |
| --- | --- |
| Refresh frequency | Daily, 05:00 UTC (`resources/job.yml`) |
| SLA | Run finishes within 1 hour (health rule alerts above 3600s); hard timeout 2 hours |
| Backfill | Re-run the job for the day; raw is kept per `load_date` |
| Time zone | UTC |

## 9. Data quality rules 🟧

Owner: Kazi Rahaman · Status: agreed

Defined in `src/dq/config/config_dq_*.json`, results written to `retaildataplatform.audit.dq_results`.

| Layer | Entity | Main rules | On failure |
| --- | --- | --- | --- |
| Silver | customers | not empty, `customer_id` not null + unique, loyalty segment accepted values | fail / warn |
| Silver | sales_orders | `order_number`, `customer_id` not null, line items ≥ 1 | fail |
| Silver | order_products | qty ≥ 1, price ≥ 0 | fail |
| Silver | promotions | discount 0–1, promo quantity ≥ 0 | fail |
| Silver | clicked_items, sales | not null keys, non-negative values | fail / warn |
| Gold | dims | key not null + unique, ranges on date parts | fail |
| Gold | fact_sales | grain unique, FKs to all 4 dims, `total_amount` formula, row count vs `order_products` | fail / warn |

## 10. Conceptual model 🟧

Customer *places* Order · Order *contains* Product (order line) · Order line *may have* Promotion ·
Customer *clicks* Product · Order *happens on* Date.

## 11. Logical model 🟧

| Entity | Business key | History |
| --- | --- | --- |
| Customer | `customer_id` | SCD 1 (`silver.customers`) + SCD 2 (`silver.customers_history`) |
| Order | `order_number` | SCD 1 |
| Product | `product_id` | overwrite |
| Date | `date_key` (yyyyMMdd) | generated |
| Sales (fact) | `order_number` + `product_id` | overwrite |

Fact grain: one row per order line.

## 12. Target design — Gold layer 🟧

Owner: Kazi Rahaman · Status: agreed

| Object | Type | Grain | Keys | Attributes / measures | Source |
| --- | --- | --- | --- | --- | --- |
| `dim_customer` | dim | customer | PK `customer_id` | first/last/full name, loyalty_segment, customer_type, created_date, last_updated | `silver.customers` |
| `dim_order` | dim | order | PK `order_number`, FK `customer_id`, `order_date_key` | order_timestamp, number_of_line_items | `silver.sales_orders` |
| `dim_product` | dim | product | PK `product_id` | product_name, unit, category ("Unknown" if missing) | `silver.order_products` + `silver.sales` |
| `dim_date` | dim | day | PK `date_key` | date, day, month, year, quarter, week | range of `silver.sales_orders.order_timestamp` |
| `fact_sales` | fact | order line | FKs `order_number`, `customer_id`, `product_id`, `order_date_key` | qty, unit_price, discount, promo_quantity, total_amount | `sales_orders` + `order_products` + `promotions` |
| `fact_pos_sales` | fact | customer + product + date | FKs `customer_id`, `product_id`, `order_date_key` | qty, unit_price, total_amount | `silver.sales` |
| `vw_sales_all_channels` | view | sale line | as facts | revenue across both channels | both facts |

Unknown members: `customer_id = -1`, `product_id = 'UNKNOWN'`, `date_key = -1`.

## 13. Physical model 🟩

Catalog `retaildataplatform`; schemas `bronze`, `silver`, `gold`, `audit`; volume `bronze.raw_data`.
All tables Delta. Bronze audit columns: `last_update_ts`, `file_path`.
Write patterns: Bronze `replaceWhere load_date` (partitioned by `load_date`); Silver quarantine then SCD1/SCD2 `MERGE`; Gold overwrite with `overwriteSchema`. Extra Bronze audit columns: `load_date`, `source_system`.

## 14. Ingestion and landing design 🟧

Raw path `/Volumes/retaildataplatform/bronze/raw_data/<source>/load_date=YYYY-MM-DD`.
SQL Server → CSV, Cosmos DB → JSON (one `json_data` column), S3 → Parquet. No transformation at ingestion.
Re-running the same day overwrites that day's folder, so ingestion is idempotent per date.

## 15. Orchestration and frequency 🟩

See `resources/retail_data_platform_job.yml` (17 tasks: setup → 3 ingestion → bronze → 3 silver → 4 dims → 2 facts → view → data_quality → governance) and the weekly maintenance job. The diagram below is the earlier version.

Job `retail-data-platform-<target>` in `resources/job.yml`:

```
ingest_sqlserver ─┐
ingest_cosmosdb  ─┼─> bronze ─┬─> silver_customers ──────> gold_dim_customer ─┐
ingest_s3        ─┘           ├─> silver_sales_orders ─┬─> gold_dim_order ────┤
                              │                        ├─> gold_dim_date ─────┤
                              └─> silver_sales ────────┼─> gold_dim_product ──┼─> data_quality
                                                       └─> gold_fact_sales ───┘
```

Serverless environment with `pymongo`; ingestion retries 2× (5 min apart); `max_concurrent_runs: 1`.

## 16. Observability and operations 🟩

Structured logging via `common_utils/logging.py` (INFO / WARNING / ERROR).
Email on failure and on duration warning. DQ results in `audit.dq_results` per `run_id`.
Re-run a day: run the job again; ingestion overwrites `load_date=<today>`, Silver merges, Gold overwrites.

## 17. Consumption layer 🟩

| Consumer | Tool | Entry point |
| --- | --- | --- |
| BI | Power BI | `retaildataplatform.gold` star schema |
| AI / ad-hoc | Databricks SQL / notebooks | `retaildataplatform.gold` |

## 18. Data dictionary 🟩

Column lists and types are in `src/silver/config/*` and `src/gold/config/*` (`select_columns`, `cast_columns`).

## 19. Lineage 🟩

Gold row → Silver table (config `source_table`) → Bronze row (`file_path`, `last_update_ts`)
→ raw file in `load_date=` folder → source system. Unity Catalog lineage shows table-level lineage.

## 20. Environments, deployment and testing 🟩

Databricks Asset Bundle: targets `dev` (default, schedule paused) and `prod` (schedule on).
`databricks bundle validate` → `deploy -t dev` → `run`.
Tests in `tests/`: unit (`rename_columns`), integration (Delta write/read), regression (Silver customers schema), smoke (Gold `dim_customer`).

## 21. Traceability 🟩

| Requirement | Design decision | Artefact | Test |
| --- | --- | --- | --- |
| One revenue number | Single `fact_sales` with derived `total_amount` | `src/gold/notebooks/fact_sales` | DQ `total_amount_formula` |
| No duplicate customers | Dedup + SCD 1 on `customer_id` | `b2s_customer`, `scdfunctions.py` | DQ `customer_id_unique` |
| Every sale links to dims | FK checks | `config_dq_fact_sales.json` | DQ `*_fk_integrity` |
| Silver schema stays stable | Config-driven casts | `config_b2s_customers.json` | `test_customer_regression.py` |

## 22. Risks, assumptions, open questions and change log 🟥

| Date | Type | Description | Resolution |
| --- | --- | --- | --- |
| 2026-10 | Assumption | All customers are in the USA (`country = 'USA'`) | Agreed in class |
| 2026-10 | Risk | Cosmos customers missing from CRM fail `customer_fk_integrity` | Check DQ results; add unknown member if needed |
| 2026-10 | Change | Own build: quarantine, SCD2 history, fact_pos_sales, unknown members, governance task, run log, weekly maintenance job, CI/CD | Done |

## 23. Acceptance and sign-off 🟩

Job runs green end to end, DQ has no `FAIL` rows, Q1–Q4 answerable from Gold. Sign-off: trainer.
