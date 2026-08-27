# Tiki E-Commerce Data Lakehouse — Technical & Benchmark Report

## 1. Goal

The system ingests, cleans, validates, and models market data from Tiki.vn using the Medallion architecture (Bronze -> Silver -> Gold).

On the technical side, the project focuses on:

- **Reliability & integrity (ACID)** — no duplicates on repeated pipeline runs (idempotency), enforced both in Delta Lake (Silver) and PostgreSQL (Gold).
- **Data quality gates** — automatic checks for primary key uniqueness, non-null constraints, and valid value ranges before loading.
- **High-throughput, resilient ingestion** — I/O optimized with multi-threading (`ThreadPoolExecutor`), connection pooling, and exponential backoff retries to work around rate limiting.

On the business side, the output is a dimensional model (star schema) in the data marts, feeding BI reports directly in Metabase.

## 2. System architecture

```
Tiki.vn public API & HTML navigation
        |  multi-threaded ingestion (ThreadPoolExecutor + requests.Session)
        v
Bronze layer (MinIO S3 bucket)
    format: raw JSON
    partition: /bronze/{categories,products,reviews}/crawl_date=YYYY-MM-DD/
        |  PySpark + Delta engine (data quality gate & sanitization)
        v
Silver layer (MinIO S3 bucket)
    format: Delta Lake (Parquet + _delta_log ACID metadata)
    operation: DeltaTable MERGE INTO (upsert by primary key)
        |  PySpark ETL: dimensional modeling (star schema) + staging upsert
        v
Gold layer (PostgreSQL 15)
    format: relational tables — dimension & fact
        |  JDBC / SQL analytical queries
        v
Metabase — market, brand, and review analysis
```

## 3. Storage footprint & compression efficiency

Converting from raw JSON to Snappy-compressed Parquet on Delta Lake cuts storage significantly:

| Layer | Storage | Format | Actual size | Optimization |
|---|---|---|---|---|
| Bronze | MinIO S3 | Raw `.json` | ~34.4 MB (products + reviews) | Stores the raw API payload, partitioned by date |
| Silver | MinIO S3 | `.parquet` (Delta Lake) | ~7.2 MB | Snappy columnar compression, nested schema cleanup, ~79% size reduction |
| Gold | PostgreSQL 15 | Relational tables | 108.5k+ rows | B-Tree indexes on primary keys, optimized for aggregation queries |

## 4. Ingestion & processing benchmarks

### Ingestion telemetry (Bronze layer)

Measured automatically via a `MetricCollector` module on a real run:

- Total wall time: 2,230.22 s (~37.17 min)
- Total HTTP requests: 23,370 calls (zero failures)
- Network I/O downloaded: 440.95 MB
- Average latency: 680.18 ms/request
- Product throughput: 4.68 SKU/s
- Overall record throughput: 47.83 records/s
- Records loaded: 238 categories | 10,434 products | 97,633 reviews

### Pipeline execution & resource footprint

Measured in a Docker Compose environment (4 cores, 4 GB RAM container):

| Task | Technology | What it does | Duration | Peak RAM |
|---|---|---|---|---|
| `scrape_tiki_to_bronze` | `requests.Session` + `ThreadPoolExecutor` | Paginated crawl, multi-threaded detail/review extraction, upload to MinIO | ~37.1 min | ~180 MB |
| `bronze_to_silver_delta` | PySpark + Delta Lake 3.1.0 | Schema mapping, DQ validation, price sanitization, `DeltaTable.merge()` | ~42 s | ~2.10 GB |
| `silver_to_gold_postgres` | PySpark + JDBC + psycopg2 | Partition snapshot, staging tables, `ON CONFLICT` upsert | ~25 s | ~1.35 GB |
| Full transform (Silver + Gold) | Spark | Processes 108,500+ records across the medallion layers | ~1 min 07 s | 2.10 GB |

## 5. Gold layer data model

Standard Kimball-style star schema:

```
                    dim_categories
                  ----------------
                  PK category_id BIGINT
                     category_name TEXT
                     url TEXT
                        |
                        | 1:N
                        v
dim_products                          fact_daily_product_snapshot
----------------                      -----------------------------
PK product_id BIGINT   1:N            FK snapshot_date DATE
   product_name TEXT  -------------->  FK product_id BIGINT
   brand TEXT                          price DOUBLE PRECISION
   short_description TEXT              original_price DOUBLE PRECISION
   image_url TEXT                      discount DOUBLE PRECISION
   created_at TIMESTAMP                discount_rate DOUBLE PRECISION
   updated_at TIMESTAMP                rating_average DOUBLE PRECISION
        |                              review_count INT
        | 1:N                         inventory_status TEXT
        v                             price_segment TEXT
fact_reviews
----------------
PK review_id BIGINT
FK product_id BIGINT
   rating DOUBLE PRECISION
   title TEXT
   content TEXT
   thank_count INT
   comment_count INT
   created_at TIMESTAMP
```

## 6. Data quality & reliability

### Automated data quality gate (`data_quality.py`)

- **Primary key uniqueness**: checks and deduplicates `category_id`, `product_id`, `review_id`.
- **Non-null constraints**: enforced on identifier and financial fields (`price`, `product_id`, `rating`).
- **Range checks & sanitization**: negative or abnormal prices (`price < 0` or `> 1 billion VND`) are flagged and reset to 0.0 with a warning, instead of failing the whole batch job.
- **Rating validation**: ratings are constrained to the range [1.0, 5.0].

### Idempotency & data freshness

- **Delta Lake (Silver)**: uses `whenMatchedUpdate()` and `whenNotMatchedInsertAll()` so repeated runs converge to the same state.
- **PostgreSQL (Gold)**: uses a staging table + `INSERT INTO ... ON CONFLICT DO UPDATE` pattern, preserving primary key constraints and audit timestamps (`created_at`, `updated_at`); the snapshot table deletes and rewrites by partition (`DELETE WHERE snapshot_date = %s`).

### Timezone standardization

The Spark session is explicitly configured with `spark.sql.session.timeZone = Asia/Ho_Chi_Minh` so review timestamps (`created_at`) display correctly in Vietnam time (UTC+7) on the dashboard.
