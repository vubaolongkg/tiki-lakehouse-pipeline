# Tiki E-Commerce Data Lakehouse

A data lakehouse pipeline that ingests, cleans, validates, and models market data from Tiki.vn. Built on the Medallion architecture (Bronze -> Silver -> Gold), with ACID transactions via Delta Lake, object storage on MinIO (S3), orchestration by Apache Airflow, serving layer on PostgreSQL data marts, and dashboards in Metabase.

## Architecture

```
Tiki.vn API/HTML
      |
      v  multi-threaded scraper, retry with backoff
Bronze (MinIO S3, raw JSON)
  |-- categories/crawl_date=YYYY-MM-DD/categories.json
  |-- products/crawl_date=YYYY-MM-DD/products.json
  `-- reviews/crawl_date=YYYY-MM-DD/reviews.json
      |
      v  PySpark 3.4+ + Delta engine, data quality gates
Silver (Delta Lake)
  |-- categories  - SCD Type 1 merge
  |-- products    - sanitization, range checks, Delta merge
  `-- reviews     - dedup, timezone normalized to Asia/Ho_Chi_Minh
      |
      v  PySpark ETL, staging + upsert
Gold (PostgreSQL 15)
  |-- dim_categories, dim_products
  |-- fact_reviews
  `-- fact_daily_product_snapshot
      |
      v  SQL / JDBC
Metabase — business reporting
```

## Key technical points

- **Resilient, high-throughput scraper**: multi-threaded extraction (`ThreadPoolExecutor`, 8 workers), HTTP connection pooling via `requests.Session`, exponential backoff to handle WAF/rate-limit interruptions.
- **Automated data quality gates**: a standalone `data_quality.py` module at the Silver layer checks primary key uniqueness, non-null constraints, numeric range checks, and timezone normalization.
- **ACID storage with Delta Lake**: `DeltaTable.merge()` for upserts (SCD Type 1), transaction history preserved in `_delta_log`, with time travel support.
- **Idempotent data marts**: loading into PostgreSQL via staging tables + `ON CONFLICT DO UPDATE`, so the pipeline can be rerun repeatedly without duplicate records or lock contention.

## Data quality framework (`data_quality.py`)

The `DataQualityChecker` module acts as a gate before data is merged into Delta Lake Silver. It runs four core checks:

```
Raw Bronze data (JSON)
        |
        v
  Data Quality Checker
  --------------------
  1. Primary key check     -> detects duplicate PKs, deduplicates
  2. Non-null check        -> required for PK, product name, price
  3. Numeric range check   -> price in [0, 1B VND], rating in [1.0, 5.0]
  4. Sanitize & quarantine -> defaults/cleans negative prices, null text
        |
        v (passed)
  Delta Lake Silver (clean table)
```

**Primary key uniqueness** — validates that `category_id`, `product_id`, and `review_id` are 100% unique. Duplicate records from API pagination are logged and removed via `dropDuplicates()` before the merge into Delta Lake.

**Non-null constraints** — blocks any record where a primary key or identifier field is null. If a required field like `category_id` or `review_id` comes back null, the pipeline raises a `ValueError` immediately rather than letting bad data flow downstream.

**Numeric range checks** — product price must fall within [0, 1,000,000,000 VND]; rating must fall strictly within [1.0, 5.0].

**Sanitization** — negative or null prices from the API are reset to 0.0 automatically. Missing text fields are backfilled (`brand = 'No brand'`, `inventory_status = 'available'`) so BI queries don't break on nulls.

## Benchmark results (single real run)

| Metric | Value |
|---|---|
| Total wall time | 2,230.22 s (~37.17 min) |
| HTTP requests | 23,370 calls (zero failures) |
| Data downloaded | 440.95 MB |
| Avg latency | 680.18 ms/request |
| SKU throughput | 4.68 SKU/s |
| Overall throughput | 47.83 records/s |
| Bronze categories | 238 records |
| Bronze products | 10,434 records |
| Bronze reviews | 97,633 records |

## Stack

- Orchestration: Apache Airflow 2.8.1 (LocalExecutor)
- Distributed processing: Apache Spark / PySpark
- Storage / table format: MinIO S3, Delta Lake 3.1.0
- Data warehouse: PostgreSQL 15
- BI: Metabase
- Data quality & testing: PySpark validation rules, Pytest
- Containerization: Docker & Docker Compose

## Repository structure

```
tiki-lakehouse-pipeline/
├── dags/
│   └── tiki_lakehouse_dag.py       # end-to-end pipeline DAG
├── src/
│   ├── extract/
│   │   ├── minio_client.py         # MinIO S3 connection & upload
│   │   └── tiki_scraper.py         # multi-threaded scraper with telemetry
│   ├── transform/
│   │   ├── spark_session_helper.py # SparkSession config (Delta, S3A, JDBC)
│   │   ├── data_quality.py         # PK, null, and range checks
│   │   ├── silver_transform.py     # Bronze -> Silver Delta merge
│   │   └── gold_transform.py       # Silver -> Gold PostgreSQL marts
├── tests/                          # unit & integration tests
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Gold layer data model

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

## Running it

### 1. Start the containers

```bash
docker compose up -d
```

### 2. Services

- Airflow: http://localhost:8080 (admin / admin)
- MinIO Console: http://localhost:9001 (admin / password123)
- Metabase: http://localhost:3000

### 3. Run the pipeline manually

```bash
# Step 1: extract raw data into Bronze (MinIO)
docker exec -it lakehouse_airflow python /opt/airflow/src/extract/tiki_scraper.py

# Step 2: clean, validate, and merge into Silver (Delta Lake)
docker exec -it lakehouse_airflow python /opt/airflow/src/transform/silver_transform.py

# Step 3: model as star schema and load into Gold (PostgreSQL)
docker exec -it lakehouse_airflow python /opt/airflow/src/transform/gold_transform.py
```

### 4. Run tests

```bash
docker exec -it lakehouse_airflow pytest /opt/airflow/tests/ -v
```

## Sample queries (Gold / PostgreSQL)

Rating distribution and helpful-vote totals across all reviews:

```sql
SELECT 
    rating,
    COUNT(*) AS total_reviews,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS percentage,
    SUM(thank_count) AS total_helpful_votes
FROM fact_reviews
GROUP BY rating
ORDER BY rating DESC;
```

Top 5 highest-rated brands (min. 100 reviews):

```sql
SELECT 
    p.brand,
    COUNT(r.review_id) AS total_reviews,
    ROUND(AVG(r.rating)::numeric, 2) AS avg_rating,
    SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS positive_rate_pct
FROM dim_products p
JOIN fact_reviews r ON p.product_id = r.product_id
WHERE p.brand IS NOT NULL AND p.brand <> 'No brand'
GROUP BY p.brand
HAVING COUNT(r.review_id) >= 100
ORDER BY avg_rating DESC, total_reviews DESC
LIMIT 5;
```
