# Tiki E-Commerce Data Lakehouse

A data lakehouse pipeline that ingests, cleans, and serves e-commerce data from Tiki.vn. Built on the Medallion architecture (Bronze -> Silver -> Gold), using Delta Lake for ACID transactions and Airflow for orchestration.

## Architecture

```
Tiki.vn API/HTML
      |
      v  multi-threaded scraper, retry with backoff
Bronze (MinIO/S3, raw JSON)
  |-- categories
  |-- products
  `-- reviews
      |
      v  PySpark + Delta, data quality checks
Silver (Delta Lake)
  |-- categories  - SCD Type 1 merge
  |-- products    - sanitization + range checks
  `-- reviews     - dedup, timezone normalized to Asia/Ho_Chi_Minh
      |
      v  dimensional modeling, staging + upsert
Gold (PostgreSQL)
  |-- dim_categories, dim_products
  |-- fact_reviews
  `-- fact_daily_product_snapshot
      |
      v
Metabase
```

## Key technical points

- **Ingestion**: multi-threaded crawler (`ThreadPoolExecutor`), connection pooling via `requests.Session`, exponential backoff to handle WAF/rate-limit blocks.
- **Data quality**: primary key uniqueness checks, non-null constraints, numeric range checks, timezone normalization.
- **Delta Lake**: uses `DeltaTable.merge()` for upserts (SCD Type 1), with time travel support.
- **Loading into Postgres**: staging table + `ON CONFLICT DO UPDATE`, so the pipeline can be rerun without creating duplicates or lock contention.

## Benchmark results (single real run)

| Metric | Value |
|---|---|
| Total wall time | 2,230.22 s (~37 min) |
| HTTP requests | 23,370 (zero failures) |
| Data downloaded | 440.95 MB |
| Avg request latency | 680.18 ms |
| Throughput | 4.68 SKU/s, 47.83 records/s |
| Bronze categories | 238 |
| Bronze products | 10,434 |
| Bronze reviews | 97,633 |

## Stack

- Airflow 2.8.1 (LocalExecutor)
- Spark 3.4.x / PySpark
- MinIO (S3-compatible), Delta Lake 3.1.0
- PostgreSQL 15 (Alpine)
- Pytest for testing
- Docker Compose

## Repository structure

```
tiki-lakehouse-pipeline/
├── dags/
│   └── tiki_lakehouse_dag.py       # daily pipeline DAG
├── src/
│   ├── extract/
│   │   ├── minio_client.py         # MinIO wrapper
│   │   └── tiki_scraper.py         # multi-threaded crawler
│   ├── transform/
│   │   ├── spark_session_helper.py # SparkSession config (Delta, S3A, JDBC)
│   │   ├── data_quality.py         # data quality rules
│   │   ├── silver_transform.py     # Bronze -> Silver
│   │   └── gold_transform.py       # Silver -> Gold
├── tests/
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Running it

### 1. Start infrastructure

```bash
docker compose up -d
```

### 2. Services

- Airflow: http://localhost:8080 (admin / admin)
- MinIO Console: http://localhost:9001 (admin / password123)
- Metabase: http://localhost:3000

### 3. Run the pipeline manually

```bash
# Bronze
docker exec -it lakehouse_airflow python /opt/airflow/src/extract/tiki_scraper.py

# Silver
docker exec -it lakehouse_airflow python /opt/airflow/src/transform/silver_transform.py

# Gold
docker exec -it lakehouse_airflow python /opt/airflow/src/transform/gold_transform.py
```

### 4. Tests

```bash
docker exec -it lakehouse_airflow pytest /opt/airflow/tests/ -v
```

## Sample queries (Gold layer)

Rating distribution across all reviews:

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
