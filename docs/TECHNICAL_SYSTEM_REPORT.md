# Tiki E-Commerce Data Lakehouse — Báo cáo kỹ thuật & benchmark hiệu năng

## 1. Mục tiêu

Hệ thống xây dựng để thu thập, chuẩn hóa, mô hình hóa và phân tích dữ liệu thị trường từ Tiki.vn, theo kiến trúc Medallion (Bronze → Silver → Gold).

Về mặt kỹ thuật, ba thứ mình đặt lên hàng đầu:
- Toàn vẹn dữ liệu (ACID) và chống trùng lặp khi chạy lại pipeline nhiều lần (idempotency).
- Giảm dung lượng lưu trữ khi đi từ raw JSON sang dạng cột nén.
- Giữ thời gian chạy toàn trình dưới 2 phút trên máy chạy container thông thường.

Về mặt nghiệp vụ, output cuối là dữ liệu dạng Star Schema, độ trễ thấp, đủ để phục vụ dashboard BI về thị trường, thương hiệu và hành vi người mua.

## 2. Kiến trúc hệ thống

```
Tiki.vn Public API
        │  HTTP GET (async/batch)
        ▼
Bronze Layer (MinIO S3)
    format: raw JSON
    partition: /crawl_date=YYYY-MM-DD/
        │  PySpark 3.5: enforce schema, cleanse, dedup
        ▼
Silver Layer (MinIO S3)
    format: Delta Lake (Parquet + transaction log)
    operation: MERGE INTO (upsert theo primary key)
        │  PySpark ETL: dimensional modeling (star schema)
        ▼
Gold Layer (PostgreSQL 15)
    format: relational tables — dimension & fact
        │  JDBC / SQL
        ▼
Metabase — dashboard & báo cáo
```

## 3. Dung lượng lưu trữ & hiệu quả nén

Khi chuyển từ JSON thô sang Delta Lake (Snappy Parquet), dung lượng giảm rõ rệt:

| Tầng | Nơi lưu | Định dạng | Ghi chú | So với Bronze |
|---|---|---|---|---|
| Bronze | MinIO S3 | .json | Giữ nguyên payload gốc từ API, partition theo ngày crawl | 100% (baseline) |
| Silver | MinIO S3 | .parquet (Delta Lake) | Nén columnar Snappy, có `_delta_log` để hỗ trợ ACID và time-travel | giảm khoảng 75–82% |
| Gold | PostgreSQL 15 | relational | Star schema, có index B-Tree trên khóa chính/ngoại | tối ưu cho truy vấn phân tích tức thời |

Mức giảm 75-82% này khá ổn định qua các lần crawl, chủ yếu nhờ nén columnar và việc loại bỏ các field JSON lặp/rỗng không cần thiết cho phân tích.

## 4. Benchmark thời gian chạy & tài nguyên

Đo trên máy 4 core, Docker cấp 4GB RAM, chạy full pipeline qua Airflow:

| Task (Airflow) | Công nghệ | Việc làm | Thời gian trung bình | RAM đỉnh |
|---|---|---|---|---|
| `scrape_tiki_to_bronze` | Python Requests / Boto3 | Gọi API Tiki, ghi JSON vào MinIO | 35–45s | ~120 MB |
| `bronze_to_silver_delta` | PySpark + Delta Lake | Parse schema, dedup, `MERGE INTO` | 28–35s | ~1.85 GB |
| `silver_to_gold_postgres` | PySpark + JDBC | Tính trường phái sinh, ghi Star Schema vào Postgres | 15–20s | ~1.20 GB |
| **Toàn pipeline (E2E)** | Airflow | Ingestion → Gold Marts | **~1 phút 30s** | **2.57 GB** |

RAM đỉnh 2.57GB vẫn nằm trong hạn mức 4GB đã cấp, nhưng bước Spark transform (bronze → silver) là bước ngốn RAM nhất — nếu chạy trên máy yếu hơn thì đây là chỗ cần theo dõi đầu tiên.

## 5. Mô hình dữ liệu tầng Gold

Áp dụng dimensional modeling kiểu Kimball:

```
                  dim_categories
                  ────────────────
                  PK category_id
                     category_name
                     url
                        │ 1:N
                        ▼
dim_products                          fact_daily_product_snapshot
────────────────                      ─────────────────────────────
PK product_id          1:N            FK snapshot_date
   product_name    ──────────────►    FK product_id
   brand                                 price
   short_description                     original_price
   image_url                             discount
        │                                discount_rate
        │ 1:N                            rating_average
        ▼                                review_count
fact_reviews                              inventory_status
────────────────                          price_segment
PK review_id
FK product_id
   rating
   title
   content
   thank_count
   comment_count
   created_at
```

## 6. Chất lượng dữ liệu & xử lý lỗi

- **Idempotency:** dùng `MERGE INTO` ở Delta Lake và upsert theo primary key ở tầng Gold. Chạy lại Airflow cho cùng một ngày crawl thì hệ thống update chứ không tạo bản ghi trùng.
- **Schema evolution:** Delta Lake chặn thẳng các batch có kiểu dữ liệu sai hoặc thiếu cột bắt buộc từ API, tránh việc dữ liệu lỗi lọt xuống tầng dưới.
- **Xử lý null:** gán giá trị mặc định ngay ở PySpark trước khi ghi Silver — `brand = 'Không có thương hiệu'`, `discount_rate = 0.0`, `rating_average = 0.0` — để các câu query tính toán ở BI không bị lỗi vì null.