# 🛒 Tiki E-Commerce Modern Data Lakehouse Pipeline

Pipeline Lakehouse end-to-end, tự động thu thập, chuẩn hóa và mô hình hóa dữ liệu thương mại điện tử từ API của Tiki.vn, theo **kiến trúc Medallion (Bronze → Silver → Gold)**, sử dụng **MinIO (S3)**, **PySpark**, **Delta Lake** và **PostgreSQL**.

---

## 📐 Tổng quan kiến trúc

```text
[ Tiki.vn REST APIs ]
          │ (Requests / JSON Streaming)
          ▼
[ MinIO Object Storage (S3-compatible) ]
   ├── Bronze Layer : Dữ liệu JSON thô, phân vùng theo ngày (`bronze/entity/crawl_date=YYYY-MM-DD/`)
   │
   │ (PySpark + Delta Lake: Schema Enforcement, Deduplication, MERGE INTO)
   ▼
   ├── Silver Layer : Bảng Delta đã làm sạch (`silver/categories`, `products`, `reviews`)
   │
   │ (PySpark ETL: Mô hình hóa chiều / Star Schema)
   ▼
[ PostgreSQL Serving Marts (Gold Layer) ]
   ├── Bảng Dimension: `dim_products`, `dim_categories`
   └── Bảng Fact     : `fact_daily_product_snapshot`, `fact_reviews`
```

---

## 🛠️ Công nghệ sử dụng & tính năng chính

- **Lớp lưu trữ:** MinIO (Object Storage tương thích S3) — giải pháp nhẹ thay thế Hadoop HDFS.
- **Định dạng bảng:** Delta Lake (giao dịch ACID, Time Travel, Upsert idempotent với `MERGE INTO`).
- **Xử lý:** Apache Spark (PySpark 3.5) chạy trên Java 17 headless.
- **Lớp phục vụ:** PostgreSQL (Data Warehouse / Marts theo mô hình Star Schema).
- **Hạ tầng:** Docker & Docker Compose, triển khai đóng gói container, dễ tái lập.

---

## 📁 Cấu trúc thư mục dự án

```text
tiki-lakehouse-pipeline/
├── dags/                        # Airflow DAGs (Điều phối luồng)
├── src/
│   ├── extract/                 # Script trích xuất dữ liệu & upload lên S3
│   │   ├── minio_client.py      # Kết nối MinIO/S3 qua Boto3 & helper upload
│   │   └── tiki_scraper.py      # Scraper API cho Categories, Listings, Details, Reviews
│   └── transform/                # Job biến đổi dữ liệu với PySpark & Delta Lake
│       ├── spark_session_helper.py # Cấu hình SparkSession với Delta & S3A jars
│       ├── silver_transform.py  # Bronze → Silver Delta Tables
│       ├── gold_transform.py    # Silver → Gold Star Schema (PostgreSQL)
│       ├── peek_silver.py       # Tiện ích xem trước dữ liệu Silver Delta
│       └── peek_gold.py         # Tiện ích xem trước dữ liệu Gold PostgreSQL
├── Dockerfile                   # Airflow image tùy chỉnh với Java 17 & dependencies
├── docker-compose.yml           # Thiết lập multi-container (MinIO, Postgres, Airflow, Metabase)
├── requirements.txt             # Các thư viện Python cần thiết
└── README.md
```

---

## 🚀 Hướng dẫn bắt đầu nhanh

### 1. Yêu cầu tiên quyết

- Docker Desktop (chạy trên Linux, macOS, hoặc Windows qua WSL2).
- Tối thiểu 4GB RAM cấp phát cho Docker.

### 2. Build và khởi động hạ tầng

Clone repository và khởi động toàn bộ các service:

```bash
# 1. Clone repo
git clone https://github.com/<your-username>/tiki-lakehouse-pipeline.git
cd tiki-lakehouse-pipeline

# 2. Build và khởi động containers
docker compose build --no-cache
docker compose up -d
```

### 3. Địa chỉ truy cập các dịch vụ

| Dịch vụ | Địa chỉ | Tài khoản |
|---|---|---|
| MinIO Storage Console | http://localhost:9001 | admin / password123 |
| Airflow UI | http://localhost:8080 | admin / admin |
| Metabase BI | http://localhost:3000 | — |

---

## 🔄 Các bước thực thi pipeline

### Bước 1: Nạp dữ liệu thô vào lớp Bronze (MinIO)

Thu thập Categories, Product Listings, Product Details, và Reviews từ API Tiki, upload dữ liệu JSON thô đã phân vùng lên MinIO:

```bash
docker exec -it lakehouse_airflow python /opt/airflow/src/extract/tiki_scraper.py
```

Đường dẫn output: `s3://lakehouse/bronze/{entity}/crawl_date=YYYY-MM-DD/`

### Bước 2: Biến đổi và làm sạch dữ liệu vào lớp Silver (Delta Lake)

Đọc dữ liệu JSON từ Bronze, kiểm tra schema, loại bỏ trùng lặp theo khóa chính, và thực hiện `MERGE INTO` (Upsert) idempotent trên các bảng Delta Lake:

```bash
docker exec -it lakehouse_airflow python /opt/airflow/src/transform/silver_transform.py
```

Đường dẫn output: `s3://lakehouse/silver/{categories|products|reviews}/`

Kiểm tra dữ liệu lớp Silver:

```bash
docker exec -it lakehouse_airflow python /opt/airflow/src/transform/peek_silver.py
```

### Bước 3: Mô hình hóa và nạp dữ liệu vào lớp Gold (PostgreSQL)

Biến đổi các bảng Delta của Silver thành Star Schema (Dimension & Fact) và ghi vào PostgreSQL để phục vụ phân tích với độ trễ thấp:

```bash
docker exec -it lakehouse_airflow python /opt/airflow/src/transform/gold_transform.py
```

Kiểm tra các bảng lớp Gold trong PostgreSQL:

```bash
docker exec -it lakehouse_airflow python /opt/airflow/src/transform/peek_gold.py
```

Hoặc truy vấn trực tiếp qua `psql`:

```bash
docker exec -it lakehouse_postgres psql -U airflow -d airflow_db -c "SELECT price_segment, COUNT(*) FROM fact_daily_product_snapshot GROUP BY price_segment;"
```

---

## 📊 Tham chiếu Schema dữ liệu (Gold Marts)

### Dimensions

| Bảng | Các cột |
|---|---|
| `dim_categories` | `category_id` (PK), `category_name`, `url` |
| `dim_products` | `product_id` (PK), `product_name`, `brand`, `short_description`, `image_url` |

### Facts

| Bảng | Các cột |
|---|---|
| `fact_daily_product_snapshot` | `snapshot_date`, `product_id` (FK), `price`, `original_price`, `discount`, `discount_rate`, `rating_average`, `review_count`, `inventory_status`, `price_segment` |
| `fact_reviews` | `review_id` (PK), `product_id` (FK), `rating`, `title`, `content`, `thank_count`, `comment_count`, `created_at` |
