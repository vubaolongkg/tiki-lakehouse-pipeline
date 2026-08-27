import io
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from minio_client import MINIO_BUCKET, get_s3_client, upload_json_to_minio

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MAX_CATEGORIES = int(os.getenv("SCRAPER_MAX_CATEGORIES", "0"))
MAX_PAGES_PER_CAT = int(os.getenv("SCRAPER_MAX_PAGES_PER_CAT", "0"))
MAX_WORKERS = int(os.getenv("SCRAPER_MAX_WORKERS", "8"))
REVIEWS_MAX_PAGES = int(os.getenv("SCRAPER_REVIEWS_MAX_PAGES", "2"))

class MetricCollector:
    """Thu thập và tính toán các chỉ số Network I/O, Latency, Throughput."""
    def __init__(self):
        self.total_bytes_downloaded = 0
        self.total_requests = 0
        self.total_latency_sec = 0.0

    def record(self, response_size_bytes, latency_sec):
        self.total_bytes_downloaded += response_size_bytes
        self.total_requests += 1
        self.total_latency_sec += latency_sec


class FullTikiLakehouseScraper:
    def __init__(self, metrics: MetricCollector):
        self.metrics = metrics
        self.s3_client = get_s3_client()
        self.bucket_name = MINIO_BUCKET
        self.session = self._create_resilient_session()

    def _create_resilient_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.8,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=30, pool_maxsize=40)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _instrumented_get(self, url, headers=None, timeout=10):
        """Wrapper tự động đo latency và bandwidth theo từng HTTP request."""
        t0 = time.time()
        res = self.session.get(url, headers=headers, timeout=timeout)
        latency = time.time() - t0
        payload_size = len(res.content) if res.content else 0
        self.metrics.record(payload_size, latency)
        return res

    def get_main_categories(self):
        url = "https://tiki.vn"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://tiki.vn/"
        }
        try:
            res = self._instrumented_get(url, headers=headers, timeout=12)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, "html.parser")
                categories = []
                links = soup.find_all("a", href=True)
                seen_ids = set()

                for link in links:
                    href = link["href"]
                    if "/c" in href:
                        full_url = href if href.startswith("https://") else f"https://tiki.vn{href}"
                        match = re.search(r"c(\d+)", href)
                        name = link.get_text(strip=True)
                        if match and name:
                            cat_id = int(match.group(1))
                            if cat_id not in seen_ids:
                                seen_ids.add(cat_id)
                                categories.append({
                                    "category_id": cat_id,
                                    "category_name": name,
                                    "url": full_url
                                })
                if categories:
                    logging.info(f"✅ Thu thập danh mục HTML: {len(categories)} categories.")
                    return categories
        except Exception as e:
            logging.error(f"Lỗi khi bóc tách categories: {e}")

        return [
            {"category_id": 1789, "category_name": "Điện Thoại - Máy Tính Bảng", "url": "https://tiki.vn/dien-thoai-may-tinh-bang/c1789"},
            {"category_id": 1846, "category_name": "Laptop - Máy Vi Tính - Linh kiện", "url": "https://tiki.vn/laptop-may-vi-tinh-linh-kien/c1846"},
            {"category_id": 8322, "category_name": "Nhà Sách Tiki", "url": "https://tiki.vn/nha-sach-tiki/c8322"},
            {"category_id": 1883, "category_name": "Nhà Cửa - Đời Sống", "url": "https://tiki.vn/nha-cua-doi-song/c1883"},
            {"category_id": 1815, "category_name": "Thiết Bị Số - Phụ Kiện Số", "url": "https://tiki.vn/thiet-bi-kts-phu-kien-so/c1815"}
        ]

    def get_all_products_by_category(self, category_id, category_name, max_pages=MAX_PAGES_PER_CAT):
        all_products = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://tiki.vn/",
            "X-Requested-With": "XMLHttpRequest"
        }

        page = 1
        while True:
            if max_pages > 0 and page > max_pages:
                break
            url = f"https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&page={page}&category={category_id}"
            try:
                res = self._instrumented_get(url, headers=headers, timeout=10)
                if res.status_code != 200:
                    break
                data = res.json().get("data", [])
                if not data:
                    break
                for p in data:
                    p_id = p.get("id")
                    if p_id:
                        all_products.append({
                            "product_id": p_id,
                            "name": p.get("name"),
                            "price": p.get("price"),
                            "original_price": p.get("original_price") or p.get("price"),
                            "discount": p.get("discount", 0),
                            "thumbnail_url": p.get("thumbnail_url"),
                            "category_id": category_id,
                            "category_name": category_name,
                            "brand": p.get("brand", {}).get("name", "Không có thương hiệu") if isinstance(p.get("brand"), dict) else "Không có thương hiệu",
                            "rating_average": p.get("rating_average", 0.0),
                            "review_count": p.get("review_count", 0),
                            "short_description": p.get("short_description", ""),
                            "inventory_status": p.get("inventory_status", "available")
                        })
                time.sleep(0.2)
                page += 1
            except Exception:
                break
        return all_products

    def get_product_detail(self, product_id, base_item=None):
        url = f"https://tiki.vn/api/v2/products/{product_id}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": "https://tiki.vn/"
        }
        try:
            res = self._instrumented_get(url, headers=headers, timeout=8)
            if res.status_code == 200:
                data = res.json()
                brand_info = data.get("brand", {})
                brand_name = brand_info.get("name") if isinstance(brand_info, dict) else "Không có thương hiệu"
                return {
                    "id": data.get("id"),
                    "name": data.get("name"),
                    "price": data.get("price"),
                    "original_price": data.get("original_price") or data.get("price"),
                    "discount": data.get("discount", 0),
                    "rating_average": data.get("rating_average", 0.0),
                    "review_count": data.get("review_count", 0),
                    "brand": brand_name,
                    "short_description": data.get("short_description", ""),
                    "thumbnail_url": data.get("thumbnail_url", ""),
                    "inventory_status": data.get("inventory_status", "available")
                }
        except Exception:
            pass

        if base_item:
            return {
                "id": product_id,
                "name": base_item.get("name"),
                "price": base_item.get("price"),
                "original_price": base_item.get("original_price"),
                "discount": base_item.get("discount", 0),
                "rating_average": base_item.get("rating_average", 0.0),
                "review_count": base_item.get("review_count", 0),
                "brand": base_item.get("brand", "Không có thương hiệu"),
                "short_description": base_item.get("short_description", ""),
                "thumbnail_url": base_item.get("thumbnail_url", ""),
                "inventory_status": base_item.get("inventory_status", "available")
            }
        return None

    def get_product_reviews(self, product_id, limit=20, max_pages=REVIEWS_MAX_PAGES):
        all_reviews = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": "https://tiki.vn/"
        }
        for page in range(1, max_pages + 1):
            url = f"https://tiki.vn/api/v2/reviews?product_id={product_id}&limit={limit}&page={page}&include=comments,contribute_info,attribute_vote_summary"
            try:
                res = self._instrumented_get(url, headers=headers, timeout=8)
                if res.status_code == 200:
                    data = res.json().get("data", [])
                    if not data:
                        break
                    for r in data:
                        all_reviews.append({
                            "id": r.get("id"),
                            "product_id": product_id,
                            "rating": r.get("rating", 5.0),
                            "title": r.get("title", ""),
                            "content": r.get("content", ""),
                            "thank_count": r.get("thank_count", 0),
                            "comment_count": r.get("comment_count", 0),
                            "created_at": r.get("created_at") or int(time.time())
                        })
                    if page >= res.json().get("paging", {}).get("last_page", 1):
                        break
                else:
                    break
                time.sleep(0.1)
            except Exception:
                break
        return all_reviews

    def process_single_item(self, base_item):
        p_id = base_item["product_id"]
        detail = self.get_product_detail(p_id, base_item)
        reviews = self.get_product_reviews(p_id)
        time.sleep(0.05)
        return detail, reviews


def run_full_pipeline_ingestion():
    start_time = time.time()
    crawl_date = datetime.now().strftime("%Y-%m-%d")
    metrics = MetricCollector()
    scraper = FullTikiLakehouseScraper(metrics)

    logging.info(f"🚀 [START PROFILING PIPELINE] Crawl Date: {crawl_date} | Workers: {MAX_WORKERS}")

    # Step 1: Categories
    categories = scraper.get_main_categories()
    upload_json_to_minio(categories, f"bronze/categories/crawl_date={crawl_date}/categories.json")
    selected_categories = categories[:MAX_CATEGORIES] if MAX_CATEGORIES > 0 else categories

    # Step 2: Listings
    all_listing_products = []
    for cat in selected_categories:
        prods = scraper.get_all_products_by_category(cat["category_id"], cat["category_name"])
        all_listing_products.extend(prods)

    unique_candidates = list({p["product_id"]: p for p in all_listing_products}.values())
    logging.info(f"📦 Tổng SKU cần trích xuất chi tiết & review: {len(unique_candidates)}")

    # Step 3: Concurrent Detail + Reviews Extraction
    all_final_products = []
    all_final_reviews = []
    processed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(scraper.process_single_item, item) for item in unique_candidates]
        for future in as_completed(futures):
            try:
                detail, revs = future.result()
                if detail:
                    all_final_products.append(detail)
                if revs:
                    all_final_reviews.extend(revs)

                processed_count += 1
                if processed_count % 100 == 0 or processed_count == len(unique_candidates):
                    elapsed = time.time() - start_time
                    inst_rate = processed_count / elapsed if elapsed > 0 else 0
                    logging.info(f"   ↳ Progress: {processed_count}/{len(unique_candidates)} ({inst_rate:.1f} SKU/s) | Reviews: {len(all_final_reviews):,}")
            except Exception as e:
                logging.error(f"Worker error: {e}")

    # Step 4: Upload to MinIO Bronze
    upload_json_to_minio(all_final_products, f"bronze/products/crawl_date={crawl_date}/products.json")
    upload_json_to_minio(all_final_reviews, f"bronze/reviews/crawl_date={crawl_date}/reviews.json")

    total_time = time.time() - start_time
    total_mb = metrics.total_bytes_downloaded / (1024 * 1024)
    avg_latency = (metrics.total_latency_sec / metrics.total_requests * 1000) if metrics.total_requests > 0 else 0
    throughput_records = (len(all_final_products) + len(all_final_reviews)) / total_time if total_time > 0 else 0
    bandwidth_mbps = (total_mb * 8) / total_time if total_time > 0 else 0

    # -------------------------------------------------------------
    # BÁO CÁO ĐO ĐẠC HIỆU NĂNG (DÙNG ĐƯA VÀO CV/PORTFOLIO)
    # -------------------------------------------------------------
    print("\n" + "=" * 65)
    print(" 📊 DATA INGESTION & PIPELINE PERFORMANCE BENCHMARK REPORT ")
    print("=" * 65)
    print(f" • Tổng thời gian thực thi (Wall Time):      {total_time:.2f} seconds ({total_time/60:.2f} mins)")
    print(f" • Tổng số HTTP Requests thực hiện:        {metrics.total_requests:,} calls")
    print(f" • Tổng lượng dữ liệu tải về (Network I/O): {total_mb:.2f} MB")
    print(f" • Độ trễ mạng trung bình (Avg Latency):    {avg_latency:.2f} ms/request")
    print(f" • Tốc độ băng thông mạng tiêu thụ:         {bandwidth_mbps:.2f} Mbps ({total_mb/total_time:.2f} MB/s)")
    print(f" • Tốc độ trích xuất sản phẩm (Throughput): {len(all_final_products)/total_time:.2f} SKU/sec")
    print(f" • Tổng Throughput dữ liệu (Overall Rate):  {throughput_records:.2f} records/sec")
    print(f" • Tổng dữ liệu Bronze thu nạp:             {len(all_final_products):,} Products | {len(all_final_reviews):,} Reviews")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    run_full_pipeline_ingestion()