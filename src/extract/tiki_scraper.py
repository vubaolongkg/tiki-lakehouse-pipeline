import re
import time
import requests
from bs4 import BeautifulSoup
from datetime import date
from minio_client import get_s3_client, upload_json_to_minio

class TikiIngestion:
    def __init__(self, s3_endpoint=None):
        self.s3_client = get_s3_client(endpoint_url=s3_endpoint)
        self.crawl_date = date.today().isoformat()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": "https://tiki.vn/"
        }

    # 1. Thu thập danh mục chính
    def fetch_categories(self):
        print("\n--- [1/4] Fetching Categories ---")
        url = "https://tiki.vn"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code != 200:
                print("Failed to fetch Tiki homepage.")
                return []
        except Exception as e:
            print(f"Error connecting to Tiki: {e}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        categories = []
        seen = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/c" in href and href.startswith("https://tiki.vn/") and href not in seen:
                seen.add(href)
                name = link.get_text(strip=True)
                match = re.search(r"c(\d+)", href)
                if match:
                    categories.append({
                        "category_id": int(match.group(1)),
                        "category_name": name,
                        "url": href
                    })

        s3_key = f"bronze/categories/crawl_date={self.crawl_date}/categories.json"
        upload_json_to_minio(categories, s3_key, self.s3_client)
        print(f"Collected {len(categories)} categories.")
        return categories

    # 2. Thu thập danh sách sản phẩm theo danh mục (Listings API)
    def fetch_products_by_category(self, category_id, category_name, max_pages=2):
        print(f"\n--- [2/4] Fetching Listings for: {category_name} (ID: {category_id}) ---")
        products = []
        
        for page in range(1, max_pages + 1):
            url = f"https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&page={page}&category={category_id}"
            try:
                res = requests.get(url, headers=self.headers, timeout=10)
                if res.status_code != 200:
                    break
                data = res.json().get("data", [])
                if not data:
                    break

                for p in data:
                    products.append({
                        "product_id": p.get("id"),
                        "name": p.get("name"),
                        "price": p.get("price"),
                        "category_id": category_id,
                        "category_name": category_name,
                        "brand": p.get("brand_name", "Không có thương hiệu"),
                        "rating": p.get("rating_average", 0),
                        "review_count": p.get("review_count", 0),
                        "thumbnail_url": p.get("thumbnail_url"),
                    })
                time.sleep(0.15)
            except Exception as e:
                print(f"Error fetching page {page}: {e}")
                break

        if products:
            s3_key = f"bronze/product_listings/crawl_date={self.crawl_date}/cat_{category_id}.json"
            upload_json_to_minio(products, s3_key, self.s3_client)
            print(f"Collected {len(products)} products for category {category_id}.")
        return products

    # 3. Thu thập chi tiết từng sản phẩm
    def fetch_product_detail(self, product_id):
        url = f"https://tiki.vn/api/v2/products/{product_id}"
        try:
            res = requests.get(url, headers=self.headers, timeout=10)
            if res.status_code == 200:
                data = res.json()
                return {
                    "id": data.get("id"),
                    "name": data.get("name"),
                    "price": data.get("price"),
                    "original_price": data.get("original_price"),
                    "discount": data.get("discount"),
                    "rating_average": data.get("rating_average"),
                    "review_count": data.get("review_count"),
                    "short_description": data.get("short_description"),
                    "description": data.get("description"),
                    "brand": data.get("brand", {}).get("name", "Không có thương hiệu") if isinstance(data.get("brand"), dict) else "Không có thương hiệu",
                    "inventory_status": data.get("inventory_status", "Không rõ"),
                    "thumbnail_url": data.get("thumbnail_url"),
                    "specifications": data.get("specifications", [])
                }
        except Exception as e:
            print(f"Error fetching product {product_id}: {e}")
        return None

    # 4. Thu thập reviews của sản phẩm
    def fetch_product_reviews(self, product_id, max_pages=1):
        all_reviews = []
        for page in range(1, max_pages + 1):
            url = f"https://tiki.vn/api/v2/reviews?product_id={product_id}&limit=20&page={page}"
            try:
                res = requests.get(url, headers=self.headers, timeout=10)
                if res.status_code == 200:
                    data = res.json()
                    reviews = data.get("data", [])
                    if not reviews:
                        break
                    for r in reviews:
                        all_reviews.append({
                            "id": r.get("id"),
                            "product_id": product_id,
                            "rating": r.get("rating"),
                            "title": r.get("title", ""),
                            "content": r.get("content", ""),
                            "thank_count": r.get("thank_count", 0),
                            "comment_count": r.get("comment_count", 0),
                            "created_at": r.get("created_at")
                        })
                    time.sleep(0.08)
                else:
                    break
            except Exception:
                break
        return all_reviews


def run_sample_ingestion():
    ingestion = TikiIngestion()    
    # 1. Fetch toàn bộ danh mục từ Homepage
    cats = ingestion.fetch_categories()
    
    # Lấy top 10 danh mục đa dạng
    sample_cats = cats[:10] if len(cats) >= 10 else cats
    if not sample_cats:
        sample_cats = [
            {"category_id": 316, "category_name": "Sách tiếng Việt"},
            {"category_id": 1789, "category_name": "Điện Thoại - Máy Tính Bảng"},
            {"category_id": 1815, "category_name": "Thiết Bị Số"},
            {"category_id": 1883, "category_name": "Nhà Cửa - Đời Sống"}
        ]
    
    all_details = []
    all_reviews = []
    seen_pids = set()
    
    for cat in sample_cats:
        # Lấy 2 trang (tối đa 80 sản phẩm / category)
        prods = ingestion.fetch_products_by_category(cat["category_id"], cat["category_name"], max_pages=2)
        
        # Cào chi tiết 25 sản phẩm mỗi danh mục (tổng ~250 sản phẩm)
        for p in prods[:25]:
            pid = p["product_id"]
            if not pid or pid in seen_pids:
                continue
            seen_pids.add(pid)
            
            # Fetch Product Detail
            detail = ingestion.fetch_product_detail(pid)
            if detail:
                all_details.append(detail)
            
            # Fetch Reviews nếu có
            if p.get("review_count", 0) > 0:
                revs = ingestion.fetch_product_reviews(pid, max_pages=1)
                all_reviews.extend(revs)
            time.sleep(0.08)
            
    # Upload chi tiết sản phẩm và reviews vào Bronze
    if all_details:
        upload_json_to_minio(all_details, f"bronze/products/crawl_date={ingestion.crawl_date}/products.json", ingestion.s3_client)
    if all_reviews:
        upload_json_to_minio(all_reviews, f"bronze/reviews/crawl_date={ingestion.crawl_date}/reviews.json", ingestion.s3_client)
        
    print(f"\n✅ Bronze Ingestion hoàn thành: {len(sample_cats)} danh mục, {len(all_details)} sản phẩm, {len(all_reviews)} reviews!")

if __name__ == "__main__":
    run_sample_ingestion()