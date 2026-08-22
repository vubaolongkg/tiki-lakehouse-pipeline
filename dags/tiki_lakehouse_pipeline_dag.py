import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

def run_command(cmd_list):
    result = subprocess.run(cmd_list, capture_output=True, text=True)
    print("=== STDOUT ===")
    print(result.stdout)
    if result.returncode != 0:
        print("=== STDERR ===")
        print(result.stderr)
        raise RuntimeError(f"Command failed with exit code {result.returncode}")

def task_scrape_bronze(**context):
    print("--- [Task 1] Scraping Tiki API to Bronze Layer ---")
    run_command(["python", "/opt/airflow/src/extract/tiki_scraper.py"])

def task_transform_silver(**context):
    ds = context["ds"]  # Lấy biến ngày chạy từ Airflow (YYYY-MM-DD)
    print(f"--- [Task 2] Transforming Bronze to Silver Delta for date: {ds} ---")
    run_command(["python", "/opt/airflow/src/transform/silver_transform.py", ds])

def task_transform_gold(**context):
    ds = context["ds"]
    print(f"--- [Task 3] Modeling Silver to Gold Marts (PostgreSQL) for date: {ds} ---")
    run_command(["python", "/opt/airflow/src/transform/gold_transform.py", ds])

with DAG(
    dag_id="tiki_lakehouse_daily_pipeline",
    default_args=default_args,
    description="Automated Daily E-Commerce Lakehouse (Bronze -> Silver -> Gold)",
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "lakehouse", "pyspark", "delta"],
) as dag:

    bronze_task = PythonOperator(
        task_id="scrape_tiki_to_bronze",
        python_callable=task_scrape_bronze,
        provide_context=True,
    )

    silver_task = PythonOperator(
        task_id="bronze_to_silver_delta",
        python_callable=task_transform_silver,
        provide_context=True,
    )

    gold_task = PythonOperator(
        task_id="silver_to_gold_postgres",
        python_callable=task_transform_gold,
        provide_context=True,
    )

    # Thứ tự phụ thuộc giữa các Task
    bronze_task >> silver_task >> gold_task