from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import subprocess
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

def check_and_run():
    client = MongoClient("mongodb://client:27017/")
    db = client["bookdb"]
    collection = db["weekly_table"]

    today_dt = datetime.today()
    target = collection.find_one({
        "end_date": (today_dt - timedelta(days=2)).strftime("%Y-%m-%d")
    })

    if target:
        print(f"[실행] {target['year']}년 {target['month']}월 {target['week_number']}주차")
        # 주차 정보를 인자로 전달
        subprocess.run([
            "python",
            "/home/hadoop/workspace/aladin_best.py",
            "--year", str(target["year"]),
            "--month", str(target["month"]),
            "--week", str(target["week_number"])
        ])
    else:
        print("[스킵] 오늘은 실행 대상이 아님")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz)
}

with DAG(
    dag_id="weekly_aladin_bestseller",
    default_args=default_args,
    description="간단 구조: 조건 확인 + 크롤링 실행",
    schedule_interval="0 2 * * *",  # 매일 2시
    catchup=False,
    tags=["aladin", "bestseller", "weekly"]
) as dag:

    run_if_needed = PythonOperator(
        task_id="check_and_crawl",
        python_callable=check_and_run
    )
