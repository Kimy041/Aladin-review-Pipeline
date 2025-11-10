from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2025, 7, 9, tzinfo=local_tz)
}

with DAG(
    dag_id="daily_review_and_analysis",
    default_args=default_args,
    description="매일 리뷰 크롤링 후 분석까지 실행",
    schedule_interval="0 3 * * *",  # 매일 새벽 3시
    catchup=False,
    tags=["aladin", "review", "spark", "daily"]
) as dag:

    crawl_reviews = BashOperator(
        task_id="crawl_reviews",
        bash_command="python /home/hadoop/workspace/aladin_best_review.py"
    )

    analyze_reviews = BashOperator(
        task_id="analyze_reviews",
        bash_command="python /home/hadoop/workspace/aladin_analysis.py"
    )

    crawl_reviews >> analyze_reviews
