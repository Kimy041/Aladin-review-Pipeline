from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import calendar
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

def generate_weekly_table():
    client = MongoClient("mongodb://client:27017/")
    db = client["bookdb"]
    collection = db["weekly_table"]
    collection.drop()  # 기존 데이터 제거 (선택적)

    # 매년 자동으로 현재 연도 기준 1월 1일부터 마지막 날까지 설정
    start_date = datetime(datetime.today().year, 1, 1)
    last_day = calendar.monthrange(start_date.year, 12)[1]
    end_date = datetime(start_date.year, 12, last_day)

    current_date = start_date

    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        first_day_of_month = datetime(year, month, 1)
        last_day_of_month = datetime(year, month, calendar.monthrange(year, month)[1])

        # 1주차: 월의 첫 날부터 그 주의 일요일까지
        day = first_day_of_month
        weekday = day.weekday()  # 월:0 ~ 일:6
        days_until_saturday = (5 - weekday) % 7
        week_end = min(day + timedelta(days=days_until_saturday), last_day_of_month)

        week_number = 1

        while day <= last_day_of_month:
            # 저장
            doc = {
                "year": year,
                "month": month,
                "week_number": week_number,
                "start_date": day.strftime("%Y-%m-%d"),
                "end_date": week_end.strftime("%Y-%m-%d")
            }
            collection.insert_one(doc)

            # 다음 주 계산
            day = week_end + timedelta(days=1)
            week_end = min(day + timedelta(days=6), last_day_of_month)
            week_number += 1

        # 다음 달로
        current_date = datetime(year, month, 28) + timedelta(days=4)  # 다음 달 아무 날짜
        current_date = datetime(current_date.year, current_date.month, 1)  # 다음 달 1일로 설정

# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
}

with DAG(
    dag_id="generate_weekly_table",
    default_args=default_args,
    schedule_interval="@yearly",
    catchup=False,
    tags=["reference", "weekly"],
    description="연 1회 기준 주차 테이블을 MongoDB에 저장"
) as dag:

    generate_table = PythonOperator(
        task_id="generate_weekly_table",
        python_callable=generate_weekly_table
    )
