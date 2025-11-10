import streamlit as st
from pyspark.sql import SparkSession
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# 한글 폰트 설정 (NanumGothic 기준)
plt.rcParams['font.family'] = 'NanumGothic'
plt.rcParams['axes.unicode_minus'] = False  # 음수 깨짐 방지

# Spark 세션
spark = SparkSession.builder \
    .appName("StreamlitBook") \
    .master("spark://namenode:7077") \
    .getOrCreate()

st.set_page_config(page_title="Aladin 리뷰 분석", layout="wide")

# 사이드바 메뉴
menu = st.sidebar.selectbox("분석 항목 선택", [
    "📚 리뷰 수 Top 도서",
    "⭐ 인기 도서",
    "📈 리뷰 트렌드",
    "👍 좋아요 많은 리뷰",
    "🗝️ 월별 키워드 분석"
    # "🗝️ 주차별 키워드 분석"
])

# 데이터 로드
top_review_counts_df = spark.read.parquet("hdfs://namenode:8020/book_analysis/top_review_counts").toPandas()
popular_df = spark.read.parquet("hdfs://namenode:8020/book_analysis/popular_books").toPandas()
trend_df = spark.read.parquet("hdfs://namenode:8020/book_analysis/review_trend").toPandas()
top_reviews_df = spark.read.parquet("hdfs://namenode:8020/book_analysis/top_reviews").toPandas()

# 분석별 페이지
if menu == "📚 리뷰 수 Top 도서":
    st.header("📚 리뷰 수 Top 도서")
    search = st.text_input("도서 제목 검색")
    filtered = top_review_counts_df[top_review_counts_df["title"].str.contains(search, case=False)]
    st.dataframe(filtered)
    st.bar_chart(filtered.set_index("title")["review_count"])

elif menu == "⭐ 인기 도서":
    st.header("⭐ 평균 평점 + 좋아요 수 기준 인기 도서")
    st.dataframe(popular_df)
    st.bar_chart(popular_df.set_index("title")["avg_rating"])

elif menu == "📈 리뷰 트렌드":
    st.header("📈 도서별 일일 리뷰 수 추이")
    selected_title = st.selectbox("도서 선택", trend_df["title"].unique())
    filtered = trend_df[trend_df["title"] == selected_title]
    st.line_chart(filtered.set_index("review_date")["daily_reviews"])

elif menu == "👍 좋아요 많은 리뷰":
    st.header("👍 좋아요 수가 많은 리뷰 Top 10")
    st.dataframe(top_reviews_df.head(10))

elif menu == "🗝️ 월별 키워드 분석":
    st.header("🗝️ 월별 키워드 분석 결과")

    # MongoDB에서 데이터 가져오기
    client = MongoClient("mongodb://client:27017/")
    db = client["bookdb"]
    collection = db["monthly_keywords"]

    # 주차 리스트 (최신순 정렬)
    all_weeks = list(collection.find({}, {"_id": 0, "year": 1, "month": 1}))
    all_weeks.sort(key=lambda x: (x['year'], x['month']), reverse=True)

    week_options = [f"{w['year']}년 {w['month']}월" for w in all_weeks]
    selected_week = st.selectbox("월 선택", week_options)

    # 선택된 주차 분해
    year, month = map(int, [s.replace("년", "").replace("월", "").strip() for s in selected_week.split()])

    # 해당 주차 키워드 가져오기
    doc = collection.find_one({"year": year, "month": month})
    if doc and "keywords" in doc and doc["keywords"]:
        keyword_df = pd.DataFrame(doc["keywords"])
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.bar(keyword_df["word"], keyword_df["count"], color="#87CEFA")
        ax.set_ylabel("Count")
        ax.set_title(f"{year}년 {month}월 키워드 빈도")
        plt.xticks(rotation=45, ha="right")
        st.pyplot(fig)
    else:
        st.warning("해당 월의 키워드 데이터가 없습니다.")
