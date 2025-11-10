from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import count, avg, sum, to_date

spark = SparkSession.builder \
    .appName("AladinReview") \
    .master("spark://namenode:7077") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.instances", "3") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.mongodb.read.connection.uri", "mongodb://client:27017") \
    .config("spark.mongodb.read.database", "bookdb") \
    .config("spark.mongodb.read.collection", "reviews") \
    .getOrCreate()

schema = StructType([
    StructField("_id", StructType([
        StructField("oid", StringType(), True)
    ]), True),
    StructField("book_id", StringType(), True),
    StructField("content", StringType(), True),
    StructField("date", StringType(), True),
    StructField("likes", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("review_type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("user", StringType(), True)
])

reviews_df = spark.read.format("mongo") \
    .option("uri", "mongodb://client:27017/") \
    .option("database", "bookdb") \
    .option("collection", "reviews") \
    .schema(schema) \
    .load() \
    .drop("_id")

# 날짜 컬럼 변환
reviews_df = reviews_df.withColumn("review_date", to_date("date"))

# 리뷰 수 Top 도서
top_review_counts = reviews_df.groupBy("book_id", "title") \
    .agg(count("*").alias("review_count")) \
    .orderBy("review_count", ascending=False)

# 평균 평점 + 좋아요 수 많은 도서
popular_books = reviews_df.groupBy("book_id", "title") \
    .agg(
        count("*").alias("review_count"),
        avg("rating").alias("avg_rating"),
        sum("likes").alias("total_likes")
    ) \
    .orderBy("avg_rating", ascending=False)

# 특정 도서 트렌드 (필요 시 title 필터 추가)
trend = reviews_df.groupBy("title", "review_date") \
    .agg(count("*").alias("daily_reviews")) \
    .orderBy("title", "review_date")

# 좋아요 수 많은 리뷰
top_reviews = reviews_df.select("title", "user", "likes", "rating", "content") \
    .orderBy("likes", ascending=False)

# HDFS 저장
top_review_counts.write.mode("overwrite") \
    .parquet("hdfs://namenode:8020/book_analysis/top_review_counts")

popular_books.write.mode("overwrite") \
    .parquet("hdfs://namenode:8020/book_analysis/popular_books")

trend.write.mode("overwrite") \
    .parquet("hdfs://namenode:8020/book_analysis/review_trend")

top_reviews.write.mode("overwrite") \
    .parquet("hdfs://namenode:8020/book_analysis/top_reviews")

spark.stop()

