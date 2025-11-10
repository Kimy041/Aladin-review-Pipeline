from pymongo import MongoClient
from opensearchpy import OpenSearch, helpers

# 1. MongoDB 연결
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["bookdb"]   # DB 이름
reviews_col = db["reviews"]   # 컬렉션 이름

# 2. OpenSearch 연결
os_client = OpenSearch(
    hosts=[{"host": "client", "port": 9200}],
    http_compress=True
)

# 3. MongoDB -> OpenSearch 변환 함수
def get_actions():
    for doc in reviews_col.find({}):
        yield {
            "_index": "reviews",
            "_id": str(doc["_id"]),  # OpenSearch 문서 ID로 사용
            "_source": {
                "mongo_id": str(doc["_id"]),  # MongoDB ID 보관용
                "user": doc.get("user"),
                "book_id": doc.get("book_id"),
                "content": doc.get("content"),
                "date": doc.get("date"),
                "likes": doc.get("likes"),
                "rating": doc.get("rating"),
                "review_type": doc.get("review_type"),
                "title": doc.get("title")
            }
        }

# 4. Bulk API로 적재
helpers.bulk(os_client, get_actions())
print("MongoDB → OpenSearch 데이터 적재 완료!")