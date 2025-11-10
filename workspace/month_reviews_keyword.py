from pymongo import MongoClient
from collections import Counter
from datetime import datetime
import re

# MongoDB 연결
client = MongoClient("mongodb://client:27017/")
db = client["bookdb"]
reviews_collection = db["reviews"]
monthly_keywords_collection = db["monthly_keywords"]

# 불용어 목록
stopwords = {'그리고', '그러나', '정말', '너무', '이런', '저런', '그런', '하면', '해서', '작가님', '나는', '있다', '책을', '책은', '읽고', '책이', '내가',
             '하다', '했다', '같다', '있는', '없는', '같은', '더', '또한', '이렇게', '저렇게', '이것', '저것', ''}

# 키워드 추출 함수
def extract_keywords(text):
    words = re.findall(r'[가-힣]{2,}', text)
    return [w for w in words if w not in stopwords]

# # 감정 키워드 리스트
# emotion_keywords = {
#     # 긍정 감정
#     "감동", "힐링", "가족", "공감", "따뜻", "웃음", "눈물", "행복", "사랑", "위로", "기쁨", "즐거움", "유쾌",
    
#     # 부정 감정
#     "지루", "답답", "복잡", "현실", "무거움", "혼란", "슬픔", "불편", "불쾌", "화남", "짜증", "절망", "충격", "실망"
# }

# # 감정 키워드 추출 함수
# def extract_emotions(text):
#     words = re.findall(r'[가-힣]{2,}', text)
#     return [w for w in words if w in emotion_keywords]

# 리뷰 전체 불러오기
reviews = reviews_collection.find({})

# 월별 키워드 누적
monthly_keywords = {}

today = datetime.today()
current_month = today.month

for review in reviews:
    try:
        date = datetime.strptime(review["date"], "%Y-%m-%d")
    except:
        continue

    if date.month == current_month:
        continue  # 7월은 제외

    if "content" not in review or not review["content"]:
        continue  # 내용 없는 리뷰는 스킵

    words = extract_keywords(review["content"])
    if not words:
        continue  # 감정 키워드 없는 리뷰 스킵

    year_month = (date.year, date.month)
    if year_month not in monthly_keywords:
        monthly_keywords[year_month] = Counter()
    monthly_keywords[year_month].update(words)

# 저장 전 기존 컬렉션 삭제
monthly_keywords_collection.drop()

# 저장
for (year, month), counter in monthly_keywords.items():
    top_keywords = [{"word": word, "count": count} for word, count in counter.most_common(20)]
    doc = {
        "year": year,
        "month": month,
        "keywords": top_keywords
    }
    monthly_keywords_collection.insert_one(doc)

print("✅ 월별 키워드 분석 결과가 MongoDB에 저장되었습니다.")
