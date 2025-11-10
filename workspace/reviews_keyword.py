from pymongo import MongoClient
from datetime import datetime
from collections import Counter
import re
import argparse

# # 불용어 목록
# stopwords = {'그리고', '그러나', '정말', '너무', '이런', '저런', '그런', '하면', '해서', '하다', '했다', '같다', '있는', '없는', ''}

# # 키워드 추출 함수
# def extract_keywords(text):
#     words = re.findall(r'[가-힣]{2,}', text)  # 2글자 이상 한글 단어만
#     return [w for w in words if w not in stopwords]

# 감정 키워드 리스트
emotion_keywords = {
    # 긍정 감정
    "감동", "힐링", "가족", "공감", "따뜻", "웃음", "눈물", "행복", "사랑", "위로", "기쁨", "즐거움", "유쾌",
    
    # 부정 감정
    "지루", "답답", "복잡", "현실", "무거움", "혼란", "슬픔", "불편", "불쾌", "화남", "짜증", "절망", "충격", "실망"
}

# 감정 키워드 추출 함수
def extract_emotions(text):
    words = re.findall(r'[가-힣]{2,}', text)
    return [w for w in words if w in emotion_keywords]

# MongoDB 연결
client = MongoClient("mongodb://client:27017/")
db = client["bookdb"]

reviews_col = db["reviews"]
weeks_col = db["weekly_table"]
result_col = db["weekly_keywords"]

# # 주차별 리뷰
# parser = argparse.ArgumentParser()
# parser.add_argument("--start_date", type=str, required=True)
# parser.add_argument("--end_date", type=str, required=True)
# args = parser.parse_args()

# start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
# end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

# query = {
#     "date": {
#         "$gte": start_date,
#         "$lt": end_date
#     }
# }

# reviews = list(reviews_col.find(query, {"content": 1, "date": 1}))

# 리뷰 전체 조회
reviews = list(reviews_col.find({}, {"content": 1, "date": 1}))

# 주차 테이블 조회
weeks = list(weeks_col.find({}))

# 주차별 키워드 그룹핑
weekly_keywords = {}

# 오늘 날짜를 루프 시작 전에 한 번만 가져옵니다.
today = datetime.now()

for review in reviews:
    if "date" not in review or "content" not in review:
        continue

    date_str = review["date"]
    review_date = datetime.strptime(date_str, "%Y-%m-%d")

    # 주차 매핑
    week_info = next((
        week for week in weeks
        if datetime.strptime(week["start_date"], "%Y-%m-%d") <= review_date <= datetime.strptime(week["end_date"], "%Y-%m-%d")
    ), None)

    if not week_info:
        continue

    # 리뷰가 속한 주의 종료일(end_date)이 오늘 날짜보다 미래인 경우, 분석에서 제외
    end_date_of_week = datetime.strptime(week_info["end_date"], "%Y-%m-%d")
    if end_date_of_week.date() >= today.date():
        continue # 다음 리뷰로 넘어갑니다.

    key = (week_info["year"], week_info["month"], week_info["week_number"])

    if key not in weekly_keywords:
        weekly_keywords[key] = []

    weekly_keywords[key].extend(extract_emotions(review["content"]))

# 결과 저장
for (year, month, week_number), keywords in weekly_keywords.items():
    # 이미 해당 주차 데이터가 있는지 확인
    exists = result_col.find_one({
        "year": year,
        "month": month,
        "week_number": week_number
    })

    # 데이터가 존재하지 않을 경우에만 새로 저장
    if not exists:
        counter = Counter(keywords)
        top_keywords = [{"word": word, "count": count} for word, count in counter.most_common(20)]

        doc = {
            "year": year,
            "month": month,
            "week_number": week_number,
            "keywords": top_keywords
        }
        result_col.insert_one(doc)

print("✅ 주차별 키워드 분석 결과 저장 완료")