import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import argparse

# MongoDB 연결
client = MongoClient("mongodb://localhost:27017/")
db = client["bookdb"]
collection = db["bestsellers"]

# 인덱스 설정: 중복 방지용 (book_id 유니크)
collection.create_index("book_id", unique=True)

# airflow에서 argument 받음
# now = datetime.strptime("2025-06-30", "%Y-%m-%d")
parser = argparse.ArgumentParser()
parser.add_argument("--year", type=str, required=True)
parser.add_argument("--month", type=str, required=True)
parser.add_argument("--week", type=str, required=True)
args = parser.parse_args()

year = args.year
month = args.month
week_number = args.week


# 크롤링 대상 URL
url = f"https://www.aladin.co.kr/shop/common/wbest.aspx?BestType=Bestseller&BranchType=1&CID=0&Year={year}&Month={month}&Week={week_number}&page=1&cnt=1000&SortOrder=1"
headers = {"User-Agent": "Mozilla/5.0"}
r = requests.get(url, headers=headers)

bs = BeautifulSoup(r.text, "html.parser")
books = bs.select("div.ss_book_box")


for rank, book in enumerate(books, start=1):
    try:
        book_id = book["itemid"]
        title_tag = book.select_one("a.bo3")
        title = title_tag.get_text(strip=True)
        link = title_tag["href"]

        a_tags = book.find_all("a")
        author = "N/A"
        isbn = "N/A"
        for a in a_tags:
            href = a.get("href", "")
            if "AuthorSearch" in href:
                author = a.get_text(strip=True)
            if "CommentReview" in href:
                isbn = href.split("#")[1].split("_")[0]
        
        rating_tag = book.select_one("span.star_score")
        rating = float(rating_tag.get_text(strip=True)) if rating_tag else None

        # 현재 주차 정보
        weekly_info = {
            "year": year,
            "month": month,
            "week_number": week_number,
            "rank": rank
        }

        # 신규 도서면 삽입
        collection.update_one(
            {"book_id": book_id},
            {
                "$setOnInsert": {
                    "book_id": book_id,
                    "weekly_ranks": []  # 빈 배열로 시작
                }
            },
            upsert=True
        )

        # 도서 정보 최신화
        collection.update_one(
            {"book_id": book_id},
            {
                "$set": {
                    "title": title,
                    "author": author,
                    "link": link,
                    "rating": rating,
                    "isbn": isbn
                }
            }
        )

        # 주차 정보 추가 (중복 없이)
        collection.update_one(
            {"book_id": book_id},
            {
                "$pull": {
                    "weekly_ranks": {
                        "year": year,
                        "month": month,
                        "week_number": week_number
                    }
                }
            }
        )

        collection.update_one(
            {"book_id": book_id},
            {
                "$push": {
                    "weekly_ranks": {
                        "year": year,
                        "month": month,
                        "week_number": week_number,
                        "rank": rank
                    }
                }
            }
        )

        print(f"[저장됨] {rank}위 - {title}")

    except Exception as e:
        print("MongoDB 저장 중 오류:", e)