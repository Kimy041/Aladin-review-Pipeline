import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import re

client = MongoClient("mongodb://localhost:27017/")
db = client["bookdb"]
reviews_col = db["reviews"]
books_col = db["bestsellers"]

headers = {"User-Agent": "Mozilla/5.0"}

def crawl_comment_reviews(book):
    page = 1
    book_id = book["book_id"]
    title = book["title"]
    
    url = "https://www.aladin.co.kr/ucl/shop/product/ajax/GetCommunityListAjax.aspx"
    while True:
        params = {
            "ProductItemId": book_id,
            "itemId": book_id,
            "pageCount": 10,
            "communitytype": "CommentReview",
            "nemoType": -1,
            "page": page,
            "startNumber": (page - 1) * 10 + 1,
            "endNumber": page * 10,
            "sort": 2,
            "IsOrderer": 1,
            "BranchType": 1,
            "IsAjax": "true",
            "pageType": 0
        }
        r = requests.get(url, params=params, headers=headers)
        bs = BeautifulSoup(r.text, "html.parser")
        reviews = bs.select("div.hundred_list")
        
        if not reviews:
            print(f"[종료] 리뷰 끝 - {title} (총 {page - 1} page)")
            break

        for r in reviews:
            try:
                # 별점
                star_imgs = r.select("div.HL_star img")
                rating = None
                if star_imgs:
                    rating = sum(1 for img in star_imgs if "icon_star_on.png" in img["src"])

                # 리뷰 내용
                content_tag = r.select_one("span[id^=spnPaper]:not([id^=spnPaperSpoiler])")
                content = content_tag.get_text(strip=True) if content_tag else ""

                # 작성자 / 날짜 / 공감 수
                left = r.select("div.left span")
                user = r.select_one("div.left a").get_text(strip=True)
                date = left[0].get_text(strip=True) if len(left) > 0 else None
                likes = 0
                if len(left) > 1:
                    like_text = left[1].get_text(strip=True)
                    like_match = re.search(r"\((\d+)\)", like_text)
                    if like_match:
                        likes = int(like_match.group(1))

                review_doc = {
                    "book_id": book_id,
                    "title": title,
                    "review_type": "CommentReview",
                    "user": user,
                    "date": date,
                    "content": content,
                    "likes": likes,
                    "rating": rating
                }

                # 기존 저장된 리뷰 조회
                existing = reviews_col.find_one({"book_id": book_id, "user": user})

                if (
                    not existing or
                    existing.get("content") != content or
                    existing.get("likes") != likes or
                    existing.get("rating") != rating
                ):
                    reviews_col.update_one(
                        {
                            "book_id": book_id,
                            "user": user
                        },
                        {"$set": review_doc},
                        upsert=True
                    )
                    print(f"[저장/업데이트] {title} - {user}")

            except Exception as e:
                print(f"[오류] 리뷰 파싱 실패: {e}")

        page += 1
        time.sleep(1)


books = books_col.find()

for book in books:
    try:
        crawl_comment_reviews(book)
    except Exception as e:
        print(f"[오류] {book['title']} 리뷰 크롤링 중 문제 발생: {e}")