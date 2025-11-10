import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()  # .env 파일 로드

CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

headers = {
    'X-Naver-Client-Id': CLIENT_ID,
    'X-Naver-Client-Secret': CLIENT_SECRET,
    'Content-Type': 'application/json'
}

payload = {
    "startDate": "2025-06-01",
    "endDate": "2025-07-26",
    "timeUnit": "week",
    "keywordGroups": [
        {
            "groupName": "소년이 온다",
            "keywords": ["소년이 온다"]
        }
    ]
}

res = requests.post(
    "https://openapi.naver.com/v1/datalab/search",
    headers=headers,
    data=json.dumps(payload)
)

print(res.status_code)
print(res.json())