import requests
from bs4 import BeautifulSoup
import json
import boto3
import datetime
import time
import random
import re
import html

# AWS 설정
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

# 환경 변수
BUCKET_NAME = "hmg5th-4-bucket"
RAW_HTML_PREFIX = "raw_html/bobae/"
LOGGING_LAMBDA_ARN = "arn:aws:lambda:ap-northeast-2:473551908409:function:crawling_log_lambda"

# 모델 정의(키워드 확장)
MODEL = {
    "palisade": ["펠리", "팰리"],
    "tucson": ["투싼"],
    "ioniq9": ["아이오닉9", "오닉9"],
    "avante": ["아반떼", "아방"]
}

TODAY = datetime.datetime.utcnow().date()
END_TIME = datetime.datetime.combine(TODAY - datetime.timedelta(days=1), datetime.time(0, 0, 0))

# 기본 URL 및 요청 헤더
BASE_URL = "https://www.bobaedream.co.kr/search"
HEADERS = {
    "User-Agent": random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    ]),
    "Referer": BASE_URL,
    "Content-Type": "application/x-www-form-urlencoded"
}

# 요청 재시도 함수
def request_with_retries(url, method="GET", data=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=HEADERS, data=data, timeout=10) if method == "POST" else requests.get(url, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                return response
            print(f"⚠️ 요청 실패 {attempt + 1}/{max_retries} (상태 코드: {response.status_code})")
        except requests.RequestException as e:
            print(f"⚠️ 요청 오류 {attempt + 1}/{max_retries}: {str(e)}")
        time.sleep(2)
    log_error("request_with_retries", url, "최대 재시도 횟수 초과")
    return None

# 에러 로그 전송 함수
def log_error(stage, url, error_message):
    log_payload = {
        "status": "error",
        "source": "bobae_extract",
        "stage": stage,
        "url": url,
        "error": error_message
    }
    lambda_client.invoke(
        FunctionName=LOGGING_LAMBDA_ARN,
        InvocationType="Event",
        Payload=json.dumps(log_payload)
    )

def lambda_handler(event, context):
    html_data = {}
    print(f"📆 크롤링 기간: {END_TIME} ~ {TODAY}")
    
    # Step Function에서 전달된 키워드 가져오기
    keywords = event.get("keywords", [])
    if not keywords:
        log_error("lambda_handler", url, "키워드 리스트가 입력되지 않음")
        print("❌ 키워드가 제공되지 않았습니다.")
        return {"status": "No Keywords Provided"}
    
    for keyword in keywords:
        print(f"📡 '{keyword}' 키워드 크롤링 시작...")
        related_keywords = MODEL[keyword]
      
        
        for sub_keyword in related_keywords:
            print(f"📡 sub: '{sub_keyword}' 키워드 크롤링 시작...")
            page = 1
            stop_flag = False

            while not stop_flag:
                print(f"📄 페이지 {page} 크롤링 중...")
                payload = {"keyword": sub_keyword, "searchField": "ALL", "colle": "community", "page": page}
                response = request_with_retries(BASE_URL, method="POST", data=payload)
                if not response:
                    break

                # 🔹 응답 인코딩 변환
                response.encoding = response.apparent_encoding
                soup = BeautifulSoup(response.text, "html.parser")
                posts = soup.select("div.search_Community ul li dt a")
                if not posts:
                    print("✅ 더 이상 게시글이 없습니다.")
                    break

                for post in posts:
                    post_url = "https://www.bobaedream.co.kr" + post["href"]
                    post_response = request_with_retries(post_url)
                    if not post_response:
                        continue

                    post_response.encoding = post_response.apparent_encoding
                    post_soup = BeautifulSoup(post_response.text, "html.parser")
                    count_group_tag = post_soup.select_one("span.countGroup")
                    count_group_text = count_group_tag.get_text(strip=True)
                    
                    date_match = re.search(r"(\d{4}\.\d{2}\.\d{2})\s*\(.*?\)\s*(\d{2}:\d{2})", count_group_text)
                    if date_match:
                        post_date = date_match.group(1).replace('.', '-')
                        post_time = date_match.group(2)
                        post_datetime = datetime.datetime.strptime(f"{post_date} {post_time}:00", "%Y-%m-%d %H:%M:%S")
                    else:
                        print(f"❌ 날짜 변환 실패: {count_group_text}")
                        continue
                    
                    if post_datetime < END_TIME:
                        print("✅ 크롤링 기간 초과. 종료.")
                        stop_flag = True
                        break
                    
                    content_tag = post_soup.select_one("div.viewbg02")
                    if not content_tag:
                        log_error("lambda_handler", post_url, "viewbg02 태그 없음")
                        continue
                    
                    content_text = html.unescape(content_tag.prettify())
                    html_data[post_url] = {"keyword": keyword, "html": content_text}
                
                if not stop_flag:
                    page += 1
                    time.sleep(1)
          
    
    if html_data:
        today_str = TODAY.strftime("%Y-%m-%d")
        file_key = f"{RAW_HTML_PREFIX}{today_str}.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(html_data, ensure_ascii=False, indent=4).encode("utf-8-sig"),
            ContentType="application/json; charset=utf-8"
        )
        print(f"✅ 크롤링 완료! 데이터 저장됨: {file_key}")
    else:
        print("⚠️ 저장할 데이터가 없습니다.")

    return {"status": "Crawling completed"}
