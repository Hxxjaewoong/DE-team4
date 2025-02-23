import json
import boto3
import pandas as pd
import io
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# AWS 클라이언트 설정
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

# 환경 변수
BUCKET_NAME = "hmg5th-4-bucket"
RAW_HTML_PREFIX = "raw_html/clien/"
PROCESSED_DATA_PREFIX = "raw_data/clien/"
LOGGING_LAMBDA_ARN = "arn:aws:lambda:ap-northeast-2:473551908409:function:crawling_log_lambda"

def extract_content(html, url, keyword):
    """HTML에서 본문 데이터를 추출"""
    try:
        soup = BeautifulSoup(html, 'html.parser')

        # 날짜
        date_element = soup.select_one("span.view_count.date")
        post_date = date_element.get_text(strip=True) if date_element else ""
        if "수정일" in post_date:
            post_date = post_date.split("수정일")[0].strip()
        post_date = datetime.strptime(post_date, "%Y-%m-%d %H:%M:%S")


        # 제목
        title_element = soup.select_one("h3.post_subject span:nth-of-type(2)")
        title = title_element.get_text(strip=True) if title_element else "제목 없음"

        # 본문 내용
        content_element = soup.select_one("div.post_article")
        content = content_element.get_text(strip=True) if content_element else ""

        # 작성자
        author_element = soup.select_one("span.nickname span")
        author = author_element.get_text(strip=True) if author_element else "알 수 없음"

        # 조회수, 추천수
        views_element = soup.select_one("span.view_count strong")
        views = views_element.get_text(strip=True).replace(",", "") if views_element else "0"

        likes_element = soup.select_one("a.symph_count strong")
        likes = likes_element.get_text(strip=True).replace(",", "") if likes_element else "0"

        hates = "0"  # Clien은 싫어요 없음

        # 본문이 있는데 파싱 실패한 경우 로그 전송
        if content_element and not content:
            error_msg = "본문이 존재하지만 파싱 실패"
            log_error("extract_content", url, error_msg)
            print(f"❌ {error_msg}")

        return {
            "site": "clien",
            "datetime": post_date if post_date else "N/A",
            "model": keyword,
            "title": title if title else "N/A",
            "content": content if content else "N/A",
            "url": url,
            "author": author if author else "N/A",
            "likes": likes if likes else "0",
            "hates": hates,
            "comments_count": len(extract_comments(soup, url, title)),
            "views": views if views else "0"
        }
    except Exception as e:
        log_error("extract_content", url, str(e))
        return None  # 🔴 실패 시 None 반환

def extract_comments(soup, url, title):
    """HTML에서 댓글 데이터를 추출"""
    comments = []
    failed_parsing = False

    try:
        comment_elements = soup.find_all("div", class_="comment_view")

        if not comment_elements:
            print(f"ℹ️ 댓글 없음: {url}")
            return []

        for comment in comment_elements:
            comment_text = comment.get_text(strip=True)
            if not comment_text:
                failed_parsing = True  # ❗ 파싱 실패 플래그 설정
            else:
                comments.append({
                    "url": url,
                    "title": title,
                    "comment": comment_text
                })
        # ❗ 파싱 실패한 댓글이 하나라도 있으면 한 번만 로그 전송
        if failed_parsing:
            error_msg = "댓글이 존재하지만 일부 파싱 실패"
            log_error("extract_comments", url, error_msg)
            print(f"❌ {error_msg}")
    except Exception as e:
        log_error("extract_comments", url, str(e))

    return comments

def log_error(stage, url, error_message):
    """Lambda로 에러 로깅 전송"""
    log_payload = {
        "status": "error",
        "source": "clien_parse",
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
    """S3에서 HTML 파일을 불러와 파싱 후 CSV로 저장"""

    today_date = datetime.utcnow().strftime('%Y-%m-%d')
    #today_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    s3_key = f"{RAW_HTML_PREFIX}{today_date}.json"

    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        html_data = json.loads(response['Body'].read().decode('utf-8-sig'))
    except s3.exceptions.NoSuchKey:
        error_msg = f"파일을 찾을 수 없음: {s3_key}"
        print(f"❌ {error_msg}")
        log_error("load_html", s3_key, error_msg)
        return {"status": "NoSuchKey", "key": s3_key}
    except json.JSONDecodeError as e:
        error_msg = f"JSON 디코딩 실패: {str(e)}"
        print(f"❌ {error_msg}")
        log_error("load_html", s3_key, error_msg) 
        return {"status": "JSONDecodeError", "error": str(e)}
    except Exception as e:
        error_msg = f"S3에서 HTML 데이터 불러오기 실패: {str(e)}"
        print(f"❌ {error_msg}")
        log_error("load_html", s3_key, error_msg)  
        return {"status": "Failed to load HTML data", "error": str(e)}

    content_data = []
    comment_data = []

    for url, data in html_data.items():
        keyword = data["keyword"]
        html = data["html"]

        # 본문 데이터 추출 (None이 아니면 추가)
        content = extract_content(html, url, keyword)
        if content:
            content_data.append(content)

            # 댓글 데이터 추출 후 추가
            comments = extract_comments(BeautifulSoup(html, "html.parser"), url, content["title"])
            comment_data.extend(comments)

    content_file_key = None
    comment_file_key = None

    # 본문 데이터를 CSV로 저장
    if content_data:
        content_df = pd.DataFrame(content_data)
        content_buffer = io.BytesIO()
        content_df.to_parquet(content_buffer, index=False)

        content_file_key = f"{PROCESSED_DATA_PREFIX}{today_date}-content.parquet"
        s3.put_object(Bucket=BUCKET_NAME, Key=content_file_key, Body=content_buffer.getvalue(), ContentType="application/octet-stream")

        print(f"✅ 본문 데이터 저장 완료: {content_file_key}")

    # 댓글 데이터를 CSV로 저장
    if comment_data:
        comment_df = pd.DataFrame(comment_data)
        comment_buffer = io.BytesIO()
        comment_df.to_parquet(comment_buffer, index=False)

        comment_file_key = f"{PROCESSED_DATA_PREFIX}{today_date}-comment.parquet"
        s3.put_object(Bucket=BUCKET_NAME, Key=comment_file_key, Body=comment_buffer.getvalue(), ContentType="application/octet-stream")
        print(f"✅ 댓글 데이터 저장 완료: {comment_file_key}")
    else:
        print(f"ℹ️ 저장할 댓글 데이터 없음: {today_date}")

    return {
        "status": "Processing completed",
        "content_file": content_file_key if content_file_key else "No content data",
        "comment_file": comment_file_key if comment_file_key else "No comment data"
    }
