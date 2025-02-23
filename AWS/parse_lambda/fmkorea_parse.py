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
RAW_HTML_PREFIX = "raw_html/fmkorea/"
PROCESSED_DATA_PREFIX = "raw_data/fmkorea/"
LOGGING_LAMBDA_ARN = "arn:aws:lambda:ap-northeast-2:473551908409:function:crawling_log_lambda"

def extract_content(html, url, keyword):
    """HTML에서 본문 데이터를 추출"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # 날짜
        date_element = soup.select_one("span.date.m_no")
        post_date = date_element.get_text(strip=True).split("수정일")[0].strip() if date_element else ""
        post_date = datetime.strptime(post_date, "%Y.%m.%d %H:%M")

        # 제목
        title_element = soup.select_one("h1 span.np_18px_span")
        title = title_element.get_text(strip=True) if title_element else "제목 없음"

        # 본문 내용
        content_element = soup.select_one(".xe_content")
        content = content_element.get_text(strip=True) if content_element else ""

        # 작성자
        author_element = soup.select_one(".member_plate")
        author = author_element.get_text(strip=True) if author_element else "알 수 없음"

        # 조회수, 추천수, 댓글수
        stats = soup.select(".btm_area .fr span b")
        views = stats[0].get_text(strip=True) if len(stats) > 0 else "0"
        likes = stats[1].get_text(strip=True) if len(stats) > 1 else "0"
        hates = "0" # 펨코는 싫어요가 없음?

        # 댓글 데이터 추출
        comments = extract_comments(soup, url, title)
        comments_count = len(comments)

        # 본문이 있는데 파싱 실패한 경우 로그 전송
        if content_element and not content:
            log_error("extract_content", url, "본문이 존재하지만 파싱 실패")

        return {
            "site": "fmkorea",
            "datetime": post_date,
            "model": keyword,
            "title": title,
            "content": content,
            "url": url,
            "author": author,
            "likes": likes,
            "hates": hates,
            "comments_count": comments_count,
            "views": views
        }
    except Exception as e:
        log_error("extract_content", url, f"본문 추출 실패: {str(e)}")
        return None  # 🔴 실패 시 None 반환

def extract_comments(soup, url, title):
    """HTML에서 **모든 댓글**을 추출"""
    comments = []
    try:
        comments_ul = soup.select_one("ul.fdb_lst_ul")
        comments_locator = comments_ul.select("div.comment-content") if comments_ul else []

        if not comments_locator:
            print(f"⚠️ 댓글이 없는 글: {url}")
            return comments  # 댓글 없음

        for comment in comments_locator:
            text = comment.get_text(strip=True)
            if text:
                comments.append({
                    "url": url,
                    "title": title,
                    "comment": text
                })

        if not comments:
            log_error("extract_comments", url, "댓글이 존재하지만 파싱 실패")
            print(f"❌ 댓글이 있는 글이지만 파싱 실패: {url}")

    except Exception as e:
        log_error("extract_comments", url, f"댓글 파싱 오류: {str(e)}")
        print(f"❌ 댓글 파싱 오류: {e}")

    return comments

def log_error(stage, url, error_message):
    """Lambda로 에러 로깅 전송"""
    log_payload = {
        "status": "error",
        "source": "fmkorea_parse",
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

    return {
        "status": "Processing completed",
        "content_file": content_file_key if content_file_key else "No content data",
        "comment_file": comment_file_key if comment_file_key else "No comment data"
    }
