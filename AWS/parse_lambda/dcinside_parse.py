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
RAW_HTML_PREFIX = "raw_html/dcinside/"
PROCESSED_DATA_PREFIX = "raw_data/dcinside/"
LOGGING_LAMBDA_ARN = "arn:aws:lambda:ap-northeast-2:473551908409:function:crawling_log_lambda"

def extract_content(html, url, keyword):
    """DCInside 게시글 본문 데이터 추출"""
    try:
        soup = BeautifulSoup(html, 'html.parser')

        # 제목
        title_element = soup.select_one("span.title_subject")
        title = title_element.get_text(strip=True) if title_element else "제목 없음"

        # 본문 내용
        content_element = soup.select_one("div.write_div")
        content = content_element.get_text(separator="\n", strip=True) if content_element else ""

        # 작성자
        author_element = soup.select_one("div.gall_writer span.nickname")
        author = author_element.get_text(strip=True) if author_element else "알 수 없음"

        # 날짜
        date_element = soup.select_one("span.gall_date")
        post_date = date_element["title"].strip() if date_element and date_element.has_attr("title") else ""
        if post_date:
            post_date = datetime.strptime(post_date, "%Y-%m-%d %H:%M:%S")
        else:
            post_date = "N/A"

        # 조회수 - "조회 64" 와 같이 나온다면 "조회"를 제거하고 숫자만 추출
        views_element = soup.select_one("span.gall_count")
        views = (views_element.get_text(strip=True)
                 .replace("조회", "").strip().replace(",", "")
                 if views_element else "0")
        # 추천수
        likes_element = soup.select_one("p.up_num")
        likes = likes_element.get_text(strip=True).replace(",", "") if likes_element else "0"

        # 비추천수
        hates_element = soup.select_one("p.down_num")
        hates = hates_element.get_text(strip=True).replace(",", "") if hates_element else "0"

        # 본문이 있는데 파싱 실패한 경우 로그 전송
        if content_element and not content:
            log_error("extract_content", url, "본문이 존재하지만 파싱 실패")
            print(f"❌ 본문 파싱 실패: {url}")

        return {
            "site": "dcinside",
            "datetime": post_date,
            "model": keyword,
            "title": title,
            "content": content,
            "url": url,
            "author": author,
            "likes": likes,
            "hates": hates,
            "comments_count": len(extract_comments(soup, url, title)),
            "views": views
        }
    except Exception as e:
        log_error("extract_content", url, str(e))
        return None  # 🔴 실패 시 None 반환

def extract_comments(soup, url, title):
    """DCInside 댓글 데이터 추출"""
    comments = []
    error_logged = False  # 🔹 로그 전송 여부 체크
    try:
        comment_elements = soup.select("ul.cmt_list li.ub-content")

        if not comment_elements:
            print(f"ℹ️ 댓글 없음: {url}")
            return []

        for comment in comment_elements:
            comment_text_element = comment.select_one("p.usertxt.ub-word")
            comment_text = comment_text_element.get_text(strip=True) if comment_text_element else ""

            if not comment_text:
                if not error_logged:  # 🔹 한 번만 에러 로깅
                    log_error("extract_comments", url, "댓글이 존재하지만 파싱 실패")
                    print(f"❌ 댓글 파싱 실패: {url}")
                    error_logged = True
                continue  # 🔹 해당 댓글만 건너뛰고 다음 댓글 처리

            comments.append({
                "url": url,
                "title": title,
                "comment": comment_text
            })
    except Exception as e:
        log_error("extract_comments", url, str(e))

    return comments

def log_error(stage, url, error_message):
    """Lambda로 에러 로깅 전송"""
    log_payload = {
        "status": "error",
        "source": "dcinside_parse",
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
        print(f"❌ 파일을 찾을 수 없음: {s3_key}")
        log_error("load_html", s3_key, "파일을 찾을 수 없음")
        return {"status": "NoSuchKey", "key": s3_key}
    except json.JSONDecodeError as e:
        print(f"❌ JSON 디코딩 실패: {str(e)}")
        log_error("load_html", s3_key, str(e))
        return {"status": "JSONDecodeError", "error": str(e)}
    except Exception as e:
        print(f"❌ S3에서 HTML 데이터 불러오기 실패: {str(e)}")
        log_error("load_html", s3_key, str(e))
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
