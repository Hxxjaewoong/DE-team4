import json
import boto3
import pandas as pd
import io
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta

# AWS 클라이언트 설정
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

# 환경 변수
BUCKET_NAME = "hmg5th-4-bucket"
RAW_HTML_PREFIX = "raw_html/bobae/"
PROCESSED_DATA_PREFIX = "raw_data/bobae/"
LOGGING_LAMBDA_ARN = "arn:aws:lambda:ap-northeast-2:473551908409:function:crawling_log_lambda"

def extract_content(html, url, keyword):
    """HTML에서 본문 데이터를 추출"""
    try:
        soup = BeautifulSoup(html, 'html.parser')

        # 🔹 제목
        title_element = soup.select_one("dt strong")
        title = title_element.get_text(strip=True)
        title = re.sub(r"\[.*?\]", "", title).strip()
        title = title if title_element else "제목 없음"

        # 🔹 작성자
        author_element = soup.select_one("a.nickName")
        author = author_element.get_text(strip=True) if author_element else "알 수 없음"

        # 🔹 조회수, 추천수, 작성시간 가져오기
        count_group_tag = soup.select_one("span.countGroup")
        if count_group_tag:
            count_group_text = count_group_tag.get_text(strip=True)
            match = re.search(r"조회([\d,]+)\|추천([\d,]+)\|", count_group_text)
            views = match.group(1).replace(",", "") if match else "0"
            likes = match.group(2).replace(",", "") if match else "0"

            cleaned_date_text = re.sub(r"조회[\d,]+\|추천[\d,]+\|", "", count_group_text).strip()
            cleaned_date_text = re.sub(r"\(.*?\)", "", cleaned_date_text).strip()
            cleaned_date_text = re.sub(r"\s+", " ", cleaned_date_text)

            try:
                post_time = datetime.strptime(cleaned_date_text, "%Y.%m.%d %H:%M")
                post_time = post_time.strftime("%Y-%m-%d %H:%M:00")
            except ValueError:
                log_error("extract_content", url, f"날짜 변환 실패: {post_time}")
                print(f"❌ 날짜 변환 실패: {post_time}")
                return None
        else:
            log_error("extract_content", url, "날짜 정보를 찾을 수 없음")
            return None

        # 🔹 본문 내용
        content_tag = soup.select_one("div.bodyCont")
        content = " ".join(content_tag.stripped_strings) if content_tag else ""
        
        # 본문이 있는데 파싱 실패한 경우 로그 전송
        if content_tag and not content:
            log_error("extract_content", url, "본문이 존재하지만 파싱 실패")

        return {
            "site": "bobae",
            "datetime": post_time,
            "model": keyword,
            "title": title,
            "content": content,
            "url": url,
            "author": author,
            "likes": likes,
            "hates": "0",  # 보배드림은 싫어요 없음
            "comments_count": len(extract_comments(soup, url, title)),
            "views": views
        }
    except Exception as e:
        log_error("extract_content", url, str(e))
        return None

def extract_comments(soup, url, title):
    """HTML에서 댓글 데이터를 추출"""
    comments = []
    try:
        comment_tags = soup.select("div.commentlistbox dd[id^='small_cmt_']")
        for comment in comment_tags:
            comment_text = " ".join(comment.stripped_strings)

            if comment_text == "":
                log_error("extract_comments", url, "댓글이 존재하지만 파싱 실패")
            else:
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
        "source": "bobae_parse",
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

        # 본문 데이터 추출
        content = extract_content(html, url, keyword)
        if content:
            content_data.append(content)

            # 댓글 데이터 추출 후 추가
            comments = extract_comments(BeautifulSoup(html, "html.parser"), url, content["title"])
            comment_data.extend(comments)

    content_file_key = None
    comment_file_key = None

    # 본문 데이터를 parquet로 저장
    if content_data:
        content_df = pd.DataFrame(content_data)
        content_buffer = io.BytesIO()
        content_df.to_parquet(content_buffer, index=False)

        content_file_key = f"{PROCESSED_DATA_PREFIX}{today_date}-content.parquet"
        s3.put_object(Bucket=BUCKET_NAME, Key=content_file_key, Body=content_buffer.getvalue(), ContentType="application/octet-stream")
        print(f"✅ 본문 데이터 저장 완료: {content_file_key}")

    # 댓글 데이터를 parquet로 저장
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
