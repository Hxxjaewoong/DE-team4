import json
import boto3
import pandas as pd
import io
import requests
import os
from datetime import datetime, timedelta

# AWS 클라이언트 설정
s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")

# 환경 변수
BUCKET_NAME = os.getenv("BUCKET_NAME", "hmg5th-4-bucket")
RAW_DATA_PREFIX = "raw_data/"
MERGED_CONTENT_PREFIX = "merge_data/contents/"
MERGED_COMMENT_PREFIX = "merge_data/comments/"
LOGGING_LAMBDA_ARN = os.getenv("LOGGING_LAMBDA_ARN", "arn:aws:lambda:ap-northeast-2:473551908409:function:crawling_log_lambda")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")  # 환경 변수에서 Slack Webhook URL 가져오기

def list_s3_files(prefix):
    """주어진 prefix로 S3에서 파일 목록 가져오기"""
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]
    except Exception as e:
        log_error("list_s3_files", f"S3 파일 목록 조회 실패: {str(e)}")
        return []

def load_csv_from_s3(file_key):
    """S3에서 CSV 파일을 불러와 DataFrame으로 변환"""
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        return pd.read_csv(io.BytesIO(response["Body"].read()), encoding="utf-8-sig")
    except Exception as e:
        log_error("load_csv_from_s3", f"S3 CSV 로드 실패 ({file_key}): {str(e)}")
        return None

def upload_csv_to_s3(df, file_key):
    """병합된 DataFrame을 S3에 CSV로 저장"""
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
        s3.put_object(Bucket=BUCKET_NAME, Key=file_key, Body=csv_buffer.getvalue().encode("utf-8-sig"), ContentType="text/csv; charset=utf-8")
        print(f"✅ 병합 데이터 저장 완료: {file_key}")
    except Exception as e:
        log_error("upload_csv_to_s3", f"병합 데이터 S3 업로드 실패 ({file_key}): {str(e)}")


def upload_parquet_to_s3(df, file_key):
    """병합된 DataFrame을 S3에 Parquet 파일로 저장"""
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        s3.put_object(Bucket=BUCKET_NAME, Key=file_key, Body=buffer.getvalue(), ContentType="application/octet-stream")
        print(f"✅ 병합 데이터 저장 완료: {file_key}")
    except Exception as e:
        log_error("upload_parquet_to_s3", f"병합 데이터 S3 업로드 실패 ({file_key}): {str(e)}")



def merge_files(file_keys):
    """S3에서 파일을 불러와 병합"""
    dataframes = [load_csv_from_s3(file_key) for file_key in file_keys if load_csv_from_s3(file_key) is not None]
    
    if not dataframes:
        print("⚠️ 병합할 데이터 없음")
        return None

    return pd.concat(dataframes, ignore_index=True)

def log_error(stage, error_message):
    """Lambda로 에러 로깅 전송"""
    log_payload = {
        "status": "error",
        "source": "merge_lambda",
        "stage": stage,
        "error": error_message,
        "timestamp": datetime.utcnow().isoformat()
    }
    try:
        lambda_client.invoke(
            FunctionName=LOGGING_LAMBDA_ARN,
            InvocationType="Event",
            Payload=json.dumps(log_payload)
        )
        print(f"📌 오류 기록: {log_payload}")
    except Exception as e:
        print(f"❌ 로그 람다 호출 실패: {str(e)}")


def lambda_handler(event, context):
    """S3에서 오늘 날짜의 본문 & 댓글 데이터를 가져와 병합 후 저장"""
    
    today_date = datetime.utcnow().strftime("%Y-%m-%d")
    #today_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    # 1️⃣ 오늘 날짜의 content.csv & comment.csv 파일 목록 가져오기
    content_files = list_s3_files(f"{RAW_DATA_PREFIX}fmkorea/{today_date}-content.csv") + \
                    list_s3_files(f"{RAW_DATA_PREFIX}dcinside/{today_date}-content.csv") + \
                    list_s3_files(f"{RAW_DATA_PREFIX}clien/{today_date}-content.csv") + \
                    list_s3_files(f"{RAW_DATA_PREFIX}bobae/{today_date}-content.csv")

    comment_files = list_s3_files(f"{RAW_DATA_PREFIX}fmkorea/{today_date}-comment.csv") + \
                    list_s3_files(f"{RAW_DATA_PREFIX}dcinside/{today_date}-comment.csv") + \
                    list_s3_files(f"{RAW_DATA_PREFIX}clien/{today_date}-comment.csv") + \
                    list_s3_files(f"{RAW_DATA_PREFIX}bobae/{today_date}-comment.csv")

    # 2️⃣ 본문 데이터 병합
    merged_content_df = merge_files(content_files)
    if merged_content_df is not None:
        merged_content_key = f"{MERGED_CONTENT_PREFIX}{today_date}.parquet"
        upload_parquet_to_s3(merged_content_df, merged_content_key)
    else:
        log_error("lambda_handler", "본문 데이터 없음: content_files 비어 있음")


    # 3️⃣ 댓글 데이터 병합
    merged_comment_df = merge_files(comment_files)
    if merged_comment_df is not None:
        merged_comment_key = f"{MERGED_COMMENT_PREFIX}{today_date}.parquet"
        upload_parquet_to_s3(merged_comment_df, merged_comment_key)
    else:
        log_error("lambda_handler", "댓글 데이터 없음: comment_files 비어 있음")


    # 4️⃣ 병합할 데이터가 없으면 Step Function에 예외 전달 및 Slack 알림
    if merged_content_df is None and merged_comment_df is None:
        error_message = "병합할 데이터가 없어 Step Function에서 병합 실패로 처리."
        log_error("lambda_handler", error_message)
        raise Exception("NoDataToMergeException: 병합할 데이터 없음.")

    return {
        "status": "Merge process completed",
        "merged_content_file": merged_content_key if merged_content_key else "No content data",
        "merged_comment_file": merged_comment_key if merged_comment_key else "No comment data"
    }