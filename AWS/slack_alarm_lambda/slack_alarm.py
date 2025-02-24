import json
import boto3
import io
import pandas as pd
import requests
import os
from datetime import datetime

# 실제 Slack 인커밍 웹훅 URL (환경 변수 또는 직접 입력)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# 화제도 임계값 (필요에 따라 조정)
THRESHOLD = 15

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    s3_client = boto3.client('s3')
    
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        
        print(f"Processing file: {key} from bucket: {bucket}")
        
        # transformed_data/alarm/ 폴더에 업로드된 CSV 파일만 처리
        if not (key.startswith("transformed_data/alarm/") and key.endswith(".csv") and "_temporary" not in key):
            continue
        
        try:
            print("Fetching object from S3...")
            response = s3_client.get_object(Bucket=bucket, Key=key)
            data = response["Body"].read()
            print(f"Fetched object, size: {len(data)} bytes")
            
            buffer = io.BytesIO(data)
            
            print("Reading CSV file into DataFrame...")
            df = pd.read_csv(buffer)
            print(f"CSV read successfully, number of rows: {len(df)}")
            
            total_posts = len(df)
            
            # 현재 날짜와 시간을 리포트 헤더에 포함
            current_date = datetime.now().strftime("%Y-%m-%d")
            current_time = datetime.now().strftime("%H")
            report_header = f"💥💥{current_date} {current_time}시 화제도 리포트💥💥\n"
            report_header += f"현재 화제도 {THRESHOLD}를 넘은 게시글 {total_posts}개에 대해 보고 드립니다.\n\n"
            print("Constructed report header:")
            print(report_header)
            
            # 각 게시글에 대해 bullet list 형식으로 작성
            report_body = ""
            for idx, row in df.iterrows():
                line = f"차종: 팰리세이드 \n 화제도: {row['popularity']:.2f}\n 제목: {row['title']} \n{row['url']}\n\n"
                report_body += line
                print(f"Added row {idx}: {line.strip()}")
            
            final_message = report_header + report_body
            print("Final report message constructed:")
            print(final_message)
            
            # Slack Webhook으로 메시지 전송
            print("Sending message to Slack...")
            payload = {"text": final_message}
            slack_response = requests.post(SLACK_WEBHOOK_URL, json=payload)
            if slack_response.status_code != 200:
                print("Failed to send Slack message:", slack_response.text)
            else:
                print("Slack alert sent successfully for file:", key)
        
        except Exception as e:
            print(f"Error processing file {key}: {str(e)}")
    
    print("Lambda processing complete.")
    return {
        "statusCode": 200,
        "body": json.dumps("Processing complete")
    }
