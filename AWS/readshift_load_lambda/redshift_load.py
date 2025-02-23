import boto3
import psycopg2
import json
import os
from datetime import datetime, timedelta, time

# 환경 변수 설정 (Redshift 접속 정보)
REDSHIFT_DATABASE = os.getenv('REDSHIFT_DATABASE')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT', 5439)

def lambda_handler(event, context):
    # 현재 시간 확인 (AWS Lambda는 기본 UTC로 실행되므로 필요에 따라 시간대 변환 고려)
    now = datetime.now()
    
    # 20시(8 PM) 이전이면 아무 작업도 수행하지 않고 종료
    if now.time() < time(20, 0):
        print("현재 시간이 20시 이전입니다. 작업을 수행하지 않고 종료합니다.")
        return

    # 20시 이후 호출된 경우, 어제 자정부터 오늘 자정까지의 데이터를 로드하도록 처리
    today_midnight = datetime.combine(now.date(), time(0, 0))
    yesterday_midnight = today_midnight - timedelta(days=1)
    print(f"데이터 필터 기간: {yesterday_midnight} ~ {today_midnight}")

    s3 = boto3.client('s3')
    # S3 이벤트에서 파일 정보 추출 (여러 파일이 포함될 수 있음)
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        file_key = record['s3']['object']['key']
        s3_path = f"s3://{bucket_name}/{file_key}"
        
        # Redshift로 Parquet 데이터 중 지정된 시간 범위의 데이터만 적재
        load_filtered_data_to_redshift(s3_path, yesterday_midnight, today_midnight)

def load_filtered_data_to_redshift(s3_path, start_time, end_time):
    """
    S3의 Parquet 데이터를 Redshift의 staging 테이블(temp_staging_table)로 COPY한 후,
    'datetime' 컬럼을 기준으로 어제 자정부터 오늘 자정까지의 데이터만 실제 대상 테이블(my_table)에 추가합니다.
    
    가정:
      - Parquet 파일에는 "datetime" 컬럼이 있으며, 형식은 'yyyy-mm-dd hh:mm:ss'입니다.
    """
    conn = psycopg2.connect(
        dbname=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT
    )
    cur = conn.cursor()
    
    try:
        # 1. S3의 Parquet 데이터를 staging 테이블로 로드
        copy_sql = f"""
        COPY temp_staging_table
        FROM '{s3_path}'
        IAM_ROLE 'arn:aws:iam::your-account-id:role/your-redshift-role'
        FORMAT AS PARQUET;
        """
        cur.execute(copy_sql)
        conn.commit()
        print("Staging 테이블로 Parquet 데이터 COPY 완료.")
        
        # 2. staging 테이블에서 어제 자정부터 오늘 자정까지의 데이터만 대상 테이블에 추가
        insert_sql = f"""
        INSERT INTO my_table
        SELECT *
        FROM temp_staging_table
        WHERE datetime >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
          AND datetime < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}';
        """
        cur.execute(insert_sql)
        conn.commit()
        print("필터된 데이터 대상 테이블에 추가 완료.")
        
        # 3. (옵션) staging 테이블 정리: 다음 로드 전에 기존 데이터를 제거
        # truncate_sql = "TRUNCATE TABLE temp_staging_table;"
        # cur.execute(truncate_sql)
        # conn.commit()
        # print("Staging 테이블 초기화 완료.")
        
    except Exception as e:
        print(f"Redshift 적재 실패: {str(e)}")
    finally:
        cur.close()
        conn.close()
