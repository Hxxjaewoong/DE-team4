# 📌 Data Pipeline Overview

<img width="1073" alt="Image" src="https://github.com/user-attachments/assets/8724207e-803d-4d31-af13-ccbcfdebd615" />

본 문서는 **데이터 크롤링, 처리, 적재 및 분석**을 자동화하는 **데이터 파이프라인**의 구조를 설명합니다.  해당 파이프라인은 **AWS Step Functions**, **Lambda**, **EMR(Spark)**, **Redshift**, **S3**, **Tableau** 등을 활용하여 운영됩니다.

---
## 🔧 사용 기술
- **AWS Step function**: 데이터 플로우 전반 실행 및 매니지먼트
- **AWS Event Bridge**: 시간 배치에 따라 step function 실행
- **AWS Lambda**: 웹사이트 별 html 크롤링 및 파싱, 병합, 레드시프트 적재, 슬랙 알람 등
- **AWS S3**: 원본 및 가공 데이터 저장
- **AWS Redshift**:  분석 데이터 저장
- **Tableau**: 데이터 시각화 및 대시보드 제공
- **Slack API**: 중요 이벤트 알림(엔지니어 - 파이프라인 모니터링/ 사용자 - 화제된 글 알림)

---

## ⏰ 1. EventBridge 스케줄러
- **EventBridge**가 매일 **8, 12, 16, 20시 (KST)**에 Step Function을 트리거합니다.
- Step Function 실행 시 크롤링부터 데이터 적재 및 분석이 자동으로 수행됩니다.

---

## 🔄 2. Step Function
### **1️⃣ 키워드 로드**
- S3의 `keywords.json` 파일을 읽어 **검색할 키워드 리스트**를 생성합니다.
- 생성된 리스트를 각 크롤링 Lambda로 전달합니다.

### **2️⃣ Extract Lambda 실행 (병렬)**
- 4개의 커뮤니티 **(DCInside, Clien, FMKorea, BobaeDream)**에서 HTML 데이터를 크롤링합니다.
- 수집된 HTML **body**를 S3 버킷 (`raw_html/{site}/yyyy-mm-dd.json`)에 저장합니다.

### **3️⃣ Parse Lambda 실행 (순차)**
- 저장된 HTML을 불러와 **본문/댓글 데이터**를 파싱합니다.
- S3 (`raw_data/{site}/yyyy-mm-dd-content.parquet`, `raw_data/{site}/yyyy-mm-dd-comment.parquet`)에 저장합니다.

### **4️⃣ Merge Lambda 실행**
- 모든 키워드, 모든 커뮤니티의 **본문과 댓글 데이터를 병합**합니다.
- 병합된 데이터는 다시 S3 (`merge_data/contents/yyyy-mm-dd.parquet`, `merge_data/comments/yyyy-mm-dd.parquet`)에 저장됩니다.

### **5️⃣ EMR 클러스터 생성 및 Spark Job 실행**
- **Spark Job**을 실행하여 데이터 변환 및 화제도 분석을 수행합니다.
- Spark 작업 완료 후 **EMR 클러스터는 종료**됩니다.

### **6️⃣ Redshift 적재**
- 병합된 데이터를 **Redshift 테이블**에 적재하는 Lambda (`Redshift_load`)를 실행합니다.
- **오늘 적재된 데이터는 "Live Data"**로 유지됩니다.
- **마지막 배치 실행 시, 업데이트가 불필요한 데이터는 "Dead Data" 테이블로 이동**됩니다.

### **7️⃣ 오류 감지 및 Slack 알림**
- 실행 중 오류 발생 시 **`crawling_log_lambda`가 Slack으로 엔지니어에게 알림**을 전송합니다.

---

## ⚡ 3. Slack 알림
### **엔지니어 - 이슈 발생 시** (`crawling_log_lambda`)
- Step Function 내 각 단계에서 **오류 발생 시 Slack 알림**을 전송합니다.
- 엔지니어는 Slack을 통해 즉시 오류를 확인하고 대응할 수 있습니다.

### **사용자 - 화제도 기준 초과 시** (`slack_alarm_lambda`)
- Spark Job 실행 후, 화제도가 특정 기준을 초과하는 게시글을 **S3의 `alarm/` 디렉터리에 저장**합니다.
- S3 파일 업로드 이벤트가 발생하면 **Slack 알림이 트리거되어 사용자에게 게시글 목록을 전송**합니다.

---

## 📊 4. Tableau 대시보드 연동
- **Redshift 및 S3 데이터를 기반으로 Tableau에서 시각화**를 수행합니다.
- **Dead Data / Live Data**를 분석하여 **트렌드 및 키워드 분석**을 지원합니다.

