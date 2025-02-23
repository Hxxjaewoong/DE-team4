# Step Function Event Scheduler (한국 시간 기준)

이 문서는 **AWS Step Function을 매일 한국 시간 기준 08:00, 12:00, 16:00, 20:00에 자동으로 실행**하는 **EventBridge Scheduler**에 대한 설명을 제공합니다.

---

## 1️⃣ 개요

본 프로젝트에서는 **AWS EventBridge**를 사용하여 **Step Function을 한국 시간 기준 특정 시간에 트리거**하는 **이벤트 스케줄러**를 구성하였습니다.

### ✅ **배치 간격**
- 배치간격은 8시부터 4시간 간격으로 실행하여 20시 종료로, 10~16시가 근무 코어타임인 HMG의 정책에 맞춰 출퇴근 전후 또는 점심시간에 확인해 볼 수 있는 모니터링 시스템이 되도록 함.

### ✅ **목표**
- 하루 **4회(08:00, 12:00, 16:00, 20:00 KST)** `Step Function`을 자동 실행
- `AWS EventBridge`에서 **Cron 표현식**을 사용하여 정기 실행
- `AWS Lambda` 없이 **EventBridge → Step Function 직접 연결**
- **비용 절감 & 유지보수 최소화**

---

## 2️⃣ 이벤트 스케줄러 구성

### 🚀 **Amazon EventBridge Rule 설정**
- **서비스:** `Amazon EventBridge Scheduler`
- **타겟:** `AWS Step Functions State Machine`
- **스케줄:** `cron(0 8,12,16,20 * * ? *)`  
- **권한 설정:** Step Function 실행을 위한 `IAM Role` 필요

---

## 3️⃣ 배포 방법

### 🛠 **1. AWS CLI를 통한 배포**
다음 명령어를 실행하여 EventBridge Rule을 생성합니다.

```sh
aws events put-rule \
    --name "StepFunctionScheduler" \
    --schedule-expression "cron(0 23,3,7,11 * * ? *)" \
    --state ENABLED
