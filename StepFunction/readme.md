# Step Function Flow Documentation
### Overview
이 Step Function은 HTML 수집, 파싱, 데이터 병합 후 EMR 클러스터를 생성하여 Spark Job을 실행하고, 결과를 Redshift로 로드하는 전체 파이프라인을 구현합니다. 오류 발생 시 각 단계마다 로그를 남겨 문제를 추적할 수 있도록 구성되어 있습니다.


* **워크플로우 요약:**

1. **GetKeywords**
    - S3에서 `config/keywords.json` 파일을 가져와 JSON으로 파싱한 후, 이후 단계에서 사용할 키워드 데이터를 저장합니다.
2. **ExecuteCrawlers (Parallel)**
    - 네 개의 분기(Branch)로 동시에 실행됩니다.
    - 각 분기에서는 해당 Lambda 함수(fmkorea_extract, bobae_extract, clien_extract, dcinside_extract)를 호출하여 키워드를 기반으로 HTML 데이터를 수집합니다.
    - 수집 후, 각각 파싱 Lambda 함수(fmkorea_parse, bobae_parse, clien_parse, dcinside_parse)를 호출합니다.
    - 각 단계에서 오류가 발생하면, 대응하는 로깅 Lambda(crawling_log_lambda)를 호출하여 에러를 기록하고 분기를 종료합니다.
3. **MergeResults**
    - 여러 크롤러에서 얻은 결과를 하나로 병합하기 위해 `merge_lambda`를 호출합니다.
    - 분기 중 일부에서 오류가 발생하면 병합 단계에서는 해당 데이터를 제외하고 병합하게 됩니다. 병합할 오늘자 데이터가 전혀 없는 경우는 에러를 로깅한 후 병합을 종료합니다.
    - 병합 결과의 상태가 "Merge process completed"이면 다음 단계로 진행하고, 그렇지 않으면 에러를 로깅하고 종료합니다.
4. **EMR 클러스터 생성 및 Spark Job 실행**
    - **LaunchEMRCluster:** EMR 클러스터를 생성합니다.
    - **WaitForClusterReady:** 클러스터가 `WAITING` 상태가 될 때까지 대기합니다.
    - **SubmitSparkJob:** Spark 작업을 클러스터에 제출합니다.
    - **WaitForSparkCompletion & CheckSparkJobStatus:** Spark 작업의 완료 여부를 주기적으로 확인합니다.
    - **EvaluateSparkJobStatus:**
        - Spark 작업이 `COMPLETED` 상태이면, 클러스터 종료(`TerminateEMRClusterForLoad`) 후 Redshift 데이터 적재로 진행합니다.
        - 작업이 `FAILED` 또는 `CANCELLED` 상태이면, 에러를 로깅한 후 클러스터를 종료합니다.
5. **Redshift 데이터 적재**
    - **RedshiftLoad:** 클러스터 종료 후, `redshift_load` Lambda 함수를 호출하여 Redshift로 데이터를 적재합니다.
    - 오류 발생 시, 별도의 에러 로깅 단계를 거칩니다.
