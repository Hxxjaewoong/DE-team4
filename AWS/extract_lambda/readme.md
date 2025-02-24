# 🚀 Extract Lambda Functions

이 문서는 **DCInside, BobaeDream, Clien, FMKorea** 사이트에서 크롤링한 HTML 데이터를 수집하는 **Extract Lambda Functions**에 대한 설명을 포함합니다.  
각 Lambda는 **Step Function에서 전달받은 키워드 목록을 기반으로 크롤링을 수행하고, HTML 데이터를 S3에 저장**하는 역할을 수행합니다.

---

## **🛠️ 1. 프로젝트 개요**
📀 **Step Function Workflow**
1. **Step Function에서 키워드 목록을 전달받음**  
2. **Extract Lambda 실행 → 해당 키워드로 크롤링 수행**  
3. **HTML 데이터를 JSON 형식으로 변환 후 S3 저장**  
4. **성공 또는 실패 로그를 기록 후 Step Function에 결과 전달**  

---

## **📂 2. Lambda 함수 개요**
### ✅ **DCInside Extract Lambda**
- **Lambda 이름:** `dcinside_extract`  

### ✅ **Clien Extract Lambda**
- **Lambda 이름:** `clien_extract`   

### ✅ **BobaeDream Extract Lambda**
- **Lambda 이름:** `bobae_extract`  

### ✅ **FMkorea Extract Lambda**
- **Lambda 이름:** `fmkorea_extract`  

---

## **📀 3. 입출력 데이터 형식**

- **입력 데이터:** Step Function에서 전달받은 키워드 목록. 예: ['palisade', 'tucson', 'ioniq9', 'avante']
- **출력 데이터:** `raw_html/{site}/yyyy-mm-dd.json` (크롤링한 HTML 데이터)  

Extract Lambda는 HTML 페이지를 JSON 파일로 저장하며, JSON 파일의 데이터 구조는 다음과 같습니다.

```json
{
  "게시글URL_1": {
    "keyword": "검색 키워드",
    "html": "<html>...</html>"
  },
  "게시글URL_2": {
    "keyword": "검색 키워드",
    "html": "<html>...</html>"
  }
}
```
--- 

## **4. Lambda Workflow**

1. 사이트 별 검색 기능 활용하여 해당 키워드를 검색하고, 기간 내의 게시글의 url, 페이지 넘버를 수집합니다.

2. 수집한 url을 방문해 크롤링하고자 하는 정보가 담겨있는 html Body 태그를 json 형태로 저장합니다.

3. 1, 2번 과정을 4개의 multiprocessing Manager dictionary를 통해 4개의 함수를 비동기적으로 실행해 간접적인 멀티프로세싱을 구현하였습니다.

4. 오류 발생 시 재시도하고, 계속 오류가 발생하는 경우 log_error를 통해 Step function에 오류를 전달하여 재시작합니다.

## **5. Lambda 구현 특이사항**

- 동적 크롤링 구현을 위해 Playwright라는 E2E 프레임워크를 사용했습니다.
- AWS Lambda는 컨테이너 환경에서 구현되어 멀티프로세싱을 지원하지 않음. 크롤링은 Network IO Bound 작업이므로 대기 시간 동안 컴퓨팅 자원을 낭비할 수 있습니다.
- Lambda는 multiprocessing Queue를 지원하지 않아 찾은 대안 3가지
    - Pipe
        - AWS Lambda에서 공식적으로 지원하는 방법
        - 프로세스가 처리하는 데이터 사이즈가 커지면 버퍼링이 걸리는 현상 발생
    - Async 구현
        - async Playwright 패키지를 이용하여 코루틴을 통한 비동기 처리 구현
        - 가장 속도 측면에서도 빠르고 멀티프로세싱을 지원하지 않는 Lambda 환경에 적합
        - 코드 가독성이 떨어지고, Browser Context가 Destroy되는 현상이 있어 안정성이 떨어짐
    - Multiprocessing Manager + Process를 통한 구현
        - Manager를 이용한 Dict, List를 이용해 공유 딕셔너리, 공유 리스트 통해 간접적으로 비동기 구현
        - 셋 중 가장 안정성이 좋았고, 코드 가독성이 높은 편이어서 여러 사이트에 재사용해야 하는 개발환경에서 적합했다.
### Multiprocessing Manager + Process를 채택하여 처리율을 높여 Lambda의 실행 시간을 감축했습니다.