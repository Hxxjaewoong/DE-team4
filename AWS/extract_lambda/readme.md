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
