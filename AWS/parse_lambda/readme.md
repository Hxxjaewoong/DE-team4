# 🚀 Parsing Lambda Functions

이 문서는 **DCInside, BobaeDream, Clien, FMKorea** 사이트에서 크롤링한 HTML 데이터를 파싱하는 **Lambda Functions**에 대한 설명을 포함합니다.  
각 Lambda는 **S3에서 HTML 데이터를 가져와 본문 및 댓글 데이터를 출력한 후 Parquet으로 저장**하는 역할을 수행합니다.

---

## **🛠️ 1. 프로젝트 개요**
📀 **Step Function Workflow**
1. **Extract Lambda** → 크롤링한 HTML 데이터 **(raw_html/{site}/yyyy-mm-dd.json)** S3에 저장  
2. **Parse Lambda** → 저장된 HTML 파일을 불러와 **본문 및 댓글 데이터**를 파싱, Parquet으로 변환 후 S3 저장  
3. **Merge Lambda** → 각 사이트별 Parquet 데이터를 병합하여 최종 데이터셋 생성  

---

## **📂 2. Lambda 함수 개요**
### ✅ **DCInside Parse Lambda**
- **Lambda 이름:** `dcinside_parse`  
- **입력 데이터:** S3에서 DCInside 크롤링 HTML (`raw_html/dcinside/yyyy-mm-dd.json`)  

### ✅ **Clien Parse Lambda**
- **Lambda 이름:** `clien_parse`  
- **입력 데이터:** S3에서 Clien 크롤링 HTML (`raw_html/clien/yyyy-mm-dd.json`)  

### ✅ **BobaeDream Parse Lambda**
- **Lambda 이름:** `bobae_parse`  
- **입력 데이터:** S3에서 BobaeDream 크롤링 HTML (`raw_html/bobae/yyyy-mm-dd.json`)  

### ✅ **FMkorea Parse Lambda**
- **Lambda 이름:** `fmkorea_parse`  
- **입력 데이터:** S3에서 FMkorea 크롤링 HTML (`raw_html/fmkorea/yyyy-mm-dd.json`)  


### ✅ **파싱 결과 저장명**
- `raw_data/{site}/yyyy-mm-dd-content.parquet` (본문)  
- `raw_data/{site}/yyyy-mm-dd-comment.parquet` (댓글) 


📀 **본문 데이터**
| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| site | string | 사이트명 (dcinside) |
| datetime | datetime | 게시글 작성 시간 |
| model | string | 검색 키워드 |
| title | string | 게시글 제목 |
| content | string | 게시글 내용 |
| url | string | 게시글 URL |
| author | string | 작성자 (사용 안함) |
| likes | int | 추천 수 |
| hates | int | 비추천 수 |
| comments_count | int | 댓글 개수 |
| views | int | 조회수 |

📀 **댓글 데이터**
| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| url | string | 게시글 URL |
| title | string | 게시글 제목 |
| comment | string | 댓글 내용 |


---

## **🔧 3. Lambda 실행**
1. **Step Function에서 Lambda와 연동해 실행**
2. **S3에서 HTML 파일 불러오기**
3. **BeautifulSoup을 이용해 HTML 파싱**
4. **본문 및 댓글 데이터를 Parquet형태로 S3에 저장**
5. **성공/실패 로그를 Lambda 로그에 전송**

---

