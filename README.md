# Sports Prediction



## 실행

Python 3.12 이상과 Windows PowerShell을 기준으로 합니다. 저장소 폴더에서 실행하세요.

~~~powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.lock
.\scripts\run.ps1
~~~

이미 이 PC에는 프로젝트의 .venv와 필요한 패키지를 준비했습니다. 다시 설치할 필요 없이 마지막 명령으로 실행할 수 있습니다. 기본 주소는 http://127.0.0.1:8501입니다. 종료는 실행 터미널에서 Ctrl+C를 누릅니다.

스크립트 실행이 제한된 경우 직접 실행할 수 있습니다.

~~~powershell
.\.venv\Scripts\python.exe -m streamlit run app.py --server.address=127.0.0.1 --server.port=8501 --server.headless=true --browser.gatherUsageStats=false
~~~

8501 포트가 사용 중이면 .\scripts\run.ps1 -Port 8502처럼 다른 포트를 지정하세요. 일반 Python 설치가 없는 Codex 환경에서는 앱의 load_workspace_dependencies 도구가 알려 주는 Python으로 .venv를 만들 수 있습니다. 번들 런타임은 프로젝트의 배포 필수조건이 아닙니다.

requirements.lock은 이번에 검증한 Python 3.12 실행·테스트 환경의 버전 목록입니다. 직접 의존성은 requirements.txt, 테스트 의존성은 requirements-dev.txt에 있습니다.

## 처음 사용하는 순서

1. **데이터 소스:** 직접 작성한 자료 또는 로컬 저장 사용권이 확인된 자료의 소스 ID·이름·출처·확인 근거를 등록합니다. 권한 확인 체크가 없는 소스는 PENDING이며 입력에 사용할 수 없습니다. 소스 설정은 보존하므로 조건 변경은 새 ID로 등록합니다.
2. **경기 입력:** 직접 등록 메뉴를 사용하거나 CSV 가져오기에서 events.csv 양식을 다운로드합니다. 다섯 종목을 처음부터 선택할 수 있습니다. 골프 라운드에는 여러 선수를 등록할 수 있습니다.
4. **저장 전 확인:** 오류와 미리보기를 확인한 후 저장 버튼을 누릅니다. 한 행이라도 잘못되면 해당 파일 전체를 저장하지 않습니다.
5. **전체 경기:** 종목·리그·참가자로 검색하고 경기 상세에서 상품, 마감, 기록, 출처, 입력 시각과 예측 보류 이유를 확인합니다.
6. **가져오기 이력:** 파일명, 인코딩, 저장·중복 건수, 입력 시각과 해시를 확인합니다.

빠른 체험은 사이드바에서 **가상 예시**를 선택하고 **전 종목 가상 예시 불러오기**를 누르세요. 가상 자료는 내 데이터와 다른 DB에 저장됩니다. 다운로드하는 가상 예시 CSV의 demo 소스도 예시 저장소에서만 활성화됩니다.

## CSV 규약

양식 파일은 templates/, 가상 데이터는 examples/에도 있습니다. UI에서 내려받을 수 있습니다.

| 파일 | 목적 | 주요 규약 |
|---|---|---|
| events.csv | 경기·대회·라운드 | event_key 고유, sport 다섯 종목, event_type match/round/tournament, participants는 홈\|원정 또는 선수 목록 |
| records.csv | 발표된 수치 기록 | 참가자 이름 정확히 일치, period·published_at 필수, value 빈값은 결측이고 0은 실제 0 |

모든 행은 활성 source_id가 필요합니다. 시간은 2026-10-12T19:00:00+09:00처럼 시간대 포함 ISO 8601로 입력하며, DB는 UTC로 저장하고 화면은 Asia/Seoul로 표시합니다. 발표 시각이 불명확한 기록은 임의로 과거 시각을 부여하지 마세요.

UTF-8(BOM 포함)과 CP949를 지원하고 파일당 최대 5MB·10,000행을 허용합니다. 헤더 오류·중복 행·미등록 경기·미확인 소스·미래 발표 시각·마감 시각 오류·무한대/NaN 수치를 검사합니다. 같은 파일 또는 같은 내용의 기존 행은 건너뛰고, 같은 식별자의 다른 값은 덮어쓰지 않고 보류합니다. CSV는 초기 등록용입니다. 일정·시장 조건 변경은 변경 이력 메뉴에서 새 버전으로 입력합니다. 참가자·리그 정정과 기록 정정 CSV는 아직 지원하지 않습니다.


## 저장과 검증

내 데이터는 data/sports.sqlite3, 가상 예시는 data/demo.sqlite3에 보관합니다. 원본 CSV 바이트, 파일 해시, 입력 시각과 감사 이력을 함께 저장하며 WAL·외래키·쓰기 트랜잭션을 사용합니다. data/와 .venv/는 Git에 포함하지 않습니다. SPORTS_DATA_DIR 환경 변수로 데이터 폴더를 별도 지정할 수 있습니다.

~~~powershell
.\.venv\Scripts\python.exe -m pytest -q
~~~

검사는 전 종목 등록·영구 저장, 원자적 입력, 중복·충돌, 시간대, 십진수 정밀도, 결측/0 구분, 소스 재검사와 Streamlit 화면의 소스 등록→CSV 저장·가상 예시·직접 등록 흐름을 확인합니다. 이번 단계에서는 예측 품질을 검증하지 않습니다.





입력 스냅샷 메뉴에서 상품과 시간대 포함 기준 시각을 선택하면 다음을 확인하고 저장할 수 있습니다.

- 기록의 published_at과 received_at이 모두 기준 시각 이하인 경우만 포함합니다.
- 결측값, 이후 발표·입력, 소스 확인이 늦거나 비활성인 기록은 제외 이유를 표시합니다.
- 사용한 버전 ID, 기록 원본 값과 ID, 제외 정보, 결정적인 입력 해시를 불변 저장하고 JSON으로 내려받습니다.

저장 모드는 INPUT_REPLAY입니다. 이 단계의 스냅샷은 모델 특징 계산 결과나 실제 사전 예측이 아니므로 운영 성능 실적으로 집계하지 않습니다. 확률도 제공하지 않습니다. 규칙을 연결해도 MODEL_UNVALIDATED 상태는 유지합니다.


## 다음 개발

다음 묶음은 결과·기록 정정 버전과 공식 적중결과 대조, 모델별 필수 입력 계약·특징 생성입니다. 모델 학습이 허용된 실제 기록을 확보한 뒤 종목별 기준 모델과 시간순 평가를 연결합니다. 아직 전체 예외 규칙이나 전 종목 모델 검증 완료를 주장하지 않습니다.


화면 업로드와 테스트 구현은 [Streamlit 파일 업로드 문서](https://docs.streamlit.io/develop/api-reference/widgets/st.file_uploader)와 [앱 테스트 문서](https://docs.streamlit.io/develop/api-reference/app-testing)를 참고했습니다.
