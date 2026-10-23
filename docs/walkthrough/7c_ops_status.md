# 7c. 운영 상태 보고서: 하루 상태를 한 장으로

작성: 2026-10-24. 상태: **완료.** 7단계(레이크하우스·운영)의 마지막 단계입니다.

## 한 줄 요약

수집 신선도, 수집 실행 실패, API 크레딧, 스트림 거부·품질 문제, 경기 전 기록, 주간 채점, 레이크 상태를 **한 장의 HTML(`data/ops/status.html`)과 JSON**으로 만듭니다. 항목마다 OK·WARN·FAIL(판단할 자료가 없으면 UNKNOWN)로 판정하고, 전체 상태는 가장 나쁜 항목입니다. 매일 DAG의 마지막 작업이 앞 작업의 성공 여부와 관계없이 만듭니다. 보는 방식은 세 후보(정적 HTML, Grafana, 텔레그램 요약) 중 **정적 HTML**로 정했습니다. 외부 접속이 필요 없어 127.0.0.1 원칙에 그대로 맞기 때문입니다.

첫 보고서는 **WARN**이었습니다. The Odds API가 HTTP 502를 한 번 돌려준 실행(그 뒤 회복)과, PC가 꺼져 있던 동안 놓친 경기 전 스냅샷 때문입니다. 판정 규칙을 실제 데이터로 보다가 기준 하나를 고쳤습니다. 늦게 온 옛 이벤트(STALE_EVENT 260건)와 규칙대로 걸러 낸 품질 문제는 **정상 동작이라 경고에서 뺐습니다**. 경고가 너무 많으면 진짜 경고를 놓칩니다.

## 판정 규칙

| 항목 | 자료 | OK | WARN | FAIL |
|---|---|---|---|---|
| 수집 신선도 | `dbt source freshness`(규칙은 `sources.yml` 한곳) | pass | warn | error |
| 수집 실행 | `ingest.runs` 소스별 | 최근 24시간 실패 없음 | 실패했다가 그 뒤 성공 | 마지막 실행이 실패 |
| API 크레딧 | The Odds API 마지막 응답 헤더(실행 통계) | 60 이상 | 60 미만(수집기가 스스로 줄이는 선) | 20 미만 |
| 스트림·품질 | `silver.stream_rejections`, `silver.quality_issues`(7일) | 없음, 또는 모두 정상 동작 | 오류 큐 사유(`contracts.DLQ_REASONS`)·계약 위반 | — |
| 경기 전 기록 | 축구·NBA 스냅샷(7일) | 놓침 없음 | 놓친 시점 있음 | — |
| 주간 채점 | `modeling.prospective_reports` | 8일 안 | 8일 넘게 새 보고서 없음 | — |
| 레이크 | `exports/latest.json`·매니페스트 | 30시간 안 | 30시간 넘음 | — |

신선도는 보고서가 dbt를 직접 실행해 결과를 `data/ops/freshness.json`에 씁니다. `dbt/target/sources.json`은 테스트 실행도 덮어써서 믿을 수 없었기 때문입니다.

## 바꾼 것

| 변경 | 위치 | 이유 |
|---|---|---|
| 보고서 모듈 | `pipelines/ops/status.py`(`collect`, 항목별 `*_check`, `render_html`, `write`) | 읽기만 하는 조회 7개 + 판정 + 한 장짜리 HTML(밝은·어두운 화면 모두, 휴대폰 폭에서도 표가 가로로 넘침 없이 스크롤) |
| 매일 DAG 마지막 작업 | `dags/openfootball_epl_daily.py`의 `ops_status`(`all_done`, 재시도 0) | 앞 작업이 실패한 날에도 보고서가 그 실패를 보여 줘야 한다 |
| Airflow 볼륨 | `infra/docker-compose.yml`의 `../data/ops` | 컨테이너가 쓴 파일을 PC에서 바로 연다(Git 제외 폴더) |
| 오류 큐 사유 목록 이동 | `contracts.DLQ_REASONS` | 소비자와 보고서가 같은 목록을 쓴다. 보고서가 Kafka 클라이언트를 불러오지 않게 |

## 사용한 기술

| 기술 | 어디에 | 어떻게 썼나 |
|---|---|---|
| SQL 집계(`DISTINCT ON`, `FILTER`) | `runs_check`, `snapshots_check` | 소스별 마지막 실행, 시점별 찍음·놓침을 한 번에 |
| dbt source freshness `--output` | `run_freshness` | 신선도 규칙을 다시 쓰지 않고 dbt 판정을 그대로, 결과 파일 경로만 따로 |
| 정적 HTML(인라인 CSS, `prefers-color-scheme`) | `render_html` | 서버 없이 파일 하나. 출처에서 온 문자열(오류 메시지 등)은 모두 이스케이프 |
| Airflow `trigger_rule="all_done"` | `ops_status` | 실패한 날도 보고서 |

## 확인 결과(2026-10-24)

| 확인 | 결과 |
|---|---|
| 실제 데이터(PC) | 전체 WARN: 수집 실행 WARN(The Odds API 24시간 실패 1건, HTTP 502, 그 뒤 성공), 경기 전 기록 WARN(7일 찍음 60·놓침 179), 신선도·크레딧(471)·주간 채점·레이크 OK |
| Airflow 컨테이너 | 같은 보고서를 `/opt/project/data/ops`에 씀 → PC `data/ops/status.html`로 열림. DAG 작업 목록에 `ops_status`, 불러오기 오류 없음 |
| 테스트(`tests/test_ops_status.py`) | 전체 = 가장 나쁜 항목, 신선도 pass/error 판정, 검사 못 하면 UNKNOWN / 출처 문자열의 `<script>` 이스케이프 / 임시 DB에서 "마지막 실패 → FAIL, 실패 후 회복 → WARN, 크레딧 45 → WARN, 늦은 옛 이벤트만 있으면 OK, 계약 위반이 생기면 WARN" |
| 전체 테스트 | 217개 통과, 1개 건너뜀(Spark 전용 파일) |

## 면접에서 나올 만한 질문

**Q. Grafana 같은 대시보드 도구를 쓰지 않은 이유는?**
보고 싶은 것은 하루 한 번 바뀌는 운영 상태이고, 보는 사람은 저 한 명입니다. 서버형 대시보드는 컨테이너·데이터 소스 설정·접속 경로가 늘고, 집 밖에서 보려면 127.0.0.1 원칙을 바꿔야 합니다. 파일 한 장은 백업·비교가 쉽고 JSON도 함께 남아 나중에 대시보드로 옮기기 쉽습니다.

**Q. 경고 기준은 어떻게 정했나요?**
코드에 이미 있는 선을 다시 썼습니다. 크레딧 60은 수집기가 스스로 줄이는 선, 신선도는 dbt 규칙 그대로입니다. 실제 데이터로 보니 정상 동작(늦은 옛 이벤트 260건)까지 경고하고 있어서 뺐습니다. "확인할 일이 있을 때만 노란색"이어야 보고서를 믿고 봅니다.

## 한계

- **Airflow 작업 상태(dbt 실패, Spark 대조 실패 등)는 보고서에 없습니다.** 그 실패는 이미 텔레그램으로 알리고, 보고서는 우리 DB·저장소만 읽습니다(Airflow 메타데이터 DB를 직접 읽지 않음).
- **하루 한 번 만듭니다.** 그 사이 상태는 직접 실행(`python -m pipelines.ops.status`)으로 봅니다.
- **집 밖에서는 볼 수 없습니다.** 파일이 PC에 있습니다(127.0.0.1 원칙).
