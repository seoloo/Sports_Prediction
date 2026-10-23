# 6a. NBA를 dbt Gold에 넣기 + 데이터 신선도 검사

작성: 2026-10-23. 상태: **완료.** 다음 6b는 NBA Gold를 Spark로 다시 계산해 대조하는 것입니다.

## 한 줄 요약

dbt Gold가 축구만 다루던 것을 **NBA까지** 넓혔습니다(모델 4개, 품질 테스트 12개). 그리고 **"수집이 멈췄는지"를 매일 검사**하는 신선도 검사(SLA)를 넣었습니다. 데이터 내용이 아니라 수집 기록을 기준으로 삼은 것이 핵심입니다. 작업 중 Windows가 재시작 뒤 S3·Kafka 포트를 예약해 PC에서 접속이 막힌 것을 발견해 포트를 옮겼습니다.

## 바꾼 것

| 변경 | 위치 | 이유 |
|---|---|---|
| NBA 경기(기준 시각) | `dbt/models/staging/stg_games_known.sql` | 축구 `stg_matches_known`과 같이 시점 선택은 Silver `game_revisions_known_at`에 맡긴다 |
| NBA 팀별 결과 | `gold.nba_team_game_results` | 끝난 경기를 팀 관점 2행(W·L, 득실 차). 축구 `team_match_results`와 같은 구조 |
| NBA 순위표 | `gold.nba_standings` | 정규 시즌만, 승률 → 득실 차 → 팀 이름 |
| NBA 배당 확률 | `gold.nba_game_odds_h2h` | 승패 배당의 오버라운드와 마진을 뺀 확률(시장 기준선) |
| 품질 테스트 12개 | `schema.yml`, `assert_nba_results_consistent.sql`, `assert_nba_odds_probabilities_consistent.sql` | 경기마다 팀 행 2개·승자 1명, 끝난 경기 동점 없음, 순위표 승 = 패·득점 = 실점, 확률 합 = 1 |
| 신선도 검사 | `dbt/models/staging/sources.yml`(`ingest` 소스) | openfootball·balldontlie는 36시간 경고 / 72시간 실패, football-data.org 수집기는 12시간 / 48시간 |
| 매일 DAG에 신선도 | `dags/openfootball_epl_daily.py`의 `source_freshness` | dbt build 뒤 실행, 실패하면 텔레그램 알림. 수집이 실패해도 실행(`all_done`) |
| 포트 이동 | `.env`, `.env.example`, `infra/docker-compose.yml`, `pipelines/config.py` | S3 59000 → 45900, Kafka 59092 → 45992(아래) |

## 왜 "수집 기록"으로 신선도를 보나

| 기준 | 문제 |
|---|---|
| 경기 버전의 수신 시각(`match_revisions.received_at`) | 내용이 바뀔 때만 새 버전이 생긴다. 휴식기나 경기 없는 날엔 수집이 정상이어도 "오래됨"으로 실패한다 |
| **수집 기록(`ingest.fetches.received_at`)** | 같은 원본을 다시 받아도 매번 남는다. **수집기가 돌았는지**를 정확히 보여 준다 |

"데이터가 바뀌었나"와 "파이프라인이 돌았나"는 다른 질문이고, SLA는 뒤쪽입니다. 테스트로 확인했습니다: 오래전 수집 한 번뿐이면 실패, 같은 원본을 지금 다시 받으면(내용 변화 없음) 통과.

## 작업 중 발견한 문제: 포트가 사라졌다

통합 테스트가 전부 건너뛰어져(skip) 확인해 보니, PC에서 S3(127.0.0.1:59000)에 접속이 안 됐습니다. 컨테이너 설정에는 포트가 있는데 실제 연결이 없었습니다.

- **원인:** Windows(Hyper-V)가 재시작 때 **58965~59064, 59065~59164번 포트를 예약 범위로 잡았고**, S3 59000과 Kafka 59092가 그 안에 들어갔습니다. 이 구간(49152 이상)은 재부팅마다 바뀔 수 있습니다.
- **해결:** Windows 설정은 건드리지 않고(시스템 설정), 이 프로젝트의 PC용 포트를 49152 아래로 옮겼습니다(S3 45900, Kafka 45992). 두 컨테이너만 다시 띄웠고, 데이터는 볼륨에 그대로입니다. 소비자는 Kafka가 다시 뜬 뒤 스스로 재연결했습니다(밀린 메시지 0).
- **남은 위험:** Postgres 55432와 Airflow 58080도 같은 구간이라 나중에 같은 일이 생길 수 있습니다. 생기면 같은 방법으로 옮기면 됩니다.

## 사용한 기술

| 기술 | 어디에 | 어떻게 썼나 |
|---|---|---|
| dbt 모델·`ref`·`source` | NBA 모델 4개 | 계보(lineage)를 dbt가 계산. 시점은 `var("as_of")`로 과거 재계산 가능 |
| dbt generic + singular 테스트 | `schema.yml`, `dbt/tests/` | 유일성·허용값은 선언형, 경기 단위·시즌 단위 정합성은 SQL로 |
| `dbt source freshness` | `sources.yml`의 `loaded_at_field`·`freshness`·`filter` | 같은 테이블(`ingest.fetches`)을 `identifier`로 세 번 등록하고 소스별 `filter`로 나눠 각자 기준을 둔다 |
| Airflow `BashOperator` + `trigger_rule="all_done"` | 매일 DAG | 신선도 실패는 작업 실패 → 텔레그램 실패 알림 |
| Windows 예약 포트 확인 | `netsh interface ipv4 show excludedportrange` | 포트가 "설정됐는데 안 열리는" 원인을 찾음 |

## 확인 결과(2026-10-23)

| 확인 | 결과 |
|---|---|
| dbt build | PASS=50(이전 34: 모델 +4, 테스트 +12) |
| 신선도 | openfootball·balldontlie·football-data.org 모두 통과 |
| NBA 순위표 | 270행. 2025-26 상위: 오클라호마시티 64승 18패, 샌안토니오 62승 21패, 디트로이트 60승 22패 |
| 포트 이동 후 | PC에서 S3(버킷 4개)·Kafka 접속 정상, 소비자 재연결·밀린 메시지 0 |
| 테스트 | dbt 통합 테스트 2개 추가(NBA Gold·기준 시각, 신선도 실패·회복) |

## 면접에서 나올 만한 질문

**Q. 데이터 신선도는 무엇으로 측정하나요?**
처음에는 데이터의 최신 수신 시각을 떠올리기 쉽지만, 이 프로젝트는 내용이 바뀔 때만 버전을 만들기 때문에 쉬는 기간엔 정상이어도 "오래됨"이 됩니다. 그래서 같은 원본이라도 매번 남는 수집 기록으로 "수집기가 돌았나"를 봤습니다. 데이터 품질(내용)과 파이프라인 상태(생존)를 다른 지표로 나눈 것입니다.

**Q. 경고와 실패를 왜 나눴나요?**
하루 한 번 도는 수집이 한 번 늦는 것(36시간)은 지켜볼 일이고, 사흘(72시간)이면 사람이 손을 써야 합니다. 실패만 알림으로 보내서 알림 피로를 줄였습니다.

## 한계

- **NBA 순위표의 경기 수:** 소스가 플레이인·NBA컵 결승을 정규 시즌으로 표시한 시즌은 일부 팀이 83경기로 계산됩니다. 실제 NBA의 동률 규칙과 컨퍼런스 구분도 없습니다.
- **배당 신선도는 보지 않습니다.** 배당은 경기 일정에 따라 받는 시점이 달라 고정 기준이 맞지 않습니다.
- **Spark 대조는 아직 축구만입니다(6b).**
