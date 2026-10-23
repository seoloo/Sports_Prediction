# 2b. football-data.org 수집기

작성: 2026-10-21. 이 단계의 커밋: `120a24c`(수집기·소비자 함께 구현). 소비자는 [2c 문서](2c_consumer.md)에서 따로 설명합니다.

## 한 줄 요약

무료 API의 요청 한도(분당 10회)를 지키면서 경기 상황에 맞춰 조회 간격을 스스로 바꾸고, 받은 원본은 Bronze에 그대로 보존한 뒤 내용이 바뀐 경우에만 Kafka로 이벤트를 보내는 수집기입니다.

## 왜 football-data.org가 필요한가

1단계의 openfootball은 하루 한 번 갱신되는 공개 데이터라 늦습니다. 10/20에 두 소스의 2026-27 시즌을 비교했더니:

| 비교 | 건수 |
|---|---|
| football-data.org는 "끝남", openfootball은 아직 "예정" | 20 |
| 중계 일정 확정으로 날짜가 바뀌었는데 openfootball에는 옛 날짜 | 31 |
| 점수가 서로 다름 | 0 |

또 football-data.org는 경기마다 `lastUpdated`(출처가 그 내용을 갱신한 시각)를 줍니다. openfootball에는 없던 값이라, "그 시점에 이 정보가 발표되어 있었나"를 더 정확히 판단할 수 있습니다.

## 흐름

```text
run_forever (컨테이너 fdorg-poller에서 계속 실행)
 └ poll_once
    ├ RateLimiter.wait            최근 60초 요청 수가 한도면 기다림
    ├ fetch_json                  GET /competitions/PL/matches?season=2026 (요청 1번에 380경기)
    │   └ 429면 서버가 알려 준 초기화 시간만큼 기다렸다 최대 3번 시도
    ├ Bronze                      응답 원본을 S3에 내용 해시 경로로 저장(같은 내용이면 다시 안 올림)
    ├ ingest.raw_files / fetches  원본 목록과 "언제 다시 확인했는지" 기록
    ├ 응답이 지난번과 같으면       이벤트를 보내지 않고 끝
    ├ 달라졌으면                  경기 380개를 normalize_match → build_event → Kafka(키 = 경기 ID)
    └ next_poll_interval          다음 조회까지 기다릴 시간 계산
```

## 설계 결정과 이유

| 결정 | 위치 | 이유 |
|---|---|---|
| 대회·시즌 전체를 한 번에 조회 | `poll_once` | 경기마다 따로 부르면 380번이 필요하지만, 목록 하나면 1번. 분당 10회 한도에 여유가 생긴다 |
| 요청 간격 제어기(최근 60초 요청 수 세기) | `RateLimiter.wait` (football_data_org.py:62) | 한도를 넘겨 막히기 전에 스스로 기다린다. 시계와 sleep을 바꿔 넣을 수 있어 실제로 기다리지 않고 테스트한다 |
| 서버가 알려 주는 남은 횟수도 따름 | `RateLimiter.update_from_headers` (:76) | 같은 키를 다른 곳에서도 쓰면 우리 계산만으로는 모자라다. 서버가 0이라고 하면 초기화 시간까지 멈춘다 |
| 429는 기다렸다 다시 시도, 다른 오류는 그대로 실패 | `fetch_json` (:84) | 한도 초과는 일시적이라 기다리면 풀린다. 403(키 문제) 같은 오류는 재시도해도 소용없어 바로 실패로 기록한다 |
| 경기 상황에 맞춘 조회 간격 | `next_poll_interval` (:133) | 아래 표. 경기가 없을 때 2분마다 부르면 쓸모없는 요청이고, 경기 중에 6시간 간격이면 결과가 늦다 |
| 응답 원본을 먼저 Bronze에 | `poll_once` | 나중에 정규화 규칙이 바뀌어도 원본에서 다시 만들 수 있다. 같은 내용은 같은 경로라 중복 저장이 없다 |
| 내용이 바뀐 경우에만 이벤트 발행 | `poll_once`의 `if is_new:` (:191) | 6시간마다 같은 380건을 보내면 소비자가 할 일만 늘어난다 |
| 결측 점수는 `None` | `normalize_match` (:115) | 아직 안 열린 경기의 점수는 0이 아니라 "모름" |
| 실패해도 수집기는 멈추지 않음 | `run_forever` (:250) | 네트워크 오류로 컨테이너가 죽지 않게 기록만 하고 5분 뒤 다시 시도. 실패는 `ingest.runs`에 남는다 |
| API 키는 `.env`에만 | `pipelines/config.py`, `docker-compose.yml`의 `FOOTBALL_DATA_ORG_API_KEY` | 약관상 공개 저장소에 키를 두면 안 된다. Git에서 제외되고 커밋 이력에도 없다 |
| 출처 표기 | `ATTRIBUTION` (:38), README | 약관 조건: "Football data provided by the Football-Data.org API" |

조회 간격 규칙:

| 상황 | 간격 |
|---|---|
| 경기 중(IN_PLAY, PAUSED)이거나, 시작 15분 전 ~ 시작 2시간 30분 후 사이 경기가 있음 | 2분 |
| 24시간 안에 시작하는 경기가 있음 | 15분 |
| 그 밖 | 6시간 |

가장 바쁜 경우에도 분당 0.5회라 한도(10회)의 5%만 씁니다.

## 2026-10-21 확인 결과

| 확인 | 결과 |
|---|---|
| 컨테이너 `fdorg-poller` | 실행 중 |
| 조회 기록(`ingest.runs`) | 2번 모두 성공. 첫 번째(10/20 23:16 UTC)는 새 내용이라 380건 발행, 두 번째(23:21)는 같은 내용이라 0건 |
| Bronze | 원본 1개(368,872바이트). 같은 응답을 다시 받았지만 새로 저장하지 않음 |
| 수집 기록(`ingest.fetches`) | 2건, 그중 새 내용 1건 |
| 요청 한도 | 조회 직후 남은 횟수 9/10 |
| 다음 조회 | 6시간 뒤. 다음 경기가 10/23 19:00 UTC라 24시간 밖 |
| 테스트 | 관련 단위 테스트 12개 + 통합 테스트 1개 통과 |

일정이 다가오면 간격이 자동으로 줄어듭니다. 10/22 19:00 UTC 이후 조회부터 15분 간격, 경기 15분 전부터 2분 간격이 됩니다.

## 직접 확인하는 방법

수집기 출력(조회할 때마다 한 줄씩):

```bash
docker compose --env-file .env -f infra/docker-compose.yml logs --tail 20 fdorg-poller
```

조회 기록과 결과(Postgres 접속 도구에서):

```sql
SELECT started_at, status, stats->>'new_content' AS new_content, stats->>'events_published' AS published,
       stats->>'next_poll_seconds' AS next_poll, error
FROM ingest.runs WHERE source_id = 'football-data-org' ORDER BY started_at DESC LIMIT 10;
```

한 번만 직접 조회(요청 1회를 씁니다):

```bash
.\.venv\Scripts\python.exe -m pipelines.ingest.football_data_org --once
```

테스트:

```bash
.\.venv\Scripts\python.exe -m pytest tests/test_streaming_unit.py tests/test_streaming_integration.py -q
```

## 테스트가 확인하는 것

| 테스트 | 확인 내용 |
|---|---|
| `test_rate_limiter_never_exceeds_ten_per_minute` | 25번 연속 요청해도 어떤 60초 구간에도 10번을 넘지 않음 |
| `test_rate_limiter_obeys_server_headers` | 서버가 남은 횟수 0, 초기화 42초라고 하면 42초 기다림 |
| `test_fetch_retries_after_429_using_reset_header` | 429를 받으면 알려 준 17초를 기다린 뒤 다시 시도해 성공 |
| `test_fetch_raises_other_http_errors_and_gives_up_after_retries` | 403은 바로 실패, 429가 계속되면 정해진 횟수 뒤 포기 |
| `test_next_poll_interval` (7가지) | 경기 중·시작 직전·종료 직후·24시간 안·그 밖·끝난 경기·경기 없음 |
| `test_normalize_keeps_missing_scores_as_none` | 안 열린 경기의 점수는 None |
| `test_poll_once_stores_bronze_and_publishes_only_when_content_changes` | 같은 응답 두 번: 처음에만 3건 발행, 두 번째는 0건, 수집 기록은 2건 |

실제 API 대신 가짜 응답과 가짜 시계를 넣어 테스트합니다. 그래서 테스트가 요청 한도를 쓰지 않고, 60초를 실제로 기다리지도 않습니다.

## 면접에서 나올 만한 질문

**Q. API 요청 한도는 어떻게 지켰나요?**
두 가지를 같이 씁니다. 하나는 최근 60초 안의 요청 시각을 기억해 10번이 차면 가장 오래된 요청이 60초를 넘길 때까지 기다리는 방식(슬라이딩 윈도)입니다. 다른 하나는 서버가 응답 헤더로 알려 주는 남은 횟수입니다. 서버가 0이라고 하면 알려 준 초기화 시간까지 멈춥니다. 그래도 429가 오면 그 시간만큼 기다렸다가 최대 3번 시도합니다.

**Q. 왜 경기마다 조회하지 않고 목록 전체를 받나요?**
요청 수 때문입니다. 경기별로 부르면 한 번 갱신에 380번이 필요해 한도로는 불가능합니다. 목록 하나는 1번이고, 바뀐 경기는 소비자가 내용 해시로 골라냅니다.

**Q. 실시간이라고 할 수 있나요?**
초 단위 실시간은 아닙니다. 무료 API라 경기 중에도 2분 간격이고 갱신 지연도 있습니다. 경기 주기에 맞춘 "준실시간"이라고 설명하는 게 정확합니다.

**Q. 매번 380건을 Kafka로 보내나요?**
아닙니다. 응답 전체의 해시가 지난번과 같으면 아무것도 보내지 않습니다. 달라졌을 때만 경기별로 보내고, 실제로 내용이 바뀐 경기만 소비자가 새 버전으로 저장합니다.

**Q. 수집기가 죽으면요?**
조회 하나가 실패해도 프로세스는 살아서 기록을 남기고 5분 뒤 다시 시도합니다. 컨테이너가 죽으면 Docker가 다시 띄웁니다(`restart: unless-stopped`). 그 사이 놓친 갱신은 다음 조회에서 전체 목록을 다시 받으므로 따라잡습니다.

## 한계

- 경기 당일에는 `lastUpdated`가 자주 바뀌어, 내용이 거의 같아도 응답 해시가 달라집니다. 그때마다 Bronze 원본(약 0.37MB)이 새로 쌓입니다. 2분 간격으로 3시간이면 약 33MB이며, 오래된 원본 정리 규칙은 아직 없습니다.
- 요청 간격 제어기는 프로세스 안에서만 셉니다. 같은 키로 다른 프로그램도 요청하면 서버 헤더에만 의존합니다.
- 지금은 프리미어리그(PL) 한 대회만 조회합니다. 다른 대회를 추가하려면 대회 코드와 팀 대조표가 필요합니다.
- 시즌은 7월에 시작한다고 보고 정합니다(`pipelines/schedule.py`). 리그마다 다르면 조정이 필요합니다.
- 라인업·부상 정보는 무료 플랜에 없어 수집하지 않습니다.
