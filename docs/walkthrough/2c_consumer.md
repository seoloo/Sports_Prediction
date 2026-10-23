# 2c. Kafka 소비자 → Silver

작성: 2026-10-21. 이 단계의 커밋: `120a24c`(소비자 구현), `d2bacf4`(Gold가 football-data.org 정보를 우선 사용).

## 한 줄 요약

Kafka의 경기 이벤트를 Silver(`silver.live_match_revisions`)에 버전으로 쌓는 소비자입니다. 같은 이벤트를 여러 번 받거나, 옛 정보가 늦게 오거나, 중간에 죽었다 다시 켜져도 최종 결과가 달라지지 않게 만들었습니다.

## 왜 이렇게까지 해야 하나

스트리밍에서는 다음 세 가지가 실제로 일어납니다.

| 상황 | 예 | 그냥 저장하면 |
|---|---|---|
| 중복 | 수집기가 같은 경기를 다시 보냄, 소비자가 재시작해 같은 메시지를 다시 읽음 | 같은 버전이 여러 개 쌓임 |
| 늦은 도착 | 10:10에 발표된 1:0이 10:30에 발표된 2:0보다 늦게 도착 | 최신 점수 2:0이 옛 점수 1:0으로 덮임 |
| 중간 장애 | DB에 저장한 직후, 오프셋을 기록하기 전에 프로그램이 죽음 | 다시 켜면 같은 메시지를 또 받음 |

## 이벤트 하나를 처리하는 순서

`handle_match_event` (pipelines/streaming/match_consumer.py:48)

```text
1. 형식 확인        깨진 JSON, 필드 누락, 모르는 schema_version        → 거부(사유 기록)
2. 팀 대조          대조표에 없는 팀 ID                                  → 거부 UNMAPPED_TEAM
3. 경기 연결        리그·시즌·홈·원정으로 찾고, 없으면 만든다
4. 중복 확인        직전 버전과 내용 해시가 같음 / 이미 반영한 event_id   → DUPLICATE(건너뜀)
5. 늦은 도착 확인    published_at이 직전 버전보다 이름                     → 거부 STALE_EVENT
6. 저장            다음 버전 번호로 live_match_revisions에 추가          → INSERTED
```

거부된 이벤트는 버리지 않고 `silver.stream_rejections`에 토픽·파티션·오프셋·사유와 함께 남깁니다(dead-letter 테이블). 중복은 정상적인 일이라 남기지 않습니다.

## 설계 결정과 이유

| 결정 | 위치 | 이유 |
|---|---|---|
| 내용 해시에서 `last_updated` 제외 | `CONTENT_FIELDS`, `content_hash` (:29, :32) | 출처가 갱신 시각만 바꾸고 내용(상태·점수·일정)은 같을 때 새 버전을 만들지 않기 위해 |
| 이미 반영한 `event_id`면 중복 | :83 | 토픽을 처음부터 다시 처리하면 옛 이벤트가 다시 온다. 이걸 "늦은 도착"으로 오해해 거부 기록을 쌓지 않게 |
| `published_at`이 더 이르면 거부 | :85 | Kafka의 순서는 "받은 순서"일 뿐, 출처가 발표한 순서와 다를 수 있다. 출처의 갱신 시각이 기준 |
| 경기가 없으면 만든다 | :72 | 경기의 정체성(리그·시즌·홈·원정)은 소스와 무관하다. openfootball이 아직 모르는 경기도 받을 수 있게 |
| 묶음(최대 500건)을 한 트랜잭션으로 | `run`의 `with conn.transaction()` (:128) | 한 건씩 커밋하면 느리다. 묶음 중간에 오류가 나면 묶음 전체를 되돌리고 오프셋도 기록하지 않아 다시 받는다 |
| DB 커밋 다음에 오프셋 커밋 | `kafka_consumer.commit` (:136) | 오프셋을 먼저 기록하고 죽으면 그 이벤트를 영영 잃는다. 순서를 반대로 하면 다시 받을 수는 있지만 잃지는 않는다 |
| 거부 기록에 `UNIQUE(topic, partition, offset, reason)` | `silver.stream_rejections` (003_streaming.sql:36) | 같은 메시지를 다시 처리해도 거부 기록이 두 번 쌓이지 않게 |
| 수정·삭제 차단 트리거 | 003_streaming.sql | 1단계와 같은 원칙. 버전과 거부 기록은 덮어쓰지 않는다 |
| 기준 시각 함수 | `silver.live_match_revisions_known_at` (:53) | "그 시점에 출처가 발표했고(published_at) 우리도 받았던(received_at)" 버전만 고른다 |
| 토픽별 처리 함수 | `dispatch_handler` (:97) | 토픽마다 처리 함수를 고르는 자리. 지금은 경기 이벤트 하나이고, 토픽이 늘면 여기서 나눈다 |

### 전달 보장: at-least-once + 멱등 저장

- Kafka 쪽: 자동 커밋을 끄고 DB 커밋 뒤에 직접 커밋합니다. 그래서 메시지를 잃지는 않지만 같은 메시지를 두 번 받을 수는 있습니다(at-least-once).
- DB 쪽: 같은 내용과 같은 event_id를 건너뛰게 만들어, 두 번 처리해도 한 번 처리한 것과 결과가 같습니다(멱등).
- 둘을 합치면 "결과적으로 정확히 한 번 반영한 것과 같은 효과"가 납니다. Kafka의 트랜잭션(exactly-once)을 쓰지 않은 이유는, 저장소가 Kafka가 아니라 Postgres라서 Kafka 트랜잭션만으로는 DB 저장까지 묶을 수 없기 때문입니다.

## Silver → Gold: 소스 우선순위

`dbt/models/staging/stg_matches_known.sql`이 두 소스를 합칩니다.

- 기준 시각에 football-data.org 버전이 있으면 그것을 쓰고, 없으면 openfootball 버전을 씁니다. 어느 쪽인지 `fixture_source` 열에 남깁니다.
- API 상태값은 SCHEDULED·LIVE·FINISHED·AWARDED·NOT_PLAYED 다섯 가지로 묶습니다.
- 효과: 2026-27 순위표가 끝난 경기 70건을 모두 반영합니다(openfootball만 쓰면 50건).
- 과거 시각으로 다시 계산하면, 그때 아직 받지 않은 football-data.org 정보는 쓰지 않고 openfootball 값으로 돌아갑니다(테스트로 확인).

## 2026-10-21 확인 결과

| 확인 | 결과 |
|---|---|
| 컨테이너 `stream-consumer` | 2시간째 실행 중 |
| 남은 메시지(LAG) | `fdorg.matches.v1` 파티션 3개 모두 0 |
| Silver | 380경기 각 1번 버전(종료 70, 예정 310). `published_at`은 API의 갱신 시각(10/20 00:20~13:25 UTC) |
| 거부 기록 | 0건 |
| 실제 토픽 380건을 새 그룹으로 처음부터 다시 처리 | 380건 모두 DUPLICATE, Silver 행 수·체크섬 처리 전후 동일, 거부 기록 0건 그대로 |
| 테스트 | 아래 표의 5개 모두 통과 |

## 직접 확인하는 방법

소비자가 어디까지 읽었는지(LAG가 0이면 다 읽은 것):

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group silver-match-writer
```

Silver 상태와 거부 기록(Postgres 접속 도구에서):

```sql
SELECT status, count(*) FROM silver.live_match_revisions_known_at(now()) GROUP BY 1;
SELECT reason, count(*) FROM silver.stream_rejections GROUP BY 1;
```

토픽을 처음부터 다시 처리해 보기(실제 DB에 아무것도 추가되지 않아야 정상):

```bash
.\.venv\Scripts\python.exe -m pipelines.streaming.match_consumer --until-idle --group demo-reprocess --topic fdorg.matches.v1
```

## 테스트가 확인하는 것

| 테스트 | 확인 내용 |
|---|---|
| `test_duplicates_late_events_and_bad_messages` | 같은 이벤트 2번, 정정(1:0 → 2:0), 늦게 온 옛 정보(1:1), 깨진 메시지, 대조표에 없는 팀을 한 번에 보냄 → 저장 2, 중복 1, 거부 3(늦은 도착·깨진 형식·팀 없음). 새 그룹으로 다시 처리해도 Silver 그대로 |
| `test_committed_offsets_are_not_reprocessed` | 같은 그룹으로 다시 켜면 새 메시지만 읽음 |
| `test_content_hash_ignores_last_updated` | 갱신 시각만 다르면 같은 내용, 점수가 다르면 다른 내용 |
| `test_decode_rejects_unknown_schema_version` | 모르는 형식 버전은 거부 |
| `test_live_source_overrides_openfootball_in_gold` | Gold가 football-data.org 점수를 쓰고, 그 정보를 받기 전 시점으로 계산하면 openfootball로 돌아감 |

## 만드는 중에 바로잡은 점

처음에는 "직전 버전과 내용이 같으면 중복"만 확인했습니다. 그런데 토픽을 처음부터 다시 처리하는 테스트에서, 이미 반영했던 옛 이벤트가 최신 버전보다 이르다는 이유로 "늦은 도착"으로 거부되고 거부 기록이 쌓였습니다. Silver 결과는 맞았지만 기록이 오해를 부릅니다. 그래서 "이미 반영한 event_id면 중복" 확인을 늦은 도착 확인보다 앞에 넣었습니다.

## 면접에서 나올 만한 질문

**Q. exactly-once를 어떻게 보장했나요?**
Kafka만으로는 보장하지 않았습니다. 저장소가 Postgres라 Kafka 트랜잭션으로는 DB 저장까지 묶을 수 없기 때문입니다. 대신 오프셋을 DB 커밋 뒤에 기록해 메시지를 잃지 않게 하고(at-least-once), DB 저장을 멱등으로 만들어 같은 메시지를 두 번 처리해도 결과가 같게 했습니다. 실제 380건을 처음부터 다시 처리해 Silver가 바뀌지 않는 것을 확인했습니다.

**Q. 순서가 뒤바뀐 이벤트는요?**
같은 경기는 같은 파티션으로 가서 Kafka 안에서는 순서가 지켜집니다. 하지만 출처가 옛 정보를 늦게 보내는 경우까지는 막을 수 없습니다. 그래서 출처의 갱신 시각(published_at)을 비교해, 더 이른 정보는 최신 값을 덮지 못하게 거부하고 사유를 남깁니다.

**Q. 잘못된 메시지 때문에 소비자가 멈추면요?**
형식이 깨지거나 팀을 모르는 메시지는 예외로 멈추지 않고 거부 테이블에 기록한 뒤 다음으로 넘어갑니다. 반대로 DB 연결 오류처럼 일시적인 문제는 묶음 전체를 되돌리고 오프셋을 기록하지 않아, 다시 시작하면 같은 묶음부터 처리합니다.

**Q. 왜 두 소스를 합치나요? 하나만 쓰면 안 되나요?**
football-data.org가 더 빠르고 갱신 시각까지 주지만, 무료 플랜은 대회와 기간이 제한되고 언제든 끊길 수 있습니다. openfootball은 느리지만 공개 도메인이라 과거 시즌 전체를 안정적으로 씁니다. 그래서 최신 정보가 있으면 football-data.org를, 없으면 openfootball을 쓰고 어느 쪽인지 기록했습니다.

## 한계

- 늦은 도착 판단은 경기마다 직전 버전 하나와만 비교합니다. 발표 시각이 같은데 내용만 다르면 정정으로 보고 새 버전을 저장합니다.
- 대조표에 없는 팀의 이벤트는 거부되며, 대조표를 고친 뒤 자동으로 다시 처리하지 않습니다(거부 기록을 보고 재처리해야 함).
- 소비자는 하나뿐입니다. 파티션이 3개라 같은 그룹으로 3개까지 늘릴 수 있지만 지금은 필요 없습니다.
- Gold의 현지 날짜 변환은 Europe/London(EPL)만 씁니다. 다른 리그를 추가하면 리그별 시간대가 필요합니다.
