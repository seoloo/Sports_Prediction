# 6c. Kafka 이벤트 스키마 계약과 오류 큐(dead letter)

작성: 2026-10-24. 상태: **완료.** 6단계(데이터 신뢰성 강화)의 마지막 단계입니다.

## 한 줄 요약

Kafka로 오가는 경기 이벤트의 모양을 **JSON Schema 파일로 계약**했습니다. 생산자(football-data.org 수집기)는 보내기 전에, 소비자는 받은 뒤에 **같은 파일로** 검사합니다. 계약을 어긴 메시지는 Silver에 들어가지 않고, 사유를 남긴 뒤 원본 바이트 그대로 **오류 큐(`<토픽>.dlq`)**로 옮깁니다. 팀 대조표에 없어서 거부된 메시지는 대조표를 고친 뒤 오류 큐에서 다시 보낼 수 있습니다.

계약을 만들고 실제 Bronze 원본 29,428경기에 돌려 보자 **football-data.org가 상태 칸에 날짜를 보낸 경기 21건**이 나왔습니다. 그중 6건은 이미 그 값 그대로 Silver에 들어가 있었습니다. 이제는 들어오는 길에서 막힙니다.

## 흐름

```text
football-data.org 수집기
  └ 경기마다 이벤트 생성 → contracts.check(생산자 쪽)
        ├ 통과 → fdorg.matches.v1
        └ 위반 → 보내지 않음, silver.quality_issues(CONTRACT_VIOLATION, 경로·사유)

match_consumer (묶음 하나 = DB 트랜잭션 하나)
  └ events.decode → contracts.check(소비자 쪽: 다른 생산자·재생기·사람이 넣은 메시지도 검사)
        ├ 통과 → 팀 대조 → Silver 버전(중복·늦은 도착 규칙은 그대로)
        └ 위반·UNMAPPED_TEAM → silver.stream_rejections + 원본을 fdorg.matches.v1.dlq로(헤더에 사유·원래 위치·detail)
  └ 오류 큐 flush 확인 → DB 커밋 → 오프셋 커밋

python -m pipelines.streaming.dlq summary   사유별 건수·예(원래 위치가 같으면 한 건)
python -m pipelines.streaming.dlq replay    대조표를 고친 뒤 UNMAPPED_TEAM 원본을 원래 토픽으로 다시
```

## 계약 내용

| 파일 | 규칙 | 바꿀 때 |
|---|---|---|
| `schemas/envelope.v1.json`(겉봉투) | 8개 필드 필수, 모르는 필드 거부, `event_id`는 64자리 16진수, 시각은 **시간대가 있는** ISO 8601만 | 필드를 더하거나 빼면 `schema_version`을 올린다 |
| `schemas/match_snapshot.v1.json`(내용) | 경기 ID·리그 코드(`en.1` 형식)·시즌(`2025-26`)·상태(허용 목록 11개)·팀 ID 필수, 점수는 0 이상 정수 또는 null, **끝난 경기는 점수 필수** | 선택 필드 추가는 v1 안에서 허용(소비자는 무시). 필수·타입·허용값을 바꾸면 v2 |
| `contracts.py` | `(event_type, schema_version)` → 스키마 등록표. 내용으로 `event_id`를 다시 계산해 맞춰 봄 | 새 이벤트 종류는 등록표에 넣어야 받는다 |

거부 사유: `INVALID_JSON`, `SCHEMA_VIOLATION`(어느 경로가 왜), `UNSUPPORTED_SCHEMA_VERSION`, `UNKNOWN_EVENT_TYPE`, `EVENT_ID_MISMATCH`. 이 다섯 개와 `UNMAPPED_TEAM`이 오류 큐로 갑니다. `STALE_EVENT`(늦게 온 옛 정보)는 규칙대로 버린 정상 동작이라 오류 큐에 넣지 않습니다.

## 바꾼 것

| 변경 | 위치 | 이유 |
|---|---|---|
| 계약 파일·검사기 | `pipelines/streaming/schemas/`, `contracts.py` | 형식 규칙을 코드 곳곳의 `KeyError` 처리가 아니라 한 파일에. 생산자·소비자가 같은 규칙 |
| decode가 계약 검사 | `events.decode` | 소비자는 검사를 통과한 이벤트만 본다. 필수 필드 존재를 따로 확인하던 코드(`MISSING_PAYLOAD_FIELD`)를 지움 |
| 생산자 쪽 검사 | `football_data_org.poll_once` | 깨진 이벤트를 토픽에 넣지 않는다. 어긴 경기만 빼고 나머지는 정상 발행, 실행 통계에 `contract_violations` |
| 오류 큐 | `match_consumer.run`, `kafka_io.dlq_topic` | 거부한 원본을 증거로 남기고 다시 보낼 수 있게. 헤더: `dlq.reason`·`dlq.source_topic/partition/offset`·`dlq.detail`·`dlq.failed_at` |
| 오류 큐 도구 | `pipelines/streaming/dlq.py` | 소비자 그룹 없이 파티션을 직접 지정해 읽음(커밋하지 않아 몇 번 읽어도 같음) |
| 의존성 | `jsonschema 4.26.0`(PC·CI·수집기 이미지) | Airflow는 스트리밍 코드를 쓰지 않아 넣지 않음 |

## 사용한 기술

| 기술 | 어디에 | 어떻게 썼나 |
|---|---|---|
| JSON Schema 2020-12 + `jsonschema`(`Draft202012Validator`, `best_match`) | `contracts.py` | 스키마 파일 자체도 `check_schema`로 검사. 여러 오류 중 가장 관련 있는 하나의 경로·메시지를 사유로 |
| 조건부 스키마(`if`/`then`) | `match_snapshot.v1.json` | "상태가 FINISHED면 점수가 정수" 같은 필드 간 규칙 |
| 스키마 버전·호환 규칙 | 겉봉투 엄격 / 내용은 추가만 허용 | 생산자가 선택 필드를 더해도 소비자가 깨지지 않고, 의미가 바뀌는 변경은 버전을 올려 명시적으로 거부되게 |
| Kafka 오류 큐(dead letter topic) + 메시지 헤더 | `<토픽>.dlq`, `kafka_io.publish(headers=…)` | 값은 원본 바이트 그대로, 맥락은 헤더에 |
| 순서 보장: 오류 큐 flush → DB 커밋 → 오프셋 커밋 | `match_consumer.run` | 중간에 죽으면 묶음 전체를 다시 받는다. 거부 메시지가 사라지지 않음(오류 큐는 최소 한 번) |
| `Consumer.assign` + `get_watermark_offsets` | `dlq.read_all` | 그룹 오프셋을 건드리지 않고 토픽 전체를 처음부터 끝까지 |
| 내용 해시 재계산 | `events.event_id` | 생산자와 소비자가 같은 함수로 계산해 맞춰 봄 |

## 확인 결과(2026-10-24)

| 확인 | 결과 |
|---|---|
| 실제 Bronze 원본 79개 파일, 29,428경기에 계약 적용 | 위반 21건, 모두 `/payload/status`(라리가, 상태 칸에 날짜). 나머지는 통과 |
| 이미 Silver에 들어간 같은 값 | 6개 버전(수정 불가 기록이라 그대로 두고 [장애 기록](../interview/error_handling.md) 31번으로) |
| 계약 단위 테스트(`tests/test_contracts.py`) | 11개: 실제 API 샘플 통과, 위반 경로 6가지, 필수 필드 누락, 겉봉투 엄격·버전·종류, 선택 필드 추가 허용과 내용 변조(`EVENT_ID_MISMATCH`) |
| 통합 테스트(Kafka·Postgres) | 깨진 JSON·대조표 밖 팀·상태 칸에 날짜 → 3건 오류 큐, 원본 바이트 그대로, 다시 처리해도 요약은 3건 / 대조표를 고치고 replay → 같은 그룹이 그 메시지만 새로 읽어 Silver에 반영 / 수집기가 계약 위반 경기는 보내지 않고 품질 문제로 기록 |
| 상시 컨테이너 | 수집기·소비자 이미지를 다시 만들어 재시작, 실행 통계에 `contract_violations` 표시, 오류 큐 토픽 2개 생성 |

## 면접에서 나올 만한 질문

**Q. 스키마 레지스트리(Confluent Schema Registry)를 왜 안 썼나요?**
레지스트리는 스키마를 중앙에서 버전 관리하고 생산자·소비자가 ID로 받아 쓰게 해 줍니다. 생산자가 하나, 이벤트 종류가 하나인 지금은 스키마 파일을 코드와 같은 저장소에 두고 같은 커밋으로 바꾸는 것이 더 단순하고 리뷰하기 쉽습니다. 계약의 핵심(엄격한 겉봉투, 버전, 호환 규칙, 양쪽 검사)은 같습니다. 팀이나 생산자가 늘어나 배포가 따로 움직이면 레지스트리로 옮길 이유가 생깁니다.

**Q. 생산자가 이미 검사하는데 소비자가 또 검사하는 이유는?**
토픽에 쓸 수 있는 것은 그 수집기만이 아닙니다. 과거 시즌 재생기, 사람이 손으로 넣은 메시지, 앞으로 생길 다른 생산자가 있습니다. 소비자 검사는 Silver를 지키는 마지막 문이고, 생산자 검사는 깨진 이벤트가 토픽에 쌓이지 않게 하는 첫 문입니다.

**Q. 오류 큐 메시지가 중복될 수 있지 않나요?**
네. 토픽을 처음부터 다시 처리하면 같은 메시지가 오류 큐에 다시 들어갑니다. 정확히 한 번을 만들려면 Kafka 트랜잭션으로 오류 큐 쓰기와 오프셋을 묶어야 하는데, 최종 저장소가 Postgres라 그것만으로는 끝나지 않습니다. 대신 원래 위치(토픽·파티션·오프셋)를 헤더에 넣어 읽는 쪽이 중복을 접고, 다시 보내도 소비자가 event_id로 거릅니다.

**Q. 상태 칸에 날짜가 온 경기는 SCHEDULED로 고쳐 넣으면 되지 않나요?**
그럴듯하지만 추측입니다. 그 값이 "연기 후 새 일정"인지 "아직 정해지지 않음"인지 출처가 말해 주지 않습니다. 이 프로젝트는 모르는 값을 만들어 넣지 않는다는 원칙이라, 막고 기록한 뒤 출처가 정상 값을 다시 보낼 때 반영합니다.

## 한계

- **생산자 쪽 검사는 football-data.org 수집기만입니다.** 과거 시즌 재생기는 검증 도구라 소비자 쪽 검사에 맡깁니다.
- **이미 Silver에 들어간 6개 버전은 고치지 않았습니다.** 수정 불가 기록이고, 그 경기의 최신 상태는 다음 정상 갱신으로 덮입니다.
- **오류 큐는 최소 한 번 저장입니다**(위 질문). 보관 기간은 Kafka 기본값(7일)입니다. 그보다 오래 남길 증거는 `silver.stream_rejections`에 있습니다(원본 앞 500바이트와 detail).
