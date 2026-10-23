# 2a. Kafka 기반

작성: 2026-10-21. 이 단계의 커밋: `569a41b`(구현), `48c84f3`(토픽 삭제 버그 수정).

## 한 줄 요약

수집기와 소비자 사이에 Kafka를 두고, 모든 이벤트가 같은 형식(겉봉투)과 같은 전달 규칙을 쓰도록 공통 기반을 만든 단계입니다.

## 왜 Kafka가 필요한가

이 프로젝트의 데이터는 원래 "시간 순서대로 일어나는 일"입니다. 일정이 바뀌고, 경기가 시작되고, 점수가 정정되고, 배당이 움직입니다. 지금까지는 이런 변화를 덮어쓰지 않고 버전으로 쌓아 왔는데, Kafka의 토픽도 같은 생각(지우지 않고 뒤에 이어 붙이는 기록)입니다. 그래서 둘이 잘 맞습니다.

수집과 반영을 Kafka로 나누면 좋은 점:

- 수집기가 멈춰도 이미 쌓인 이벤트는 소비자가 계속 처리합니다. 소비자가 멈춰도 수집기는 계속 쌓아 두고, 소비자가 다시 켜지면 멈춘 곳부터 이어서 읽습니다.
- 같은 이벤트를 처음부터 다시 흘려 소비자를 검증할 수 있습니다(2d 과거 시즌 재생).
- 나중에 같은 이벤트를 읽는 다른 소비자(예: 알림)를 추가하기 쉽습니다.

## 구성

```text
football-data.org 수집기 ─┐                     ┌─ 소비자 → Silver(Postgres)
과거 시즌 재생기 ─────────┘   (127.0.0.1:45992)  └─ (재생은 별도 sports_replay DB)
```

| 토픽 | 파티션 | 용도 |
|---|---|---|
| `fdorg.matches.v1` | 3 | football-data.org 경기 일정·상태·점수 |
| `replay.matches.v1` | 3 | 과거 시즌 재생(소비자 검증용) |

## 설계 결정과 이유

| 결정 | 위치 | 이유 |
|---|---|---|
| ZooKeeper 없는 KRaft 단일 노드 | `infra/docker-compose.yml`의 `kafka` 서비스 | 개인 PC에서 컨테이너 하나로 충분하고, 최신 Kafka의 기본 방식이라서. 메모리는 512MB로 제한 |
| 접속 주소 3가지(컨테이너끼리 `kafka:9092`, 이 PC에서 `127.0.0.1:45992`, 내부 합의용 9093) | 같은 곳 `KAFKA_LISTENERS`, `KAFKA_ADVERTISED_LISTENERS` | 컨테이너 안(수집기·소비자·Airflow)과 밖(이 PC의 테스트·명령)에서 접속 주소가 다르기 때문. 외부 접속을 막으려고 127.0.0.1에만 엶 |
| 토픽 자동 생성 끔 | 같은 곳 `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | 오타 난 토픽 이름이 조용히 새로 생기는 사고를 막고, 파티션 수를 코드로 정하기 위해 |
| 토픽 이름에 `.v1` | `pipelines/streaming/kafka_io.py`의 `TOPICS` | 이벤트 형식이 크게 바뀌면 새 토픽(v2)으로 옮겨, 옛 소비자와 새 소비자가 섞이지 않게 |
| 메시지 키 = 경기 ID | 수집기의 `publish` 호출 | Kafka는 같은 키를 같은 파티션에 넣고 파티션 안에서는 순서를 지킨다. 같은 경기의 이벤트 순서가 보장된다 |
| 멱등 생산자 + `acks=all` | `kafka_io.producer` | 보내다 네트워크 오류로 다시 보내도 브로커에 같은 메시지가 두 번 쌓이지 않게. 모든 복제본에 저장된 뒤에 성공으로 본다 |
| 소비자 자동 커밋 끔 | `kafka_io.consumer` | "여기까지 읽었다"(오프셋) 기록을 DB 저장이 끝난 뒤에 직접 하기 위해(실제 처리는 2c) |
| `event_id` = 출처·종류·키·내용·발표 시각의 해시 | `pipelines/streaming/events.py`의 `build_event` | 같은 내용을 다시 받아도 같은 ID라 중복을 알아볼 수 있다. 받은 시각(`observed_at`)은 일부러 뺐다 |
| `schema_version` 확인 | `events.decode` | 모르는 형식의 이벤트를 조용히 잘못 해석하지 않고 거부하기 위해 |

## 이벤트 겉봉투 예시

```json
{
  "schema_version": 1,
  "event_id": "ed3367fe5ecb872d…",
  "source_id": "football-data-org",
  "event_type": "match_snapshot",
  "key": "560600",
  "published_at": "2026-10-24T13:50:00+00:00",
  "observed_at": "2026-10-21T00:36:00+00:00",
  "payload": {"external_match_id": "560600", "status": "FINISHED", "ft_home": 2, "ft_away": 1}
}
```

- `published_at`: 출처가 그 내용을 발표·갱신한 시각. 늦게 도착한 옛 정보를 가려내는 기준(2c).
- `observed_at`: 우리가 받은 시각. 기준 시각(as_of) 시점 선택에 쓰인다.

## 2026-10-21 확인 결과

| 확인 | 결과 |
|---|---|
| Kafka 상태 | healthy, 127.0.0.1:45992에서만 접속 |
| `fdorg.matches.v1` | 메시지 380건, 소비자가 안 읽은 메시지(LAG) 0 |
| `replay.matches.v1` | 837건(재생 검증 때 보낸 것) |
| 같은 내용 두 번 보내기 | Kafka에는 2건(offset 0, 1)으로 저장, 받은 시각은 5분 차이, `event_id`는 같음 → 소비자가 중복으로 알아봄 |

## 찾아서 고친 버그

- 증상: 테스트가 쓰고 지웠어야 할 임시 토픽 29개가 브로커에 남아 있었다.
- 원인: 삭제 요청을 보낸 뒤 결과를 기다리지 않았다. 게다가 `admin(...).delete_topics(...)`처럼 관리 클라이언트를 한 줄로 만들어 쓰면, 결과를 기다리는 동안 클라이언트가 메모리에서 정리되어 연결이 끊긴다("Broker handle destroyed").
- 수정: `kafka_io.delete_topics`가 클라이언트를 변수로 붙잡아 두고 삭제 완료까지 기다린다. 테스트와 재생기가 이 함수를 쓴다.
- 확인: 남은 29개를 지우고, 관련 테스트 11개를 다시 돌린 뒤에도 실제 토픽 3개만 남았다.

## 직접 확인하는 방법

저장소 폴더에서, Docker Desktop이 켜져 있을 때 실행합니다.

토픽 목록과 파티션 수:

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

소비자가 어디까지 읽었는지(LAG가 0이면 다 읽은 것):

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group silver-match-writer
```

관련 테스트:

```bash
.\.venv\Scripts\python.exe -m pytest tests/test_streaming_unit.py tests/test_streaming_integration.py -q
```

## 면접에서 나올 만한 질문

**Q. 왜 Kafka를 썼나요? 데이터 양이 작은데요.**
양보다 성격 때문입니다. 일정 변경, 점수 정정, 배당 변경은 모두 시간 순서대로 일어나는 이벤트이고, 이 프로젝트는 처음부터 값을 덮어쓰지 않고 버전으로 쌓는 구조였습니다. Kafka도 지우지 않고 이어 붙이는 기록이라 같은 모델입니다. 수집과 반영을 분리해 한쪽이 멈춰도 다른 쪽이 계속 돌고, 과거 이벤트를 다시 흘려 검증할 수 있다는 점도 이유입니다.

**Q. 메시지가 중복되면 어떻게 하나요?**
두 겹으로 막습니다. 생산자는 멱등 설정이라 재전송해도 브로커에 두 번 쌓이지 않습니다. 그래도 수집기가 같은 내용을 다시 보내는 경우가 있어, 내용으로 만든 `event_id`와 내용 해시로 소비자가 중복을 알아보고 건너뜁니다(2c).

**Q. 순서는 어떻게 보장하나요?**
메시지 키를 경기 ID로 둬서 같은 경기의 이벤트가 같은 파티션에 들어가게 했습니다. Kafka는 파티션 안의 순서를 지킵니다. 그래도 출처 자체가 옛 정보를 늦게 보내는 경우가 있어, 소비자가 `published_at`을 비교해 더 오래된 정보가 최신 값을 덮지 못하게 했습니다(2c).

**Q. 왜 자동 커밋을 껐나요?**
자동 커밋을 켜면 DB에 저장하기 전에 "읽었다"고 기록될 수 있고, 그 사이 프로그램이 죽으면 그 이벤트는 영영 처리되지 않습니다. 그래서 DB 저장을 마친 뒤에 직접 커밋합니다. 대신 같은 이벤트를 두 번 받을 수 있는데, 저장이 중복을 건너뛰도록 만들어 결과가 달라지지 않게 했습니다(at-least-once + 멱등 저장).

**Q. ZooKeeper는 왜 없나요?**
Kafka 3.3부터 쓸 수 있고 4.0부터 기본이 된 KRaft 방식을 썼습니다. Kafka가 스스로 메타데이터를 관리해, 개인 PC에서 컨테이너 하나로 충분합니다.

## 한계

- 단일 브로커라 복제가 없습니다. 개인 PC용이며, 운영 환경이라면 브로커 3대 이상과 복제 계수 3이 필요합니다.
- 이벤트 형식 검사는 필수 필드와 버전만 봅니다. Avro·JSON Schema 같은 스키마 레지스트리는 아직 쓰지 않습니다.
