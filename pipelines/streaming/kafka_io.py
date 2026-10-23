"""Kafka 연결·토픽·생산자·소비자 공통 설정.

전달 보장 방식
- 생산자: 멱등 생산자(enable.idempotence)와 acks=all. 재전송이 일어나도 브로커에 같은 메시지가 두 번 쌓이지 않는다.
- 소비자: 자동 커밋을 끄고, DB 트랜잭션을 커밋한 다음에 오프셋을 커밋한다(at-least-once).
  DB 저장 직후 죽으면 같은 메시지를 다시 받지만, Silver 저장은 내용 해시로 중복을 건너뛰므로 결과는 한 번 처리한 것과 같다.
"""
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from ..config import Settings, load_settings

# 토픽 이름 → 파티션 수. 같은 경기 이벤트는 같은 키(경기 ID)로 보내 한 파티션 안에서 순서가 유지된다.
TOPICS = {
    "fdorg.matches.v1": 3,        # football-data.org 경기 일정·상태·점수
    "replay.matches.v1": 3,       # 과거 시즌 재생(소비자 검증용)
    # 오류 큐(dead letter, 6c): 계약을 어겼거나 지금은 처리할 수 없는 메시지의 원본. 순서가 중요하지 않아 파티션 1개.
    "fdorg.matches.v1.dlq": 1,
    "replay.matches.v1.dlq": 1,
}
DLQ_SUFFIX = ".dlq"


def dlq_topic(topic: str) -> str:
    """원래 토픽 → 그 토픽의 오류 큐 이름."""
    return topic + DLQ_SUFFIX


def admin(settings: Settings | None = None) -> AdminClient:
    return AdminClient({"bootstrap.servers": (settings or load_settings()).kafka_bootstrap})


def ensure_topics(settings: Settings | None = None, topics: dict | None = None) -> list[str]:
    """정해진 토픽이 없으면 만든다. 새로 만든 토픽 이름 목록을 돌려준다(이미 있으면 그대로 둔다)."""
    client = admin(settings)
    existing = set(client.list_topics(timeout=10).topics)
    wanted = [NewTopic(name, num_partitions=partitions, replication_factor=1)
              for name, partitions in (topics or TOPICS).items() if name not in existing]
    for name, future in (client.create_topics(wanted) if wanted else {}).items():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != 36:  # TOPIC_ALREADY_EXISTS: 다른 프로세스가 먼저 만든 경우
                raise
    return [topic.topic for topic in wanted]


def delete_topics(names: list[str], settings: Settings | None = None, timeout: float = 30) -> None:
    """토픽을 지우고 삭제가 끝날 때까지 기다린다(테스트·재생용 임시 토픽 정리).

    AdminClient.delete_topics는 요청만 보내고 바로 돌아오므로, 결과를 기다리지 않으면
    프로그램이 먼저 끝나 삭제가 일어나지 않을 수 있다.
    클라이언트를 변수에 붙잡아 두는 것도 중요하다. admin(...).delete_topics(...)처럼 한 줄로 쓰면
    결과를 기다리는 동안 클라이언트가 정리되어 "Broker handle destroyed" 오류로 삭제가 끝나지 않는다.
    """
    if not names:
        return
    client = admin(settings)
    for name, future in client.delete_topics(names, operation_timeout=timeout).items():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != 3:  # UNKNOWN_TOPIC_OR_PART: 이미 없는 토픽
                raise


def producer(settings: Settings | None = None, client_id: str = "sports-pipeline") -> Producer:
    return Producer({"bootstrap.servers": (settings or load_settings()).kafka_bootstrap,
                     "client.id": client_id, "enable.idempotence": True, "acks": "all",
                     "linger.ms": 20, "compression.type": "lz4"})


def publish(kafka_producer: Producer, topic: str, key: str | bytes | None, value: bytes,
            headers: dict[str, str] | None = None) -> None:
    """메시지 하나를 보낸다(비동기). 실패하면 flush 때 예외로 드러나도록 전달 결과를 확인한다."""
    errors = []
    kafka_producer.produce(topic, key=key.encode("utf-8") if isinstance(key, str) else key, value=value,
                           headers=[(name, text.encode("utf-8")) for name, text in (headers or {}).items()] or None,
                           on_delivery=lambda err, msg: errors.append(err) if err else None)
    kafka_producer.poll(0)
    if errors:
        raise KafkaException(errors[0])


def flush(kafka_producer: Producer, timeout: float = 30) -> None:
    """보낸 메시지가 모두 브로커에 저장될 때까지 기다린다. 남은 메시지가 있으면 오류."""
    remaining = kafka_producer.flush(timeout)
    if remaining:
        raise KafkaException(f"{remaining}건을 보내지 못했습니다.")


def consumer(group_id: str, topics: list[str], settings: Settings | None = None) -> Consumer:
    """자동 커밋을 끈 소비자. 처음 붙는 그룹은 토픽의 처음부터 읽는다."""
    kafka_consumer = Consumer({"bootstrap.servers": (settings or load_settings()).kafka_bootstrap,
                               "group.id": group_id, "enable.auto.commit": False,
                               "auto.offset.reset": "earliest", "isolation.level": "read_committed"})
    kafka_consumer.subscribe(topics)
    return kafka_consumer
