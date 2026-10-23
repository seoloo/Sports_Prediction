"""오류 큐(dead letter queue) 보기·다시 보내기(로드맵 6c).

소비자(match_consumer)는 계약을 어긴 메시지와 팀 대조표에 없는 메시지의 원본을 <토픽>.dlq에 옮긴다.
- summary: 사유별 건수와 예. 같은 원래 위치(토픽·파티션·오프셋)를 여러 번 옮겼어도 한 건으로 센다
  (토픽을 처음부터 다시 처리하면 같은 메시지가 다시 들어간다. 오류 큐는 최소 한 번 저장이다).
- replay: 고친 뒤(예: 대조표에 팀 추가) 고른 사유의 원본을 원래 토픽으로 다시 보낸다. 소비자는 event_id로 중복을 거르므로
  여러 번 보내도 Silver 결과는 같다. 계약 위반 원본은 다시 보내도 또 거부되므로 기본으로는 UNMAPPED_TEAM만 보낸다.
읽기는 소비자 그룹 없이 파티션을 직접 지정해(assign) 처음부터 끝까지 읽는다. 오프셋을 커밋하지 않아 몇 번 읽어도 같다.
실행
  python -m pipelines.streaming.dlq summary [--topic fdorg.matches.v1.dlq]
  python -m pipelines.streaming.dlq replay [--topic fdorg.matches.v1.dlq] [--reason UNMAPPED_TEAM]
"""
import argparse
import json
from collections import Counter

from confluent_kafka import Consumer, TopicPartition

from ..config import load_settings
from . import kafka_io

DEFAULT_TOPIC = kafka_io.dlq_topic("fdorg.matches.v1")
REPLAYABLE = ("UNMAPPED_TEAM",)


def read_all(topic: str, settings=None, timeout: float = 10) -> list[dict]:
    """오류 큐의 모든 메시지(원래 위치가 같은 것은 처음 하나만). 각 항목: 헤더 + key + value(원본 바이트)."""
    settings = settings or load_settings()
    reader = Consumer({"bootstrap.servers": settings.kafka_bootstrap, "group.id": "dlq-reader",
                       "enable.auto.commit": False})
    try:
        metadata = reader.list_topics(topic, timeout=timeout).topics.get(topic)
        if metadata is None or metadata.error is not None:
            return []
        ranges = {}
        for partition in metadata.partitions:
            low, high = reader.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout)
            if high > low:
                ranges[partition] = (low, high)
        reader.assign([TopicPartition(topic, partition, low) for partition, (low, _) in ranges.items()])
        found, seen, remaining = [], set(), sum(high - low for low, high in ranges.values())
        while remaining > 0:
            message = reader.poll(timeout)
            if message is None:
                raise TimeoutError(f"{topic}에서 {remaining}건을 더 읽지 못했습니다.")
            if message.error():
                raise RuntimeError(message.error())
            remaining -= 1
            headers = {name: value.decode("utf-8") for name, value in (message.headers() or [])}
            position = (headers.get("dlq.source_topic"), headers.get("dlq.source_partition"), headers.get("dlq.source_offset"))
            if position in seen:
                continue
            seen.add(position)
            found.append({**headers, "key": message.key(), "value": message.value()})
        return found
    finally:
        reader.close()


def summary(topic: str = DEFAULT_TOPIC, settings=None) -> dict:
    messages = read_all(topic, settings)
    examples = {}
    for item in messages:
        examples.setdefault(item["dlq.reason"], {
            "source": f"{item['dlq.source_topic']}[{item['dlq.source_partition']}]@{item['dlq.source_offset']}",
            "detail": json.loads(item.get("dlq.detail") or "{}")})
    return {"topic": topic, "messages": len(messages), "by_reason": dict(Counter(item["dlq.reason"] for item in messages)),
            "examples": examples}


def replay(topic: str = DEFAULT_TOPIC, reasons=REPLAYABLE, settings=None) -> dict:
    """고른 사유의 원본을 원래 토픽으로 다시 보낸다. 보낸 건수를 원래 토픽별로 돌려준다."""
    settings = settings or load_settings()
    chosen = [item for item in read_all(topic, settings) if item["dlq.reason"] in reasons]
    sent = Counter()
    if chosen:
        producer = kafka_io.producer(settings, client_id="dlq-replay")
        for item in chosen:
            kafka_io.publish(producer, item["dlq.source_topic"], item["key"], item["value"],
                             {"dlq.replayed_from": f"{topic}:{item['dlq.source_partition']}:{item['dlq.source_offset']}"})
            sent[item["dlq.source_topic"]] += 1
        kafka_io.flush(producer)
    return {"topic": topic, "reasons": list(reasons), "replayed": dict(sent)}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Kafka 오류 큐(dead letter) 보기·다시 보내기")
    parser.add_argument("command", choices=["summary", "replay"])
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--reason", action="append", help=f"replay할 사유(여러 번 지정 가능). 기본: {', '.join(REPLAYABLE)}")
    args = parser.parse_args(argv)
    result = summary(args.topic) if args.command == "summary" else replay(args.topic, tuple(args.reason or REPLAYABLE))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
