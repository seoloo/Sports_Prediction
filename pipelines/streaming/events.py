"""Kafka 이벤트 형식.

모든 이벤트는 같은 겉봉투(envelope)를 쓴다.
- event_id: 출처·종류·키·내용으로 만든 해시. 같은 내용을 두 번 보내면 event_id도 같아 중복을 알아볼 수 있다.
- published_at: 출처가 그 내용을 발표·갱신한 시각(모르면 None). observed_at: 우리가 받은 시각.
  이 두 시각이 기준 시각(as_of) 선택과 "늦게 도착한 옛 이벤트" 판단에 쓰인다.
- schema_version: 형식이 바뀌면 올린다. 소비자는 모르는 버전을 거부한다.
"""
import hashlib
import json
from datetime import datetime

SCHEMA_VERSION = 1
REQUIRED_FIELDS = ("schema_version", "event_id", "source_id", "event_type", "key", "published_at", "observed_at", "payload")


def canonical_json(value) -> str:
    """항상 같은 문자열이 나오는 JSON(키 정렬, 공백 없음)."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def build_event(*, source_id: str, event_type: str, key: str, payload: dict,
                published_at: datetime | None, observed_at: datetime) -> dict:
    """이벤트 dict를 만든다. event_id는 관찰 시각을 빼고 계산해, 같은 내용을 다시 받아도 같은 값이 된다."""
    content = {"source_id": source_id, "event_type": event_type, "key": key, "payload": payload,
               "published_at": published_at.isoformat() if published_at else None}
    return {"schema_version": SCHEMA_VERSION,
            "event_id": hashlib.sha256(canonical_json(content).encode()).hexdigest(),
            **content, "observed_at": observed_at.isoformat()}


def encode(event: dict) -> bytes:
    return canonical_json(event).encode("utf-8")


def decode(raw: bytes) -> dict:
    """바이트 → 이벤트 dict. 형식이 틀리면 ValueError(소비자가 거부 사유로 기록한다)."""
    try:
        event = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("INVALID_JSON") from exc
    if not isinstance(event, dict) or any(field not in event for field in REQUIRED_FIELDS):
        raise ValueError("MISSING_FIELDS")
    if event["schema_version"] != SCHEMA_VERSION:
        raise ValueError("UNSUPPORTED_SCHEMA_VERSION")
    return event
