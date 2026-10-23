"""Kafka 이벤트 형식.

모든 이벤트는 같은 겉봉투(envelope)를 쓴다.
- event_id: 출처·종류·키·내용으로 만든 해시. 같은 내용을 두 번 보내면 event_id도 같아 중복을 알아볼 수 있다.
- published_at: 출처가 그 내용을 발표·갱신한 시각(모르면 None). observed_at: 우리가 받은 시각.
  이 두 시각이 기준 시각(as_of) 선택과 "늦게 도착한 옛 이벤트" 판단에 쓰인다.
- schema_version: 형식이 바뀌면 올린다. 소비자는 모르는 버전을 거부한다.
형식 자체는 JSON Schema 계약으로 검사한다(contracts.py, schemas/, 6c).
"""
import hashlib
import json
from datetime import datetime

SCHEMA_VERSION = 1


def canonical_json(value) -> str:
    """항상 같은 문자열이 나오는 JSON(키 정렬, 공백 없음)."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def event_id(*, source_id: str, event_type: str, key: str, payload: dict, published_at: str | None) -> str:
    """관찰 시각을 뺀 내용의 해시. 같은 내용을 다시 받아도 같은 값이 된다(소비자가 다시 계산해 맞춰 본다)."""
    content = {"source_id": source_id, "event_type": event_type, "key": key, "payload": payload,
               "published_at": published_at}
    return hashlib.sha256(canonical_json(content).encode()).hexdigest()


def build_event(*, source_id: str, event_type: str, key: str, payload: dict,
                published_at: datetime | None, observed_at: datetime) -> dict:
    """이벤트 dict를 만든다."""
    content = {"source_id": source_id, "event_type": event_type, "key": key, "payload": payload,
               "published_at": published_at.isoformat() if published_at else None}
    return {"schema_version": SCHEMA_VERSION, "event_id": event_id(**content), **content,
            "observed_at": observed_at.isoformat()}


def encode(event: dict) -> bytes:
    return canonical_json(event).encode("utf-8")


def decode(raw: bytes) -> dict:
    """바이트 → 계약을 지킨 이벤트 dict. 어기면 ContractViolation(ValueError, 소비자가 사유를 기록하고 오류 큐로 옮긴다)."""
    from .contracts import ContractViolation, check
    try:
        event = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractViolation("INVALID_JSON", {"message": str(exc)[:300]}) from exc
    check(event)
    return event
