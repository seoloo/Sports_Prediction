"""이벤트 계약(로드맵 6c): Kafka 메시지가 지켜야 할 형식을 JSON Schema로 정하고, 생산자와 소비자가 같은 파일로 검사한다.

- 겉봉투(schemas/envelope.v1.json)는 엄격하다. 모르는 필드도 거부한다.
- 내용(payload)은 event_type과 schema_version으로 스키마를 고른다(schemas/<event_type>.v<N>.json).
  선택 필드를 더하는 것은 같은 버전 안에서 허용하고(소비자는 모르는 필드를 무시), 필수 필드·타입·허용값을 바꾸면 버전을 올린다.
- event_id는 내용으로 다시 계산해 맞춰 본다. 내용이 중간에 바뀌었거나 생산자가 해시를 잘못 만들면 거부한다.
검사에 걸린 메시지는 Silver에 들어가지 않는다. 소비자는 사유를 남기고 원본을 오류 큐(<토픽>.dlq)로 옮긴다.
"""
import json
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import best_match

from . import events

SCHEMA_DIR = Path(__file__).with_name("schemas")
ENVELOPE_SCHEMA = "envelope.v1.json"
# (event_type, schema_version) → payload 스키마. 새 이벤트 종류나 버전은 여기에 등록해야 받는다.
PAYLOAD_SCHEMAS = {("match_snapshot", 1): "match_snapshot.v1.json"}


class ContractViolation(ValueError):
    """계약 위반. reason은 거부 사유 코드, detail은 어디가 왜 틀렸는지(경로·메시지·스키마)."""

    def __init__(self, reason: str, detail: dict | None = None):
        super().__init__(reason)
        self.reason, self.detail = reason, detail or {}


@lru_cache
def validator(name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMA_DIR / name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)   # 스키마 파일 자체가 틀리면 바로 실패
    return Draft202012Validator(schema)


def _validate(name: str, value, prefix: str) -> None:
    error = best_match(validator(name).iter_errors(value))
    if error is not None:
        path = prefix + "".join(f"/{part}" for part in error.absolute_path)
        raise ContractViolation("SCHEMA_VIOLATION", {"schema": name, "path": path or "/", "message": error.message[:300]})


def check(event) -> None:
    """이벤트 dict가 계약을 지키는지 검사한다. 어기면 ContractViolation."""
    if not isinstance(event, dict):
        raise ContractViolation("SCHEMA_VIOLATION", {"schema": ENVELOPE_SCHEMA, "path": "/", "message": "object가 아님"})
    version = event.get("schema_version")
    if version is not None and version != events.SCHEMA_VERSION:
        raise ContractViolation("UNSUPPORTED_SCHEMA_VERSION", {"schema_version": version})
    _validate(ENVELOPE_SCHEMA, event, "")
    name = PAYLOAD_SCHEMAS.get((event["event_type"], version))
    if name is None:
        raise ContractViolation("UNKNOWN_EVENT_TYPE", {"event_type": event["event_type"], "schema_version": version})
    _validate(name, event["payload"], "/payload")
    expected = events.event_id(source_id=event["source_id"], event_type=event["event_type"], key=event["key"],
                               payload=event["payload"], published_at=event["published_at"])
    if expected != event["event_id"]:
        raise ContractViolation("EVENT_ID_MISMATCH", {"event_id": event["event_id"], "expected": expected})
