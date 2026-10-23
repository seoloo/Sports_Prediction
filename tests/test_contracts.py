"""6c 이벤트 계약 단위 테스트(Kafka·DB 없이). 생산자·소비자가 같은 contracts.check를 쓴다."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipelines.ingest.football_data_org import _parse_utc, normalize_match
from pipelines.streaming import contracts, events

SAMPLE = json.loads((Path(__file__).parent / "fixtures" / "fdorg_matches_sample.json").read_text(encoding="utf-8"))
NOW = datetime(2025, 8, 16, 16, 0, tzinfo=timezone.utc)


def snapshot(**changes) -> dict:
    """실제 API 응답 첫 경기로 만든 정상 이벤트. changes는 payload를 바꾼 뒤 event_id까지 다시 계산한다."""
    match = normalize_match(SAMPLE["matches"][0])
    payload = {**match, "competition": "en.1", "season": "2025-26", "raw_file_id": 1, **changes}
    return events.build_event(source_id="football-data-org", event_type="match_snapshot", key=payload["external_match_id"],
                              payload=payload, published_at=_parse_utc(match["last_updated"]), observed_at=NOW)


def violation(event) -> contracts.ContractViolation:
    with pytest.raises(contracts.ContractViolation) as caught:
        contracts.check(event)
    return caught.value


def test_schema_files_are_valid_and_real_api_sample_passes():
    for name in [contracts.ENVELOPE_SCHEMA, *contracts.PAYLOAD_SCHEMAS.values()]:
        contracts.validator(name)   # check_schema: 스키마 파일 자체의 문법
    for raw in SAMPLE["matches"]:
        match = normalize_match(raw)
        contracts.check(events.build_event(
            source_id="football-data-org", event_type="match_snapshot", key=match["external_match_id"],
            payload={**match, "competition": "en.1", "season": "2025-26", "raw_file_id": None},
            published_at=_parse_utc(match["last_updated"]), observed_at=NOW))


@pytest.mark.parametrize("changes, path", [
    ({"status": "2026-10-21 18:00:00Z"}, "/payload/status"),   # 실제로 football-data.org가 보낸 값(라리가 21건)
    ({"ft_home": -1}, "/payload/ft_home"),
    ({"ft_home": "2"}, "/payload/ft_home"),
    ({"status": "FINISHED", "ft_home": None}, "/payload/ft_home"),   # 끝난 경기인데 점수 없음
    ({"competition": "EPL"}, "/payload/competition"),
    ({"utc_kickoff": "2025-08-16 14:00:00"}, "/payload/utc_kickoff"),   # 시간대 없는 시각
])
def test_payload_violations_point_to_the_field(changes, path):
    caught = violation(snapshot(**changes))
    assert caught.reason == "SCHEMA_VIOLATION" and caught.detail["path"] == path
    assert caught.detail["schema"] == "match_snapshot.v1.json"


def test_missing_required_payload_field():
    event = snapshot()
    del event["payload"]["home_team_id"]
    event["event_id"] = events.event_id(**{k: event[k] for k in ("source_id", "event_type", "key", "payload", "published_at")})
    assert violation(event).reason == "SCHEMA_VIOLATION"


def test_envelope_is_strict_and_versions_are_explicit():
    assert violation({**snapshot(), "extra": 1}).detail["path"] == "/"          # 겉봉투에 모르는 필드
    assert violation({**snapshot(), "observed_at": "2025-08-16T16:00:00"}).detail["path"] == "/observed_at"
    assert violation({**snapshot(), "schema_version": 2}).reason == "UNSUPPORTED_SCHEMA_VERSION"
    assert violation(["not", "an", "object"]).reason == "SCHEMA_VIOLATION"
    other = snapshot()
    other["event_type"] = "lineup"
    assert violation(other).reason == "UNKNOWN_EVENT_TYPE"


def test_payload_may_grow_optional_fields_but_not_change_silently():
    contracts.check(snapshot(referee="M. Oliver"))   # 선택 필드 추가는 같은 버전 안에서 허용
    tampered = snapshot()
    tampered["payload"]["ft_home"] = 5               # 해시를 다시 계산하지 않고 내용만 바뀜
    caught = violation(tampered)
    assert caught.reason == "EVENT_ID_MISMATCH" and caught.detail["expected"] != tampered["event_id"]


def test_decode_reports_invalid_json_as_contract_violation():
    for raw in (b"{not json", b"\xff\xfe"):
        with pytest.raises(contracts.ContractViolation, match="INVALID_JSON"):
            events.decode(raw)
    assert events.decode(events.encode(snapshot()))["event_type"] == "match_snapshot"
