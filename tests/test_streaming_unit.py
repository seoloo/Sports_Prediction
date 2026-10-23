"""스트리밍·football-data.org 수집기 단위 테스트. Kafka·DB·네트워크 없이 실행된다."""
import io
import json
import urllib.error
from datetime import datetime, timedelta, timezone
from email.message import Message
from pathlib import Path

import pytest

from pipelines.ingest.football_data_org import (IDLE_INTERVAL, LIVE_INTERVAL, MATCHDAY_INTERVAL, RateLimiter, fetch_json,
                                                next_poll_interval, normalize_match, season_start_year)
from pipelines.streaming import events
from pipelines.streaming.match_consumer import content_hash

SAMPLE = json.loads((Path(__file__).parent / "fixtures" / "fdorg_matches_sample.json").read_text(encoding="utf-8"))
NOW = datetime(2025, 8, 16, 12, 0, tzinfo=timezone.utc)


class FakeClock:
    """실제로 기다리지 않는 시계. sleep을 부르면 시간만 앞으로 간다."""
    def __init__(self):
        self.now, self.slept = 0.0, []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(round(seconds, 3))
        self.now += seconds


def test_normalize_keeps_missing_scores_as_none():
    finished, scheduled = (normalize_match(m) for m in SAMPLE["matches"][:2])
    assert (finished["ft_home"], finished["ft_away"], finished["ht_home"]) == (2, 1, 1)
    assert finished["home_team_id"] == "1" and finished["external_match_id"] == "9001"
    assert scheduled["ft_home"] is None and scheduled["status"] == "TIMED"
    assert season_start_year("2026-27") == 2026


@pytest.mark.parametrize("matches, now, expected", [
    ([{"status": "IN_PLAY", "utc_kickoff": "2025-08-16T11:00:00Z"}], NOW, LIVE_INTERVAL),
    ([{"status": "TIMED", "utc_kickoff": "2025-08-16T12:10:00Z"}], NOW, LIVE_INTERVAL),           # 10분 뒤 시작
    ([{"status": "TIMED", "utc_kickoff": "2025-08-16T10:00:00Z"}], NOW, LIVE_INTERVAL),           # 2시간 전 시작, 종료 처리 전
    ([{"status": "TIMED", "utc_kickoff": "2025-08-17T09:00:00Z"}], NOW, MATCHDAY_INTERVAL),       # 21시간 뒤
    ([{"status": "TIMED", "utc_kickoff": "2025-08-20T09:00:00Z"}], NOW, IDLE_INTERVAL),
    ([{"status": "FINISHED", "utc_kickoff": "2025-08-16T11:30:00Z"}], NOW, IDLE_INTERVAL),
    ([], NOW, IDLE_INTERVAL),
])
def test_next_poll_interval(matches, now, expected):
    assert next_poll_interval(matches, now) == expected


def test_rate_limiter_never_exceeds_ten_per_minute():
    clock = FakeClock()
    limiter = RateLimiter(10, clock=clock, sleep=clock.sleep)
    times = []
    for _ in range(25):
        limiter.wait()
        times.append(clock.now)
    # 어떤 60초 구간에도 10회를 넘지 않는다.
    assert all(sum(1 for t in times if start <= t < start + 60) <= 10 for start in times)
    assert clock.slept[0] == 60.0


def test_rate_limiter_obeys_server_headers():
    clock = FakeClock()
    limiter = RateLimiter(10, clock=clock, sleep=clock.sleep)
    limiter.update_from_headers({"X-Requests-Available-Minute": "0", "X-RequestCounter-Reset": "42"})
    limiter.wait()
    assert clock.slept == [42]


class FakeResponse(io.BytesIO):
    def __init__(self, body, headers):
        super().__init__(body)
        self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def http_error(code, headers=None):
    message = Message()
    for key, value in (headers or {}).items():
        message[key] = value
    return urllib.error.HTTPError("https://api.football-data.org/v4/x", code, "error", message, io.BytesIO(b"{}"))


def test_fetch_retries_after_429_using_reset_header():
    clock = FakeClock()
    limiter = RateLimiter(10, clock=clock, sleep=clock.sleep)
    calls = []

    def opener(request, timeout):
        calls.append(request.get_header("X-auth-token"))
        if len(calls) == 1:
            raise http_error(429, {"X-RequestCounter-Reset": "17"})
        return FakeResponse(b'{"ok": true}', {"X-Requests-Available-Minute": "8"})

    body, headers = fetch_json("/competitions/PL/matches", "test-key", limiter, opener=opener)
    assert json.loads(body) == {"ok": True} and calls == ["test-key", "test-key"]
    assert clock.slept == [17]


def test_fetch_raises_other_http_errors_and_gives_up_after_retries():
    clock = FakeClock()
    limiter = RateLimiter(10, clock=clock, sleep=clock.sleep)
    with pytest.raises(urllib.error.HTTPError) as forbidden:
        fetch_json("/x", "k", limiter, opener=lambda r, timeout: (_ for _ in ()).throw(http_error(403)))
    assert forbidden.value.code == 403
    with pytest.raises(urllib.error.HTTPError) as limited:
        fetch_json("/x", "k", limiter, opener=lambda r, timeout: (_ for _ in ()).throw(http_error(429)), max_attempts=2)
    assert limited.value.code == 429


def test_event_id_depends_on_content_not_observation_time():
    common = dict(source_id="s", event_type="match_snapshot", key="9001", payload={"a": 1},
                  published_at=NOW)
    first = events.build_event(**common, observed_at=NOW)
    later = events.build_event(**common, observed_at=NOW + timedelta(hours=1))
    changed = events.build_event(**{**common, "payload": {"a": 2}}, observed_at=NOW)
    assert first["event_id"] == later["event_id"] != changed["event_id"]
    assert json.loads(events.encode(first)) == first   # 계약 검사를 거친 decode는 test_contracts에서


@pytest.mark.parametrize("raw, reason", [
    (b"not json", "INVALID_JSON"), (b'{"a": 1}', "SCHEMA_VIOLATION"), (b"\xff\xfe", "INVALID_JSON"),
])
def test_decode_rejects_bad_messages(raw, reason):
    with pytest.raises(ValueError, match=reason):
        events.decode(raw)


def test_decode_rejects_unknown_schema_version():
    event = events.build_event(source_id="s", event_type="t", key="k", payload={}, published_at=None, observed_at=NOW)
    with pytest.raises(ValueError, match="UNSUPPORTED_SCHEMA_VERSION"):
        events.decode(events.encode({**event, "schema_version": 99}))


def test_content_hash_ignores_last_updated():
    match = normalize_match(SAMPLE["matches"][0])
    assert content_hash(match) == content_hash({**match, "last_updated": "2099-01-01T00:00:00Z"})
    assert content_hash(match) != content_hash({**match, "ft_home": 3})


def test_inject_faults_is_deterministic_and_puts_late_events_after_results():
    from pipelines.streaming.replay import inject_faults
    ordered = []
    for i in range(40):
        ordered.append((NOW + timedelta(hours=i), f"m{i}", f"s{i}".encode(), "SCHEDULED"))
        ordered.append((NOW + timedelta(days=10, hours=i), f"m{i}", f"f{i}".encode(), "FINISHED"))
    ordered.sort(key=lambda item: item[0])
    first = inject_faults(ordered, seed=3, duplicate_rate=0.2, late_rate=0.2)
    assert first == inject_faults(ordered, seed=3, duplicate_rate=0.2, late_rate=0.2)
    sent, duplicates, late = first
    assert len(sent) == len(ordered) + duplicates and duplicates > 0 and late > 0
    for key in {item[1] for item in ordered}:
        positions = [i for i, item in enumerate(sent) if item[1] == key]
        statuses = [sent[i][3] for i in positions]
        if statuses.index("FINISHED") < statuses.index("SCHEDULED"):  # 늦게 도착한 일정 발표
            late -= 1
    assert late == 0
    assert inject_faults(ordered, seed=3, duplicate_rate=0, late_rate=0) == (ordered, 0, 0)


class SlowJoinConsumer:
    """처음 join_polls번은 파티션 배정 전(빈 응답)이고, 그 뒤 메시지 없이 빈 응답만 주는 가짜 소비자."""
    def __init__(self, join_polls):
        self.join_polls, self.polls, self.closed = join_polls, 0, False

    def consume(self, num_messages, timeout):
        self.polls += 1
        return []

    def assignment(self):
        return ["partition-0"] if self.join_polls is not None and self.polls > self.join_polls else []

    def close(self):
        self.closed = True


class NoTransactionConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def transaction(self):
        return self


def offline_consumer_run(monkeypatch, fake, **kwargs):
    """Kafka·DB 없이 match_consumer.run의 반복만 돌린다."""
    from pipelines.streaming import match_consumer
    monkeypatch.setattr(match_consumer.kafka_io, "ensure_topics", lambda *a, **k: None)
    monkeypatch.setattr(match_consumer.kafka_io, "consumer", lambda *a, **k: fake)
    monkeypatch.setattr(match_consumer, "connect", lambda *a, **k: NoTransactionConnection())
    monkeypatch.setattr(match_consumer, "migrate", lambda conn: [])
    monkeypatch.setattr(match_consumer, "sync_aliases", lambda *a, **k: None)
    monkeypatch.setattr(match_consumer, "load_alias_file", lambda path: [])
    return match_consumer.run(topics=["t"], settings=object(), until_idle=True, idle_polls=3, **kwargs)


def test_idle_polls_are_counted_only_after_partitions_are_assigned(monkeypatch):
    # 느린 CI에서 실제로 일어난 일: 그룹 배정 중의 빈 응답 3번을 "다 읽음"으로 보고 메시지를 하나도 안 읽고 끝남.
    fake = SlowJoinConsumer(join_polls=6)
    assert offline_consumer_run(monkeypatch, fake) == {}
    assert fake.polls == 6 + 3 and fake.closed


def test_consumer_fails_loudly_when_never_assigned(monkeypatch):
    from pipelines.streaming import match_consumer
    monkeypatch.setattr(match_consumer, "ASSIGNMENT_TIMEOUT_SECONDS", 0)
    with pytest.raises(TimeoutError, match="배정"):
        offline_consumer_run(monkeypatch, SlowJoinConsumer(join_polls=None))
