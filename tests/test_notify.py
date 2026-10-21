"""텔레그램 알림 테스트. 실제로 보내지 않는다(가짜 HTTP·가짜 sender). 중복 방지는 Postgres가 있을 때만."""
import io
import json
import urllib.error
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from pipelines import snapshots
from pipelines.ingest import openfootball
from pipelines.notify import alerts, telegram
from tests.test_pipeline_integration import SAMPLE_PATH, isolated_settings, query  # noqa: F401 (fixture 재사용)

TOKEN = "123456:SECRET-token-value"
KICKOFF = datetime(2026, 5, 24, 15, 0, tzinfo=timezone.utc)   # 예시 원본의 Delta FC - Alpha FC


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def http_error(code: int, body: dict) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(f"https://api.telegram.org/bot{TOKEN}/sendMessage", code, "error", {},
                                  io.BytesIO(json.dumps(body).encode()))


def test_split_message_respects_the_limit_and_keeps_lines():
    assert telegram.split_message("a\nb\nc", limit=3) == ["a\nb", "c"]
    assert telegram.split_message("x" * 7, limit=3) == ["xxx", "xxx", "x"]
    assert telegram.split_message("") == [""]
    assert all(len(part) <= telegram.MAX_LENGTH for part in telegram.split_message("가" * 10000))


def test_call_retries_429_and_never_leaks_the_token():
    responses = [http_error(429, {"ok": False, "parameters": {"retry_after": 7}}),
                 FakeResponse(json.dumps({"ok": True, "result": {"message_id": 1}}).encode())]
    slept, seen = [], []

    def opener(request, timeout):
        seen.append(request.full_url)
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    assert telegram.call("sendMessage", TOKEN, {"chat_id": "1", "text": "hi"}, opener=opener, sleep=slept.append) == {
        "message_id": 1}
    assert slept == [7] and len(seen) == 2

    def unauthorized(request, timeout):
        raise http_error(401, {"ok": False, "description": f"Unauthorized for bot{TOKEN}"})

    with pytest.raises(telegram.TelegramError) as caught:
        telegram.call("sendMessage", TOKEN, {}, opener=unauthorized)
    assert "401" in str(caught.value) and TOKEN not in str(caught.value) and caught.value.__cause__ is None

    def offline(request, timeout):
        raise urllib.error.URLError(f"cannot reach api.telegram.org/bot{TOKEN}")

    with pytest.raises(telegram.TelegramError) as caught:
        telegram.call("getUpdates", TOKEN, {}, opener=offline)
    assert TOKEN not in str(caught.value)


class Configured:
    telegram_bot_token = TOKEN
    telegram_chat_id = "42"


class NotConfigured:
    telegram_bot_token = None
    telegram_chat_id = None


def test_send_splits_and_reports_not_configured():
    sent = []

    def opener(request, timeout):
        sent.append(request.data)
        return FakeResponse(b'{"ok": true, "result": {}}')

    assert telegram.send("a" * 5000, Configured(), opener=opener) == {"status": "SENT", "parts": 2}
    assert len(sent) == 2 and b"chat_id=42" in sent[0]
    assert telegram.send("hello", NotConfigured()) == {"status": "NOT_CONFIGURED", "parts": 0}


def test_chat_ids_lists_chats_that_messaged_the_bot():
    updates = [{"message": {"chat": {"id": 42, "type": "private", "first_name": "Me"}}},
               {"message": {"chat": {"id": 42, "type": "private", "first_name": "Me"}}},
               {"edited_message": {}}]

    def opener(request, timeout):
        return FakeResponse(json.dumps({"ok": True, "result": updates}).encode())

    assert telegram.chat_ids(Configured(), opener=opener) == [{"chat_id": 42, "type": "private", "name": "Me"}]


def test_prematch_text_hides_probabilities_unless_ready():
    base = {"match_id": 1, "kickoff_utc": datetime(2026, 10, 24, 11, 30, tzinfo=timezone.utc),
            "home": "Aston Villa FC", "away": "Manchester City FC", "prob_home": 0.257, "prob_draw": 0.222,
            "prob_away": 0.521}
    text = alerts.prematch_text([
        {**base, "offset_label": "T-24H", "status": "TAKEN", "display_status": "WITHHELD",
         "withheld_reasons": ["MODEL_NOT_VALIDATED"]},
        {**base, "offset_label": "T-3H", "status": "MISSED", "display_status": None, "withheld_reasons": None},
        {**base, "offset_label": "T-1H", "status": "TAKEN", "display_status": "READY", "withheld_reasons": []}])
    assert "10/24(토) 20:30 Aston Villa FC vs Manchester City FC" in text   # 한국 시각
    assert "24시간 전: 찍음 · 예측 보류(모델 미검증)" in text and "3시간 전: 놓침" in text
    assert "1시간 전: 홈 25.7% · 무 22.2% · 원정 52.1%" in text
    assert text.count("%") == 3   # 보류된 예측의 확률은 보내지 않는다


@pytest.fixture
def season(isolated_settings):
    openfootball.ingest("2025-26", "en.1", payload=SAMPLE_PATH.read_bytes(), source_url="file:sample.json",
                        settings=isolated_settings, received_at=datetime(2025, 8, 17, tzinfo=timezone.utc))
    return replace(isolated_settings, telegram_bot_token=TOKEN, telegram_chat_id="42")


def test_prematch_notifications_are_sent_once(season):
    now = KICKOFF - timedelta(hours=2, minutes=30)
    snapshots.run(now=now, settings=season)   # T-24H 놓침, T-3H 찍음
    outbox = []

    def sender(text, settings):
        outbox.append(text)
        return {"status": "SENT", "parts": 1}

    assert alerts.notify_prematch(now=now, settings=season, sender=lambda text, settings: {
        "status": "NOT_CONFIGURED", "parts": 0})["status"] == "NOT_CONFIGURED"
    first = alerts.notify_prematch(now=now, settings=season, sender=sender)
    again = alerts.notify_prematch(now=now, settings=season, sender=sender)
    assert first == {"status": "SENT", "parts": 1, "kind": "PREMATCH", "items": 2} and again["status"] == "NOTHING_NEW"
    assert len(outbox) == 1 and "Delta FC vs Alpha FC" in outbox[0] and "24시간 전: 놓침" in outbox[0]
    assert query(season, "SELECT kind, notification_key FROM notify.sent_messages ORDER BY 2")[0][0] == "PREMATCH"
    # 다음 시점(T-1H)이 새로 찍히면 그것만 보낸다.
    later = KICKOFF - timedelta(minutes=40)
    snapshots.run(now=later, settings=season)
    alerts.notify_prematch(now=later, settings=season, sender=sender)
    assert len(outbox) == 2 and "1시간 전" in outbox[1] and "3시간 전" not in outbox[1]
    # 이틀이 지난 기록은 처음 설정했을 때 한꺼번에 보내지 않는다.
    assert alerts.notify_prematch(now=later + timedelta(days=3), settings=season, sender=sender)["status"] == "NOTHING_NEW"


def test_failure_alert_is_sent_once_and_redacts_the_token(season):
    outbox = []

    def sender(text, settings):
        outbox.append(text)
        return {"status": "SENT", "parts": 1}

    first = alerts.notify_failure("dag", "task", "run-1", 3, f"RuntimeError: bad url bot{TOKEN}", settings=season,
                                  sender=sender)
    again = alerts.notify_failure("dag", "task", "run-1", 3, "RuntimeError", settings=season, sender=sender)
    assert first["status"] == "SENT" and again["status"] == "ALREADY_SENT" and len(outbox) == 1
    assert "dag.task" in outbox[0] and TOKEN not in outbox[0]


def test_failure_callback_never_raises(monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("database is down")
    monkeypatch.setattr(alerts, "notify_failure", broken)
    alerts.airflow_failure_callback({"ti": object()})   # 속성이 없는 ti에도 예외를 밖으로 내지 않는다


def test_weekly_text_summarizes_the_report():
    summary = {"week_start": "2026-10-19", "active_model": {"name": "poisson_calibrated", "gate_status": "NOT_VALIDATED"},
               "snapshots": {"T-24H": {"TAKEN": 9, "MISSED": 1}},
               "week": {"T-24H": {"matches": 9, "log_loss": 0.98, "vs_market_pre": {"market_log_loss": 0.95}},
                        "T-3H": {"matches": 0}, "T-1H": {"matches": 0}},
               "cumulative": {"T-24H": {"matches": 9, "log_loss": 0.98}, "T-3H": {"matches": 0}, "T-1H": {"matches": 0}},
               "withheld_reasons": {"MODEL_NOT_VALIDATED": 9}}
    text = alerts.weekly_text(summary)
    assert "2026-10-19 주" in text and "평가용 기록" in text
    assert "24시간 전: 찍음 9 · 놓침 1 · 채점 9경기 로그 손실 0.980(시장 0.950)" in text and "모델 미검증 9" in text
