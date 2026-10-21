"""5a 다중 리그 테스트. 리그 목록, 수집기 반복, 모델이 없는 리그의 보류를 확인한다."""
from datetime import datetime, timezone

import pytest

from pipelines.ingest import football_data_org as fdorg
from pipelines.ingest.openfootball import LEAGUE_TIMEZONES
from pipelines.leagues import BY_FDORG, LEAGUES, MODELED


def test_league_table_is_consistent():
    codes = [league.code for league in LEAGUES]
    assert codes == ["en.1", "de.1", "es.1", "it.1", "nl.1"] and len(set(codes)) == len(codes)
    assert set(BY_FDORG) == {"PL", "BL1", "PD", "SA", "DED"}
    assert fdorg.COMPETITION_TO_LEAGUE == {"PL": "en.1", "BL1": "de.1", "PD": "es.1", "SA": "it.1", "DED": "nl.1"}
    # openfootball 현지 시각을 UTC로 바꿀 시간대가 모든 리그에 있어야 한다.
    assert all(LEAGUE_TIMEZONES[league.code] == league.timezone for league in LEAGUES)
    assert set(MODELED) <= set(codes)


def test_poller_cycles_through_competitions_and_survives_one_failure(monkeypatch):
    polled = []

    def fake_poll(competition, **kwargs):
        polled.append(competition)
        if competition == "PD":
            raise RuntimeError("temporary outage")
        return {"next_poll_seconds": 21600}

    class Stop(Exception):
        pass

    def stop_after_first_cycle(seconds):
        raise Stop

    monkeypatch.setattr(fdorg, "poll_once", fake_poll)
    monkeypatch.setattr(fdorg, "load_settings", lambda: object())
    monkeypatch.setattr(fdorg.kafka_io, "ensure_topics", lambda settings: None)
    monkeypatch.setattr(fdorg.kafka_io, "producer", lambda settings, client_id: None)
    monkeypatch.setattr(fdorg.time, "sleep", stop_after_first_cycle)
    with pytest.raises(Stop):
        fdorg.run_forever()
    assert polled == ["PL", "BL1", "PD", "SA", "DED"]   # 한 리그가 실패해도 나머지는 조회한다


def test_unmodeled_league_is_withheld_without_probabilities():
    from pipelines.modeling.prospective import Predictor
    predictor = Predictor.__new__(Predictor)   # DB 없이 predict만 확인
    predictor.active = {"name": "poisson", "model_version": "v", "run_id": 1, "report_id": 1, "gate_status": "NOT_VALIDATED"}
    predictor.reasons, predictor.model, predictor.training, predictor.training_hash = [], None, [], "h"
    predictor.promoted, predictor.last_played = {}, {}
    from collections import Counter
    predictor.history = Counter()
    row = predictor.predict("FC Bayern München", "Borussia Dortmund", "2026-27",
                            datetime(2026, 10, 24, tzinfo=timezone.utc), "de.1")
    assert row["probabilities"] is None and row["withheld_reasons"] == ["LEAGUE_NOT_MODELED"]
    assert predictor.predict("A", "B", competition="en.1")["withheld_reasons"][0] == "MODEL_NOT_VALIDATED"
