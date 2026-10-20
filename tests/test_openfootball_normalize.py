"""openfootball 정규화 단위 테스트. DB·네트워크·Docker 없이 실행된다."""
import json
from datetime import date, time
from pathlib import Path

import pytest

from pipelines.ingest.openfootball import local_to_utc, normalize_document, parse_score

SAMPLE = json.loads((Path(__file__).parent / "fixtures" / "openfootball_en1_sample.json").read_text(encoding="utf-8"))


def by_pair(matches):
    return {(match.home, match.away): match for match in matches}


def test_sample_normalizes_formats_and_reports_issues():
    matches, issues = normalize_document(SAMPLE, league="en.1")
    found = by_pair(matches)
    assert len(matches) == 5
    # 객체 형식: 전반·최종 점수
    alpha_beta = found[("Alpha FC", "Beta FC")]
    assert (alpha_beta.ht_home, alpha_beta.ht_away, alpha_beta.ft_home, alpha_beta.ft_away) == (1, 0, 2, 1)
    assert alpha_beta.status == "FINISHED"
    # 배열 형식: 최종 점수만 있음. 전반은 0이 아니라 모름(None)
    gamma_delta = found[("Gamma FC", "Delta FC")]
    assert (gamma_delta.ht_home, gamma_delta.ft_home, gamma_delta.ft_away) == (None, 0, 0)
    # ht 키가 없는 객체도 전반은 모름
    assert found[("Beta FC", "Gamma FC")].ht_home is None
    # 점수가 없으면 예정 경기
    assert found[("Delta FC", "Alpha FC")].status == "SCHEDULED"
    # 시각이 없으면 UTC 시각도 만들지 않음
    assert found[("Alpha FC", "Gamma FC")].kickoff_utc is None
    assert sorted(issue.issue_code for issue in issues) == ["DUPLICATE_FIXTURE", "HALF_TIME_EXCEEDS_FULL_TIME"]


def test_local_time_uses_league_timezone_across_daylight_saving():
    matches, _ = normalize_document(SAMPLE, league="en.1")
    found = by_pair(matches)
    # 8월은 영국 서머타임(UTC+1), 12월은 GMT(UTC+0)
    assert found[("Alpha FC", "Beta FC")].kickoff_utc.isoformat() == "2025-08-16T14:00:00+00:00"
    assert found[("Beta FC", "Gamma FC")].kickoff_utc.isoformat() == "2025-12-26T12:30:00+00:00"
    assert all(match.local_timezone == "Europe/London" for match in matches)


def test_nonexistent_local_time_is_rejected_not_shifted():
    # 2026-03-29 01:30은 영국 서머타임 시작으로 존재하지 않는 시각
    assert local_to_utc(date(2026, 3, 29), time(1, 30), "Europe/London") is None
    document = {"matches": [{"date": "2026-03-29", "time": "01:30", "team1": "A", "team2": "B"}]}
    matches, issues = normalize_document(document, league="en.1")
    assert not matches and issues[0].issue_code == "NONEXISTENT_LOCAL_TIME"


@pytest.mark.parametrize("raw, expected", [
    (None, (None, None, None)),
    ([2, 1], (None, (2, 1), None)),
    ({"ft": [2, 1], "ht": [0, 1]}, ((0, 1), (2, 1), None)),
    ({"ft": [2, 1]}, (None, (2, 1), None)),
    ({"ht": [1, 0]}, (None, None, "HALF_TIME_WITHOUT_FULL_TIME")),
    ([True, 0], (None, None, "INVALID_SCORE_FORMAT")),
    ([-1, 0], (None, None, "INVALID_SCORE_FORMAT")),
    ({"ft": [1]}, (None, None, "INVALID_SCORE_FORMAT")),
    ("2-1", (None, None, "INVALID_SCORE_FORMAT")),
])
def test_parse_score_formats(raw, expected):
    assert parse_score(raw) == expected


@pytest.mark.parametrize("record, code", [
    ({"date": "2025-08-16", "team1": "A", "team2": ""}, "MISSING_TEAM"),
    ({"date": "2025-08-16", "team1": "A", "team2": "A"}, "SAME_TEAM"),
    ({"date": "16/08/2025", "team1": "A", "team2": "B"}, "INVALID_DATE"),
    ({"date": "2025-08-16", "time": "3pm", "team1": "A", "team2": "B"}, "INVALID_TIME"),
    ("not a record", "INVALID_RECORD"),
])
def test_invalid_records_are_reported(record, code):
    matches, issues = normalize_document({"matches": [record]}, league="en.1")
    assert not matches and [issue.issue_code for issue in issues] == [code]


def test_missing_matches_array():
    assert normalize_document({"name": "x"}, league="en.1")[1][0].issue_code == "MISSING_MATCHES_ARRAY"


def test_content_hash_changes_only_with_content():
    first, _ = normalize_document(SAMPLE, league="en.1")
    second, _ = normalize_document(json.loads(json.dumps(SAMPLE)), league="en.1")
    assert [m.content_hash() for m in first] == [m.content_hash() for m in second]
    changed = json.loads(json.dumps(SAMPLE))
    changed["matches"][0]["score"]["ft"] = [3, 1]
    third, _ = normalize_document(changed, league="en.1")
    assert first[0].content_hash() != third[0].content_hash()
    assert [m.content_hash() for m in first[1:]] == [m.content_hash() for m in third[1:]]
