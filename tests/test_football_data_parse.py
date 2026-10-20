"""football-data.co.uk CSV 파싱 단위 테스트. 실제 원본은 재배포하지 않으므로 같은 열 형식의 가상 파일을 쓴다."""
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from pipelines.ingest.football_data_co_uk import (extract_quotes, landing_files, load_alias_file, parse_csv,
                                                  parse_price, season_from_filename)

FIXTURE = Path(__file__).parent / "fixtures" / "E0_2025-26.csv"


def fixtures_by_pair():
    fixtures, issues = parse_csv(FIXTURE.read_bytes(), division="E0")
    return {(f.home, f.away): f for f in fixtures}, issues


@pytest.mark.parametrize("name, expected", [
    ("E0_2025-26.csv", ("E0", "2025-26")),
    ("E0_2021_22.csv", ("E0", "2021-22")),
    ("E0_2099-00.csv", ("E0", "2099-00")),
])
def test_season_from_filename(name, expected):
    assert season_from_filename(name) == expected


@pytest.mark.parametrize("name", ["E0.csv", "E0_2025-27.csv", "E1_2025-26.csv", "E0_2025-26.txt"])
def test_bad_filenames_are_rejected(name):
    with pytest.raises(ValueError):
        season_from_filename(name)


@pytest.mark.parametrize("text, expected", [
    ("1.80", Decimal("1.80")), ("", None), ("  ", None), ("0", "INVALID"), ("1", "INVALID"), ("abc", "INVALID"),
    ("NaN", "INVALID"),
])
def test_parse_price(text, expected):
    assert parse_price(text) == expected


def test_quotes_are_found_by_column_name_and_counted():
    found, _ = fixtures_by_pair()
    assert len(found[("Alpha", "Beta")].quotes) == 23
    assert len(found[("Gamma", "Delta")].quotes) == 20  # Pinnacle 칸이 비어 있음: 0으로 만들지 않고 없는 배당
    assert len(found[("Beta", "Gamma")].quotes) == 18
    assert len(found[("Delta", "Alpha")].quotes) == 13


def test_bookmaker_names_markets_and_lines():
    found, _ = fixtures_by_pair()
    quotes = {(q.bookmaker, q.market, q.line, q.selection, q.timing): q.price for q in found[("Alpha", "Beta")].quotes}
    assert quotes[("B365", "1X2", Decimal(0), "HOME", "PRE_MATCH")] == Decimal("1.80")
    assert quotes[("PS", "1X2", Decimal(0), "AWAY", "PRE_MATCH")] == Decimal("4.60")
    assert quotes[("AVG", "1X2", Decimal(0), "DRAW", "CLOSING")] == Decimal("3.60")
    assert quotes[("B365", "OU", Decimal("2.5"), "OVER", "PRE_MATCH")] == Decimal("1.90")
    assert quotes[("AVG", "OU", Decimal("2.5"), "UNDER", "CLOSING")] == Decimal("1.88")
    assert quotes[("B365", "AH", Decimal("-0.75"), "HOME", "PRE_MATCH")] == Decimal("1.95")
    assert quotes[("AVG", "AH", Decimal("-0.5"), "AWAY", "CLOSING")] == Decimal("2.05")


def test_invalid_values_are_reported_not_stored():
    found, issues = fixtures_by_pair()
    codes = sorted((issue.issue_code, issue.detail.get("column")) for issue in issues)
    assert codes == [("INVALID_HANDICAP_LINE", "AHh"), ("INVALID_PRICE", "B365CH")]
    beta_gamma = found[("Beta", "Gamma")]
    assert not any(q.bookmaker == "B365" and q.timing == "CLOSING" and q.selection == "HOME" for q in beta_gamma.quotes)
    assert not any(q.market == "AH" for q in beta_gamma.quotes)


def test_scores_dates_and_scheduled_matches():
    found, _ = fixtures_by_pair()
    assert (found[("Alpha", "Beta")].ft_home, found[("Alpha", "Beta")].ft_away) == (2, 1)
    assert found[("Delta", "Alpha")].ft_home is None
    assert found[("Beta", "Gamma")].local_date == date(2025, 12, 26)


def test_old_betfair_draw_column_is_not_confused_with_bfd_bookmaker():
    old_headers = {"BFH", "BFD", "BFA"}
    quotes, _ = extract_quotes({"BFH": "2.0", "BFD": "3.5", "BFA": "4.0"}, old_headers, date(2022, 1, 1))
    assert {(q.bookmaker, q.selection) for q in quotes} == {("BF", "HOME"), ("BF", "DRAW"), ("BF", "AWAY")}
    new_headers = {"BFDH", "BFDD", "BFDA"}
    quotes, _ = extract_quotes({"BFDH": "2.0", "BFDD": "3.5", "BFDA": "4.0"}, new_headers, date(2025, 9, 1))
    assert {(q.bookmaker, q.selection) for q in quotes} == {("BFD", "HOME"), ("BFD", "DRAW"), ("BFD", "AWAY")}


def test_rows_with_problems():
    header = "Div,Date,HomeTeam,AwayTeam,FTHG,FTAG\n"
    rows = ["E1,01/09/2025,A,B,1,0", "E0,01/09/2025,A,A,1,0", "E0,31/02/2025,A,B,1,0", "E0,01/09/2025,A,B,1,",
            "E0,01/09/25,C,D,1,0", "E0,02/09/2025,C,D,0,0", ",,,,,"]
    fixtures, issues = parse_csv((header + "\n".join(rows)).encode(), division="E0")
    assert [issue.issue_code for issue in issues] == ["UNEXPECTED_DIVISION", "INVALID_TEAMS", "INVALID_VALUE",
                                                      "PARTIAL_SCORE", "DUPLICATE_FIXTURE"]
    assert [(f.home, f.local_date) for f in fixtures] == [("C", date(2025, 9, 1))]
    assert parse_csv(b"Date,HomeTeam\n", division="E0")[1][0].issue_code == "MISSING_COLUMNS"


def test_landing_files_and_alias_file(tmp_path):
    for name in ("E0_2025-26.csv", "E0_2021_22.csv", "notes.txt", "E0.csv"):
        (tmp_path / name).write_text("x", encoding="utf-8")
    assert [path.name for path in landing_files(tmp_path)] == ["E0_2021_22.csv", "E0_2025-26.csv"]
    assert landing_files(tmp_path / "missing") == []
    aliases = load_alias_file()
    co_uk = [row for row in aliases if row["source_id"] == "football-data-co-uk"]
    assert len(co_uk) == 29 and len({row["alias"] for row in co_uk}) == 29
    assert {"alias": "Nott'm Forest", "canonical_name": "Nottingham Forest FC"}.items() <= next(
        row for row in aliases if row["alias"] == "Nott'm Forest").items()
