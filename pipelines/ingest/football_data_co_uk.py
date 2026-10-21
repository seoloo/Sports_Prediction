"""football-data.co.uk 결과·배당 CSV 수집(로드맵 1c).

약관상 자동 다운로드가 금지되어 있어, 이 수집기는 URL을 열지 않고 사용자가 직접 내려받은 로컬 파일만 읽는다.
흐름
1. 파일 이름(E0_2025-26.csv 또는 E0_2025_26.csv)에서 리그와 시즌을 읽는다.
2. Bronze: 원본 CSV를 내용 해시 경로로 S3에 보존한다. 원본은 재배포하지 않는다(공개 저장소에 올리지 않음).
3. 파싱(parse_csv, 순수 함수): 시즌마다 열 구성이 달라 열 위치가 아니라 열 이름으로 배당을 찾는다.
4. 연결: 검토한 팀 이름 대조표(mappings/team_aliases.csv)로 openfootball 경기에 붙인다. 이름을 추측하지 않는다.
   날짜·점수가 openfootball과 다르면 덮어쓰지 않고 품질 문제로 남긴다(소스 간 교차 검증).
5. Silver: 배당을 한 줄 형태로 COPY한 뒤 한 번의 SQL로 "값이 바뀐 것만" 다음 버전으로 추가한다.
   끝난 경기의 기록(슈팅·유효 슈팅·퇴장, 3g)도 같은 방식으로 silver.match_stats_revisions에 넣는다.

실행
  python -m pipelines.ingest.football_data_co_uk                       # 기본 landing 폴더의 모든 파일
  python -m pipelines.ingest.football_data_co_uk --dir 다른폴더
  python -m pipelines.ingest.football_data_co_uk --suggest-aliases     # 팀 이름 대조표 후보 출력(검토용)
"""
import argparse
import csv
import hashlib
import io
import re
import sys
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from ..aliases import ALIAS_FILE, load_alias_file, sync_aliases
from ..config import PROJECT_ROOT, load_settings
from ..db import connect, migrate
from ..object_store import client, ensure_bucket, put_if_absent

SOURCE_ID = "football-data-co-uk"
LANDING_DIR = PROJECT_ROOT / "data" / "landing" / "football-data-co-uk"

# 리그 코드(football-data) → openfootball 리그 코드
DIVISION_TO_COMPETITION = {"E0": "en.1"}
FILENAME_PATTERN = re.compile(r"^(?P<division>E0)_(?P<start>\d{4})[-_](?P<end>\d{2})\.csv$")

# 1X2 배당을 찾을 북메이커 코드(football-data Notes 기준). 열 이름은 코드+H/D/A, 마감은 코드+C+H/D/A.
BOOKMAKERS_1X2 = ("B365", "BW", "IW", "PS", "WH", "VC", "BF", "1XB", "BFE", "BFD", "BMGM", "BV", "CL", "LB",
                  "PP", "SKB", "Max", "Avg")
# 언더오버 2.5·아시안 핸디캡 배당을 주는 코드. 여기서는 Pinnacle이 PS가 아니라 P로 표기된다.
BOOKMAKERS_TOTALS_AH = ("B365", "P", "Max", "Avg", "BFE")
# 저장할 때 쓰는 통일된 이름
BOOKMAKER_NAME = {"P": "PS", "Max": "MAX", "Avg": "AVG"}

# football-data 안내: Pinnacle 배당은 2025-07-23 이후 갱신이 늦다. 값은 저장하되 표시를 남긴다.
PINNACLE_STALE_SINCE = date(2025, 7, 23)
PINNACLE_CAUTION = "PINNACLE_STALE_SINCE_2025_07_23"

# 끝난 경기의 기록 열(football-data Notes): 슈팅, 유효 슈팅, 퇴장. 저장 이름 → CSV 열 이름
STAT_COLUMNS = {"home_shots": "HS", "away_shots": "AS", "home_shots_on_target": "HST",
                "away_shots_on_target": "AST", "home_red_cards": "HR", "away_red_cards": "AR"}

# 같은 데이터셋을 동시에 수집하지 않게 잡는 advisory lock 앞 번호
INGEST_LOCK_NAMESPACE = 7_302_202


@dataclass(frozen=True)
class OddsQuote:
    """배당 하나. line은 1X2=0, 언더오버=기준 득점, 아시안 핸디캡=홈 기준 핸디캡."""
    bookmaker: str
    market: str
    line: Decimal
    selection: str
    timing: str
    price: Decimal


@dataclass(frozen=True)
class FixtureRow:
    """CSV 한 행에서 읽은 경기 정보와 배당 목록. 점수가 비어 있으면 None(아직 안 열린 경기).

    stats는 끝난 경기의 기록(STAT_COLUMNS의 저장 이름 → 값). 열이 없거나 비어 있으면 None(0으로 채우지 않음).
    """
    record_index: int
    local_date: date
    local_time: time | None
    home: str
    away: str
    ft_home: int | None
    ft_away: int | None
    quotes: tuple
    stats: dict | None = None


@dataclass(frozen=True)
class QualityIssue:
    record_index: int
    issue_code: str
    detail: dict


def season_from_filename(name: str) -> tuple[str, str]:
    """파일 이름에서 (리그 코드, 시즌)을 읽는다. 예: E0_2021_22.csv → ("E0", "2021-22"). 형식이 다르면 ValueError."""
    match = FILENAME_PATTERN.match(name)
    if not match:
        raise ValueError(f"파일 이름 형식이 다릅니다: {name} (예: E0_2025-26.csv)")
    start, end = int(match["start"]), match["end"]
    if f"{(start + 1) % 100:02d}" != end:
        raise ValueError(f"시즌 연도가 이어지지 않습니다: {name}")
    return match["division"], f"{start}-{end}"


def parse_price(text: str):
    """배당 문자열 → Decimal. 빈칸이면 None(배당 없음, 0이 아님). 숫자가 아니거나 1 이하면 "INVALID"."""
    text = (text or "").strip()
    if not text:
        return None
    try:
        price = Decimal(text)
    except InvalidOperation:
        return "INVALID"
    return price if price.is_finite() and price > 1 else "INVALID"


def _parse_line(text: str):
    """핸디캡 기준선 문자열 → Decimal. 빈칸이면 None."""
    text = (text or "").strip()
    if not text:
        return None
    try:
        line = Decimal(text)
    except InvalidOperation:
        return "INVALID"
    return line if line.is_finite() and abs(line) <= 10 else "INVALID"


def extract_quotes(row: dict, headers: set[str], match_date: date):
    """한 행에서 배당 목록과 (열, 문제 코드) 목록을 뽑는다. 있는 열만 읽으므로 시즌마다 열이 달라도 된다."""
    quotes, problems = [], []

    def add(column, bookmaker, market, line, selection, timing):
        if column not in headers:
            return
        price = parse_price(row.get(column))
        if price is None:
            return
        if price == "INVALID":
            problems.append((column, "INVALID_PRICE"))
            return
        name = BOOKMAKER_NAME.get(bookmaker, bookmaker)
        quotes.append(OddsQuote(name, market, line, selection, timing, price))

    for code in BOOKMAKERS_1X2:
        # "BFD"는 옛 파일에서는 Betfair(BF)의 무승부 열이고, 새 파일에서는 BFD 북메이커 코드다. 같이 있으면 모호하다.
        if code == "BF" and "BFDH" in headers:
            continue
        for selection, suffix in (("HOME", "H"), ("DRAW", "D"), ("AWAY", "A")):
            add(f"{code}{suffix}", code, "1X2", Decimal(0), selection, "PRE_MATCH")
            add(f"{code}C{suffix}", code, "1X2", Decimal(0), selection, "CLOSING")
    for code in BOOKMAKERS_TOTALS_AH:
        for selection, sign in (("OVER", ">"), ("UNDER", "<")):
            add(f"{code}{sign}2.5", code, "OU", Decimal("2.5"), selection, "PRE_MATCH")
            add(f"{code}C{sign}2.5", code, "OU", Decimal("2.5"), selection, "CLOSING")
    for timing, line_column, infix in (("PRE_MATCH", "AHh", ""), ("CLOSING", "AHCh", "C")):
        line = _parse_line(row.get(line_column)) if line_column in headers else None
        if line == "INVALID":
            problems.append((line_column, "INVALID_HANDICAP_LINE"))
            continue
        if line is None:
            continue
        for code in BOOKMAKERS_TOTALS_AH:
            add(f"{code}{infix}AHH", code, "AH", line, "HOME", timing)
            add(f"{code}{infix}AHA", code, "AH", line, "AWAY", timing)
    return quotes, problems


def extract_stats(row: dict, headers: set[str]):
    """끝난 경기 한 행의 기록과 (열, 문제 코드) 목록. 기록 열이 하나라도 없거나 비어 있으면 기록 없음(None)."""
    if any(column not in headers for column in STAT_COLUMNS.values()):
        return None, []
    values = {}
    for name, column in STAT_COLUMNS.items():
        text = (row.get(column) or "").strip()
        if not text:
            return None, []
        if not text.isdigit():
            return None, [(column, "INVALID_STAT")]
        values[name] = int(text)
    for side in ("home", "away"):
        if values[f"{side}_shots_on_target"] > values[f"{side}_shots"]:
            return None, [(STAT_COLUMNS[f"{side}_shots_on_target"], "INCONSISTENT_STAT")]   # 유효 슈팅 > 슈팅
    return values, []


def _parse_date(text: str) -> date:
    """dd/mm/yyyy(최근 파일) 또는 dd/mm/yy(옛 파일) 날짜."""
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(text.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(text)


def _parse_goals(text: str):
    text = (text or "").strip()
    if not text:
        return None
    if not text.isdigit():
        raise ValueError(text)
    return int(text)


def parse_csv(payload: bytes, *, division: str) -> tuple[list[FixtureRow], list[QualityIssue]]:
    """CSV 바이트를 경기 행 목록과 품질 문제 목록으로 바꾼다. DB·네트워크를 쓰지 않는 순수 함수다."""
    text = payload.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text, newline=""))
    headers = set(reader.fieldnames or [])
    missing = {"Div", "Date", "HomeTeam", "AwayTeam"} - headers
    if missing:
        return [], [QualityIssue(-1, "MISSING_COLUMNS", {"columns": sorted(missing)})]
    fixtures, issues, seen = [], [], set()
    for index, row in enumerate(reader):
        def reject(code, **detail):
            issues.append(QualityIssue(index, code, {"row": {key: row.get(key) for key in
                                                             ("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG")}, **detail}))

        if not any((value or "").strip() for value in row.values()):
            continue  # 파일 끝의 빈 줄
        if row.get("Div") != division:
            reject("UNEXPECTED_DIVISION")
            continue
        home, away = (row.get("HomeTeam") or "").strip(), (row.get("AwayTeam") or "").strip()
        if not home or not away or home == away:
            reject("INVALID_TEAMS")
            continue
        try:
            match_date = _parse_date(row["Date"])
            ft_home, ft_away = _parse_goals(row.get("FTHG")), _parse_goals(row.get("FTAG"))
            match_time = time.fromisoformat(row["Time"].strip()) if (row.get("Time") or "").strip() else None
        except (ValueError, KeyError) as exc:
            reject("INVALID_VALUE", value=str(exc))
            continue
        if (ft_home is None) != (ft_away is None):
            reject("PARTIAL_SCORE")
            continue
        if (home, away) in seen:
            reject("DUPLICATE_FIXTURE")
            continue
        seen.add((home, away))
        quotes, problems = extract_quotes(row, headers, match_date)
        stats, stat_problems = extract_stats(row, headers) if ft_home is not None else (None, [])
        for column, code in problems + stat_problems:
            reject(code, column=column, value=row.get(column))
        fixtures.append(FixtureRow(index, match_date, match_time, home, away, ft_home, ft_away, tuple(quotes), stats))
    return fixtures, issues


def link_fixtures(conn: psycopg.Connection, fixtures: list[FixtureRow], *, competition: str, season: str):
    """경기 행을 openfootball 경기(match_id)에 연결한다. 돌려주는 값: ([(경기 행, match_id)], 품질 문제 목록)

    연결 근거는 대조표의 팀 이름과 리그·시즌·홈/원정이다. 날짜·점수가 openfootball 최신 버전과 다르면
    연결은 유지하되 DATE_MISMATCH·SCORE_MISMATCH로 남긴다. 어느 쪽이 맞는지 이 단계에서 판단하지 않는다.
    """
    team_ids = dict(conn.execute("SELECT alias, team_id FROM silver.team_aliases WHERE source_id=%s", (SOURCE_ID,)).fetchall())
    linked, issues = [], []
    for fixture in fixtures:
        detail = {"home": fixture.home, "away": fixture.away, "date": fixture.local_date.isoformat()}
        unknown = [name for name in (fixture.home, fixture.away) if name not in team_ids]
        if unknown:
            issues.append(QualityIssue(fixture.record_index, "UNMAPPED_TEAM", {**detail, "names": unknown}))
            continue
        row = conn.execute("""SELECT m.match_id, r.local_date, r.ft_home, r.ft_away
                              FROM silver.matches m
                              JOIN silver.match_revisions_known_at(now()) r USING (match_id)
                              WHERE m.competition_code=%s AND m.season=%s AND m.home_team_id=%s AND m.away_team_id=%s""",
                           (competition, season, team_ids[fixture.home], team_ids[fixture.away])).fetchone()
        if not row:
            issues.append(QualityIssue(fixture.record_index, "MATCH_NOT_FOUND", detail))
            continue
        match_id, other_date, other_home, other_away = row
        if other_date != fixture.local_date:
            issues.append(QualityIssue(fixture.record_index, "DATE_MISMATCH",
                                       {**detail, "openfootball_date": other_date.isoformat()}))
        if fixture.ft_home is not None and other_home is not None and (fixture.ft_home, fixture.ft_away) != (other_home, other_away):
            issues.append(QualityIssue(fixture.record_index, "SCORE_MISMATCH",
                                       {**detail, "score": [fixture.ft_home, fixture.ft_away],
                                        "openfootball_score": [other_home, other_away]}))
        linked.append((fixture, match_id))
    return linked, issues


def write_odds(conn: psycopg.Connection, linked, *, raw_file_id: int, received_at: datetime) -> int:
    """배당을 임시 테이블에 COPY한 뒤, 키별 최신 버전과 값이 다른 것만 다음 버전으로 넣는다. 넣은 행 수를 돌려준다.

    수만 행을 한 줄씩 비교하면 DB 왕복이 수만 번 생기므로, 비교와 삽입을 SQL 한 번으로 처리한다(집합 연산).
    """
    conn.execute("""CREATE TEMP TABLE odds_stage (
                      match_id BIGINT, bookmaker TEXT, market TEXT, line NUMERIC(6,2), selection TEXT,
                      timing TEXT, price NUMERIC(10,4), caution TEXT) ON COMMIT DROP""")
    with conn.cursor().copy("COPY odds_stage FROM STDIN") as copy:
        for fixture, match_id in linked:
            for quote in fixture.quotes:
                caution = (PINNACLE_CAUTION if quote.bookmaker == "PS" and fixture.local_date >= PINNACLE_STALE_SINCE
                           else None)
                copy.write_row((match_id, quote.bookmaker, quote.market, quote.line, quote.selection,
                                quote.timing, quote.price, caution))
    inserted = conn.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (match_id, bookmaker, market, line, selection, timing)
                   match_id, bookmaker, market, line, selection, timing, revision_no, price, caution
            FROM silver.odds_revisions
            WHERE source_id = %(source)s AND match_id IN (SELECT DISTINCT match_id FROM odds_stage)
            ORDER BY match_id, bookmaker, market, line, selection, timing, revision_no DESC
        )
        INSERT INTO silver.odds_revisions
        SELECT s.match_id, %(source)s, s.bookmaker, s.market, s.line, s.selection, s.timing,
               COALESCE(l.revision_no, 0) + 1, s.price, s.caution, NULL, %(received)s, %(raw_file)s
        FROM odds_stage s
        LEFT JOIN latest l USING (match_id, bookmaker, market, line, selection, timing)
        WHERE l.price IS DISTINCT FROM s.price OR l.caution IS DISTINCT FROM s.caution""",
                            {"source": SOURCE_ID, "received": received_at, "raw_file": raw_file_id}).rowcount
    return inserted


def write_stats(conn: psycopg.Connection, linked, *, raw_file_id: int, received_at: datetime) -> int:
    """경기 기록을 임시 테이블에 COPY한 뒤, 경기별 최신 버전과 값이 다른 것만 다음 버전으로 넣는다."""
    columns = list(STAT_COLUMNS)
    conn.execute(f"""CREATE TEMP TABLE stats_stage (match_id BIGINT, {", ".join(f"{c} INTEGER" for c in columns)})
                     ON COMMIT DROP""")
    with conn.cursor().copy("COPY stats_stage FROM STDIN") as copy:
        for fixture, match_id in linked:
            if fixture.stats:
                copy.write_row((match_id, *[fixture.stats[c] for c in columns]))
    changed = " OR ".join(f"l.{c} IS DISTINCT FROM s.{c}" for c in columns)
    return conn.execute(f"""
        WITH latest AS (
            SELECT DISTINCT ON (match_id) *
            FROM silver.match_stats_revisions
            WHERE source_id = %(source)s AND match_id IN (SELECT match_id FROM stats_stage)
            ORDER BY match_id, revision_no DESC
        )
        INSERT INTO silver.match_stats_revisions (match_id, source_id, revision_no, {", ".join(columns)},
                                                  published_at, received_at, raw_file_id)
        SELECT s.match_id, %(source)s, COALESCE(l.revision_no, 0) + 1, {", ".join(f"s.{c}" for c in columns)},
               NULL, %(received)s, %(raw_file)s
        FROM stats_stage s LEFT JOIN latest l USING (match_id)
        WHERE l.match_id IS NULL OR {changed}""",
                        {"source": SOURCE_ID, "received": received_at, "raw_file": raw_file_id}).rowcount


def ingest_file(path: Path, *, settings=None, received_at: datetime | None = None, alias_file: Path = ALIAS_FILE) -> dict:
    """CSV 파일 하나를 Bronze·Silver에 수집하고 실행 통계를 돌려준다. 같은 파일을 다시 넣어도 배당 버전은 늘지 않는다."""
    division, season = season_from_filename(path.name)
    competition = DIVISION_TO_COMPETITION[division]
    settings = settings or load_settings()
    dataset = f"{division}/{season}"
    payload = path.read_bytes()
    run_id = uuid.uuid4()
    with connect(settings) as conn:
        migrate(conn)
        conn.execute("INSERT INTO ingest.runs VALUES (%s, %s, %s, %s, NULL, 'RUNNING', NULL, NULL)",
                     (run_id, SOURCE_ID, dataset, datetime.now(timezone.utc)))
        conn.commit()
        try:
            received_at = received_at or datetime.now(timezone.utc)
            sha256 = hashlib.sha256(payload).hexdigest()
            object_key = f"{SOURCE_ID}/{division}/{season}/sha256={sha256}.csv"
            store = client(settings)
            ensure_bucket(store, settings.bronze_bucket)
            bronze_uploaded = put_if_absent(store, settings.bronze_bucket, object_key, payload,
                                            {"source-file": path.name, "received-at": received_at.isoformat()},
                                            content_type="text/csv")
            fixtures, parse_issues = parse_csv(payload, division=division)
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s, hashtext(%s))", (INGEST_LOCK_NAMESPACE, dataset))
                sync_aliases(conn, load_alias_file(alias_file), received_at)
                inserted = conn.execute("""INSERT INTO ingest.raw_files
                                           (source_id, dataset, source_url, sha256, byte_size, object_key, first_received_at)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                                           ON CONFLICT (source_id, dataset, sha256) DO NOTHING RETURNING raw_file_id""",
                                        (SOURCE_ID, dataset, f"file:{path.name}", sha256, len(payload), object_key,
                                         received_at)).fetchone()
                is_new_content = inserted is not None
                raw_file_id = inserted[0] if inserted else conn.execute(
                    "SELECT raw_file_id FROM ingest.raw_files WHERE source_id=%s AND dataset=%s AND sha256=%s",
                    (SOURCE_ID, dataset, sha256)).fetchone()[0]
                conn.execute("INSERT INTO ingest.fetches (run_id, raw_file_id, received_at, is_new_content) VALUES (%s, %s, %s, %s)",
                             (run_id, raw_file_id, received_at, is_new_content))
                linked, link_issues = link_fixtures(conn, fixtures, competition=competition, season=season)
                odds_new = write_odds(conn, linked, raw_file_id=raw_file_id, received_at=received_at)
                stats_new = write_stats(conn, linked, raw_file_id=raw_file_id, received_at=received_at)
                issues = parse_issues + link_issues
                if is_new_content:
                    for issue in issues:
                        conn.execute("""INSERT INTO silver.quality_issues (run_id, raw_file_id, record_index, issue_code, detail, created_at)
                                        VALUES (%s, %s, %s, %s, %s, %s)""",
                                     (run_id, raw_file_id, issue.record_index, issue.issue_code, Jsonb(issue.detail), received_at))
            stats = {"dataset": dataset, "file": path.name, "sha256": sha256, "new_content": is_new_content,
                     "bronze_uploaded": bronze_uploaded, "fixtures": len(fixtures), "linked": len(linked),
                     "quotes": sum(len(fixture.quotes) for fixture, _ in linked), "odds_new": odds_new,
                     "stats": sum(1 for fixture, _ in linked if fixture.stats), "stats_new": stats_new,
                     "issues": dict(Counter(issue.issue_code for issue in issues))}
            conn.execute("UPDATE ingest.runs SET status='SUCCEEDED', finished_at=%s, stats=%s WHERE run_id=%s",
                         (datetime.now(timezone.utc), Jsonb(stats), run_id))
            conn.commit()
            return stats
        except Exception as exc:
            conn.rollback()
            conn.execute("UPDATE ingest.runs SET status='FAILED', finished_at=%s, error=%s WHERE run_id=%s",
                         (datetime.now(timezone.utc), f"{type(exc).__name__}: {exc}", run_id))
            conn.commit()
            raise


def landing_files(directory: Path = LANDING_DIR) -> list[Path]:
    """landing 폴더에서 이름 규칙에 맞는 CSV를 시즌 순서로 돌려준다. 폴더가 없으면 빈 목록."""
    if not directory.exists():
        return []
    return sorted((path for path in directory.iterdir() if FILENAME_PATTERN.match(path.name)),
                  key=lambda path: season_from_filename(path.name))


def suggest_aliases(conn: psycopg.Connection, paths: list[Path]) -> list[dict]:
    """같은 시즌·같은 날짜에 열린 openfootball 경기를 근거로 팀 이름 대응 후보를 만든다(검토용, 저장하지 않음).

    football-data 이름 하나가 등장한 모든 경기에서 같은 openfootball 팀과 짝지어지면 supporting = appearances다.
    이름이 비슷한지는 보지 않는다. 결과는 사람이 검토한 뒤 mappings/team_aliases.csv에 옮긴다.
    """
    votes, appearances = defaultdict(Counter), Counter()
    for path in paths:
        division, season = season_from_filename(path.name)
        fixtures, _ = parse_csv(path.read_bytes(), division=division)
        for fixture in fixtures:
            candidates = conn.execute("""SELECT home.canonical_name, away.canonical_name
                                         FROM silver.matches m
                                         JOIN silver.match_revisions_known_at(now()) r USING (match_id)
                                         JOIN silver.teams home ON home.team_id = m.home_team_id
                                         JOIN silver.teams away ON away.team_id = m.away_team_id
                                         WHERE m.competition_code=%s AND m.season=%s AND r.local_date=%s""",
                                      (DIVISION_TO_COMPETITION[division], season, fixture.local_date)).fetchall()
            appearances[fixture.home] += 1
            appearances[fixture.away] += 1
            for of_home, of_away in candidates:
                votes[fixture.home][of_home] += 1
                votes[fixture.away][of_away] += 1
    suggestions = []
    for alias in sorted(appearances):
        canonical, supporting = votes[alias].most_common(1)[0] if votes[alias] else ("", 0)
        suggestions.append({"source_id": SOURCE_ID, "alias": alias, "canonical_name": canonical,
                            "evidence": f"same-date fixtures {supporting}/{appearances[alias]}"})
    return suggestions


def main(argv=None):
    parser = argparse.ArgumentParser(description="football-data.co.uk CSV(직접 내려받은 파일)를 수집합니다.")
    parser.add_argument("--dir", type=Path, default=LANDING_DIR, help=f"CSV 폴더. 기본: {LANDING_DIR}")
    parser.add_argument("--suggest-aliases", action="store_true", help="팀 이름 대조표 후보를 CSV로 출력(저장하지 않음)")
    args = parser.parse_args(argv)
    paths = landing_files(args.dir)
    if not paths:
        print(f"처리할 CSV가 없습니다: {args.dir} (파일 이름 예: E0_2025-26.csv)")
        return 1
    if args.suggest_aliases:
        with connect() as conn:
            rows = suggest_aliases(conn, paths)
        writer = csv.DictWriter(sys.stdout, fieldnames=["source_id", "alias", "canonical_name", "evidence"], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        return 0
    for path in paths:
        print(ingest_file(path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
