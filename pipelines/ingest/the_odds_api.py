"""The Odds API 수집기(로드맵 5b): 다섯 리그의 다가올 경기 배당(1X2, 유럽 북메이커).

흐름(prematch_snapshots DAG가 스냅샷 직전 30분마다 부른다)
1. /events(무료)로 리그별 다가올 경기와 시작 시각을 본다.
2. 경기 시작 24시간 전·1시간 전 스냅샷이 곧(35분 안) 오는 리그만 /odds를 조회한다(1크레딧, 그 리그 경기 전체).
   같은 시각에 시작하는 경기는 한 번에 받고, 90분 안에 받은 적이 있으면 다시 받지 않는다. 3시간 전 스냅샷은
   24시간 전에 받은 배당을 쓴다(무료 월 500크레딧 안에서 다섯 리그를 돌리기 위해).
3. 원본 JSON은 Bronze(내용 해시 경로), 배당은 silver.odds_revisions(source_id = the-odds-api). 값이 바뀐 것만 새 버전.
   published_at = 북메이커 시장의 last_update, received_at = 받은 시각.
4. 팀 이름은 검토한 대조표(mappings/team_aliases.csv, source_id = the-odds-api)로만 경기에 연결한다.

키는 URL에 들어가므로 오류 메시지에서 지운다. 남은 크레딧이 RESERVE_CREDITS 아래면 1시간 전 조회만 한다.
실행: python -m pipelines.ingest.the_odds_api [--force 리그코드] [--suggest-aliases]
"""
import argparse
import difflib
import hashlib
import json
import sys
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.types.json import Jsonb

from ..aliases import ALIAS_FILE, load_alias_file, sync_aliases
from ..config import load_settings
from ..db import connect, migrate
from ..leagues import BY_CODE, LEAGUES
from ..object_store import client, ensure_bucket, put_if_absent
from .football_data_co_uk import season_of

SOURCE_ID = "the-odds-api"
API_BASE = "https://api.the-odds-api.com/v4"
REGION, MARKET = "eu", "h2h"
DUE_OFFSETS = (timedelta(hours=24), timedelta(hours=1))   # 이 시점 스냅샷 직전에만 배당을 받는다
LOOKAHEAD = timedelta(minutes=35)                          # DAG 30분 주기 + 여유
REFETCH_AFTER = timedelta(minutes=90)
RESERVE_CREDITS = 60


class OddsApiError(RuntimeError):
    """키를 지운 오류."""


def call(path: str, api_key: str, params: dict | None = None, *, opener=urllib.request.urlopen) -> tuple[object, dict]:
    url = f"{API_BASE}{path}?" + urllib.parse.urlencode({"apiKey": api_key, **(params or {})})
    try:
        with opener(url, timeout=30) as response:
            return json.loads(response.read()), {key.lower(): value for key, value in response.headers.items()}
    except urllib.error.HTTPError as exc:
        raise OddsApiError(f"{path} 실패: HTTP {exc.code}") from None
    except urllib.error.URLError as exc:
        raise OddsApiError(str(f"{path} 접속 실패: {exc.reason}").replace(api_key, "<redacted>")) from None


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def due_offsets(events: list[dict], now: datetime, low_credits: bool = False) -> list[str]:
    """지금 배당을 받아야 하는 이유(예: ["T-24H 2026-10-24T14:00"]). 비어 있으면 받지 않는다."""
    offsets = DUE_OFFSETS[-1:] if low_credits else DUE_OFFSETS
    reasons = []
    for event in events:
        kickoff = _utc(event["commence_time"])
        for offset in offsets:
            due = kickoff - offset
            if now - timedelta(minutes=5) <= due <= now + LOOKAHEAD:
                reasons.append(f"T-{int(offset.total_seconds() // 3600)}H {kickoff:%Y-%m-%dT%H:%M}")
    return sorted(set(reasons))


def quotes(event: dict) -> list[dict]:
    """한 경기의 1X2 배당 목록. 결과 이름은 홈 팀 이름 → HOME, 원정 → AWAY, Draw → DRAW."""
    names = {event["home_team"]: "HOME", event["away_team"]: "AWAY", "Draw": "DRAW"}
    rows = []
    for bookmaker in event.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            if market["key"] != MARKET:
                continue
            prices = {names.get(outcome["name"]): float(outcome["price"]) for outcome in market["outcomes"]}
            if set(prices) != {"HOME", "DRAW", "AWAY"} or any(price <= 1 for price in prices.values()):
                continue   # 세 결과가 다 없거나 1 이하 배당은 버린다(형식이 틀린 값)
            for selection, price in prices.items():
                rows.append({"bookmaker": bookmaker["key"], "selection": selection, "price": price,
                             "published_at": _utc(market.get("last_update") or bookmaker["last_update"])})
    return rows


def _simplify(name: str) -> str:
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    for word in ("fc", "cf", "afc", "sc", "ac", "ssc", "as", "club", "calcio", "de", "1.", "04", "05", "1899", "1846", "1907"):
        text = f" {text} ".replace(f" {word} ", " ").strip()
    return " ".join(text.replace("-", " ").split())


def similarity(left: str, right: str) -> float:
    return difflib.SequenceMatcher(None, _simplify(left), _simplify(right)).ratio()


def suggest_aliases(conn: psycopg.Connection, events: list[dict], league: str) -> list[dict]:
    """The Odds API 팀 이름 → openfootball 기준 이름 후보(검토용, 저장하지 않음).

    근거: 같은 리그에서 시작 시각이 정확히 같은 경기 중, 두 팀 이름이 가장 비슷한 경기. 동시에 시작하는 경기가 여럿이라
    이름 유사도로 고르고, 유사도와 함께 남긴다(사람이 낮은 것을 따로 본다).
    """
    suggestions = {}
    for event in events:
        kickoff = _utc(event["commence_time"])
        rows = conn.execute("""SELECT h.canonical_name, a.canonical_name
                               FROM silver.matches m
                               JOIN silver.match_revisions_known_at(now()) r USING (match_id)
                               LEFT JOIN silver.live_match_revisions_known_at(now()) l
                                      ON l.match_id = m.match_id AND l.source_id = 'football-data-org'
                               JOIN silver.teams h ON h.team_id = m.home_team_id
                               JOIN silver.teams a ON a.team_id = m.away_team_id
                               WHERE m.competition_code = %s AND COALESCE(l.kickoff_utc, r.kickoff_utc) = %s""",
                            (league, kickoff)).fetchall()
        if not rows:
            continue
        home, away = max(rows, key=lambda pair: similarity(event["home_team"], pair[0]) + similarity(event["away_team"], pair[1]))
        for name, canonical in ((event["home_team"], home), (event["away_team"], away)):
            score = similarity(name, canonical)
            if name not in suggestions or score > suggestions[name]["score"]:
                suggestions[name] = {"source_id": SOURCE_ID, "alias": name, "canonical_name": canonical, "score": round(score, 2),
                                     "evidence": f"same kickoff {kickoff:%Y-%m-%dT%H:%MZ} ({league}); name similarity {score:.2f}"}
    return sorted(suggestions.values(), key=lambda row: row["score"])


def last_fetch(conn: psycopg.Connection, league: str):
    row = conn.execute("""SELECT max(started_at) FROM ingest.runs WHERE source_id = %s AND dataset = %s
                          AND status = 'SUCCEEDED' AND (stats->>'odds_fetched')::boolean""", (SOURCE_ID, league)).fetchone()
    return row[0]


def write_odds(conn: psycopg.Connection, league: str, events: list[dict], *, raw_file_id: int, received_at: datetime) -> dict:
    """경기를 대조표로 연결하고, 키별 최신 버전과 값이 다른 배당만 다음 버전으로 넣는다."""
    teams = dict(conn.execute("SELECT alias, team_id FROM silver.team_aliases WHERE source_id = %s", (SOURCE_ID,)).fetchall())
    linked, unmatched = [], []
    for event in events:
        kickoff = _utc(event["commence_time"])
        home, away = teams.get(event["home_team"]), teams.get(event["away_team"])
        row = conn.execute("""SELECT match_id FROM silver.matches WHERE competition_code = %s AND season = %s
                              AND home_team_id = %s AND away_team_id = %s""",
                           (league, season_of(kickoff.date()), home, away)).fetchone() if home and away else None
        if row is None:
            unmatched.append({"home": event["home_team"], "away": event["away_team"], "kickoff": event["commence_time"],
                              "reason": "UNMAPPED_TEAM" if not (home and away) else "MATCH_NOT_FOUND"})
            continue
        linked.append((row[0], quotes(event)))
    conn.execute("""CREATE TEMP TABLE odds_api_stage (match_id BIGINT, bookmaker TEXT, selection TEXT, price NUMERIC(10, 4),
                                                       published_at TIMESTAMPTZ) ON COMMIT DROP""")
    with conn.cursor().copy("COPY odds_api_stage FROM STDIN") as copy:
        for match_id, rows in linked:
            for quote in rows:
                copy.write_row((match_id, quote["bookmaker"], quote["selection"], round(quote["price"], 4), quote["published_at"]))
    inserted = conn.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (match_id, bookmaker, selection) match_id, bookmaker, selection, revision_no, price
            FROM silver.odds_revisions
            WHERE source_id = %(source)s AND market = '1X2' AND timing = 'PRE_MATCH'
              AND match_id IN (SELECT DISTINCT match_id FROM odds_api_stage)
            ORDER BY match_id, bookmaker, selection, revision_no DESC
        )
        INSERT INTO silver.odds_revisions
        SELECT s.match_id, %(source)s, s.bookmaker, '1X2', 0, s.selection, 'PRE_MATCH', COALESCE(l.revision_no, 0) + 1,
               s.price, NULL, s.published_at, %(received)s, %(raw_file)s
        FROM odds_api_stage s LEFT JOIN latest l USING (match_id, bookmaker, selection)
        WHERE l.price IS DISTINCT FROM s.price""",
                            {"source": SOURCE_ID, "received": received_at, "raw_file": raw_file_id}).rowcount
    return {"events": len(events), "linked": len(linked), "odds_new": inserted, "unmatched": unmatched}


def fetch_league(conn: psycopg.Connection, league: str, settings, *, reasons: list[str], opener=urllib.request.urlopen) -> dict:
    """한 리그의 배당을 받아 Bronze·Silver에 넣고 실행 기록을 남긴다."""
    run_id = uuid.uuid4()
    conn.execute("INSERT INTO ingest.runs VALUES (%s, %s, %s, %s, NULL, 'RUNNING', NULL, NULL)",
                 (run_id, SOURCE_ID, league, datetime.now(timezone.utc)))
    conn.commit()
    try:
        events, headers = call(f"/sports/{BY_CODE[league].odds_api}/odds", settings.the_odds_api_key,
                               {"regions": REGION, "markets": MARKET, "oddsFormat": "decimal"}, opener=opener)
        received_at = datetime.now(timezone.utc)
        payload = json.dumps(events, ensure_ascii=False, sort_keys=True).encode()
        sha256 = hashlib.sha256(payload).hexdigest()
        object_key = f"{SOURCE_ID}/{league}/{received_at:%Y-%m-%d}/sha256={sha256}.json"
        store = client(settings)
        ensure_bucket(store, settings.bronze_bucket)
        put_if_absent(store, settings.bronze_bucket, object_key, payload, {"received-at": received_at.isoformat()})
        with conn.transaction():
            inserted = conn.execute("""INSERT INTO ingest.raw_files (source_id, dataset, source_url, sha256, byte_size,
                                       object_key, first_received_at) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                       ON CONFLICT (source_id, dataset, sha256) DO NOTHING RETURNING raw_file_id""",
                                    (SOURCE_ID, league, f"{API_BASE}/sports/{BY_CODE[league].odds_api}/odds", sha256,
                                     len(payload), object_key, received_at)).fetchone()
            raw_file_id = inserted[0] if inserted else conn.execute(
                "SELECT raw_file_id FROM ingest.raw_files WHERE source_id=%s AND dataset=%s AND sha256=%s",
                (SOURCE_ID, league, sha256)).fetchone()[0]
            conn.execute("INSERT INTO ingest.fetches (run_id, raw_file_id, received_at, is_new_content) VALUES (%s, %s, %s, %s)",
                         (run_id, raw_file_id, received_at, inserted is not None))
            result = write_odds(conn, league, events, raw_file_id=raw_file_id, received_at=received_at)
            for index, problem in enumerate(result["unmatched"]):
                conn.execute("""INSERT INTO silver.quality_issues (run_id, raw_file_id, record_index, issue_code, detail, created_at)
                                VALUES (%s, %s, %s, %s, %s, %s)""",
                             (run_id, raw_file_id, index, problem["reason"], Jsonb(problem), received_at))
        stats = {"odds_fetched": True, "reasons": reasons, "events": result["events"], "linked": result["linked"],
                 "odds_new": result["odds_new"], "unmatched": len(result["unmatched"]),
                 "credits_remaining": headers.get("x-requests-remaining"), "credits_last": headers.get("x-requests-last")}
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


def run_due(*, now: datetime | None = None, settings=None, force: tuple[str, ...] = (), opener=urllib.request.urlopen) -> dict:
    """리그마다 지금 배당을 받아야 하는지 보고, 필요한 리그만 받는다. 키가 없으면 NOT_CONFIGURED."""
    now, settings = now or datetime.now(timezone.utc), settings or load_settings()
    if not settings.the_odds_api_key:
        return {"status": "NOT_CONFIGURED"}
    report, credits = {}, None
    with connect(settings) as conn:
        migrate(conn)
        with conn.transaction():
            sync_aliases(conn, load_alias_file(ALIAS_FILE), now)
        for league in LEAGUES:
            events, headers = call(f"/sports/{league.odds_api}/events", settings.the_odds_api_key, opener=opener)
            credits = headers.get("x-requests-remaining", credits)
            low = credits is not None and int(credits) < RESERVE_CREDITS
            reasons = ["FORCED"] if league.code in force else due_offsets(events, now, low_credits=low)
            previous = last_fetch(conn, league.code)
            if not reasons or (league.code not in force and previous and now - previous < REFETCH_AFTER):
                report[league.code] = {"fetched": False, "events": len(events), "reasons": reasons}
                continue
            report[league.code] = {"fetched": True, **fetch_league(conn, league.code, settings, reasons=reasons, opener=opener)}
            credits = report[league.code]["credits_remaining"]
    return {"status": "OK", "credits_remaining": credits, "leagues": report}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="The Odds API 배당 수집(다섯 리그)")
    parser.add_argument("--force", nargs="*", default=[], help="시점과 관계없이 지금 받을 리그 코드(예: en.1)")
    parser.add_argument("--suggest-aliases", action="store_true", help="팀 이름 대조표 후보 출력(무료 /events만 사용)")
    args = parser.parse_args(argv)
    settings = load_settings()
    if args.suggest_aliases:
        with connect(settings) as conn:
            rows = []
            for league in LEAGUES:
                events, _ = call(f"/sports/{league.odds_api}/events", settings.the_odds_api_key)
                rows += [{**row, "league": league.code} for row in suggest_aliases(conn, events, league.code)]
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    print(json.dumps(run_due(settings=settings, force=tuple(args.force)), ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
