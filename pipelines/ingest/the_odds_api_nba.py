"""The Odds API NBA 배당 수집(로드맵 5d-b): 다가올 NBA 경기의 승패(2지선다) 배당, 유럽 북메이커.

예산: 축구 다섯 리그가 이미 월 약 300크레딧을 쓴다(무료 월 500). NBA는 거의 매일 경기가 있어서 경기 시각마다 받으면
예산을 넘는다. 그래서 하루 경기 묶음(미국 동부 날짜 기준)의 첫 경기 1시간 전에 한 번만 받는다(1크레딧).
한 번의 /odds 응답에 다가올 경기 전체가 들어 있어, 그날 늦은 경기와 다음 날 경기 배당도 함께 받는다. 월 약 30크레딧.

흐름(prematch_snapshots DAG가 30분마다 부른다)
1. /events(무료)로 다가올 경기를 보고, 그날 첫 경기 1시간 전이 곧(35분 안) 오면 /odds를 받는다.
2. 원본은 Bronze(내용 해시 경로), 배당은 silver.game_odds_revisions(값이 바뀐 것만 새 버전).
3. 팀 이름은 검토한 대조표(mappings/team_aliases.csv, source_id = the-odds-api, sport = basketball)로만 연결하고,
   같은 홈·원정이면서 시작 시각이 3시간 안인 silver.games 경기에 붙인다(두 소스의 시작 시각이 몇 분 다를 수 있다).
남은 크레딧이 RESERVE_CREDITS 아래면 NBA는 받지 않는다(축구 우선).

실행: python -m pipelines.ingest.the_odds_api_nba [--force] [--suggest-aliases]
"""
import argparse
import hashlib
import json
import sys
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg
from psycopg.types.json import Jsonb

from ..aliases import ALIAS_FILE, load_alias_file, sync_aliases
from ..config import load_settings
from ..db import connect, migrate
from ..object_store import client, ensure_bucket, put_if_absent
from .the_odds_api import (API_BASE, LOOKAHEAD, REFETCH_AFTER, REGION, RESERVE_CREDITS, SOURCE_ID, _utc, call,
                           last_fetch, similarity)

SPORT_KEY = "basketball_nba"
DATASET = "nba"
MARKET = "h2h"
DUE_BEFORE_FIRST_TIP = timedelta(hours=1)
LINK_WINDOW = timedelta(hours=3)
SLATE_TIMEZONE = ZoneInfo("America/New_York")   # NBA 일정의 하루 = 미국 동부 날짜


def slate_first_tips(events: list[dict]) -> dict:
    """미국 동부 날짜 → 그날 첫 경기 시작 시각(UTC)."""
    firsts = {}
    for event in events:
        tip = _utc(event["commence_time"])
        day = tip.astimezone(SLATE_TIMEZONE).date()
        firsts[day] = min(firsts.get(day, tip), tip)
    return firsts


def due_reasons(events: list[dict], now: datetime) -> list[str]:
    """지금 받아야 하는 이유(예: ["FIRST-TIP-1H 2026-10-22T23:10"]). 비어 있으면 받지 않는다."""
    reasons = []
    for tip in slate_first_tips(events).values():
        due = tip - DUE_BEFORE_FIRST_TIP
        if now - timedelta(minutes=5) <= due <= now + LOOKAHEAD:
            reasons.append(f"FIRST-TIP-1H {tip:%Y-%m-%dT%H:%M}")
    return sorted(reasons)


MARKET_NAMES = {"h2h": "H2H", "spreads": "SPREAD", "totals": "TOTAL"}


def quotes(event: dict, markets: tuple = (MARKET,)) -> list[dict]:
    """한 경기의 배당. 승패·핸디캡은 홈 팀 → HOME, 원정 팀 → AWAY, 언더오버는 Over → OVER, Under → UNDER.

    핸디캡의 line은 홈 팀 핸디캡(원정 핸디캡은 그 반대 부호여야 한다), 언더오버의 line은 총점 기준선, 승패는 0.
    두 결과가 다 없거나, 1 이하 배당이거나, 기준선이 맞지 않으면 그 북메이커·시장은 버린다.
    """
    names = {event["home_team"]: "HOME", event["away_team"]: "AWAY", "Over": "OVER", "Under": "UNDER"}
    expected = {"h2h": {"HOME", "AWAY"}, "spreads": {"HOME", "AWAY"}, "totals": {"OVER", "UNDER"}}
    rows = []
    for bookmaker in event.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            if market["key"] not in markets:
                continue
            outcomes = {names.get(o["name"]): o for o in market["outcomes"]}
            if set(outcomes) != expected[market["key"]] or any(float(o["price"]) <= 1 for o in outcomes.values()):
                continue
            if market["key"] == "h2h":
                line = 0.0
            elif market["key"] == "spreads":
                line = float(outcomes["HOME"].get("point", "nan"))
                if not line == -float(outcomes["AWAY"].get("point", "nan")):
                    continue
            else:
                line = float(outcomes["OVER"].get("point", "nan"))
                if not line == float(outcomes["UNDER"].get("point", "nan")) or line <= 0:
                    continue
            for selection, outcome in outcomes.items():
                rows.append({"bookmaker": bookmaker["key"], "market": MARKET_NAMES[market["key"]], "line": line,
                             "selection": selection, "price": float(outcome["price"]),
                             "published_at": _utc(market.get("last_update") or bookmaker["last_update"])})
    return rows


def find_game(conn: psycopg.Connection, home_id: int, away_id: int, tip: datetime):
    """같은 홈·원정이면서 시작 시각이 LINK_WINDOW 안인 경기. 시각을 모르는 경기는 미국 동부 날짜가 같으면 후보."""
    rows = conn.execute("""SELECT g.game_id FROM silver.games g
                           JOIN silver.game_revisions_known_at(now()) k USING (game_id)
                           WHERE g.home_team_id = %s AND g.away_team_id = %s
                             AND (k.tipoff_utc BETWEEN %s AND %s OR (k.tipoff_utc IS NULL AND k.local_date = %s))""",
                        (home_id, away_id, tip - LINK_WINDOW, tip + LINK_WINDOW,
                         tip.astimezone(SLATE_TIMEZONE).date())).fetchall()
    return rows[0][0] if len(rows) == 1 else ("AMBIGUOUS" if rows else None)


def write_odds(conn: psycopg.Connection, events: list[dict], *, raw_file_id: int, received_at: datetime,
               markets: tuple = (MARKET,)) -> dict:
    """경기를 대조표로 연결하고, 키(북메이커·시장·기준선·선택)별 최신 버전과 값이 다른 배당만 다음 버전으로 넣는다."""
    teams = dict(conn.execute("""SELECT a.alias, a.team_id FROM silver.team_aliases a JOIN silver.teams t USING (team_id)
                                 WHERE a.source_id = %s AND t.sport = 'basketball'""", (SOURCE_ID,)).fetchall())
    linked, unmatched = [], []
    for event in events:
        home, away = teams.get(event["home_team"]), teams.get(event["away_team"])
        game = find_game(conn, home, away, _utc(event["commence_time"])) if home and away else None
        if not isinstance(game, int):
            reason = "UNMAPPED_TEAM" if not (home and away) else ("MATCH_AMBIGUOUS" if game else "MATCH_NOT_FOUND")
            unmatched.append({"home": event["home_team"], "away": event["away_team"], "kickoff": event["commence_time"],
                              "reason": reason})
            continue
        linked.append((game, quotes(event, markets)))
    conn.execute("""CREATE TEMP TABLE game_odds_stage (game_id BIGINT, bookmaker TEXT, market TEXT, line NUMERIC(6, 2),
                                                        selection TEXT, price NUMERIC(10, 4), published_at TIMESTAMPTZ)
                    ON COMMIT DROP""")
    with conn.cursor().copy("COPY game_odds_stage FROM STDIN") as copy:
        for game_id, rows in linked:
            for quote in rows:
                copy.write_row((game_id, quote["bookmaker"], quote["market"], quote["line"], quote["selection"],
                                round(quote["price"], 4), quote["published_at"]))
    inserted = conn.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (game_id, bookmaker, market, line, selection) game_id, bookmaker, market, line, selection,
                   revision_no, price
            FROM silver.game_odds_revisions
            WHERE source_id = %(source)s AND game_id IN (SELECT DISTINCT game_id FROM game_odds_stage)
            ORDER BY game_id, bookmaker, market, line, selection, revision_no DESC
        )
        INSERT INTO silver.game_odds_revisions (game_id, source_id, bookmaker, market, selection, revision_no, price,
                                                published_at, received_at, raw_file_id, line)
        SELECT s.game_id, %(source)s, s.bookmaker, s.market, s.selection, COALESCE(l.revision_no, 0) + 1,
               s.price, s.published_at, %(received)s, %(raw_file)s, s.line
        FROM game_odds_stage s LEFT JOIN latest l USING (game_id, bookmaker, market, line, selection)
        WHERE l.price IS DISTINCT FROM s.price""",
                            {"source": SOURCE_ID, "received": received_at, "raw_file": raw_file_id}).rowcount
    return {"events": len(events), "linked": len(linked), "odds_new": inserted, "unmatched": unmatched}


def fetch(conn: psycopg.Connection, settings, *, reasons: list[str], started_at: datetime | None = None,
          opener=urllib.request.urlopen, markets: tuple = (MARKET,), dataset: str = DATASET) -> dict:
    """NBA 배당을 받아 Bronze·Silver에 넣고 실행 기록을 남긴다(시장 수만큼 크레딧). started_at은 재조회 간격의 기준이다."""
    run_id = uuid.uuid4()
    conn.execute("INSERT INTO ingest.runs VALUES (%s, %s, %s, %s, NULL, 'RUNNING', NULL, NULL)",
                 (run_id, SOURCE_ID, dataset, started_at or datetime.now(timezone.utc)))
    conn.commit()
    try:
        events, headers = call(f"/sports/{SPORT_KEY}/odds", settings.the_odds_api_key,
                               {"regions": REGION, "markets": ",".join(markets), "oddsFormat": "decimal"}, opener=opener)
        received_at = datetime.now(timezone.utc)
        payload = json.dumps(events, ensure_ascii=False, sort_keys=True).encode()
        sha256 = hashlib.sha256(payload).hexdigest()
        object_key = f"{SOURCE_ID}/{dataset}/{received_at:%Y-%m-%d}/sha256={sha256}.json"
        store = client(settings)
        ensure_bucket(store, settings.bronze_bucket)
        put_if_absent(store, settings.bronze_bucket, object_key, payload, {"received-at": received_at.isoformat()})
        with conn.transaction():
            inserted = conn.execute("""INSERT INTO ingest.raw_files (source_id, dataset, source_url, sha256, byte_size,
                                       object_key, first_received_at) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                       ON CONFLICT (source_id, dataset, sha256) DO NOTHING RETURNING raw_file_id""",
                                    (SOURCE_ID, dataset, f"{API_BASE}/sports/{SPORT_KEY}/odds", sha256, len(payload),
                                     object_key, received_at)).fetchone()
            raw_file_id = inserted[0] if inserted else conn.execute(
                "SELECT raw_file_id FROM ingest.raw_files WHERE source_id=%s AND dataset=%s AND sha256=%s",
                (SOURCE_ID, dataset, sha256)).fetchone()[0]
            conn.execute("INSERT INTO ingest.fetches (run_id, raw_file_id, received_at, is_new_content) VALUES (%s, %s, %s, %s)",
                         (run_id, raw_file_id, received_at, inserted is not None))
            result = write_odds(conn, events, raw_file_id=raw_file_id, received_at=received_at, markets=markets)
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


def run_due(*, now: datetime | None = None, settings=None, force: bool = False, opener=urllib.request.urlopen) -> dict:
    """지금 NBA 배당을 받아야 하면 받는다. 키가 없으면 NOT_CONFIGURED."""
    now, settings = now or datetime.now(timezone.utc), settings or load_settings()
    if not settings.the_odds_api_key:
        return {"status": "NOT_CONFIGURED"}
    with connect(settings) as conn:
        migrate(conn)
        with conn.transaction():
            sync_aliases(conn, load_alias_file(ALIAS_FILE), now)
        events, headers = call(f"/sports/{SPORT_KEY}/events", settings.the_odds_api_key, opener=opener)
        credits = headers.get("x-requests-remaining")
        reasons = ["FORCED"] if force else due_reasons(events, now)
        if not force and credits is not None and int(credits) < RESERVE_CREDITS:
            return {"status": "OK", "fetched": False, "events": len(events), "reasons": ["LOW_CREDITS"], "credits_remaining": credits}
        previous = last_fetch(conn, DATASET)
        if not reasons or (not force and previous and now - previous < REFETCH_AFTER):
            return {"status": "OK", "fetched": False, "events": len(events), "reasons": reasons, "credits_remaining": credits}
        return {"status": "OK", "fetched": True, **fetch(conn, settings, reasons=reasons, started_at=now, opener=opener)}


LINES_DATASET = "nba-lines"
LINE_MARKETS = ("spreads", "totals")
LINES_REFETCH_AFTER = timedelta(minutes=60)


def fetch_lines(*, now: datetime | None = None, settings=None, force: bool = False, opener=urllib.request.urlopen) -> dict:
    """핸디캡·언더오버 배당을 요청할 때만 받는다(6c, 2크레딧: 시장 2개 × 지역 1개). 자동 수집에는 넣지 않는다.

    1시간 안에 받은 적이 있으면 다시 받지 않는다(force 제외). 크레딧이 RESERVE_CREDITS 아래면 받지 않는다(축구 자동 수집 우선).
    """
    now, settings = now or datetime.now(timezone.utc), settings or load_settings()
    if not settings.the_odds_api_key:
        return {"status": "NOT_CONFIGURED"}
    with connect(settings) as conn:
        migrate(conn)
        with conn.transaction():
            sync_aliases(conn, load_alias_file(ALIAS_FILE), now)
        previous = last_fetch(conn, LINES_DATASET)
        if not force and previous and now - previous < LINES_REFETCH_AFTER:
            return {"status": "OK", "fetched": False, "reasons": ["RECENT"], "last_fetch": previous.isoformat()}
        _, headers = call(f"/sports/{SPORT_KEY}/events", settings.the_odds_api_key, opener=opener)   # 무료: 남은 크레딧 확인
        credits = headers.get("x-requests-remaining")
        if credits is not None and int(credits) < RESERVE_CREDITS + len(LINE_MARKETS):
            return {"status": "OK", "fetched": False, "reasons": ["LOW_CREDITS"], "credits_remaining": credits}
        return {"status": "OK", "fetched": True, **fetch(conn, settings, reasons=["ON_DEMAND"], started_at=now, opener=opener,
                                                         markets=LINE_MARKETS, dataset=LINES_DATASET)}


def suggest_aliases(conn: psycopg.Connection, events: list[dict]) -> list[dict]:
    """The Odds API 팀 이름 → balldontlie 기준 이름 후보(검토용, 저장하지 않음).

    근거: 시작 시각이 LINK_WINDOW 안인 NBA 경기 중 두 팀 이름이 가장 비슷한 경기. 유사도와 근거 경기 수를 함께 남긴다.
    """
    found = {}
    for event in events:
        tip = _utc(event["commence_time"])
        rows = conn.execute("""SELECT h.canonical_name, a.canonical_name FROM silver.games g
                               JOIN silver.game_revisions_known_at(now()) k USING (game_id)
                               JOIN silver.teams h ON h.team_id = g.home_team_id JOIN silver.teams a ON a.team_id = g.away_team_id
                               WHERE k.tipoff_utc BETWEEN %s AND %s""", (tip - LINK_WINDOW, tip + LINK_WINDOW)).fetchall()
        if not rows:
            continue
        home, away = max(rows, key=lambda pair: similarity(event["home_team"], pair[0]) + similarity(event["away_team"], pair[1]))
        for name, canonical in ((event["home_team"], home), (event["away_team"], away)):
            entry = found.setdefault((name, canonical), {"games": 0, "score": round(similarity(name, canonical), 2)})
            entry["games"] += 1
    best = {}
    for (name, canonical), entry in found.items():
        if name not in best or entry["games"] > best[name]["games"]:
            best[name] = {"source_id": SOURCE_ID, "alias": name, "canonical_name": canonical, "score": entry["score"],
                          "games": entry["games"], "candidates": sum(1 for key in found if key[0] == name)}
    return sorted(best.values(), key=lambda row: (row["score"], row["alias"]))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="The Odds API NBA 배당 수집")
    parser.add_argument("--force", action="store_true", help="시점과 관계없이 지금 받기(1크레딧)")
    parser.add_argument("--suggest-aliases", action="store_true", help="팀 이름 대조표 후보 출력(무료 /events만 사용)")
    args = parser.parse_args(argv)
    settings = load_settings()
    if args.suggest_aliases:
        with connect(settings) as conn:
            events, _ = call(f"/sports/{SPORT_KEY}/events", settings.the_odds_api_key)
            print(json.dumps(suggest_aliases(conn, events), ensure_ascii=False, indent=2))
        return 0
    print(json.dumps(run_due(settings=settings, force=args.force), ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
