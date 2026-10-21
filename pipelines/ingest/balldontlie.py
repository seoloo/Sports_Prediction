"""balldontlie NBA 경기 일정·결과 수집(로드맵 5d).

흐름
1. /v1/games?seasons[]=시작연도 를 cursor로 끝까지 받는다(쪽당 100경기, 무료 등급 분당 5회를 RateLimiter로 지킴).
2. Bronze: 받은 쪽(page)마다 응답 바이트를 그대로 내용 해시 경로에 저장한다. 같은 내용이면 다시 올리지 않는다.
3. 정규화: 상태(예정·진행 중·종료·연기)와 점수를 명시적으로 처리한다(normalize_game). 예정 경기의 0점은 "모름"이다.
4. Silver: 소스 경기 ID가 정체성(silver.games), 바뀌는 내용은 버전(silver.game_revisions). 바뀐 경기만 새 버전.
5. 검사를 통과하지 못한 경기는 버리지 않고 이유와 함께 quality_issues에 남긴다.

축구와 분리: NBA는 같은 홈·원정 조합이 한 시즌에 여러 번 열려 silver.matches(리그·시즌·홈·원정이 한 번뿐)에 맞지 않는다.

실행: python -m pipelines.ingest.balldontlie --season 2025-26 [2024-25 ...]
키가 없으면(BALLDONTLIE_API_KEY) NOT_CONFIGURED를 돌려주고 아무것도 하지 않는다.
"""
import argparse
import hashlib
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from ..config import load_settings
from ..db import connect, migrate
from ..object_store import client, ensure_bucket, put_if_absent
from .rate_limit import RateLimiter
from .openfootball import INGEST_LOCK_NAMESPACE

SOURCE_ID = "balldontlie"
COMPETITION = "nba"
API_BASE = "https://api.balldontlie.io/v1"
PER_PAGE = 100            # 소스 최대값. 정규 시즌 1,230경기 + 플레이오프 ≈ 14쪽
MAX_PER_MINUTE = 5        # 무료 등급 한도
MAX_PAGES = 40            # cursor가 끝나지 않는 이상 응답을 막는 안전장치


class BalldontlieError(RuntimeError):
    """요청 실패. 키는 헤더로 보내므로 URL에 없지만, 메시지에 헤더를 넣지 않는다."""


@dataclass(frozen=True)
class NormalizedGame:
    """정규화를 통과한 경기 한 건. 점수가 None이면 "모름"이다(0으로 채우지 않음)."""
    record_index: int
    external_id: str
    postseason: bool
    home_external_id: str
    home_name: str
    away_external_id: str
    away_name: str
    local_date: date
    tipoff_utc: datetime | None
    status: str
    home_score: int | None
    away_score: int | None
    overtimes: int | None

    def content_hash(self) -> str:
        """바뀔 수 있는 내용만으로 만든 해시. 같으면 새 버전을 만들지 않는다."""
        content = {key: value for key, value in asdict(self).items()
                   if key in ("local_date", "tipoff_utc", "status", "home_score", "away_score", "overtimes")}
        return hashlib.sha256(json.dumps(content, default=str, sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True)
class QualityIssue:
    record_index: int
    issue_code: str
    detail: dict


def season_label(start_year: int) -> str:
    """2025 → "2025-26". balldontlie의 season=2025는 2025-26 시즌이다."""
    return f"{start_year}-{(start_year + 1) % 100:02d}"


def season_start_year(season: str) -> int:
    """"2025-26" → 2025. 형식이 다르면 오류."""
    start, end = season.split("-")
    if len(start) != 4 or len(end) != 2 or (int(start) + 1) % 100 != int(end):
        raise ValueError(f"시즌 형식이 아닙니다: {season} (예: 2025-26)")
    return int(start)


def current_season(now: datetime) -> str:
    """지금 진행 중이거나 가장 최근 시즌. NBA는 10월에 시작해 6월에 끝나므로 9월부터 새 시즌으로 본다."""
    return season_label(now.year if now.month >= 9 else now.year - 1)


def _status(raw: dict) -> str:
    state = str(raw.get("status_state") or "").lower()
    text = str(raw.get("status") or "")
    if raw.get("postponed"):
        return "POSTPONED"
    if state == "final" or text.lower() == "final":
        return "FINISHED"
    if state in ("in_progress", "halftime", "live") or (raw.get("period") or 0) > 0:
        return "IN_PROGRESS"
    return "SCHEDULED"


def _parse_utc(text):
    if not text:
        return None
    parsed = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else None   # 시간대 없는 시각은 추측하지 않는다


def normalize_game(raw: dict, record_index: int) -> tuple[NormalizedGame | None, QualityIssue | None]:
    """API 경기 객체 하나 → (정규화 경기, 품질 문제). DB·네트워크를 쓰지 않는 순수 함수."""
    try:
        home, away = raw["home_team"], raw["visitor_team"]
        local_date = date.fromisoformat(str(raw["date"])[:10])
        ids = (str(raw["id"]), str(home["id"]), str(away["id"]))
    except (KeyError, TypeError, ValueError) as exc:
        return None, QualityIssue(record_index, "MALFORMED_GAME", {"error": f"{type(exc).__name__}: {exc}",
                                                                    "id": raw.get("id") if isinstance(raw, dict) else None})
    if ids[1] == ids[2]:
        return None, QualityIssue(record_index, "SAME_TEAM", {"id": ids[0]})
    status = _status(raw)
    home_score, away_score = raw.get("home_team_score"), raw.get("visitor_team_score")
    if status in ("SCHEDULED", "POSTPONED"):
        home_score = away_score = None      # 시작 전 0점은 실제 점수가 아니다
    elif not (isinstance(home_score, int) and isinstance(away_score, int)) or home_score < 0 or away_score < 0:
        return None, QualityIssue(record_index, "INVALID_SCORE", {"id": ids[0], "home": home_score, "away": away_score})
    if status == "FINISHED" and home_score == away_score:
        return None, QualityIssue(record_index, "TIED_FINAL", {"id": ids[0], "score": [home_score, away_score]})
    period = raw.get("period")
    overtimes = max(period - 4, 0) if status == "FINISHED" and isinstance(period, int) and period >= 4 else None
    return NormalizedGame(record_index, ids[0], bool(raw.get("postseason")), ids[1], home.get("full_name") or "",
                          ids[2], away.get("full_name") or "", local_date, _parse_utc(raw.get("datetime")), status,
                          home_score, away_score, overtimes), None


def fetch_page(start_year: int, cursor, api_key: str, limiter: RateLimiter, *, opener=urllib.request.urlopen,
               max_attempts: int = 3) -> bytes:
    """한 쪽을 받는다. 429는 60초 기다렸다가 다시 시도하고, 그 밖의 오류는 키 없이 올린다."""
    params = [("seasons[]", start_year), ("per_page", PER_PAGE)] + ([("cursor", cursor)] if cursor is not None else [])
    url = f"{API_BASE}/games?{urllib.parse.urlencode(params)}"
    for attempt in range(1, max_attempts + 1):
        limiter.wait()
        request = urllib.request.Request(url, headers={"Authorization": api_key, "User-Agent": "sports-prediction-local/0.1"})
        try:
            with opener(request, timeout=30) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < max_attempts:
                limiter.blocked_until = limiter.clock() + 60
                continue
            hint = " (키가 없거나 무료 등급에서 쓸 수 없는 엔드포인트)" if exc.code == 401 else ""
            raise BalldontlieError(f"balldontlie HTTP {exc.code} for season {start_year}{hint}") from None
        except (urllib.error.URLError, TimeoutError) as exc:
            raise BalldontlieError(f"balldontlie 연결 실패: {type(exc).__name__}") from None
    raise RuntimeError("unreachable")


def fetch_season(start_year: int, api_key: str, *, limiter: RateLimiter | None = None,
                 opener=urllib.request.urlopen) -> list[bytes]:
    """시즌 전체를 쪽 단위 원본 바이트 목록으로 받는다. meta.next_cursor가 없으면 끝."""
    limiter = limiter or RateLimiter(MAX_PER_MINUTE)
    pages, cursor = [], None
    for _ in range(MAX_PAGES):
        payload = fetch_page(start_year, cursor, api_key, limiter, opener=opener)
        pages.append(payload)
        cursor = (json.loads(payload).get("meta") or {}).get("next_cursor")
        if cursor is None:
            return pages
    raise BalldontlieError(f"{MAX_PAGES}쪽을 넘었습니다(cursor가 끝나지 않음)")


def _team_id(conn: psycopg.Connection, external_id: str, name: str, now: datetime) -> int:
    """balldontlie 팀 ID의 team_id. 처음 보면 농구 팀과 별칭을 만든다(이 소스가 NBA의 기준 소스).

    같은 이름의 축구 팀이 이미 있으면 섞지 않고 오류를 낸다.
    """
    row = conn.execute("SELECT team_id FROM silver.team_aliases WHERE source_id=%s AND alias=%s",
                       (SOURCE_ID, external_id)).fetchone()
    if row:
        return row[0]
    existing = conn.execute("SELECT team_id, sport FROM silver.teams WHERE canonical_name=%s", (name,)).fetchone()
    if existing and existing[1] != "basketball":
        raise ValueError(f"팀 이름이 다른 종목 팀과 겹칩니다: {name}")
    team_id = existing[0] if existing else conn.execute(
        "INSERT INTO silver.teams (canonical_name, created_at, sport) VALUES (%s, %s, 'basketball') RETURNING team_id",
        (name, now)).fetchone()[0]
    conn.execute("INSERT INTO silver.team_aliases VALUES (%s, %s, %s, %s)", (SOURCE_ID, external_id, team_id, now))
    return team_id


def write_silver(conn: psycopg.Connection, games: list[tuple[NormalizedGame, int]], *, season: str,
                 received_at: datetime) -> dict:
    """(정규화 경기, raw_file_id) 목록을 Silver에 저장한다. 호출한 쪽의 트랜잭션 안에서 실행한다."""
    counts = {"games_new": 0, "revisions_new": 0, "unchanged": 0}
    for game, raw_file_id in games:
        row = conn.execute("SELECT game_id FROM silver.games WHERE source_id=%s AND external_id=%s",
                           (SOURCE_ID, game.external_id)).fetchone()
        if row:
            game_id = row[0]
        else:
            home_id = _team_id(conn, game.home_external_id, game.home_name, received_at)
            away_id = _team_id(conn, game.away_external_id, game.away_name, received_at)
            game_id = conn.execute("""INSERT INTO silver.games (sport, competition_code, season, source_id, external_id,
                                      postseason, home_team_id, away_team_id, created_at)
                                      VALUES ('basketball', %s, %s, %s, %s, %s, %s, %s, %s) RETURNING game_id""",
                                   (COMPETITION, season, SOURCE_ID, game.external_id, game.postseason,
                                    home_id, away_id, received_at)).fetchone()[0]
            counts["games_new"] += 1
        latest = conn.execute("""SELECT revision_no, content_hash FROM silver.game_revisions
                                 WHERE game_id=%s ORDER BY revision_no DESC LIMIT 1""", (game_id,)).fetchone()
        content_hash = game.content_hash()
        if latest and latest[1] == content_hash:
            counts["unchanged"] += 1
            continue
        conn.execute("""INSERT INTO silver.game_revisions VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)""",
                     (game_id, (latest[0] + 1) if latest else 1, game.local_date, game.tipoff_utc, game.status,
                      game.home_score, game.away_score, game.overtimes, received_at, raw_file_id, content_hash))
        counts["revisions_new"] += 1
    return counts


def ingest(season: str, *, pages: list[bytes] | None = None, settings=None, received_at: datetime | None = None,
           opener=urllib.request.urlopen) -> dict:
    """한 시즌을 수집해 Bronze·Silver에 저장하고 실행 통계를 돌려준다.

    pages를 주면 내려받지 않고 그 바이트를 쓴다(테스트·오프라인 재처리용).
    같은 내용을 다시 받아도 Bronze 객체·raw_files·경기 버전은 늘지 않고 fetches 기록만 추가된다.
    """
    settings = settings or load_settings()
    start_year = season_start_year(season)
    if pages is None and not settings.balldontlie_api_key:
        return {"status": "NOT_CONFIGURED", "season": season, "reason": "BALLDONTLIE_API_KEY가 .env에 없습니다"}
    dataset = f"{COMPETITION}/{season}"
    source_url = f"{API_BASE}/games?seasons[]={start_year}"
    run_id = uuid.uuid4()
    with connect(settings) as conn:
        migrate(conn)
        conn.execute("INSERT INTO ingest.runs VALUES (%s, %s, %s, %s, NULL, 'RUNNING', NULL, NULL)",
                     (run_id, SOURCE_ID, dataset, datetime.now(timezone.utc)))
        conn.commit()
        try:
            pages = pages if pages is not None else fetch_season(start_year, settings.balldontlie_api_key, opener=opener)
            received_at = received_at or datetime.now(timezone.utc)
            store = client(settings)
            ensure_bucket(store, settings.bronze_bucket)
            hashed = [(hashlib.sha256(page).hexdigest(), page) for page in pages]
            uploaded = 0
            for sha256, page in hashed:
                uploaded += put_if_absent(store, settings.bronze_bucket, f"{SOURCE_ID}/{dataset}/sha256={sha256}.json", page,
                                          {"source-url": source_url, "received-at": received_at.isoformat()})
            games, issues, seen, record_index = [], [], set(), 0
            for page_no, (sha256, page) in enumerate(hashed):
                for raw in json.loads(page).get("data") or []:
                    game, issue = normalize_game(raw, record_index)
                    if game and game.external_id in seen:
                        game, issue = None, QualityIssue(record_index, "DUPLICATE_GAME", {"id": game.external_id})
                    if game:
                        seen.add(game.external_id)
                        games.append((page_no, game))
                    else:
                        issues.append((page_no, issue))
                    record_index += 1
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s, hashtext(%s))", (INGEST_LOCK_NAMESPACE, dataset))
                raw_ids, new_content = [], []
                for sha256, page in hashed:
                    inserted = conn.execute("""INSERT INTO ingest.raw_files
                                               (source_id, dataset, source_url, sha256, byte_size, object_key, first_received_at)
                                               VALUES (%s, %s, %s, %s, %s, %s, %s)
                                               ON CONFLICT (source_id, dataset, sha256) DO NOTHING RETURNING raw_file_id""",
                                            (SOURCE_ID, dataset, source_url, sha256, len(page),
                                             f"{SOURCE_ID}/{dataset}/sha256={sha256}.json", received_at)).fetchone()
                    raw_id = inserted[0] if inserted else conn.execute(
                        "SELECT raw_file_id FROM ingest.raw_files WHERE source_id=%s AND dataset=%s AND sha256=%s",
                        (SOURCE_ID, dataset, sha256)).fetchone()[0]
                    conn.execute("INSERT INTO ingest.fetches (run_id, raw_file_id, received_at, is_new_content) VALUES (%s, %s, %s, %s)",
                                 (run_id, raw_id, received_at, inserted is not None))
                    raw_ids.append(raw_id)
                    new_content.append(inserted is not None)
                # 이미 본 쪽이어도 Silver 비교는 항상 한다(원본이 A → B → A로 되돌아간 경우를 맞추기 위해).
                counts = write_silver(conn, [(game, raw_ids[page_no]) for page_no, game in games],
                                      season=season, received_at=received_at)
                for page_no, issue in issues:
                    if new_content[page_no]:   # 같은 내용당 한 번만
                        conn.execute("""INSERT INTO silver.quality_issues (run_id, raw_file_id, record_index, issue_code, detail, created_at)
                                        VALUES (%s, %s, %s, %s, %s, %s)""",
                                     (run_id, raw_ids[page_no], issue.record_index, issue.issue_code, Jsonb(issue.detail), received_at))
            stats = {"status": "OK", "dataset": dataset, "pages": len(pages), "new_pages": sum(new_content),
                     "bronze_uploaded": uploaded, "records": record_index, "valid_games": len(games),
                     "quality_issues": len(issues), **counts}
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


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="balldontlie NBA 경기 수집")
    parser.add_argument("--season", nargs="*", help="시즌(예: 2025-26). 없으면 현재 시즌")
    args = parser.parse_args(argv)
    seasons = args.season or [current_season(datetime.now(timezone.utc))]
    results = [ingest(season) for season in seasons]
    print(json.dumps(results, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
