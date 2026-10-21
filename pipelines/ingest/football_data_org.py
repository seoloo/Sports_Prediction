"""football-data.org API 수집기(로드맵 2b).

약관 확인 사항(docs/football_data_sources.md): 무료 플랜 분당 10회, 출처 표기
"Football data provided by the Football-Data.org API", API 키는 공개 저장소에 두지 않는다(.env에만).

흐름(poll_once)
1. 대회·시즌의 전체 경기 목록을 한 번에 조회한다(요청 1회).
2. Bronze: 응답 원본을 내용 해시 경로로 S3에 보존하고 ingest.raw_files·fetches에 기록한다.
3. 응답 내용이 지난번과 같으면 이벤트를 보내지 않는다. 바뀌었으면 경기마다 이벤트를 Kafka로 보낸다.
   Silver 반영은 소비자(pipelines/streaming/match_consumer.py)가 한다. 수집과 반영을 나눠, 한쪽이 멈춰도 다른 쪽에 영향이 없다.

실행
  python -m pipelines.ingest.football_data_org --once            # 한 번 조회
  python -m pipelines.ingest.football_data_org                   # 계속 실행(경기 상황에 맞춰 조회 간격 자동 조절)
"""
import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from psycopg.types.json import Jsonb

from ..config import load_settings
from ..db import connect, migrate
from ..object_store import client, ensure_bucket, put_if_absent
from ..streaming import events, kafka_io

SOURCE_ID = "football-data-org"
API_BASE = "https://api.football-data.org/v4"
TOPIC = "fdorg.matches.v1"
ATTRIBUTION = "Football data provided by the Football-Data.org API"

# football-data.org 대회 코드 → openfootball 리그 코드
# 대회 코드 → openfootball 리그 코드(5a: 다섯 리그, pipelines/leagues.py)
from ..leagues import LEAGUES  # noqa: E402

COMPETITION_TO_LEAGUE = {league.fdorg: league.code for league in LEAGUES}

# 경기 중이거나 곧 시작·방금 끝난 경기가 있으면 자주, 아니면 드물게 조회한다(분당 10회 한도 안에서 충분히 여유 있음).
LIVE_INTERVAL = timedelta(minutes=2)
MATCHDAY_INTERVAL = timedelta(minutes=15)
IDLE_INTERVAL = timedelta(hours=6)
LIVE_STATUSES = {"IN_PLAY", "PAUSED"}


class RateLimiter:
    """최근 60초 안의 요청 수를 세어 한도(기본 분당 10회)를 넘지 않게 기다린다.

    서버가 알려 주는 남은 횟수(X-Requests-Available-Minute)가 0이면 초기화 시각까지 기다린다.
    clock·sleep을 바꿔 넣을 수 있어 실제로 기다리지 않고 테스트할 수 있다.
    """

    def __init__(self, max_per_minute: int = 10, clock=time.monotonic, sleep=time.sleep):
        self.max_per_minute, self.clock, self.sleep = max_per_minute, clock, sleep
        self.sent = deque()
        self.blocked_until = 0.0

    def wait(self) -> None:
        """다음 요청을 보내도 될 때까지 기다린다."""
        now = self.clock()
        if self.blocked_until > now:
            self.sleep(self.blocked_until - now)
            now = self.clock()
        while self.sent and now - self.sent[0] >= 60:
            self.sent.popleft()
        if len(self.sent) >= self.max_per_minute:
            self.sleep(60 - (now - self.sent[0]))
            now = self.clock()
            self.sent.popleft()
        self.sent.append(now)

    def update_from_headers(self, headers) -> None:
        """서버가 남은 요청이 0이라고 하면 초기화(X-RequestCounter-Reset초)까지 막는다."""
        lowered = {key.lower(): value for key, value in headers.items()}
        available, reset = lowered.get("x-requests-available-minute"), lowered.get("x-requestcounter-reset")
        if available is not None and available.isdigit() and int(available) == 0 and reset and reset.isdigit():
            self.blocked_until = self.clock() + int(reset)


def fetch_json(path: str, api_key: str, limiter: RateLimiter, *, opener=urllib.request.urlopen, max_attempts: int = 3):
    """GET 요청 하나. (응답 바이트, 헤더 dict)를 돌려준다.

    429(요청 한도 초과)를 받으면 서버가 알려 준 초기화 시간(없으면 60초)만큼 기다렸다가 다시 시도한다.
    그 밖의 HTTP 오류는 그대로 올려 호출한 쪽(실행 기록)이 실패로 남기게 한다.
    """
    for attempt in range(1, max_attempts + 1):
        limiter.wait()
        request = urllib.request.Request(f"{API_BASE}{path}", headers={"X-Auth-Token": api_key})
        try:
            with opener(request, timeout=30) as response:
                headers = dict(response.headers)
                limiter.update_from_headers(headers)
                return response.read(), headers
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or attempt == max_attempts:
                raise
            reset = {key.lower(): value for key, value in (exc.headers or {}).items()}.get("x-requestcounter-reset", "60")
            limiter.blocked_until = limiter.clock() + (int(reset) if str(reset).isdigit() else 60)
    raise RuntimeError("unreachable")


def season_start_year(season: str) -> int:
    """"2026-27" → 2026 (football-data.org의 season 매개변수)."""
    return int(season.split("-")[0])


def _parse_utc(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00")) if text else None


def normalize_match(raw: dict) -> dict:
    """API 경기 객체 → 이벤트 payload. 결측 점수는 None으로 둔다(0으로 채우지 않음). DB·네트워크를 쓰지 않는 순수 함수."""
    score = raw.get("score") or {}
    full, half = score.get("fullTime") or {}, score.get("halfTime") or {}
    return {
        "external_match_id": str(raw["id"]),
        "utc_kickoff": raw.get("utcDate"),
        "status": raw.get("status"),
        "matchday": raw.get("matchday"),
        "home_team_id": str(raw["homeTeam"]["id"]), "home_team_name": raw["homeTeam"].get("name"),
        "away_team_id": str(raw["awayTeam"]["id"]), "away_team_name": raw["awayTeam"].get("name"),
        "ft_home": full.get("home"), "ft_away": full.get("away"),
        "ht_home": half.get("home"), "ht_away": half.get("away"),
        "duration": score.get("duration"),
        "last_updated": raw.get("lastUpdated"),
    }


def next_poll_interval(matches: list[dict], now: datetime) -> timedelta:
    """다음 조회까지 기다릴 시간. 경기 중이거나 시작 15분 전~종료 예상(시작 후 2시간 30분) 사이 경기가 있으면 2분,
    24시간 안에 경기가 있으면 15분, 그 밖에는 6시간."""
    soon = False
    for match in matches:
        kickoff = _parse_utc(match.get("utc_kickoff"))
        if match.get("status") in LIVE_STATUSES:
            return LIVE_INTERVAL
        if kickoff is None or match.get("status") in ("FINISHED", "CANCELLED", "AWARDED"):
            continue
        if kickoff - timedelta(minutes=15) <= now <= kickoff + timedelta(hours=2, minutes=30):
            return LIVE_INTERVAL
        if now < kickoff <= now + timedelta(hours=24):
            soon = True
    return MATCHDAY_INTERVAL if soon else IDLE_INTERVAL


def poll_once(competition: str = "PL", season: str | None = None, *, settings=None, limiter: RateLimiter | None = None,
              opener=urllib.request.urlopen, kafka_producer=None, now: datetime | None = None, topic: str = TOPIC) -> dict:
    """한 번 조회해 Bronze에 보존하고, 내용이 바뀌었으면 경기별 이벤트를 보낸다. 실행 통계를 돌려준다."""
    from ..schedule import seasons_for
    settings = settings or load_settings()
    if not settings.football_data_org_api_key:
        raise RuntimeError(".env에 FOOTBALL_DATA_ORG_API_KEY가 없습니다.")
    now = now or datetime.now(timezone.utc)
    season = season or seasons_for(now.date())[-1]
    dataset = f"{competition}/{season}"
    run_id = uuid.uuid4()
    with connect(settings) as conn:
        migrate(conn)
        conn.execute("INSERT INTO ingest.runs VALUES (%s, %s, %s, %s, NULL, 'RUNNING', NULL, NULL)",
                     (run_id, SOURCE_ID, dataset, datetime.now(timezone.utc)))
        conn.commit()
        try:
            payload, headers = fetch_json(f"/competitions/{competition}/matches?season={season_start_year(season)}",
                                          settings.football_data_org_api_key, limiter or RateLimiter(), opener=opener)
            received_at = datetime.now(timezone.utc)
            sha256 = hashlib.sha256(payload).hexdigest()
            object_key = f"{SOURCE_ID}/{competition}/{season}/sha256={sha256}.json"
            store = client(settings)
            ensure_bucket(store, settings.bronze_bucket)
            put_if_absent(store, settings.bronze_bucket, object_key, payload,
                          {"source-url": f"{API_BASE}/competitions/{competition}/matches", "received-at": received_at.isoformat()})
            with conn.transaction():
                inserted = conn.execute("""INSERT INTO ingest.raw_files
                                           (source_id, dataset, source_url, sha256, byte_size, object_key, first_received_at)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                                           ON CONFLICT (source_id, dataset, sha256) DO NOTHING RETURNING raw_file_id""",
                                        (SOURCE_ID, dataset, f"{API_BASE}/competitions/{competition}/matches", sha256,
                                         len(payload), object_key, received_at)).fetchone()
                is_new = inserted is not None
                raw_file_id = inserted[0] if inserted else conn.execute(
                    "SELECT raw_file_id FROM ingest.raw_files WHERE source_id=%s AND dataset=%s AND sha256=%s",
                    (SOURCE_ID, dataset, sha256)).fetchone()[0]
                conn.execute("INSERT INTO ingest.fetches (run_id, raw_file_id, received_at, is_new_content) VALUES (%s, %s, %s, %s)",
                             (run_id, raw_file_id, received_at, is_new))
            matches = [normalize_match(raw) for raw in json.loads(payload)["matches"]]
            published = 0
            if is_new:
                # 응답 전체가 지난번과 같으면 보낼 것이 없다. 바뀌었으면 모든 경기를 보내고, 소비자가 바뀐 경기만 새 버전으로 만든다.
                kafka_producer = kafka_producer or kafka_io.producer(settings, client_id="fdorg-poller")
                for match in matches:
                    event = events.build_event(
                        source_id=SOURCE_ID, event_type="match_snapshot", key=match["external_match_id"],
                        payload={**match, "competition": COMPETITION_TO_LEAGUE[competition], "season": season,
                                 "raw_file_id": raw_file_id},
                        published_at=_parse_utc(match["last_updated"]), observed_at=received_at)
                    kafka_io.publish(kafka_producer, topic, match["external_match_id"], events.encode(event))
                    published += 1
                kafka_io.flush(kafka_producer)
            stats = {"dataset": dataset, "new_content": is_new, "matches": len(matches), "events_published": published,
                     "requests_left_this_minute": {k.lower(): v for k, v in headers.items()}.get("x-requests-available-minute"),
                     "next_poll_seconds": int(next_poll_interval(matches, now).total_seconds())}
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


def suggest_aliases(conn, matches: list[dict], *, league: str, season: str) -> list[dict]:
    """football-data.org 팀 ID → openfootball 기준 이름 후보를 같은 날짜 경기 근거로 만든다(검토용, 저장하지 않음).

    별칭은 이름이 아니라 팀 ID로 둔다. 팀 이름 표기는 바뀔 수 있지만 ID는 바뀌지 않기 때문이다.
    """
    from collections import Counter, defaultdict
    votes, appearances, names = defaultdict(Counter), Counter(), {}
    for match in matches:
        kickoff = _parse_utc(match["utc_kickoff"])
        if kickoff is None:
            continue
        rows = conn.execute("""SELECT home.canonical_name, away.canonical_name
                               FROM silver.matches m
                               JOIN silver.match_revisions_known_at(now()) r USING (match_id)
                               JOIN silver.teams home ON home.team_id = m.home_team_id
                               JOIN silver.teams away ON away.team_id = m.away_team_id
                               WHERE m.competition_code=%s AND m.season=%s AND r.local_date BETWEEN %s AND %s""",
                            (league, season, (kickoff - timedelta(days=1)).date(), (kickoff + timedelta(days=1)).date())).fetchall()
        for side in ("home", "away"):
            team_id = match[f"{side}_team_id"]
            appearances[team_id] += 1
            names[team_id] = match[f"{side}_team_name"]
            for of_home, of_away in rows:
                votes[team_id][of_home if side == "home" else of_away] += 1
    suggestions = []
    for team_id in sorted(appearances, key=int):
        canonical, supporting = votes[team_id].most_common(1)[0] if votes[team_id] else ("", 0)
        suggestions.append({"source_id": SOURCE_ID, "alias": team_id, "canonical_name": canonical,
                            "evidence": f"{names[team_id]}; same-date(±1d) fixtures {supporting}/{appearances[team_id]}"})
    return suggestions


def run_forever(competitions: tuple[str, ...] | None = None) -> None:
    """계속 실행하면서 경기 상황에 맞춘 간격으로 조회한다. 오류가 나면 기록하고 5분 뒤 다시 시도한다.

    5a: 여러 대회를 한 반복에서 차례로 조회하고(분당 10회 한도는 RateLimiter 하나가 함께 지킨다),
    대회별 다음 조회 시각을 따로 둔다. 경기 중인 리그만 2분마다, 나머지는 각자의 간격대로.
    """
    competitions = competitions or tuple(COMPETITION_TO_LEAGUE)
    limiter = RateLimiter()
    settings = load_settings()
    kafka_io.ensure_topics(settings)
    kafka_producer = kafka_io.producer(settings, client_id="fdorg-poller")
    due = {competition: 0.0 for competition in competitions}   # 대회 → 다음 조회 시각(time.monotonic 기준)
    while True:
        for competition in competitions:
            if time.monotonic() < due[competition]:
                continue
            try:
                stats = poll_once(competition, settings=settings, limiter=limiter, kafka_producer=kafka_producer)
                due[competition] = time.monotonic() + stats["next_poll_seconds"]
                print(json.dumps(stats, ensure_ascii=False), flush=True)
            except Exception as exc:  # 네트워크·API 오류로 수집기가 멈추지 않게 한다(실패는 ingest.runs에 남는다).
                print(f"poll failed ({competition}): {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
                due[competition] = time.monotonic() + 300
        time.sleep(max(1.0, min(due.values()) - time.monotonic()))


def main(argv=None):
    parser = argparse.ArgumentParser(description=f"football-data.org 경기 수집기. {ATTRIBUTION}")
    parser.add_argument("--competition", choices=sorted(COMPETITION_TO_LEAGUE),
                        help="대회 하나만(기본: 다섯 리그 모두, --once에서는 PL)")
    parser.add_argument("--season", help="예: 2026-27. 기본은 현재 시즌")
    parser.add_argument("--once", action="store_true", help="한 번만 조회")
    args = parser.parse_args(argv)
    if args.once:
        kafka_io.ensure_topics()
        print(json.dumps(poll_once(args.competition or "PL", args.season), ensure_ascii=False, indent=2))
    else:
        run_forever((args.competition,) if args.competition else None)


if __name__ == "__main__":
    main()
