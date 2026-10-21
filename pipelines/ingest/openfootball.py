"""openfootball(CC0) 리그 일정·결과 배치 수집(로드맵 1b).

흐름
1. 원본 JSON을 받는다(GitHub raw URL, 또는 테스트용 로컬 파일).
2. Bronze: 내용 해시를 경로로 S3 호환 저장소(SeaweedFS)에 저장한다. 같은 내용이면 다시 올리지 않는다.
3. 정규화: 형식이 섞인 점수, 시간대 없는 경기 시각, 빠진 전반 점수를 명시적으로 처리한다(normalize_document).
4. Silver: 경기 정체성(matches)과 바뀌는 내용(match_revisions)을 나눠 저장한다. 내용이 바뀐 경기만 새 버전을 추가한다.
5. 검사를 통과하지 못한 레코드는 버리지 않고 이유와 함께 quality_issues에 남긴다.

실행: python -m pipelines.ingest.openfootball --season 2025-26 [--league en.1] [--file 경로]
"""
import argparse
import hashlib
import json
import urllib.request
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

import psycopg
from psycopg.types.json import Jsonb

from ..config import load_settings
from ..db import connect, migrate
from ..object_store import client, ensure_bucket, put_if_absent

SOURCE_ID = "openfootball"
RAW_URL_TEMPLATE = "https://raw.githubusercontent.com/openfootball/football.json/master/{season}/{league}.json"

# 리그 코드 → 경기 시각을 해석할 현지 시간대. openfootball의 time에는 시간대 표기가 없어서 가정이 필요하다.
LEAGUE_TIMEZONES = {"en.1": "Europe/London", "es.1": "Europe/Madrid", "de.1": "Europe/Berlin",
                    "it.1": "Europe/Rome", "fr.1": "Europe/Paris", "nl.1": "Europe/Amsterdam"}

# 수집 실행끼리 같은 데이터셋을 동시에 쓰지 않도록 잡는 Postgres advisory lock의 앞 번호
INGEST_LOCK_NAMESPACE = 7_302_201


@dataclass(frozen=True)
class NormalizedMatch:
    """정규화를 통과한 경기 한 건. Silver에 저장할 값만 담는다.

    ht_*/ft_*가 None이면 "모름"이다. 0으로 채우지 않는다.
    kickoff_utc는 현지 시각과 가정한 시간대로 계산한 값이며, 시각이 없으면 None이다.
    """
    record_index: int
    home: str
    away: str
    round: str | None
    local_date: date
    local_time: time | None
    local_timezone: str
    kickoff_utc: datetime | None
    status: str
    ht_home: int | None
    ht_away: int | None
    ft_home: int | None
    ft_away: int | None

    def content_hash(self) -> str:
        """경기 내용(일정·점수)의 해시. 직전 버전과 같으면 새 버전을 만들지 않는 데 쓴다."""
        content = {key: (value.isoformat() if hasattr(value, "isoformat") else value)
                   for key, value in asdict(self).items() if key not in ("record_index", "home", "away")}
        return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class QualityIssue:
    """Silver에 넣지 않은 레코드와 그 이유."""
    record_index: int
    issue_code: str
    detail: dict


def _score_pair(value):
    """[홈, 원정] 점수 쌍을 검사해 (홈, 원정)으로 돌려준다. 형식이 틀리면 None.

    bool은 int의 하위 타입이라 True가 1로 통과하지 않도록 따로 막는다.
    """
    if (isinstance(value, list) and len(value) == 2
            and all(isinstance(score, int) and not isinstance(score, bool) and score >= 0 for score in value)):
        return value[0], value[1]
    return None


def parse_score(raw_score):
    """openfootball의 score 필드를 (전반 점수, 최종 점수, 오류 코드)로 바꾼다.

    실제 파일에서 확인한 형식
    - 없음: 아직 열리지 않은 경기 → (None, None, None)
    - {"ft": [h, a], "ht": [h, a]}: 전반·최종 점수. ht가 없는 경기도 있다.
    - [h, a]: 최종 점수만 있는 축약 형식. 전반 점수는 모름(None)으로 둔다.
    """
    if raw_score is None:
        return None, None, None
    if isinstance(raw_score, list):
        full_time = _score_pair(raw_score)
        return (None, full_time, None) if full_time else (None, None, "INVALID_SCORE_FORMAT")
    if isinstance(raw_score, dict):
        full_time = _score_pair(raw_score["ft"]) if "ft" in raw_score else None
        half_time = _score_pair(raw_score["ht"]) if "ht" in raw_score else None
        if ("ft" in raw_score and full_time is None) or ("ht" in raw_score and half_time is None):
            return None, None, "INVALID_SCORE_FORMAT"
        if half_time and not full_time:
            return None, None, "HALF_TIME_WITHOUT_FULL_TIME"
        if half_time and (half_time[0] > full_time[0] or half_time[1] > full_time[1]):
            return None, None, "HALF_TIME_EXCEEDS_FULL_TIME"
        return half_time, full_time, None
    return None, None, "INVALID_SCORE_FORMAT"


def local_to_utc(local_date: date, local_time: time, timezone_name: str):
    """현지 날짜·시각을 UTC로 바꾼다. 서머타임 전환으로 존재하지 않는 시각이면 None.

    zoneinfo는 존재하지 않는 시각도 조용히 변환하므로, UTC→현지로 되돌렸을 때 같은 시각인지 확인한다.
    """
    zone = ZoneInfo(timezone_name)
    local = datetime.combine(local_date, local_time, tzinfo=zone)
    utc = local.astimezone(timezone.utc)
    if utc.astimezone(zone).replace(tzinfo=None) != local.replace(tzinfo=None):
        return None
    return utc


def normalize_document(document: dict, *, league: str) -> tuple[list[NormalizedMatch], list[QualityIssue]]:
    """openfootball 리그 JSON 한 개를 정규화한다. DB·네트워크를 쓰지 않는 순수 함수라 단위 테스트가 쉽다.

    돌려주는 값: (통과한 경기 목록, 품질 문제 목록)
    한 시즌에 같은 홈·원정 조합이 두 번 나오면 두 번째부터 DUPLICATE_FIXTURE로 제외한다.
    """
    timezone_name = LEAGUE_TIMEZONES[league]
    matches, issues, seen_fixtures = [], [], set()
    records = document.get("matches")
    if not isinstance(records, list):
        return [], [QualityIssue(-1, "MISSING_MATCHES_ARRAY", {})]
    for index, record in enumerate(records):
        def reject(code, **detail):
            issues.append(QualityIssue(index, code, {"record": record, **detail}))

        if not isinstance(record, dict):
            reject("INVALID_RECORD")
            continue
        home, away = record.get("team1"), record.get("team2")
        if not isinstance(home, str) or not isinstance(away, str) or not home.strip() or not away.strip():
            reject("MISSING_TEAM")
            continue
        home, away = home.strip(), away.strip()
        if home == away:
            reject("SAME_TEAM")
            continue
        try:
            local_date = date.fromisoformat(record.get("date", ""))
        except (TypeError, ValueError):
            reject("INVALID_DATE")
            continue
        local_time, kickoff_utc = None, None
        if record.get("time") is not None:
            try:
                local_time = time.fromisoformat(record["time"])
            except (TypeError, ValueError):
                reject("INVALID_TIME")
                continue
            kickoff_utc = local_to_utc(local_date, local_time, timezone_name)
            if kickoff_utc is None:
                reject("NONEXISTENT_LOCAL_TIME", timezone=timezone_name)
                continue
        half_time, full_time, score_error = parse_score(record.get("score"))
        if score_error:
            reject(score_error)
            continue
        if (home, away) in seen_fixtures:
            reject("DUPLICATE_FIXTURE")
            continue
        seen_fixtures.add((home, away))
        matches.append(NormalizedMatch(
            record_index=index, home=home, away=away, round=record.get("round"),
            local_date=local_date, local_time=local_time, local_timezone=timezone_name, kickoff_utc=kickoff_utc,
            status="FINISHED" if full_time else "SCHEDULED",
            ht_home=half_time[0] if half_time else None, ht_away=half_time[1] if half_time else None,
            ft_home=full_time[0] if full_time else None, ft_away=full_time[1] if full_time else None))
    return matches, issues


def fetch_url(url: str) -> bytes:
    """원본 JSON을 내려받는다. openfootball은 CC0라 자동 수집이 허용된다(docs/football_data_sources.md)."""
    request = urllib.request.Request(url, headers={"User-Agent": "sports-prediction-local/0.1"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def _team_id(conn: psycopg.Connection, name: str, now: datetime) -> int:
    """openfootball 팀 이름의 team_id. 처음 보는 이름이면 팀과 별칭을 만든다.

    openfootball은 기준 소스라 자동 등록한다. 다른 소스의 이름은 이렇게 자동 추측하지 않는다.
    """
    row = conn.execute("SELECT team_id FROM silver.team_aliases WHERE source_id=%s AND alias=%s",
                       (SOURCE_ID, name)).fetchone()
    if row:
        return row[0]
    team_id = conn.execute("""INSERT INTO silver.teams (canonical_name, created_at) VALUES (%s, %s)
                              ON CONFLICT (canonical_name) DO UPDATE SET canonical_name = EXCLUDED.canonical_name
                              RETURNING team_id""", (name, now)).fetchone()[0]
    conn.execute("INSERT INTO silver.team_aliases VALUES (%s, %s, %s, %s)", (SOURCE_ID, name, team_id, now))
    return team_id


def write_silver(conn: psycopg.Connection, matches: list[NormalizedMatch], *, competition: str, season: str,
                 raw_file_id: int, received_at: datetime) -> dict:
    """정규화한 경기를 Silver에 저장하고 건수를 돌려준다. 호출한 쪽의 트랜잭션 안에서 실행한다.

    경기가 처음이면 matches와 1번 버전을, 이미 있으면 직전 버전과 내용 해시를 비교해 다를 때만 다음 버전을 추가한다.
    published_at은 openfootball이 제공하지 않으므로 NULL(모름)로 둔다.
    """
    counts = {"matches_new": 0, "revisions_new": 0, "unchanged": 0}
    for match in matches:
        home_id, away_id = _team_id(conn, match.home, received_at), _team_id(conn, match.away, received_at)
        row = conn.execute("""SELECT match_id FROM silver.matches WHERE competition_code=%s AND season=%s
                              AND home_team_id=%s AND away_team_id=%s""",
                           (competition, season, home_id, away_id)).fetchone()
        if row:
            match_id = row[0]
        else:
            match_id = conn.execute("""INSERT INTO silver.matches (competition_code, season, home_team_id, away_team_id, created_at)
                                       VALUES (%s, %s, %s, %s, %s) RETURNING match_id""",
                                    (competition, season, home_id, away_id, received_at)).fetchone()[0]
            counts["matches_new"] += 1
        latest = conn.execute("""SELECT revision_no, content_hash FROM silver.match_revisions
                                 WHERE match_id=%s ORDER BY revision_no DESC LIMIT 1""", (match_id,)).fetchone()
        content_hash = match.content_hash()
        if latest and latest[1] == content_hash:
            counts["unchanged"] += 1
            continue
        conn.execute("""INSERT INTO silver.match_revisions VALUES
                        (%s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)""",
                     (match_id, (latest[0] + 1) if latest else 1, match.round, match.local_date, match.local_time,
                      match.local_timezone, match.kickoff_utc, match.status, match.ht_home, match.ht_away,
                      match.ft_home, match.ft_away, received_at, raw_file_id, content_hash))
        counts["revisions_new"] += 1
    return counts


def ingest(season: str, league: str = "en.1", *, payload: bytes | None = None, source_url: str | None = None,
           settings=None, received_at: datetime | None = None) -> dict:
    """openfootball 한 시즌 파일을 수집해 Bronze·Silver에 저장하고 실행 통계를 돌려준다.

    payload를 주면 내려받지 않고 그 바이트를 쓴다(테스트·오프라인 재처리용).
    같은 파일을 여러 번 수집해도 Bronze 객체·raw_files·경기 버전은 늘지 않고 fetches 기록만 추가된다.
    """
    if league not in LEAGUE_TIMEZONES:
        raise ValueError(f"지원하지 않는 리그 코드입니다: {league}")
    settings = settings or load_settings()
    dataset = f"{league}/{season}"
    source_url = source_url or RAW_URL_TEMPLATE.format(season=season, league=league)
    run_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)
    with connect(settings) as conn:
        migrate(conn)
        conn.execute("INSERT INTO ingest.runs VALUES (%s, %s, %s, %s, NULL, 'RUNNING', NULL, NULL)",
                     (run_id, SOURCE_ID, dataset, started_at))
        conn.commit()
        try:
            payload = payload if payload is not None else fetch_url(source_url)
            received_at = received_at or datetime.now(timezone.utc)
            sha256 = hashlib.sha256(payload).hexdigest()
            object_key = f"{SOURCE_ID}/{league}/{season}/sha256={sha256}.json"
            store = client(settings)
            ensure_bucket(store, settings.bronze_bucket)
            # Bronze를 먼저 올린다. DB 저장이 실패해도 같은 내용은 같은 경로라 다음 실행에서 그대로 재사용된다.
            bronze_uploaded = put_if_absent(store, settings.bronze_bucket, object_key, payload,
                                            {"source-url": source_url, "received-at": received_at.isoformat()})
            document = json.loads(payload.decode("utf-8"))
            matches, issues = normalize_document(document, league=league)
            with conn.transaction():
                # 같은 데이터셋을 동시에 수집하면 버전 번호가 겹칠 수 있으므로 데이터셋 단위로 잠근다.
                conn.execute("SELECT pg_advisory_xact_lock(%s, hashtext(%s))", (INGEST_LOCK_NAMESPACE, dataset))
                inserted = conn.execute("""INSERT INTO ingest.raw_files
                                           (source_id, dataset, source_url, sha256, byte_size, object_key, first_received_at)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                                           ON CONFLICT (source_id, dataset, sha256) DO NOTHING RETURNING raw_file_id""",
                                        (SOURCE_ID, dataset, source_url, sha256, len(payload), object_key, received_at)).fetchone()
                is_new_content = inserted is not None
                raw_file_id = inserted[0] if inserted else conn.execute(
                    "SELECT raw_file_id FROM ingest.raw_files WHERE source_id=%s AND dataset=%s AND sha256=%s",
                    (SOURCE_ID, dataset, sha256)).fetchone()[0]
                conn.execute("INSERT INTO ingest.fetches (run_id, raw_file_id, received_at, is_new_content) VALUES (%s, %s, %s, %s)",
                             (run_id, raw_file_id, received_at, is_new_content))
                # 이미 본 내용이어도 Silver 비교는 항상 한다. 원본이 A → B → A로 되돌아간 경우
                # A를 건너뛰면 최신 버전이 B에 머물기 때문이다. 같으면 버전이 늘지 않으므로 반복 실행해도 안전하다.
                counts = write_silver(conn, matches, competition=league, season=season,
                                      raw_file_id=raw_file_id, received_at=received_at)
                if is_new_content:
                    # 품질 문제는 같은 내용당 한 번만 기록한다.
                    for issue in issues:
                        conn.execute("""INSERT INTO silver.quality_issues (run_id, raw_file_id, record_index, issue_code, detail, created_at)
                                        VALUES (%s, %s, %s, %s, %s, %s)""",
                                     (run_id, raw_file_id, issue.record_index, issue.issue_code, Jsonb(issue.detail), received_at))
            stats = {"dataset": dataset, "sha256": sha256, "new_content": is_new_content, "bronze_uploaded": bronze_uploaded,
                     "records": len(document.get("matches") or []), "valid_matches": len(matches),
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


def main(argv=None):
    """명령줄 실행 진입점."""
    parser = argparse.ArgumentParser(description="openfootball 일정·결과를 Bronze(S3)와 Silver(Postgres)에 수집합니다.")
    parser.add_argument("--season", required=True, help="시즌 폴더 이름. 예: 2025-26")
    parser.add_argument("--league", default="en.1", choices=sorted(LEAGUE_TIMEZONES), help="리그 코드. 기본 en.1(EPL)")
    parser.add_argument("--file", help="내려받지 않고 이 로컬 JSON 파일을 사용")
    args = parser.parse_args(argv)
    payload = open(args.file, "rb").read() if args.file else None
    source_url = f"file:{args.file}" if args.file else None
    stats = ingest(args.season, args.league, payload=payload, source_url=source_url)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
