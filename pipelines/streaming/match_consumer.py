
이벤트 하나를 처리하는 규칙(handle_match_event)
1. 형식이 틀리거나 팀이 대조표에 없으면 저장하지 않고 silver.stream_rejections에 이유를 남긴다.
2. 경기(리그·시즌·홈·원정)가 없으면 만든다. 경기 정체성은 소스와 무관하다.
3. 직전 버전과 내용이 같거나, 이미 반영했던 같은 event_id면 중복으로 건너뛴다(재전송·재처리에도 결과가 같다).
4. 직전 버전보다 published_at이 이른 이벤트는 "늦게 도착한 옛 정보"라 최신 값을 덮지 않도록 거부한다(STALE_EVENT).
5. 그 밖에는 다음 버전을 추가한다.

오프셋은 DB 트랜잭션을 커밋한 뒤에 커밋한다(at-least-once + 멱등 저장 = 결과적으로 한 번 처리한 효과).
실행: python -m pipelines.streaming.match_consumer [--until-idle]
"""
import argparse
import hashlib
import json
from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from ..aliases import ALIAS_FILE, load_alias_file, sync_aliases
from ..config import load_settings
from ..db import connect, migrate
from . import events, kafka_io

GROUP_ID = "silver-match-writer"
# 이 소비자가 읽는 토픽. 토픽마다 처리 함수가 다르다(dispatch_handler).
CONTENT_FIELDS = ("status", "utc_kickoff", "matchday", "ht_home", "ht_away", "ft_home", "ft_away", "duration")


def content_hash(payload: dict) -> str:
    """경기 내용의 해시. last_updated처럼 내용이 아닌 값은 빼서, 시각만 바뀐 재전송을 중복으로 본다."""
    return hashlib.sha256(events.canonical_json({field: payload.get(field) for field in CONTENT_FIELDS}).encode()).hexdigest()


def _parse_time(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00")) if text else None


def reject(conn, *, topic, partition, offset, event_id, reason, detail, now) -> str:
    conn.execute("""INSERT INTO silver.stream_rejections (topic, partition, "offset", event_id, reason, detail, rejected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                 (topic, partition, offset, event_id, reason, Jsonb(detail), now))
    return f"REJECTED:{reason}"


def handle_match_event(conn: psycopg.Connection, raw: bytes, *, topic: str, partition: int, offset: int,
                       now: datetime) -> str:
    """이벤트 하나를 처리하고 결과(INSERTED / DUPLICATE / REJECTED:이유)를 돌려준다. 호출한 쪽 트랜잭션 안에서 실행한다."""
    position = {"topic": topic, "partition": partition, "offset": offset, "now": now}
    try:
        event = events.decode(raw)
    except ValueError as exc:
        return reject(conn, event_id=None, reason=str(exc), detail={"raw": raw[:500].decode("utf-8", "replace")}, **position)
    payload, source_id = event["payload"], event["source_id"]
    try:
        competition, season = payload["competition"], payload["season"]
        home_key, away_key, external_id = payload["home_team_id"], payload["away_team_id"], payload["external_match_id"]
    except KeyError as exc:
        return reject(conn, event_id=event["event_id"], reason="MISSING_PAYLOAD_FIELD", detail={"field": str(exc)}, **position)
    teams = dict(conn.execute("SELECT alias, team_id FROM silver.team_aliases WHERE source_id=%s AND alias = ANY(%s)",
                              (source_id, [home_key, away_key])).fetchall())
    if home_key not in teams or away_key not in teams:
        return reject(conn, event_id=event["event_id"], reason="UNMAPPED_TEAM",
                      detail={"home": [home_key, payload.get("home_team_name")], "away": [away_key, payload.get("away_team_name")]},
                      **position)
    row = conn.execute("""SELECT match_id FROM silver.matches WHERE competition_code=%s AND season=%s
                          AND home_team_id=%s AND away_team_id=%s""",
                       (competition, season, teams[home_key], teams[away_key])).fetchone()
    match_id = row[0] if row else conn.execute(
        """INSERT INTO silver.matches (competition_code, season, home_team_id, away_team_id, created_at)
           VALUES (%s, %s, %s, %s, %s) RETURNING match_id""",
        (competition, season, teams[home_key], teams[away_key], now)).fetchone()[0]
    published_at, new_hash = _parse_time(event["published_at"]), content_hash(payload)
    latest = conn.execute("""SELECT revision_no, content_hash, published_at FROM silver.live_match_revisions
                             WHERE match_id=%s AND source_id=%s ORDER BY revision_no DESC LIMIT 1""",
                          (match_id, source_id)).fetchone()
    if latest and latest[1] == new_hash:
        return "DUPLICATE"
    # 이미 반영했던 바로 그 이벤트를 다시 받은 경우(토픽을 처음부터 재처리 등)는 "늦은 이벤트"가 아니라 중복이다.
    if conn.execute("SELECT 1 FROM silver.live_match_revisions WHERE event_id=%s LIMIT 1", (event["event_id"],)).fetchone():
        return "DUPLICATE"
    if latest and published_at and latest[2] and published_at < latest[2]:
        return reject(conn, event_id=event["event_id"], reason="STALE_EVENT",
                      detail={"match_id": match_id, "published_at": published_at.isoformat(),
                              "latest_published_at": latest[2].isoformat()}, **position)
    conn.execute("""INSERT INTO silver.live_match_revisions VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                 (match_id, source_id, (latest[0] + 1) if latest else 1, external_id, payload.get("status"),
                  _parse_time(payload.get("utc_kickoff")), payload.get("matchday"), payload.get("ht_home"),
                  payload.get("ht_away"), payload.get("ft_home"), payload.get("ft_away"), payload.get("duration"),
                  published_at, now, event["event_id"], new_hash))
    return "INSERTED"


def dispatch_handler(topic: str, settings=None):
        from functools import partial
    return handle_match_event


def run(*, topics=None, group_id: str = GROUP_ID, settings=None, until_idle: bool = False, idle_polls: int = 5,
        handler=None, alias_file=ALIAS_FILE) -> dict:
    """토픽을 읽어 Silver에 반영한다. until_idle=True이면 새 메시지가 idle_polls번 연속 없을 때 멈춘다(테스트·재생용)."""
    settings = settings or load_settings()
    kafka_io.ensure_topics(settings)
    kafka_consumer = kafka_io.consumer(group_id, topics or TOPICS, settings)
    counts, empty_polls = {}, 0
    try:
        with connect(settings) as conn:
            migrate(conn)
            with conn.transaction():
                sync_aliases(conn, load_alias_file(alias_file), datetime.now(timezone.utc))
            while True:
                messages = kafka_consumer.consume(num_messages=500, timeout=1.0)
                if not messages:
                    empty_polls += 1
                    if until_idle and empty_polls >= idle_polls:
                        break
                    continue
                empty_polls = 0
                now = datetime.now(timezone.utc)
                # 묶음 하나를 한 트랜잭션으로 저장한다. 중간에 실패하면 오프셋을 커밋하지 않아 묶음 전체를 다시 받는다.
                with conn.transaction():
                    for message in messages:
                        if message.error():
                            raise RuntimeError(message.error())
                        handle = handler or dispatch_handler(message.topic(), settings)
                        outcome = handle(conn, message.value(), topic=message.topic(), partition=message.partition(),
                                         offset=message.offset(), now=now)
                        counts[outcome] = counts.get(outcome, 0) + 1
                kafka_consumer.commit(asynchronous=False)
    finally:
        kafka_consumer.close()
    return counts


def main(argv=None):
    parser.add_argument("--until-idle", action="store_true", help="새 메시지가 없으면 종료")
    parser.add_argument("--group", default=GROUP_ID)
    args = parser.parse_args(argv)
    print(json.dumps(run(topics=args.topic, group_id=args.group, until_idle=args.until_idle), ensure_ascii=False))


if __name__ == "__main__":
    main()
