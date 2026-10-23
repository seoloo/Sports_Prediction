"""Kafka 소비자 → Silver(로드맵 2c). 경기 이벤트를 silver.live_match_revisions에 버전으로 쌓는다.

이벤트 하나를 처리하는 규칙(handle_match_event)
1. 계약(JSON Schema, contracts.py)을 어기거나 팀이 대조표에 없으면 저장하지 않고 silver.stream_rejections에 이유를 남기고,
   원본은 오류 큐(<토픽>.dlq)로 옮긴다(6c). 대조표를 고친 뒤 오류 큐에서 다시 보낼 수 있다(pipelines/streaming/dlq.py).
2. 경기(리그·시즌·홈·원정)가 없으면 만든다. 경기 정체성은 소스와 무관하다.
3. 직전 버전과 내용이 같거나, 이미 반영했던 같은 event_id면 중복으로 건너뛴다(재전송·재처리에도 결과가 같다).
4. 직전 버전보다 published_at이 이른 이벤트는 "늦게 도착한 옛 정보"라 최신 값을 덮지 않도록 거부한다(STALE_EVENT).
5. 그 밖에는 다음 버전을 추가한다.

오프셋은 DB 트랜잭션을 커밋한 뒤에 커밋한다(at-least-once + 멱등 저장 = 결과적으로 한 번 처리한 효과).
오류 큐로 보낸 메시지는 브로커 저장을 확인(flush)한 뒤에 DB·오프셋을 커밋한다. 그래서 거부한 메시지가 어디에도 없이 사라지지 않는다.
실행: python -m pipelines.streaming.match_consumer [--until-idle]
"""
import argparse
import hashlib
import json
import time
from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from ..aliases import ALIAS_FILE, load_alias_file, sync_aliases
from ..config import load_settings
from ..db import connect, migrate
from . import events, kafka_io
from .contracts import ContractViolation

GROUP_ID = "silver-match-writer"
# 이 소비자가 읽는 토픽. 토픽마다 처리 함수가 다르다(dispatch_handler).
TOPICS = ["fdorg.matches.v1"]
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
    except ContractViolation as exc:
        event_id = None
        try:
            event_id = json.loads(raw).get("event_id")
        except (ValueError, AttributeError):
            pass
        return reject(conn, event_id=event_id if isinstance(event_id, str) else None, reason=exc.reason,
                      detail={**exc.detail, "raw": raw[:500].decode("utf-8", "replace")}, **position)
    payload, source_id = event["payload"], event["source_id"]   # 필수 필드와 타입은 계약이 보장한다
    competition, season = payload["competition"], payload["season"]
    home_key, away_key, external_id = payload["home_team_id"], payload["away_team_id"], payload["external_match_id"]
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
    """토픽 이름 → 처리 함수. 지금은 경기 이벤트(handle_match_event) 하나이고, 토픽이 늘면 여기서 나눈다."""
    return handle_match_event


ASSIGNMENT_TIMEOUT_SECONDS = 30   # until_idle 모드에서 파티션 배정을 기다리는 최대 시간
# 오류 큐로 보내는 거부 사유. 계약 위반은 고칠 수 없는 원본이라 증거로, UNMAPPED_TEAM은 대조표를 고친 뒤 다시 보낼 수 있어서.
# STALE_EVENT(늦게 온 옛 정보)는 규칙대로 버린 정상 동작이라 보내지 않는다.
DLQ_REASONS = {"INVALID_JSON", "SCHEMA_VIOLATION", "UNSUPPORTED_SCHEMA_VERSION", "UNKNOWN_EVENT_TYPE", "EVENT_ID_MISMATCH",
               "UNMAPPED_TEAM"}


def dead_letter_headers(conn, message, reason: str, group_id: str, now: datetime) -> dict[str, str]:
    """오류 큐 메시지의 헤더: 원래 위치, 사유, 거부 기록의 detail(어디가 왜 틀렸는지). 값(원본 바이트)은 그대로 둔다."""
    row = conn.execute("""SELECT detail FROM silver.stream_rejections
                          WHERE topic=%s AND partition=%s AND "offset"=%s AND reason=%s""",
                       (message.topic(), message.partition(), message.offset(), reason)).fetchone()
    detail = {key: value for key, value in (row[0] if row else {}).items() if key != "raw"}
    return {"dlq.reason": reason, "dlq.source_topic": message.topic(), "dlq.source_partition": str(message.partition()),
            "dlq.source_offset": str(message.offset()), "dlq.consumer_group": group_id, "dlq.failed_at": now.isoformat(),
            "dlq.detail": events.canonical_json(detail)[:1000]}


def run(*, topics=None, group_id: str = GROUP_ID, settings=None, until_idle: bool = False, idle_polls: int = 5,
        handler=None, alias_file=ALIAS_FILE) -> dict:
    """토픽을 읽어 Silver에 반영한다. until_idle=True이면 새 메시지가 idle_polls번 연속 없을 때 멈춘다(테스트·재생용).

    빈 응답은 파티션을 배정받은 뒤부터 센다. 새 그룹은 배정까지 몇 초 걸려서, 그 사이의 빈 응답을 세면
    메시지가 남아 있는데도 "다 읽었다"며 끝날 수 있다(느린 CI에서 실제로 발생). 배정이
    ASSIGNMENT_TIMEOUT_SECONDS 안에 안 되면 조용히 끝내지 않고 오류를 낸다.
    """
    settings = settings or load_settings()
    topics = topics or TOPICS
    kafka_io.ensure_topics(settings)
    kafka_io.ensure_topics(settings, {kafka_io.dlq_topic(topic): 1 for topic in topics})
    kafka_consumer = kafka_io.consumer(group_id, topics, settings)
    counts, empty_polls, dlq_producer = {}, 0, None
    assignment_deadline = time.monotonic() + ASSIGNMENT_TIMEOUT_SECONDS
    try:
        with connect(settings) as conn:
            migrate(conn)
            with conn.transaction():
                sync_aliases(conn, load_alias_file(alias_file), datetime.now(timezone.utc))
            while True:
                messages = kafka_consumer.consume(num_messages=500, timeout=1.0)
                if not messages:
                    if not kafka_consumer.assignment():   # 아직 그룹 참여·파티션 배정 중
                        if until_idle and time.monotonic() > assignment_deadline:
                            raise TimeoutError(f"{ASSIGNMENT_TIMEOUT_SECONDS}초 안에 파티션을 배정받지 못했습니다: {topics}")
                        continue
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
                        reason = outcome.split(":", 1)[1] if outcome.startswith("REJECTED:") else None
                        if reason in DLQ_REASONS:
                            dlq_producer = dlq_producer or kafka_io.producer(settings, client_id=f"{group_id}-dlq")
                            kafka_io.publish(dlq_producer, kafka_io.dlq_topic(message.topic()), message.key(),
                                             message.value(), dead_letter_headers(conn, message, reason, group_id, now))
                            counts["DEAD_LETTERED"] = counts.get("DEAD_LETTERED", 0) + 1
                    if dlq_producer:
                        kafka_io.flush(dlq_producer)   # 오류 큐 저장을 확인한 뒤에 DB를 커밋(실패하면 묶음 전체를 다시)
                kafka_consumer.commit(asynchronous=False)
    finally:
        kafka_consumer.close()
        if dlq_producer:
            dlq_producer.flush(5)
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description="경기 이벤트를 Silver에 반영합니다.")
    parser.add_argument("--until-idle", action="store_true", help="새 메시지가 없으면 종료")
    parser.add_argument("--topic", action="append", help="읽을 토픽(여러 번 지정 가능). 기본: fdorg.matches.v1")
    parser.add_argument("--group", default=GROUP_ID)
    args = parser.parse_args(argv)
    print(json.dumps(run(topics=args.topic, group_id=args.group, until_idle=args.until_idle), ensure_ascii=False))


if __name__ == "__main__":
    main()
