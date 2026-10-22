"""알림 내용 만들기와 보내기(중복 없이).

세 가지를 보낸다.
- PREMATCH: 경기 전 스냅샷(찍음·놓침)과 그때의 예측 상태. 검증 기준을 통과하지 못한 모델의 확률은 보내지 않고
  보류 사유만 보낸다(화면에 확률을 보여 주지 않는 원칙과 같음). READY인 예측만 확률을 보낸다.
- WEEKLY: 주간 채점 보고서 요약(modeling.prospective_reports).
- FAILURE: Airflow 작업 실패(마지막 재시도까지 실패한 경우).
보낸 알림은 notify.sent_messages에 남겨 같은 알림을 다시 보내지 않는다. 시각은 한국 시각으로 보여 준다.
"""
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg

from ..config import load_settings
from ..db import connect, migrate
from . import telegram

SEOUL = ZoneInfo("Asia/Seoul")
PREMATCH_LOOKBACK = timedelta(hours=48)   # 처음 설정했을 때 오래된 기록이 한꺼번에 오지 않도록 최근 이틀만
PREMATCH_STALE_AFTER = timedelta(hours=24)   # 시작한 지 하루가 지난 경기는 이미 지난 소식이라 알리지 않는다
OFFSET_LABEL = {"T-24H": "24시간 전", "T-3H": "3시간 전", "T-1H": "1시간 전"}
REASON_LABEL = {"MODEL_NOT_VALIDATED": "모델 미검증", "LOW_TEAM_HISTORY(<5)": "팀 기록 부족",
                "INSUFFICIENT_TRAINING_DATA(<100)": "학습 자료 부족", "NO_ACTIVE_MODEL": "활성 모델 없음",
                "MODEL_CODE_CHANGED": "평가받지 않은 모델 코드", "LEAGUE_NOT_MODELED": "이 리그 모델 없음",
                "INSUFFICIENT_TRAINING_DATA(<200)": "학습 자료 부족"}
WEEKDAYS = "월화수목금토일"
log = logging.getLogger(__name__)


def seoul(moment: datetime) -> str:
    local = moment.astimezone(SEOUL)
    return f"{local:%m/%d}({WEEKDAYS[local.weekday()]}) {local:%H:%M}"


def reason_text(reasons: list[str]) -> str:
    return ", ".join(REASON_LABEL.get(reason, "예측 오류" if reason.startswith("PREDICTION_FAILED") else reason)
                     for reason in reasons)


def already_sent(conn: psycopg.Connection, kind: str, keys: list[str]) -> set[str]:
    return {key for (key,) in conn.execute(
        "SELECT notification_key FROM notify.sent_messages WHERE kind = %s AND notification_key = ANY(%s)", (kind, keys))}


def deliver(conn: psycopg.Connection, kind: str, keys: list[str], text: str, settings, now: datetime, sender=None) -> dict:
    """보내고, 보냈으면 키마다 기록한다. 설정이 없으면 기록하지 않아 나중에 설정하면 보낼 수 있다."""
    result = (sender or telegram.send)(text, settings)
    if result["status"] == "SENT":
        digest = hashlib.sha256(text.encode()).hexdigest()
        with conn.transaction():
            for key in keys:
                conn.execute("""INSERT INTO notify.sent_messages (kind, notification_key, message_sha256, parts, sent_at)
                                VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                             (kind, key, digest, result["parts"], now))
    return {**result, "kind": kind, "items": len(keys)}


def pending_prematch(conn: psycopg.Connection, now: datetime) -> list[dict]:
    """최근 이틀 안에 기록됐고 경기가 아직 지난 일이 아닌 스냅샷 중, 아직 알리지 않은 것(예측이 여러 버전이면 각각)."""
    rows = conn.execute("""
        SELECT i.match_id, i.offset_label, i.status, i.kickoff_utc, h.canonical_name, a.canonical_name,
               p.display_status, p.withheld_reasons, p.prob_home, p.prob_draw, p.prob_away,
               v.market_home, v.market_draw, v.market_away
        FROM snapshots.prematch_inputs i
        JOIN silver.matches m USING (match_id)
        JOIN silver.teams h ON h.team_id = m.home_team_id
        JOIN silver.teams a ON a.team_id = m.away_team_id
        LEFT JOIN snapshots.prematch_predictions p USING (match_id, offset_label)
        WHERE i.recorded_at > %s AND i.kickoff_utc > %s
          AND NOT EXISTS (SELECT 1 FROM notify.sent_messages s WHERE s.kind = 'PREMATCH'
                          AND s.notification_key = i.match_id || ':' || i.offset_label)
        ORDER BY i.kickoff_utc, i.match_id, i.due_at""", (now - PREMATCH_LOOKBACK, now - PREMATCH_STALE_AFTER)).fetchall()
    keys = ("match_id", "offset_label", "status", "kickoff_utc", "home", "away", "display_status", "withheld_reasons",
    return [dict(zip(keys, row)) for row in rows]


def value_line(row: dict) -> str | None:
    if not row.get("value_status"):
        return None
    if row["value_status"] == "COMPARED":
    if row.get("market_home") is not None:
        result["market"] = {"HOME": float(row["market_home"]), "DRAW": float(row["market_draw"]),
                            "AWAY": float(row["market_away"])}


def prematch_text(rows: list[dict]) -> str:
    lines = [f"⚽ 경기 전 스냅샷 {len(rows)}건"]
    current = None
    for row in rows:
        if row["match_id"] != current:
            current = row["match_id"]
            lines += ["", f"{seoul(row['kickoff_utc'])} {row['home']} vs {row['away']}"]
        label = OFFSET_LABEL[row["offset_label"]]
        if row["status"] == "MISSED":
            lines.append(f"· {label}: 놓침(그 시각에 기록하지 못함)")
        elif row["display_status"] == "READY":
            lines.append(f"· {label}: 홈 {float(row['prob_home']):.1%} · 무 {float(row['prob_draw']):.1%} · "
                         f"원정 {float(row['prob_away']):.1%}")
        elif row["display_status"] == "WITHHELD":
            lines.append(f"· {label}: 찍음 · 예측 보류({reason_text(row['withheld_reasons'])})")
        else:
            lines.append(f"· {label}: 찍음 · 예측 기록 없음")
        if row["status"] == "TAKEN" and (value := value_line(row)):
            lines.append(f"  {value}")
    return "\n".join(lines)


def pending_nba_prematch(conn: psycopg.Connection, now: datetime) -> list[dict]:
    """NBA 스냅샷 중 아직 알리지 않은 것(5d-d). 알림 키는 nba:경기:시점으로 축구와 겹치지 않는다."""
    rows = conn.execute("""
        SELECT i.game_id, i.offset_label, i.status, i.tipoff_utc, h.canonical_name, a.canonical_name,
               p.display_status, p.withheld_reasons, p.prob_home,
        FROM snapshots.game_prematch_inputs i
        JOIN silver.games g USING (game_id)
        JOIN silver.teams h ON h.team_id = g.home_team_id JOIN silver.teams a ON a.team_id = g.away_team_id
        LEFT JOIN snapshots.game_prematch_predictions p USING (game_id, offset_label)
        WHERE i.recorded_at > %s AND i.tipoff_utc > %s
          AND NOT EXISTS (SELECT 1 FROM notify.sent_messages s WHERE s.kind = 'PREMATCH'
                          AND s.notification_key = 'nba:' || i.game_id || ':' || i.offset_label)
        ORDER BY i.tipoff_utc, i.game_id, i.due_at""", (now - PREMATCH_LOOKBACK, now - PREMATCH_STALE_AFTER)).fetchall()
    keys = ("game_id", "offset_label", "status", "tipoff_utc", "home", "away", "display_status", "withheld_reasons",
            "market_home", "market_away")
    return [dict(zip(keys, row)) for row in rows]


def nba_text(rows: list[dict]) -> str:
    """NBA 알림. 놓친 시점은 줄마다 쓰지 않고 건수만(처음 켰을 때 이미 시작한 경기가 한꺼번에 놓침으로 잡힌다)."""
    taken = [row for row in rows if row["status"] == "TAKEN"]
    lines = [f"🏀 NBA 경기 전 스냅샷 {len(taken)}건"]
    current = None
    for row in taken:
        if row["game_id"] != current:
            current = row["game_id"]
            lines += ["", f"{seoul(row['tipoff_utc'])} {row['home']} vs {row['away']}"]
        label = OFFSET_LABEL[row["offset_label"]]
        if row["display_status"] == "READY":
            lines.append(f"· {label}: 홈 승 {float(row['prob_home']):.1%}")
        elif row["display_status"] == "WITHHELD":
            lines.append(f"· {label}: 찍음 · 예측 보류({reason_text(row['withheld_reasons'])})")
        else:
            lines.append(f"· {label}: 찍음 · 예측 기록 없음")
        if row["value_status"]:
            if row["value_status"] == "COMPARED":
            if row["market_home"] is not None:
                result["market"] = {"HOME": float(row["market_home"]), "AWAY": float(row["market_away"])}
    missed = len(rows) - len(taken)
    if missed:
        lines += ["", f"놓친 시점 {missed}건(그 시각에 기록하지 못함)"]
    return "\n".join(lines)


def notify_prematch(*, now: datetime | None = None, settings=None, sender=None) -> dict:
    """아직 알리지 않은 경기 전 스냅샷(축구·NBA)을 한 메시지로 보낸다. 없으면 보내지 않는다."""
    now, settings = now or datetime.now(timezone.utc), settings or load_settings()
    with connect(settings) as conn:
        migrate(conn)
        rows, nba_rows = pending_prematch(conn, now), pending_nba_prematch(conn, now)
        if not any(row["status"] == "TAKEN" for row in nba_rows):
            nba_rows = []   # 놓친 시점만 있으면 보내지 않는다. 다음에 찍은 스냅샷을 알릴 때 건수로 함께 알린다
        if not rows and not nba_rows:
            return {"status": "NOTHING_NEW", "kind": "PREMATCH", "items": 0}
        keys = sorted({f"{row['match_id']}:{row['offset_label']}" for row in rows}
                      | {f"nba:{row['game_id']}:{row['offset_label']}" for row in nba_rows})
        parts = ([prematch_text(rows)] if rows else []) + ([nba_text(nba_rows)] if nba_rows else [])
        return deliver(conn, "PREMATCH", keys, "\n\n".join(parts), settings, now, sender)


def weekly_text(summary: dict) -> str:
    active = summary.get("active_model") or {}
    lines = [f"📊 주간 예측 보고서 ({summary['week_start']} 주)",
             f"모델: {active.get('name', '없음')} · 검증 상태 {active.get('gate_status', '-')}"]
    if active.get("gate_status") != "VALIDATED":
        lines.append("검증 전 모델이라 확률은 평가용 기록입니다.")
    lines.append("")
    for label in ("T-24H", "T-3H", "T-1H"):
        counts = summary["snapshots"].get(label, {})
        week = summary["week"][label]
        score = (f" · 채점 {week['matches']}경기 로그 손실 {week['log_loss']:.3f}" if week.get("matches") else "")
        market = week.get("vs_market_pre")
        versus = f"(시장 {market['market_log_loss']:.3f})" if market else ""
        lines.append(f"{OFFSET_LABEL[label]}: 찍음 {counts.get('TAKEN', 0)} · 놓침 {counts.get('MISSED', 0)}{score}{versus}")
    total = summary["cumulative"]["T-24H"]
    if total.get("matches"):
        lines.append(f"누적(24시간 전): {total['matches']}경기 로그 손실 {total['log_loss']:.3f}")
    if summary["withheld_reasons"]:
        lines.append("보류 사유: " + ", ".join(f"{reason_text([reason])} {count}"
                                            for reason, count in sorted(summary["withheld_reasons"].items())))
    value = summary.get("value")
    nba = summary.get("nba")
    if nba and nba.get("snapshots"):   # 5e: NBA 경기 전 예측 채점
        lines += ["", "🏀 NBA"]
        for label in ("T-24H", "T-3H", "T-1H"):
            counts, week = nba["snapshots"].get(label, {}), nba["week"][label]
            score = f" · 채점 {week['games']}경기 로그 손실 {week['log_loss']:.3f}" if week.get("games") else ""
            market = week.get("vs_market_pre")
            versus = f"(시장 {market['market_log_loss']:.3f})" if market else ""
            lines.append(f"{OFFSET_LABEL[label]}: 찍음 {counts.get('TAKEN', 0)} · 놓침 {counts.get('MISSED', 0)}{score}{versus}")
            v = nba["value"]
    return "\n".join(lines)


def notify_weekly(*, now: datetime | None = None, settings=None, sender=None) -> dict:
    """가장 최근 주간 보고서를 보낸다. 같은 보고서(report_id)는 한 번만."""
    now, settings = now or datetime.now(timezone.utc), settings or load_settings()
    with connect(settings) as conn:
        migrate(conn)
        row = conn.execute("SELECT report_id, results FROM modeling.prospective_reports ORDER BY report_id DESC LIMIT 1").fetchone()
        if row is None:
            return {"status": "NOTHING_NEW", "kind": "WEEKLY", "items": 0}
        key = str(row[0])
        if already_sent(conn, "WEEKLY", [key]):
            return {"status": "ALREADY_SENT", "kind": "WEEKLY", "items": 0}
        return deliver(conn, "WEEKLY", [key], weekly_text(row[1]), settings, now, sender)


def failure_text(dag_id: str, task_id: str, run_id: str, error: str) -> str:
    return f"⚠️ 작업 실패: {dag_id}.{task_id}\n실행: {run_id}\n오류: {error[:500]}"


def notify_failure(dag_id: str, task_id: str, run_id: str, try_number: int, error: str, *,
                   now: datetime | None = None, settings=None, sender=None) -> dict:
    now, settings = now or datetime.now(timezone.utc), settings or load_settings()
    key = f"{dag_id}:{task_id}:{run_id}:{try_number}"
    error = telegram.redact(error, settings.telegram_bot_token)
    with connect(settings) as conn:
        migrate(conn)
        if already_sent(conn, "FAILURE", [key]):
            return {"status": "ALREADY_SENT", "kind": "FAILURE", "items": 0}
        return deliver(conn, "FAILURE", [key], failure_text(dag_id, task_id, run_id, error), settings, now, sender)


def airflow_failure_callback(context) -> None:
    """Airflow on_failure_callback. 알림이 실패해도 원래 실패를 가리지 않도록 예외를 밖으로 내지 않는다."""
    try:
        instance = context["ti"]
        notify_failure(instance.dag_id, instance.task_id, instance.run_id, getattr(instance, "try_number", 0),
                       f"{type(context.get('exception')).__name__}: {context.get('exception')}")
    except Exception:   # noqa: BLE001
        log.exception("실패 알림을 보내지 못했습니다")
