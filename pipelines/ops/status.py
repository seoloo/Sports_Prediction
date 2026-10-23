"""운영 상태 보고서(로드맵 7c): 하루 상태를 한 장의 HTML(+ JSON)로.

확인 항목과 판정(OK / WARN / FAIL, 판단할 자료가 없으면 UNKNOWN). 전체 상태는 가장 나쁜 항목이다.
- 수집 신선도: dbt source freshness 결과를 그대로(규칙은 dbt/models/staging/sources.yml 한곳). pass/warn/error → OK/WARN/FAIL
- 수집 실행: 소스별 최근 실행. 마지막 실행이 실패면 FAIL, 최근 24시간 안에 실패가 있었지만 그 뒤 성공했으면 WARN
- API 크레딧: The Odds API 마지막 응답의 남은 크레딧. 60 미만 WARN(수집기가 스스로 줄이는 선), 20 미만 FAIL
- 스트림·품질: 최근 7일 거부 이벤트와 수집 품질 문제. 오류 큐로 간 사유(contracts.DLQ_REASONS)나 계약 위반이 있으면 WARN.
  늦게 온 옛 정보(STALE_EVENT)와 규칙대로 걸러 낸 품질 문제(TIED_FINAL 등)는 정상 동작이라 보여 주기만 한다
- 경기 전 기록: 최근 7일 축구·NBA 스냅샷 찍음·놓침. 놓침이 있으면 WARN(PC가 꺼져 있었다는 뜻)
- 주간 채점: 마지막 주간 보고서가 8일보다 오래되면 WARN. 누적 채점(모델 vs 시장)을 함께 보여 준다
- 레이크: 마지막 내보내기가 30시간보다 오래되면 WARN
보고서는 읽기만 한다(데이터를 바꾸지 않음). 파일은 data/ops/(Git 제외)에 쓴다.
실행: python -m pipelines.ops.status [--out data/ops/status.html] [--skip-freshness]
"""
import argparse
import html
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from ..config import PROJECT_ROOT, load_settings
from ..db import connect

SEOUL = ZoneInfo("Asia/Seoul")
LEVELS = ("OK", "UNKNOWN", "WARN", "FAIL")   # 뒤로 갈수록 나쁨
CREDITS_WARN, CREDITS_FAIL = 60, 20
WEEKLY_MAX_AGE = timedelta(days=8)
LAKE_MAX_AGE = timedelta(hours=30)
WINDOW = timedelta(days=7)
DEFAULT_OUT = PROJECT_ROOT / "data" / "ops" / "status.html"
FRESHNESS_STATUS = {"pass": "OK", "warn": "WARN", "error": "FAIL", "runtime error": "FAIL"}


def worst(statuses) -> str:
    statuses = list(statuses)
    return max(statuses, key=LEVELS.index) if statuses else "UNKNOWN"


def check(name: str, status: str, summary: str, columns=(), rows=()) -> dict:
    return {"name": name, "status": status, "summary": summary, "columns": list(columns), "rows": [list(r) for r in rows]}


def seoul(moment) -> str:
    if moment is None:
        return "—"
    if isinstance(moment, str):
        moment = datetime.fromisoformat(moment.replace("Z", "+00:00"))
    return f"{moment.astimezone(SEOUL):%m/%d %H:%M}"


def freshness_check(results: list[dict] | None) -> dict:
    """dbt sources.json의 results → 소스별 신선도. results가 None이면 검사를 못 한 것(UNKNOWN)."""
    if results is None:
        return check("수집 신선도", "UNKNOWN", "dbt source freshness를 실행하지 못했습니다.")
    rows, statuses = [], []
    for item in sorted(results, key=lambda r: r["unique_id"]):
        status = FRESHNESS_STATUS.get(item.get("status"), "UNKNOWN")
        statuses.append(status)
        criteria = item.get("criteria") or {}
        limit = lambda key: (f"{criteria[key]['count']}{criteria[key]['period'][0]}" if criteria.get(key) else "—")
        age = item.get("max_loaded_at_time_ago_in_s")
        rows.append([item["unique_id"].split(".")[-1].removesuffix("_fetches"), status, seoul(item.get("max_loaded_at")),
                     f"{age / 3600:.1f}시간 전" if age is not None else "—", f"{limit('warn_after')} / {limit('error_after')}"])
    overall = worst(statuses)
    late = [row[0] for row in rows if row[1] != "OK"]
    summary = "모든 수집기가 기준 안에 있습니다." if overall == "OK" else f"기준을 넘은 수집기: {', '.join(late)}"
    return check("수집 신선도", overall, summary, ["수집기", "상태", "마지막 수집", "경과", "경고 / 실패 기준"], rows)


def runs_check(conn, now: datetime) -> dict:
    rows = conn.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (source_id) source_id, status, started_at, error
            FROM ingest.runs WHERE status <> 'RUNNING' ORDER BY source_id, started_at DESC)
        SELECT l.source_id, l.status, l.started_at,
               (SELECT count(*) FROM ingest.runs r WHERE r.source_id = l.source_id AND r.started_at > %(since)s),
               (SELECT count(*) FROM ingest.runs r WHERE r.source_id = l.source_id AND r.started_at > %(since)s
                                                    AND r.status = 'FAILED'),
               (SELECT r.error FROM ingest.runs r WHERE r.source_id = l.source_id AND r.status = 'FAILED'
                ORDER BY r.started_at DESC LIMIT 1)
        FROM latest l ORDER BY l.source_id""", {"since": now - timedelta(hours=24)}).fetchall()
    table, statuses = [], []
    for source, last_status, started, runs_24h, failed_24h, last_error in rows:
        status = "FAIL" if last_status == "FAILED" else "WARN" if failed_24h else "OK"
        statuses.append(status)
        table.append([source, status, f"{last_status} {seoul(started)}", runs_24h, failed_24h,
                      (last_error or "")[:160] if failed_24h or last_status == "FAILED" else ""])
    overall = worst(statuses)
    summary = ("최근 24시간 실패 없음" if overall == "OK" else
               f"최근 24시간 실패 {sum(row[4] for row in table)}건(" + ", ".join(r[0] for r in table if r[1] != "OK") + ")")
    return check("수집 실행", overall, summary, ["소스", "판정", "마지막 실행", "24시간 실행", "24시간 실패", "마지막 실패 사유"], table)


def credits_check(conn) -> dict:
    row = conn.execute("""SELECT started_at, stats->>'credits_remaining' FROM ingest.runs
                          WHERE source_id = 'the-odds-api' AND stats ? 'credits_remaining'
                          ORDER BY started_at DESC LIMIT 1""").fetchone()
    if row is None or row[1] is None:
        return check("API 크레딧", "UNKNOWN", "The Odds API 응답 기록이 없습니다.")
    remaining = int(row[1])
    status = "FAIL" if remaining < CREDITS_FAIL else "WARN" if remaining < CREDITS_WARN else "OK"
    return check("API 크레딧", status, f"The Odds API 남은 크레딧 {remaining}(무료 월 500, {seoul(row[0])} 기준)",
                 ["소스", "남은 크레딧", "확인 시각", "경고 / 실패 기준"],
                 [["the-odds-api", remaining, seoul(row[0]), f"{CREDITS_WARN} / {CREDITS_FAIL} 미만"]])


def stream_check(conn, now: datetime) -> dict:
    since = now - WINDOW
    rejected = conn.execute("""SELECT '스트림 거부' AS kind, reason, count(*), max(rejected_at) FROM silver.stream_rejections
                               WHERE rejected_at > %s GROUP BY reason""", (since,)).fetchall()
    quality = conn.execute("""SELECT '수집 품질 문제', issue_code, count(*), max(created_at) FROM silver.quality_issues
                              WHERE created_at > %s GROUP BY issue_code""", (since,)).fetchall()
    from ..streaming.contracts import DLQ_REASONS
    attention = lambda kind, reason: reason in DLQ_REASONS if kind == "스트림 거부" else reason == "CONTRACT_VIOLATION"
    rows = sorted(rejected + quality, key=lambda r: (r[0], -r[2]))
    flagged = sum(count for kind, reason, count, _ in rows if attention(kind, reason))
    if not rows:
        summary = "최근 7일 거부·품질 문제 없음"
    elif flagged:
        summary = f"확인 필요 {flagged}건(오류 큐 사유·계약 위반). 오류 큐: python -m pipelines.streaming.dlq summary"
    else:
        summary = f"최근 7일 {sum(r[2] for r in rows)}건, 모두 규칙대로 걸러 낸 것(늦게 온 옛 정보·출처 오류)"
    return check("스트림·품질", "WARN" if flagged else "OK", summary, ["종류", "사유", "건수", "마지막", "확인 필요"],
                 [[kind, reason, count, seoul(last), "예" if attention(kind, reason) else ""]
                  for kind, reason, count, last in rows])


def snapshots_check(conn, now: datetime) -> dict:
    window = {"since": now - WINDOW, "now": now}
    rows = []
    for sport, table, time_column in (("축구", "snapshots.prematch_inputs", "kickoff_utc"),
                                      ("NBA", "snapshots.game_prematch_inputs", "tipoff_utc")):
        for label, taken, missed in conn.execute(f"""
                SELECT offset_label, count(*) FILTER (WHERE status = 'TAKEN'), count(*) FILTER (WHERE status = 'MISSED')
                FROM {table} WHERE {time_column} > %(since)s AND {time_column} <= %(now)s + interval '25 hours'
                GROUP BY offset_label ORDER BY offset_label""", window).fetchall():
            rows.append([sport, label, taken, missed])
    if not rows:
        return check("경기 전 기록", "UNKNOWN", "최근 7일 경기 전 스냅샷이 없습니다.")
    missed = sum(row[3] for row in rows)
    taken = sum(row[2] for row in rows)
    summary = f"최근 7일 찍음 {taken} · 놓침 {missed}" + ("(PC·Docker가 꺼져 있던 시간)" if missed else "")
    return check("경기 전 기록", "WARN" if missed else "OK", summary, ["종목", "시점", "찍음", "놓침"], rows)


def weekly_check(conn, now: datetime) -> dict:
    row = conn.execute("""SELECT week_start, created_at, results FROM modeling.prospective_reports
                          ORDER BY report_id DESC LIMIT 1""").fetchone()
    if row is None:
        return check("주간 채점", "UNKNOWN", "주간 보고서가 아직 없습니다.")
    week_start, created_at, results = row
    rows = []
    for sport, part, count_key in (("축구", results, "matches"), ("NBA", results.get("nba") or {}, "games")):
        for label, metric in sorted((part.get("cumulative") or {}).items()):
            market = metric.get("vs_market_pre") or {}
            rows.append([sport, label, metric.get(count_key, 0), metric.get("log_loss", "—"),
                         market.get("model_log_loss", "—"), market.get("market_log_loss", "—")])
    status = "WARN" if now - created_at > WEEKLY_MAX_AGE else "OK"
    summary = f"{week_start} 주 보고서({seoul(created_at)} 작성)" + (" — 8일 넘게 새 보고서가 없음" if status == "WARN" else "")
    return check("주간 채점", status, summary,
                 ["종목", "시점", "채점 경기", "로그 손실", "시장 비교: 모델", "시장 비교: 시장"], rows)


def lake_check(store, bucket: str, now: datetime) -> dict:
    from ..lake.export import PREFIX
    from ..object_store import read_object
    try:
        snapshot = json.loads(read_object(store, bucket, f"{PREFIX}/latest.json"))["snapshot"]
        manifest = json.loads(read_object(store, bucket, f"{PREFIX}/{snapshot}/_manifest.json"))
    except Exception as exc:   # noqa: BLE001 (보고서는 실패를 보여 주고 계속 만든다)
        return check("레이크", "UNKNOWN", f"마지막 내보내기를 읽지 못했습니다: {type(exc).__name__}")
    exported_at = datetime.fromisoformat(manifest["exported_at"])
    status = "WARN" if now - exported_at > LAKE_MAX_AGE else "OK"
    return check("레이크", status, f"마지막 내보내기 {snapshot}({seoul(exported_at)}) · Iceberg 스냅샷·Spark 대조는 매일 DAG가 검사",
                 ["테이블", "행"], [[name, table["rows"]] for name, table in manifest["tables"].items()])


def run_freshness(settings, path: Path) -> list[dict] | None:
    """dbt source freshness를 실행해 결과 파일을 path에 쓰고 results를 돌려준다. dbt를 못 찾거나 결과가 없으면 None."""
    from ..dbt_runner import run_dbt
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        run_dbt(["source", "freshness", "--quiet", "--output", str(path.resolve())], settings)   # 실패(error)여도 파일은 남는다
        return json.loads(path.read_text(encoding="utf-8"))["results"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return None


def collect(settings=None, *, now: datetime | None = None, freshness: list[dict] | None = None, store=None) -> dict:
    from ..object_store import client
    settings, now = settings or load_settings(), now or datetime.now(timezone.utc)
    with connect(settings) as conn:
        checks = [freshness_check(freshness), runs_check(conn, now), credits_check(conn), stream_check(conn, now),
                  snapshots_check(conn, now), weekly_check(conn, now)]
    checks.append(lake_check(store or client(settings), settings.lake_bucket, now))
    return {"generated_at": now.isoformat(), "overall": worst(check["status"] for check in checks), "checks": checks}


STYLE = """
:root { --bg:#f6f7fb; --card:#fff; --text:#1d2433; --muted:#667085; --line:#e4e7ec;
        --OK:#067647; --OK-bg:#ecfdf3; --WARN:#b54708; --WARN-bg:#fffaeb; --FAIL:#b42318; --FAIL-bg:#fef3f2;
        --UNKNOWN:#475467; --UNKNOWN-bg:#f2f4f7; }
@media (prefers-color-scheme: dark) { :root { --bg:#101419; --card:#1a2029; --text:#e6e9ef; --muted:#98a2b3; --line:#2b3340;
        --OK:#75e0a7; --OK-bg:#0b2b1c; --WARN:#fec84b; --WARN-bg:#33240a; --FAIL:#fda29b; --FAIL-bg:#3a1513;
        --UNKNOWN:#cfd4dc; --UNKNOWN-bg:#262c36; } }
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--text); font:14px/1.5 "Pretendard","Noto Sans KR",system-ui,sans-serif; }
main { max-width:1100px; margin:0 auto; padding:24px 16px 48px; }
header { display:flex; flex-wrap:wrap; gap:12px; align-items:center; justify-content:space-between; margin-bottom:16px; }
h1 { font-size:22px; margin:0; } .meta { color:var(--muted); font-size:13px; }
.badge { display:inline-block; padding:2px 10px; border-radius:999px; font-weight:700; font-size:12px; }
.grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(150px, 1fr)); gap:8px; margin-bottom:20px; }
.tile, section { background:var(--card); border:1px solid var(--line); border-radius:12px; }
.tile { padding:10px 12px; } .tile b { display:block; font-size:13px; margin-bottom:4px; }
section { padding:14px 16px; margin-bottom:12px; overflow-x:auto; }
section h2 { font-size:16px; margin:0 8px 4px 0; display:inline-block; } section p { margin:4px 0 10px; color:var(--muted); }
table { border-collapse:collapse; width:100%; font-size:13px; } th, td { text-align:left; padding:6px 8px; border-top:1px solid var(--line); }
th { color:var(--muted); font-weight:600; } td.num { text-align:right; font-variant-numeric:tabular-nums; }
"""


def badge(status: str) -> str:
    return f'<span class="badge" style="color:var(--{status});background:var(--{status}-bg)">{status}</span>'


def render_html(report: dict) -> str:
    esc = lambda value: html.escape(str(value))
    tiles = "".join(f'<div class="tile"><b>{esc(c["name"])}</b>{badge(c["status"])}</div>' for c in report["checks"])
    sections = []
    for c in report["checks"]:
        table = ""
        if c["rows"]:
            head = "".join(f"<th>{esc(col)}</th>" for col in c["columns"])
            body = "".join("<tr>" + "".join(
                f'<td class="num">{esc(v)}</td>' if isinstance(v, (int, float)) else f"<td>{esc(v)}</td>" for v in row)
                + "</tr>" for row in c["rows"])
            table = f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
        sections.append(f'<section><h2>{esc(c["name"])}</h2>{badge(c["status"])}<p>{esc(c["summary"])}</p>{table}</section>')
    return (f'<!doctype html><html lang="ko"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width, initial-scale=1"><title>운영 상태</title>'
            f'<style>{STYLE}</style></head><body><main><header><div><h1>운영 상태 {badge(report["overall"])}</h1>'
            f'<div class="meta">{esc(seoul(report["generated_at"]))} 한국 시각 기준 · 읽기 전용 보고서</div></div></header>'
            f'<div class="grid">{tiles}</div>{"".join(sections)}</main></body></html>')


def write(report: dict, out: Path) -> dict:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_html(report), encoding="utf-8")
    out.with_suffix(".json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {"overall": report["overall"], "html": str(out), "checks": {c["name"]: c["status"] for c in report["checks"]}}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="운영 상태 보고서(HTML·JSON)")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--skip-freshness", action="store_true", help="dbt source freshness를 실행하지 않음(UNKNOWN)")
    args = parser.parse_args(argv)
    settings = load_settings()
    freshness = None if args.skip_freshness else run_freshness(settings, args.out.with_name("freshness.json"))
    print(json.dumps(write(collect(settings, freshness=freshness), args.out), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
