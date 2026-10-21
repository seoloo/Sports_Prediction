"""텔레그램 Bot API로 내 대화방에 메시지를 보낸다.

- 토큰은 URL 경로에 들어가므로(…/bot<토큰>/sendMessage), 오류 메시지·로그에 URL이나 토큰이 남지 않게 지운다.
- 메시지 한 개는 4096자까지라 줄 단위로 나눠 보낸다.
- 429(요청 한도 초과)는 서버가 알려 준 retry_after만큼 기다렸다가 다시 보낸다. 그 밖의 오류는 그대로 올린다.
- 토큰이나 대화방 ID가 없으면 보내지 않고 NOT_CONFIGURED를 돌려준다(알림은 선택 기능).

명령줄
python -m pipelines.notify.telegram chat-id   봇에게 온 메시지에서 대화방 ID를 찾는다(먼저 봇에게 아무 말이나 보낸다)
python -m pipelines.notify.telegram test      시험 메시지를 한 번 보낸다
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from ..config import load_settings

API_BASE = "https://api.telegram.org"
MAX_LENGTH = 4096


class TelegramError(RuntimeError):
    """토큰을 지운 오류. 원래 예외의 메시지에 토큰이 섞여 있어도 밖으로 나가지 않는다."""


def redact(text: str, token: str | None) -> str:
    return text.replace(token, "<redacted>") if token else text


def split_message(text: str, limit: int = MAX_LENGTH) -> list[str]:
    """줄 단위로 limit 이하 조각으로 나눈다. 한 줄이 limit보다 길면 그 줄만 잘라 나눈다."""
    parts, current = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            if current:
                parts.append(current)
                current = ""
            parts.append(line[:limit])
            line = line[limit:]
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > limit:
            parts.append(current)
            current = line
        else:
            current = candidate
    if current or not parts:
        parts.append(current)
    return parts


def call(method: str, token: str, params: dict, *, opener=urllib.request.urlopen, sleep=time.sleep,
         max_attempts: int = 3) -> dict:
    """Bot API 호출 하나. 결과(result)를 돌려준다."""
    data = urllib.parse.urlencode(params).encode()
    for attempt in range(1, max_attempts + 1):
        request = urllib.request.Request(f"{API_BASE}/bot{token}/{method}", data=data)
        try:
            with opener(request, timeout=30) as response:
                body = json.loads(response.read())
            if not body.get("ok"):
                raise TelegramError(redact(f"{method} 실패: {body.get('description')}", token))
            return body["result"]
        except urllib.error.HTTPError as exc:
            try:
                body = json.loads(exc.read() or b"{}")
            except ValueError:
                body = {}
            retry_after = body.get("parameters", {}).get("retry_after")
            if exc.code == 429 and attempt < max_attempts:
                sleep(int(retry_after or 5))
                continue
            raise TelegramError(redact(f"{method} 실패: HTTP {exc.code} {body.get('description', '')}", token)) from None
        except urllib.error.URLError as exc:
            raise TelegramError(redact(f"{method} 접속 실패: {exc.reason}", token)) from None
    raise TelegramError(f"{method} 실패: 재시도 {max_attempts}회 초과")


def send(text: str, settings=None, **kwargs) -> dict:
    """내 대화방으로 보낸다. {"status": "SENT", "parts": n} 또는 {"status": "NOT_CONFIGURED"}."""
    settings = settings or load_settings()
    if not (settings.telegram_bot_token and settings.telegram_chat_id):
        return {"status": "NOT_CONFIGURED", "parts": 0}
    parts = split_message(text)
    for part in parts:
        call("sendMessage", settings.telegram_bot_token,
             {"chat_id": settings.telegram_chat_id, "text": part, "disable_web_page_preview": "true"}, **kwargs)
    return {"status": "SENT", "parts": len(parts)}


def chat_ids(settings=None, **kwargs) -> list[dict]:
    """봇에게 최근 온 메시지의 대화방 목록(ID, 종류, 이름). 대화방 ID를 처음 찾을 때만 쓴다."""
    settings = settings or load_settings()
    if not settings.telegram_bot_token:
        raise TelegramError(".env에 TELEGRAM_BOT_TOKEN이 없습니다.")
    found = {}
    for update in call("getUpdates", settings.telegram_bot_token, {"timeout": 0}, **kwargs):
        chat = (update.get("message") or update.get("my_chat_member") or {}).get("chat")
        if chat:
            found[chat["id"]] = {"chat_id": chat["id"], "type": chat.get("type"),
                                 "name": chat.get("username") or chat.get("title") or chat.get("first_name")}
    return list(found.values())


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="텔레그램 알림 설정 도우미")
    parser.add_argument("command", choices=["chat-id", "test"])
    args = parser.parse_args(argv)
    try:
        if args.command == "chat-id":
            chats = chat_ids()
            print(json.dumps(chats, ensure_ascii=False, indent=2) if chats else
                  "봇에게 온 메시지가 없습니다. 텔레그램에서 봇에게 아무 메시지나 보낸 뒤 다시 실행하세요.")
        else:
            print(json.dumps(send("Sports_Prediction 알림 시험 메시지입니다."), ensure_ascii=False))
    except TelegramError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
