"""요청 한도를 지키는 공용 제한기. 소스마다 분당 한도가 다르다(football-data.org 10회, balldontlie 5회).

Kafka 등 무거운 의존성이 없어 Airflow 이미지에서도 그대로 불러올 수 있다.
"""
import time
from collections import deque


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
