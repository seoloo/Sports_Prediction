-- 텔레그램 알림 발송 기록. 같은 알림(kind, notification_key)은 한 번만 보낸다(DAG 재시도·재실행에도 중복 없음).
-- 보낸 뒤 기록하므로 "최소 한 번": 보낸 직후 기록이 실패하면 다음 실행에서 한 번 더 갈 수 있다.
-- 메시지 본문은 저장하지 않고 해시만 남긴다(무엇을 보냈는지 대조용).

CREATE SCHEMA IF NOT EXISTS notify;

CREATE TABLE notify.sent_messages (
  kind TEXT NOT NULL CHECK (kind IN ('PREMATCH', 'WEEKLY', 'FAILURE', 'TEST')),
  notification_key TEXT NOT NULL,
  message_sha256 CHAR(64) NOT NULL,
  parts INT NOT NULL CHECK (parts >= 1),     -- 4096자 제한으로 나눠 보낸 메시지 수
  sent_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (kind, notification_key)
);

CREATE TRIGGER sent_messages_immutable BEFORE UPDATE OR DELETE ON notify.sent_messages
  FOR EACH ROW EXECUTE FUNCTION ingest.reject_change();
