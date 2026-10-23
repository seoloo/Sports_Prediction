"""파이프라인 설정 단위 테스트. .env 생성이 기존 값을 덮어쓰지 않고, 비밀번호가 무작위인지 확인한다."""
import pytest

from pipelines.config import init_env, load_settings, read_env_file


def test_init_env_creates_random_secrets_once(tmp_path, monkeypatch):
    for key in ("POSTGRES_USER", "POSTGRES_PASSWORD", "S3_ACCESS_KEY", "S3_SECRET_KEY", "POSTGRES_PORT", "S3_PORT"):
        monkeypatch.delenv(key, raising=False)
    first, second = tmp_path / "a.env", tmp_path / "b.env"
    assert init_env(first) and init_env(second)
    a, b = read_env_file(first), read_env_file(second)
    assert a["POSTGRES_PASSWORD"] != "change-me" and a["POSTGRES_PASSWORD"] != b["POSTGRES_PASSWORD"]
    assert a["S3_SECRET_KEY"] != b["S3_SECRET_KEY"]
    first.write_text(first.read_text(encoding="utf-8") + "# keep\n", encoding="utf-8")
    assert init_env(first) is False and first.read_text(encoding="utf-8").endswith("# keep\n")
    settings = load_settings(first)
    assert settings.postgres_host == "127.0.0.1" and settings.s3_endpoint_url == "http://127.0.0.1:45900"
    assert "connect_timeout=10" in settings.postgres_conninfo


def test_missing_env_gives_instruction(tmp_path, monkeypatch):
    for key in ("POSTGRES_USER", "POSTGRES_PASSWORD", "S3_ACCESS_KEY", "S3_SECRET_KEY"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(RuntimeError, match="init-env"):
        load_settings(tmp_path / "missing.env")


@pytest.mark.parametrize("today, expected", [
    ("2026-10-21", ["2025-26", "2026-27"]),
    ("2026-07-01", ["2025-26", "2026-27"]),
    ("2026-06-30", ["2024-25", "2025-26"]),
    ("2099-12-31", ["2098-99", "2099-00"]),
])
def test_seasons_for(today, expected):
    from datetime import date
    from pipelines.schedule import seasons_for
    assert seasons_for(date.fromisoformat(today)) == expected
