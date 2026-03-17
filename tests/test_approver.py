"""
tests/test_approver.py — Unit tests for approver.py

Covers:
  - send_telegram()  — fires when env vars set, no-ops when absent
  - send_discord()   — fires when env var set, no-ops when absent
  - safe_move()      — basic move, collision rename
  - main loop        — SENT / DUPE / EMPTY branches
"""

import sys
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db
import approver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolated_db(tmp_path):
    db.reset()
    db.init(tmp_path / "test.sqlite")
    yield tmp_path
    db.reset()


@pytest.fixture()
def dirs(tmp_path):
    d = {
        "queue":       tmp_path / "queue",
        "sent":        tmp_path / "sent",
        "sent_dupes":  tmp_path / "sent_dupes",
        "empty":       tmp_path / "skip" / "empty_posts",
        "logs":        tmp_path / "logs",
    }
    for p in d.values():
        p.mkdir(parents=True)

    with (
        patch.object(approver, "QUEUE",          d["queue"]),
        patch.object(approver, "SENT_DIR",        d["sent"]),
        patch.object(approver, "SENT_DUPES_DIR",  d["sent_dupes"]),
        patch.object(approver, "EMPTY_DIR",       d["empty"]),
        patch.object(approver, "LOGS",            d["logs"]),
        patch.object(approver, "LOG",             d["logs"] / "approver.log"),
    ):
        yield d


# ---------------------------------------------------------------------------
# send_telegram
# ---------------------------------------------------------------------------

class TestSendTelegram:
    def test_no_op_when_env_vars_absent(self):
        with (
            patch.object(approver, "TELEGRAM_BOT",  ""),
            patch.object(approver, "TELEGRAM_CHAT", ""),
            patch.object(approver, "http_post") as mock_post,
        ):
            approver.send_telegram("hello")
        mock_post.assert_not_called()

    def test_no_op_when_only_bot_set(self):
        with (
            patch.object(approver, "TELEGRAM_BOT",  "mybot"),
            patch.object(approver, "TELEGRAM_CHAT", ""),
            patch.object(approver, "http_post") as mock_post,
        ):
            approver.send_telegram("hello")
        mock_post.assert_not_called()

    def test_fires_when_both_vars_set(self):
        with (
            patch.object(approver, "TELEGRAM_BOT",  "mybot"),
            patch.object(approver, "TELEGRAM_CHAT", "12345"),
            patch.object(approver, "http_post") as mock_post,
        ):
            approver.send_telegram("test message")
        mock_post.assert_called_once()
        url, payload = mock_post.call_args[0]
        assert "mybot" in url
        assert payload["chat_id"] == "12345"
        assert payload["text"] == "test message"

    def test_truncates_long_messages(self):
        long_msg = "x" * 5000
        with (
            patch.object(approver, "TELEGRAM_BOT",  "mybot"),
            patch.object(approver, "TELEGRAM_CHAT", "12345"),
            patch.object(approver, "http_post") as mock_post,
        ):
            approver.send_telegram(long_msg)
        _, payload = mock_post.call_args[0]
        assert len(payload["text"]) == 3900


# ---------------------------------------------------------------------------
# send_discord
# ---------------------------------------------------------------------------

class TestSendDiscord:
    def test_no_op_when_webhook_absent(self):
        with (
            patch.object(approver, "DISCORD_WEBHOOK", ""),
            patch.object(approver, "http_post") as mock_post,
        ):
            approver.send_discord("hello")
        mock_post.assert_not_called()

    def test_fires_when_webhook_set(self):
        with (
            patch.object(approver, "DISCORD_WEBHOOK", "https://discord.example/webhook"),
            patch.object(approver, "http_post") as mock_post,
        ):
            approver.send_discord("test")
        mock_post.assert_called_once()
        url, payload = mock_post.call_args[0]
        assert url == "https://discord.example/webhook"
        assert payload["content"] == "test"

    def test_truncates_to_1900_chars(self):
        with (
            patch.object(approver, "DISCORD_WEBHOOK", "https://discord.example/webhook"),
            patch.object(approver, "http_post") as mock_post,
        ):
            approver.send_discord("y" * 3000)
        _, payload = mock_post.call_args[0]
        assert len(payload["content"]) == 1900


# ---------------------------------------------------------------------------
# safe_move
# ---------------------------------------------------------------------------

class TestSafeMove:
    def test_moves_file(self, tmp_path):
        src = tmp_path / "a.txt"
        src.write_text("hello")
        dst_dir = tmp_path / "dest"
        dst_dir.mkdir()
        result = approver.safe_move(src, dst_dir)
        assert not src.exists()
        assert result.exists()
        assert result.parent == dst_dir

    def test_collision_gets_timestamp_suffix(self, tmp_path):
        src = tmp_path / "a.txt"
        src.write_text("hello")
        dst_dir = tmp_path / "dest"
        dst_dir.mkdir()
        (dst_dir / "a.txt").write_text("existing")
        result = approver.safe_move(src, dst_dir)
        assert result.name != "a.txt"
        assert result.exists()


# ---------------------------------------------------------------------------
# Main loop branches
# ---------------------------------------------------------------------------

POST_TEXT = '"Kansas tax receipts reached $665M"\nSource: https://ksrevenue.gov/report.pdf'


def _make_post(queue_dir: Path, name: str = "report.post.txt", text: str = POST_TEXT) -> Path:
    p = queue_dir / name
    p.write_text(text, encoding="utf-8")
    return p


class TestMainLoop:
    def _run_one_iteration(self, dirs):
        """Run main loop for exactly one iteration then stop via StopIteration."""
        call_count = 0

        original_sleep = time.sleep

        def fake_sleep(n):
            nonlocal call_count
            call_count += 1
            raise StopIteration("done")

        with (
            patch.object(approver, "DISCORD_WEBHOOK", ""),
            patch.object(approver, "TELEGRAM_BOT",    ""),
            patch.object(approver, "TELEGRAM_CHAT",   ""),
            patch("time.sleep", side_effect=fake_sleep),
        ):
            try:
                approver.main()
            except StopIteration:
                pass

    def test_sends_new_post(self, dirs):
        _make_post(dirs["queue"])
        with (
            patch.object(approver, "DISCORD_WEBHOOK", "https://discord.example/hook"),
            patch.object(approver, "TELEGRAM_BOT",    "bot"),
            patch.object(approver, "TELEGRAM_CHAT",   "chat"),
            patch.object(approver, "http_post") as mock_post,
            patch("time.sleep", side_effect=StopIteration),
        ):
            try:
                approver.main()
            except StopIteration:
                pass
        # Discord + Telegram = 2 calls
        assert mock_post.call_count == 2
        assert len(list(dirs["sent"].glob("*.post.txt"))) == 1
        assert len(list(dirs["queue"].glob("*.post.txt"))) == 0

    def test_duplicate_post_moved_to_sent_dupes(self, dirs):
        p = _make_post(dirs["queue"])
        h = db.sha256_text(POST_TEXT)
        db.save_post_hash(h, "already.post.txt")

        with (
            patch.object(approver, "DISCORD_WEBHOOK", ""),
            patch.object(approver, "TELEGRAM_BOT",    ""),
            patch.object(approver, "TELEGRAM_CHAT",   ""),
            patch("time.sleep", side_effect=StopIteration),
        ):
            try:
                approver.main()
            except StopIteration:
                pass

        assert len(list(dirs["sent_dupes"].glob("*.post.txt"))) == 1
        assert len(list(dirs["sent"].glob("*"))) == 0

    def test_empty_post_moved_to_empty_dir(self, dirs):
        _make_post(dirs["queue"], text="   ")

        with (
            patch.object(approver, "DISCORD_WEBHOOK", ""),
            patch.object(approver, "TELEGRAM_BOT",    ""),
            patch.object(approver, "TELEGRAM_CHAT",   ""),
            patch("time.sleep", side_effect=StopIteration),
        ):
            try:
                approver.main()
            except StopIteration:
                pass

        assert len(list(dirs["empty"].glob("*.post.txt"))) == 1
        assert len(list(dirs["sent"].glob("*"))) == 0

    def test_sent_post_recorded_in_db(self, dirs):
        _make_post(dirs["queue"])

        with (
            patch.object(approver, "DISCORD_WEBHOOK", ""),
            patch.object(approver, "TELEGRAM_BOT",    ""),
            patch.object(approver, "TELEGRAM_CHAT",   ""),
            patch("time.sleep", side_effect=StopIteration),
        ):
            try:
                approver.main()
            except StopIteration:
                pass

        h = db.sha256_text(POST_TEXT)
        assert db.is_post_hash_seen(h)
