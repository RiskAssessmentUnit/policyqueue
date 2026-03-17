"""
tests/test_db.py — Unit tests for db.py

Tests cover:
  - Schema initialisation
  - URL seen/unseen roundtrip
  - PDF hash and URL lookups
  - Post-hash deduplication
  - Event insertion
  - update_pdf_processed stage transitions
  - SHA256 helpers
  - JSON state migration
"""

import json
import sys
import time
from pathlib import Path

import pytest

# Make sure the project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolated_db(tmp_path):
    """Give every test its own fresh database."""
    db.reset()
    db.init(tmp_path / "test.sqlite")
    yield tmp_path
    db.reset()


# ---------------------------------------------------------------------------
# Schema / init
# ---------------------------------------------------------------------------

class TestInit:
    def test_tables_created(self, isolated_db):
        con = db._get()
        tables = {
            r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {"urls", "pdfs", "events", "post_hashes", "meta"}.issubset(tables)

    def test_schema_version_set(self, isolated_db):
        row = db._get().execute("SELECT v FROM meta WHERE k='schema_version'").fetchone()
        assert row is not None
        assert row[0] == "2"

    def test_idempotent(self, tmp_path):
        """Calling init twice on the same path must not raise."""
        db.reset()
        db.init(tmp_path / "idem.sqlite")
        db.init(tmp_path / "idem.sqlite")


# ---------------------------------------------------------------------------
# URL state
# ---------------------------------------------------------------------------

class TestUrlState:
    def test_unseen_url_returns_false(self):
        assert db.is_url_seen("https://budget.kansas.gov/") is False

    def test_mark_url_seen_roundtrip(self):
        url = "https://budget.kansas.gov/reports"
        db.mark_url_seen(url)
        assert db.is_url_seen(url) is True

    def test_mark_url_stores_domain(self):
        url = "https://ksrevenue.gov/page"
        db.mark_url_seen(url)
        row = db._get().execute("SELECT domain FROM urls WHERE url=?", (url,)).fetchone()
        assert row[0] == "ksrevenue.gov"

    def test_mark_url_error_status(self):
        url = "https://postaudit.ks.gov/missing"
        db.mark_url_seen(url, status="error", http_code=404, error="not found")
        row = db._get().execute(
            "SELECT status, last_http_code, last_error FROM urls WHERE url=?", (url,)
        ).fetchone()
        assert row == ("error", 404, "not found")

    def test_mark_url_updates_existing(self):
        url = "https://budget.kansas.gov/same"
        db.mark_url_seen(url, status="ok")
        db.mark_url_seen(url, status="error", http_code=503)
        row = db._get().execute(
            "SELECT status, last_http_code FROM urls WHERE url=?", (url,)
        ).fetchone()
        assert row == ("error", 503)

    def test_different_urls_independent(self):
        db.mark_url_seen("https://budget.kansas.gov/a")
        assert db.is_url_seen("https://budget.kansas.gov/b") is False


# ---------------------------------------------------------------------------
# PDF hash / URL state
# ---------------------------------------------------------------------------

class TestPdfState:
    def test_unknown_hash_returns_none(self):
        assert db.get_filename_for_hash("0" * 64) is None

    def test_save_pdf_and_hash_lookup(self):
        sha = "a" * 64
        db.save_pdf(sha, "report.pdf", "https://ksrevenue.gov/report.pdf", 1024, "/tmp/report.pdf")
        assert db.get_filename_for_hash(sha) == "report.pdf"

    def test_pdf_url_seen_after_save(self):
        sha = "b" * 64
        url = "https://budget.kansas.gov/doc.pdf"
        db.save_pdf(sha, "doc.pdf", url, 512, "/tmp/doc.pdf")
        assert db.is_pdf_url_seen(url) is True

    def test_pdf_url_unseen_before_save(self):
        assert db.is_pdf_url_seen("https://budget.kansas.gov/never.pdf") is False

    def test_get_url_for_filename_known(self):
        sha = "c" * 64
        url = "https://postaudit.ks.gov/audit.pdf"
        db.save_pdf(sha, "audit.pdf", url, 2048, "/tmp/audit.pdf")
        assert db.get_url_for_filename("audit.pdf") == url

    def test_get_url_for_filename_unknown(self):
        assert db.get_url_for_filename("ghost.pdf") == "UNKNOWN_URL"

    def test_save_pdf_also_marks_url_seen(self):
        url = "https://budget.kansas.gov/auto-url.pdf"
        db.save_pdf("d" * 64, "auto-url.pdf", url, 100, "/tmp/auto-url.pdf")
        assert db.is_url_seen(url) is True

    def test_duplicate_sha_upserts(self):
        sha = "e" * 64
        db.save_pdf(sha, "first.pdf", "https://budget.kansas.gov/first.pdf", 100, "/a")
        db.save_pdf(sha, "first.pdf", "https://budget.kansas.gov/mirror.pdf", 100, "/b")
        # Original filename preserved; no duplicate row
        rows = db._get().execute("SELECT COUNT(*) FROM pdfs WHERE sha256=?", (sha,)).fetchone()
        assert rows[0] == 1

    def test_update_pdf_processed_posted(self):
        sha = "f" * 64
        db.save_pdf(sha, "processed.pdf", "https://budget.kansas.gov/p.pdf", 500, "/tmp/p.pdf")
        db.update_pdf_processed("processed.pdf", score=8, post_path="queue/processed.post.txt")
        row = db._get().execute(
            "SELECT stage, signal_score, last_post_path FROM pdfs WHERE filename=?",
            ("processed.pdf",)
        ).fetchone()
        assert row[0] == "posted"
        assert row[1] == 8
        assert row[2] == "queue/processed.post.txt"

    def test_update_pdf_processed_no_post(self):
        sha = "9" * 64
        db.save_pdf(sha, "low.pdf", "https://budget.kansas.gov/low.pdf", 200, "/tmp/low.pdf")
        db.update_pdf_processed("low.pdf", score=2, post_path=None)
        row = db._get().execute(
            "SELECT stage FROM pdfs WHERE filename=?", ("low.pdf",)
        ).fetchone()
        assert row[0] == "processed"


# ---------------------------------------------------------------------------
# Post-hash state
# ---------------------------------------------------------------------------

class TestPostHashState:
    def test_unseen_hash_returns_false(self):
        assert db.is_post_hash_seen("0" * 64) is False

    def test_save_and_check_roundtrip(self):
        h = db.sha256_text("Kansas revenue rose 5% in FY2025.")
        db.save_post_hash(h, "report.post.txt")
        assert db.is_post_hash_seen(h) is True

    def test_different_hashes_independent(self):
        h1 = db.sha256_text("post one")
        h2 = db.sha256_text("post two")
        db.save_post_hash(h1, "one.post.txt")
        assert db.is_post_hash_seen(h2) is False

    def test_save_post_hash_idempotent(self):
        h = db.sha256_text("idempotent post")
        db.save_post_hash(h, "idem.post.txt")
        db.save_post_hash(h, "idem.post.txt")  # should not raise
        rows = db._get().execute(
            "SELECT COUNT(*) FROM post_hashes WHERE sha256=?", (h,)
        ).fetchone()
        assert rows[0] == 1


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

class TestEvents:
    def test_add_event_inserts_row(self):
        db.add_event("SAVED", domain="ksrevenue.gov", url="https://ksrevenue.gov/r.pdf",
                     pdf_name="r.pdf", details="bytes=1024")
        rows = db._get().execute(
            "SELECT kind, domain, pdf_name FROM events WHERE kind='SAVED'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0] == ("SAVED", "ksrevenue.gov", "r.pdf")

    def test_add_event_minimal(self):
        db.add_event("CYCLE_DONE")
        row = db._get().execute(
            "SELECT kind FROM events WHERE kind='CYCLE_DONE'"
        ).fetchone()
        assert row is not None

    def test_add_event_timestamp_is_recent(self):
        before = int(time.time()) - 2
        db.add_event("TEST_TS")
        after = int(time.time()) + 2
        row = db._get().execute("SELECT ts FROM events WHERE kind='TEST_TS'").fetchone()
        assert before <= row[0] <= after


# ---------------------------------------------------------------------------
# SHA256 helpers
# ---------------------------------------------------------------------------

class TestHashHelpers:
    def test_sha256_bytes_length(self):
        assert len(db.sha256_bytes(b"hello")) == 64

    def test_sha256_bytes_deterministic(self):
        assert db.sha256_bytes(b"abc") == db.sha256_bytes(b"abc")

    def test_sha256_bytes_different_for_different_input(self):
        assert db.sha256_bytes(b"abc") != db.sha256_bytes(b"xyz")

    def test_sha256_text_matches_bytes(self):
        s = "Kansas FY2025 revenue"
        assert db.sha256_text(s) == db.sha256_bytes(s.encode("utf-8", errors="ignore"))

    def test_sha256_text_empty_string(self):
        result = db.sha256_text("")
        assert len(result) == 64

    def test_sha256_text_none_safe(self):
        result = db.sha256_text(None)
        assert len(result) == 64


# ---------------------------------------------------------------------------
# JSON migration
# ---------------------------------------------------------------------------

class TestJsonMigration:
    def test_migrates_seen_urls(self, tmp_path):
        state = tmp_path / "state"
        state.mkdir()
        (state / "seen_urls.json").write_text(
            json.dumps(["https://budget.kansas.gov/", "https://ksrevenue.gov/"]),
            encoding="utf-8"
        )
        db.reset()
        db.init(tmp_path / "migrated.sqlite")

        assert db.is_url_seen("https://budget.kansas.gov/")
        assert db.is_url_seen("https://ksrevenue.gov/")

    def test_migrates_seen_posthash(self, tmp_path):
        state = tmp_path / "state"
        state.mkdir()
        sha = "a1b2c3" + "0" * 58
        (state / "seen_posthash.json").write_text(
            json.dumps({sha: "old_post.post.txt"}), encoding="utf-8"
        )
        db.reset()
        db.init(tmp_path / "migrated2.sqlite")

        assert db.is_post_hash_seen(sha)

    def test_renames_json_to_bak_after_migration(self, tmp_path):
        state = tmp_path / "state"
        state.mkdir()
        src = state / "seen_urls.json"
        src.write_text(json.dumps([]), encoding="utf-8")

        db.reset()
        db.init(tmp_path / "migrated3.sqlite")

        assert not src.exists()
        assert (state / "seen_urls.json.bak").exists()

    def test_missing_state_dir_is_fine(self, tmp_path):
        """No state/ directory — init must not raise."""
        db.reset()
        db.init(tmp_path / "no_state.sqlite")
        assert db._get() is not None
