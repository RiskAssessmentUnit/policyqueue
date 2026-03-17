"""
tests/test_processor.py — Unit tests for processor.py

Tests cover:
  - pick_files()     — file listing, ordering, limit
  - _queue_post()    — new post, duplicate post, filename collision
  - process_one()    — all decision branches:
      * PyMuPDF missing → archive immediately
      * No evidence required and absent → skip
      * Score below threshold → skip
      * Post generation returns empty → skip
      * All checks pass → post queued, PDF archived
"""

import json
import sys
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db
import processor


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
    """Create and patch all processor path constants."""
    d = {
        "facts":   tmp_path / "facts",
        "queue":   tmp_path / "queue",
        "archive": tmp_path / "archive",
        "inbox":   tmp_path / "inbox",
        "bigbox":  tmp_path / "big",
    }
    for p in d.values():
        p.mkdir(parents=True)

    with (
        patch.object(processor, "FACTS",   d["facts"]),
        patch.object(processor, "QUEUE",   d["queue"]),
        patch.object(processor, "ARCHIVE", d["archive"]),
        patch.object(processor, "BIGBOX",  d["bigbox"]),
    ):
        yield d


def _make_pdf(folder: Path, name: str = "report.pdf") -> Path:
    """Write a minimal placeholder PDF to a directory."""
    p = folder / name
    p.write_bytes(b"%PDF-1.4 fake content")
    return p


GOOD_FACTS = {
    "program_type": "REVENUE_REPORT",
    "title": "FY2025 Revenue Report",
    "jurisdiction": "Kansas",
    "locations": ["Topeka"],
    "entities": [],
    "key_numbers": [
        {"label": "Total Taxes", "value": 665000000, "unit": "USD", "year": 2025},
        {"label": "Sales Tax",   "value": 123000000, "unit": "USD", "year": 2025},
        {"label": "Income Tax",  "value": 321000000, "unit": "USD", "year": 2025},
        {"label": "Other",       "value":  50000000, "unit": "USD", "year": 2025},
    ],
    "events": [
        {"date": "2024-09", "year": 2024, "description": "Report issued"},
        {"date": None,      "year": 2025, "description": "FY end"},
    ],
    "evidence": [{"quote": "Total Taxes were $665M in FY2025", "note": "Page 5"}],
    "uncertainties": [],
    "recommended_next_queries": [],
}

SOURCE_URL = "https://ksrevenue.gov/fy2025-revenue.pdf"
GOOD_POST  = f'"Total Taxes were $665M in FY2025"\nSource: {SOURCE_URL}'


# ---------------------------------------------------------------------------
# pick_files
# ---------------------------------------------------------------------------

class TestPickFiles:
    def test_empty_folder_returns_empty(self, dirs):
        assert processor.pick_files(dirs["inbox"], 10) == []

    def test_returns_only_pdfs(self, dirs):
        (dirs["inbox"] / "doc.pdf").write_bytes(b"%PDF")
        (dirs["inbox"] / "note.txt").write_text("hello")
        result = processor.pick_files(dirs["inbox"], 10)
        assert len(result) == 1
        assert result[0].suffix == ".pdf"

    def test_respects_limit(self, dirs):
        for i in range(5):
            _make_pdf(dirs["inbox"], f"report{i}.pdf")
        result = processor.pick_files(dirs["inbox"], 3)
        assert len(result) == 3

    def test_ordered_oldest_first(self, dirs):
        # Create files with explicit mtime gaps
        p1 = _make_pdf(dirs["inbox"], "old.pdf")
        time.sleep(0.02)
        p2 = _make_pdf(dirs["inbox"], "new.pdf")
        result = processor.pick_files(dirs["inbox"], 10)
        assert result[0].name == "old.pdf"
        assert result[1].name == "new.pdf"

    def test_limit_zero_returns_empty(self, dirs):
        _make_pdf(dirs["inbox"])
        assert processor.pick_files(dirs["inbox"], 0) == []


# ---------------------------------------------------------------------------
# _queue_post
# ---------------------------------------------------------------------------

class TestQueuePost:
    def test_new_post_written_to_queue(self, dirs):
        name = processor._queue_post(GOOD_POST, "report.pdf")
        assert name != ""
        assert (dirs["queue"] / name).exists()

    def test_duplicate_post_not_written(self, dirs):
        processor._queue_post(GOOD_POST, "report.pdf")
        name2 = processor._queue_post(GOOD_POST, "report.pdf")
        assert name2 == ""

    def test_duplicate_detected_via_db(self, dirs):
        h = db.sha256_text(GOOD_POST)
        db.save_post_hash(h, "already_sent.post.txt")
        name = processor._queue_post(GOOD_POST, "report.pdf")
        assert name == ""

    def test_collision_gets_hash_suffix(self, dirs):
        # Pre-create queue file with the same stem name
        (dirs["queue"] / "report.post.txt").write_text("other content")
        name = processor._queue_post(GOOD_POST, "report.pdf")
        assert name != ""
        assert name != "report.post.txt"
        assert (dirs["queue"] / name).exists()

    def test_post_content_written_correctly(self, dirs):
        name = processor._queue_post(GOOD_POST, "report.pdf")
        content = (dirs["queue"] / name).read_text(encoding="utf-8")
        assert content == GOOD_POST


# ---------------------------------------------------------------------------
# process_one — mocked PDF extraction and LLM calls
# ---------------------------------------------------------------------------

class TestProcessOne:
    def _run(self, pdf_path, facts=None, score=8, post=GOOD_POST, require_evidence=True):
        """Helper: patch all external calls and run process_one."""
        facts = facts or GOOD_FACTS
        import unittest.mock
        mock_fitz = MagicMock()
        with (
            patch.dict(sys.modules, {"fitz": mock_fitz}),
            patch.object(processor,            "_pdf_to_text",  return_value="fake extracted text"),
            patch.object(processor.extract_mod, "extract_facts", return_value=facts),
            patch.object(processor.extract_mod, "score_facts",   return_value=score),
            patch.object(processor.extract_mod, "generate_post", return_value=post),
            patch.object(processor,             "REQUIRE_EVIDENCE", require_evidence),
        ):
            processor.process_one(pdf_path, SOURCE_URL)

    # --- PyMuPDF missing ---

    def test_pymupdf_missing_archives_pdf(self, dirs):
        pdf = _make_pdf(dirs["inbox"])
        with patch.dict(sys.modules, {"fitz": None}):
            processor.process_one(pdf, SOURCE_URL)
        assert not pdf.exists()
        assert len(list(dirs["archive"].glob("*.pdf"))) == 1

    # --- Evidence gate ---

    def test_no_evidence_skipped_when_required(self, dirs):
        pdf = _make_pdf(dirs["inbox"])
        facts_no_evidence = {**GOOD_FACTS, "evidence": []}
        self._run(pdf, facts=facts_no_evidence, score=8, require_evidence=True)
        assert not pdf.exists()                                    # archived
        assert list(dirs["queue"].glob("*.post.txt")) == []        # not queued

    def test_no_evidence_passes_when_not_required(self, dirs):
        pdf = _make_pdf(dirs["inbox"])
        facts_no_evidence = {**GOOD_FACTS, "evidence": []}
        self._run(pdf, facts=facts_no_evidence, score=8, require_evidence=False)
        assert not pdf.exists()
        assert len(list(dirs["queue"].glob("*.post.txt"))) == 1    # queued

    # --- Score gate ---

    def test_low_score_skipped(self, dirs):
        pdf = _make_pdf(dirs["inbox"])
        with patch.object(processor, "POST_SCORE_MIN", 6):
            self._run(pdf, score=3)
        assert not pdf.exists()
        assert list(dirs["queue"].glob("*.post.txt")) == []

    def test_exactly_minimum_score_passes(self, dirs):
        pdf = _make_pdf(dirs["inbox"])
        with patch.object(processor, "POST_SCORE_MIN", 6):
            self._run(pdf, score=6)
        assert not pdf.exists()
        assert len(list(dirs["queue"].glob("*.post.txt"))) == 1

    # --- Post generation gate ---

    def test_empty_post_skipped(self, dirs):
        pdf = _make_pdf(dirs["inbox"])
        self._run(pdf, score=8, post="")
        assert not pdf.exists()
        assert list(dirs["queue"].glob("*.post.txt")) == []

    def test_whitespace_only_post_skipped(self, dirs):
        pdf = _make_pdf(dirs["inbox"])
        self._run(pdf, score=8, post="   \n  ")
        assert not pdf.exists()
        assert list(dirs["queue"].glob("*.post.txt")) == []

    # --- Successful path ---

    def test_successful_post_queued(self, dirs):
        pdf = _make_pdf(dirs["inbox"], "fy2025.pdf")
        self._run(pdf, score=9)
        posts = list(dirs["queue"].glob("*.post.txt"))
        assert len(posts) == 1

    def test_pdf_archived_on_success(self, dirs):
        pdf = _make_pdf(dirs["inbox"], "fy2025.pdf")
        self._run(pdf, score=9)
        assert not pdf.exists()
        assert len(list(dirs["archive"].glob("*.pdf"))) == 1

    def test_facts_json_written(self, dirs):
        pdf = _make_pdf(dirs["inbox"], "fy2025.pdf")
        self._run(pdf, score=9)
        json_file = dirs["facts"] / "fy2025.json"
        assert json_file.exists()
        data = json.loads(json_file.read_text())
        assert data["program_type"] == "REVENUE_REPORT"

    def test_db_event_written_on_success(self, dirs):
        pdf = _make_pdf(dirs["inbox"], "fy2025.pdf")
        self._run(pdf, score=9)
        row = db._get().execute(
            "SELECT kind FROM events WHERE kind='PROCESS_POSTED'"
        ).fetchone()
        assert row is not None

    def test_db_event_written_on_skip(self, dirs):
        pdf = _make_pdf(dirs["inbox"], "lowsig.pdf")
        with patch.object(processor, "POST_SCORE_MIN", 6):
            self._run(pdf, score=2)
        row = db._get().execute(
            "SELECT kind, details FROM events WHERE kind='PROCESS_SKIP'"
        ).fetchone()
        assert row is not None
        assert "score=2" in row[1]

    def test_db_pdf_record_updated_on_success(self, dirs):
        pdf = _make_pdf(dirs["inbox"], "tracked.pdf")
        sha = db.sha256_bytes(pdf.read_bytes())
        db.save_pdf(sha, "tracked.pdf", SOURCE_URL, pdf.stat().st_size, str(pdf))
        self._run(pdf, score=9)
        row = db._get().execute(
            "SELECT stage, signal_score FROM pdfs WHERE filename='tracked.pdf'"
        ).fetchone()
        assert row[0] == "posted"
        assert row[1] == 9

    def test_source_url_passed_to_extract_facts(self, dirs):
        pdf = _make_pdf(dirs["inbox"])
        mock_fitz = MagicMock()
        with (
            patch.dict(sys.modules, {"fitz": mock_fitz}),
            patch.object(processor,            "_pdf_to_text",  return_value="text"),
            patch.object(processor.extract_mod, "extract_facts", return_value=GOOD_FACTS) as mock_extract,
            patch.object(processor.extract_mod, "score_facts",   return_value=9),
            patch.object(processor.extract_mod, "generate_post", return_value=GOOD_POST),
        ):
            processor.process_one(pdf, SOURCE_URL)
        mock_extract.assert_called_once_with("text", SOURCE_URL)
