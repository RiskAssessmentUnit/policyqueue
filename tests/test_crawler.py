"""
tests/test_crawler.py — Unit tests for crawler.py

Tests cover:
  - _clean_filename()    — sanitisation
  - _url_domain()        — domain extraction
  - _allowed_url()       — domain allowlist
  - _ext_of_url()        — extension parsing
  - _is_pdf_url()        — detection by extension and content-type
  - _extract_links()     — HTML link parsing
  - _should_save_pdf()   — focus-gate allow/block logic
  - _save_pdf_bytes()    — dup detection, focus blocking, new-file write
  - crawl_cycle()        — integration: HTML page → PDF link → save
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import urllib.error

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db
import crawler


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
def inbox(tmp_path):
    """Temp inbox directory, patched into crawler.INBOX."""
    d = tmp_path / "inbox"
    d.mkdir()
    with patch.object(crawler, "INBOX", d):
        yield d


# ---------------------------------------------------------------------------
# _clean_filename
# ---------------------------------------------------------------------------

class TestCleanFilename:
    def test_normal_name_unchanged(self):
        assert crawler._clean_filename("report-2024.pdf") == "report-2024.pdf"

    def test_spaces_replaced_with_underscore(self):
        assert crawler._clean_filename("my report.pdf") == "my_report.pdf"

    def test_consecutive_underscores_collapsed(self):
        assert crawler._clean_filename("a  b") == "a_b"

    def test_special_chars_stripped(self):
        result = crawler._clean_filename("file<>|:*.pdf")
        assert "<" not in result
        assert ">" not in result

    def test_long_name_truncated_at_160(self):
        long = "a" * 200 + ".pdf"
        result = crawler._clean_filename(long)
        assert len(result) <= 160

    def test_empty_string_returns_empty(self):
        assert crawler._clean_filename("") == ""


# ---------------------------------------------------------------------------
# _url_domain
# ---------------------------------------------------------------------------

class TestUrlDomain:
    def test_extracts_domain(self):
        assert crawler._url_domain("https://budget.kansas.gov/reports") == "budget.kansas.gov"

    def test_strips_port(self):
        assert crawler._url_domain("http://localhost:8080/path") == "localhost:8080"

    def test_lowercases(self):
        assert crawler._url_domain("HTTPS://KSRevenue.GOV/page") == "ksrevenue.gov"

    def test_invalid_url_returns_empty(self):
        assert crawler._url_domain("not-a-url") == ""


# ---------------------------------------------------------------------------
# _allowed_url
# ---------------------------------------------------------------------------

class TestAllowedUrl:
    def test_exact_match_allowed(self):
        assert crawler._allowed_url("https://budget.kansas.gov/page")

    def test_subdomain_allowed(self):
        assert crawler._allowed_url("https://sub.budget.kansas.gov/page")

    def test_different_domain_blocked(self):
        assert not crawler._allowed_url("https://example.com/page")

    def test_partial_match_not_allowed(self):
        # "notbudget.kansas.gov" should NOT match "budget.kansas.gov"
        assert not crawler._allowed_url("https://notbudget.kansas.gov/page")

    def test_empty_url_blocked(self):
        assert not crawler._allowed_url("")


# ---------------------------------------------------------------------------
# _ext_of_url
# ---------------------------------------------------------------------------

class TestExtOfUrl:
    def test_pdf_extension(self):
        assert crawler._ext_of_url("https://example.com/report.pdf") == ".pdf"

    def test_jpg_extension(self):
        assert crawler._ext_of_url("https://example.com/img.jpg") == ".jpg"

    def test_no_extension(self):
        assert crawler._ext_of_url("https://example.com/page") == ""

    def test_query_string_ignored(self):
        assert crawler._ext_of_url("https://example.com/report.pdf?v=1") == ".pdf"

    def test_extension_lowercased(self):
        assert crawler._ext_of_url("https://example.com/FILE.PDF") == ".pdf"


# ---------------------------------------------------------------------------
# _is_pdf_url
# ---------------------------------------------------------------------------

class TestIsPdfUrl:
    def test_pdf_extension_detected(self):
        assert crawler._is_pdf_url("https://example.com/doc.pdf")

    def test_non_pdf_extension_not_detected(self):
        assert not crawler._is_pdf_url("https://example.com/page.html")

    def test_content_type_detected(self):
        assert crawler._is_pdf_url(
            "https://example.com/download",
            headers={"Content-Type": "application/pdf"}
        )

    def test_html_content_type_not_pdf(self):
        assert not crawler._is_pdf_url(
            "https://example.com/page",
            headers={"Content-Type": "text/html"}
        )

    def test_no_extension_no_header_not_pdf(self):
        assert not crawler._is_pdf_url("https://example.com/download")


# ---------------------------------------------------------------------------
# _extract_links
# ---------------------------------------------------------------------------

class TestExtractLinks:
    BASE = "https://budget.kansas.gov/"

    def test_extracts_absolute_href(self):
        html = '<a href="https://budget.kansas.gov/report.pdf">PDF</a>'
        links = crawler._extract_links(html, self.BASE)
        assert "https://budget.kansas.gov/report.pdf" in links

    def test_resolves_relative_href(self):
        html = '<a href="/reports/revenue.pdf">PDF</a>'
        links = crawler._extract_links(html, self.BASE)
        assert "https://budget.kansas.gov/reports/revenue.pdf" in links

    def test_skips_anchor_links(self):
        html = '<a href="#section">Jump</a>'
        links = crawler._extract_links(html, self.BASE)
        assert links == []

    def test_skips_mailto_links(self):
        html = '<a href="mailto:info@ks.gov">Email</a>'
        links = crawler._extract_links(html, self.BASE)
        assert links == []

    def test_skips_javascript_links(self):
        html = '<a href="javascript:void(0)">Click</a>'
        links = crawler._extract_links(html, self.BASE)
        assert links == []

    def test_multiple_links_extracted(self):
        html = '''
            <a href="/a.pdf">A</a>
            <a href="/b.pdf">B</a>
            <a href="#skip">Skip</a>
        '''
        links = crawler._extract_links(html, self.BASE)
        assert len(links) == 2


# ---------------------------------------------------------------------------
# _should_save_pdf
# ---------------------------------------------------------------------------

class TestShouldSavePdf:
    def test_revenue_allowed(self):
        assert crawler._should_save_pdf("https://ksrevenue.gov/revenue-report.pdf", "revenue-report.pdf")

    def test_tax_allowed(self):
        assert crawler._should_save_pdf("https://budget.kansas.gov/tax-summary.pdf", "tax-summary.pdf")

    def test_star_bond_allowed(self):
        assert crawler._should_save_pdf("https://ksrevenue.gov/star-bond-2024.pdf", "star-bond-2024.pdf")

    def test_dmv_blocked(self):
        assert not crawler._should_save_pdf("https://ksrevenue.gov/dmv-form.pdf", "dmv-form.pdf")

    def test_driver_blocked(self):
        assert not crawler._should_save_pdf("https://ksrevenue.gov/driver-license.pdf", "driver-license.pdf")

    def test_alcohol_blocked(self):
        assert not crawler._should_save_pdf("https://ksrevenue.gov/alcohol-permit.pdf", "alcohol-permit.pdf")

    def test_unmatched_returns_false(self):
        # Neither allow nor block keyword → default reject
        assert not crawler._should_save_pdf("https://budget.kansas.gov/unrelated.pdf", "unrelated.pdf")

    def test_block_takes_priority_over_allow(self):
        # Contains both "tax" (allow) and "refund" (block)
        assert not crawler._should_save_pdf(
            "https://ksrevenue.gov/tax-refund-form.pdf", "tax-refund-form.pdf"
        )


# ---------------------------------------------------------------------------
# _save_pdf_bytes
# ---------------------------------------------------------------------------

class TestSavePdfBytes:
    PDF_BYTES = b"%PDF-1.4 fake revenue report content"
    PDF_URL   = "https://ksrevenue.gov/revenue-2024.pdf"

    def test_new_pdf_saved_to_inbox(self, inbox):
        name = crawler._save_pdf_bytes(self.PDF_URL, self.PDF_BYTES)
        assert name != ""
        assert (inbox / name).exists()

    def test_new_pdf_recorded_in_db(self, inbox):
        crawler._save_pdf_bytes(self.PDF_URL, self.PDF_BYTES)
        sha = db.sha256_bytes(self.PDF_BYTES)
        assert db.get_filename_for_hash(sha) is not None

    def test_duplicate_content_not_saved_again(self, inbox):
        # First save
        crawler._save_pdf_bytes(self.PDF_URL, self.PDF_BYTES)
        # Second save — same bytes, different URL
        name2 = crawler._save_pdf_bytes(
            "https://ksrevenue.gov/mirror-revenue.pdf", self.PDF_BYTES
        )
        assert name2 == ""
        # Only one file in inbox
        pdfs = list(inbox.glob("*.pdf"))
        assert len(pdfs) == 1

    def test_focus_blocked_pdf_not_saved(self, inbox):
        name = crawler._save_pdf_bytes(
            "https://ksrevenue.gov/dmv-handbook.pdf", b"%PDF dmv content"
        )
        assert name == ""
        assert list(inbox.glob("*.pdf")) == []

    def test_filename_collision_gets_hash_suffix(self, inbox):
        # Pre-create a file with the same name
        (inbox / "revenue-2024.pdf").write_bytes(b"other content")
        name = crawler._save_pdf_bytes(self.PDF_URL, self.PDF_BYTES)
        # Should have gotten a hash suffix
        assert name != "revenue-2024.pdf"
        assert (inbox / name).exists()


# ---------------------------------------------------------------------------
# crawl_cycle — integration with mocked HTTP
# ---------------------------------------------------------------------------

HTML_WITH_PDF = (
    b"<html><body>"
    b'<a href="/reports/revenue-report.pdf">Revenue Report</a>'
    b"</body></html>"
)
FAKE_PDF = b"%PDF-1.4 Kansas revenue FY2025 report content"


def _make_fetch_side_effect(html_url_host: str):
    """Return a _fetch mock that serves HTML then the PDF."""
    def side_effect(url):
        if url.endswith(".pdf"):
            return 200, FAKE_PDF, {"Content-Type": "application/pdf"}
        return 200, HTML_WITH_PDF, {"Content-Type": "text/html; charset=utf-8"}
    return side_effect


class TestCrawlCycle:
    SEED = "https://budget.kansas.gov/"

    def test_returns_count_of_saved_pdfs(self, inbox):
        with (
            patch.object(crawler, "SEEDS", [self.SEED]),
            patch.object(crawler, "CRAWL_PAGES_PER_CYCLE", 5),
            patch("crawler._fetch", side_effect=_make_fetch_side_effect(self.SEED)),
        ):
            saved = crawler.crawl_cycle()

        assert saved == 1
        assert len(list(inbox.glob("*.pdf"))) == 1

    def test_already_seen_url_not_revisited(self, inbox):
        db.mark_url_seen(self.SEED)
        with (
            patch.object(crawler, "SEEDS", [self.SEED]),
            patch.object(crawler, "CRAWL_PAGES_PER_CYCLE", 5),
            patch("crawler._fetch") as mock_fetch,
        ):
            crawler.crawl_cycle()
            mock_fetch.assert_not_called()

    def test_out_of_domain_url_skipped(self, inbox):
        with (
            patch.object(crawler, "SEEDS", ["https://evil.com/"]),
            patch.object(crawler, "CRAWL_PAGES_PER_CYCLE", 5),
            patch("crawler._fetch") as mock_fetch,
        ):
            crawler.crawl_cycle()
            mock_fetch.assert_not_called()

    def test_returns_zero_when_no_pdfs_found(self, inbox):
        html_no_pdfs = b"<html><body><a href='/page'>just a page</a></body></html>"
        with (
            patch.object(crawler, "SEEDS", [self.SEED]),
            patch.object(crawler, "CRAWL_PAGES_PER_CYCLE", 5),
            patch("crawler._fetch", return_value=(200, html_no_pdfs, {"Content-Type": "text/html"})),
        ):
            saved = crawler.crawl_cycle()
        assert saved == 0

    def test_http_error_recorded_in_db(self, inbox):
        err = urllib.error.HTTPError(self.SEED, 502, "Bad Gateway", {}, None)
        with (
            patch.object(crawler, "SEEDS", [self.SEED]),
            patch.object(crawler, "CRAWL_PAGES_PER_CYCLE", 5),
            patch.object(crawler, "MAX_RETRIES", 1),
            patch("crawler._fetch", side_effect=err),
        ):
            crawler.crawl_cycle()

        row = db._get().execute(
            "SELECT kind FROM events WHERE kind='ERROR'"
        ).fetchone()
        assert row is not None
