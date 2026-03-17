"""
tests/test_extract.py — Unit tests for extract.py

Tests cover:
  - score_facts()     — pure scoring logic, no API
  - merge_facts()     — pure dedup/merge logic, no API
  - _truncate()       — pure text truncation
  - extract_facts()   — mocked Claude API (tool_use path + error paths)
  - generate_post()   — mocked Claude API (valid + rejection paths)
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import extract


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tool_use_response(input_data: dict):
    """Build a minimal mock anthropic response containing a tool_use block."""
    tool_block = SimpleNamespace(type="tool_use", input=input_data)
    return SimpleNamespace(content=[tool_block])


def _make_text_response(text: str):
    """Build a minimal mock anthropic response containing a text block."""
    text_block = SimpleNamespace(type="text", text=text)
    return SimpleNamespace(content=[text_block])


GOOD_FACTS = {
    "program_type": "REVENUE_REPORT",
    "title": "FY2025 State General Fund Receipts",
    "jurisdiction": "Kansas",
    "locations": ["Topeka"],
    "entities": [{"name": "KDOR", "type": "GOV_BODY"}],
    "key_numbers": [
        {"label": "Total Taxes", "value": 665620422, "unit": "USD", "year": 2025},
        {"label": "Sales Tax",   "value": 123456789, "unit": "USD", "year": 2025},
        {"label": "Income Tax",  "value": 987654321, "unit": "USD", "year": 2025},
        {"label": "Other",       "value": 111111111, "unit": "USD", "year": 2025},
    ],
    "events": [
        {"date": "2024-09-03", "year": 2024, "description": "Report released"},
        {"date": None,         "year": 2025, "description": "Fiscal year end"},
    ],
    "evidence": [{"quote": "Total Taxes were $665M in FY2025", "note": "Page 5"}],
    "uncertainties": [],
    "recommended_next_queries": [],
}


# ---------------------------------------------------------------------------
# score_facts
# ---------------------------------------------------------------------------

class TestScoreFacts:
    def test_high_signal_type_scores_three(self):
        for pt in ("STAR_BOND", "REVENUE_REPORT", "BUDGET", "AUDIT"):
            f = {**extract.EMPTY_FACTS, "program_type": pt}
            assert extract.score_facts(f) >= 3

    def test_low_signal_type_scores_zero_base(self):
        f = {**extract.EMPTY_FACTS, "program_type": "OTHER"}
        assert extract.score_facts(f) == 0

    def test_title_adds_one(self):
        f = {**extract.EMPTY_FACTS, "program_type": "OTHER", "title": "Some Title"}
        assert extract.score_facts(f) == 1

    def test_key_numbers_capped_at_four(self):
        f = {
            **extract.EMPTY_FACTS,
            "program_type": "OTHER",
            "key_numbers": [{"label": f"k{i}", "value": i, "unit": "USD"} for i in range(10)],
        }
        assert extract.score_facts(f) == 4

    def test_events_capped_at_two(self):
        f = {
            **extract.EMPTY_FACTS,
            "program_type": "OTHER",
            "events": [{"description": f"e{i}"} for i in range(5)],
        }
        assert extract.score_facts(f) == 2

    def test_evidence_adds_one(self):
        f = {**extract.EMPTY_FACTS, "program_type": "OTHER",
             "evidence": [{"quote": "q", "note": "p1"}]}
        assert extract.score_facts(f) == 1

    def test_full_signal_doc(self):
        score = extract.score_facts(GOOD_FACTS)
        # 3 (REVENUE_REPORT) + 1 (title) + 4 (4 key_numbers) + 2 (2 events) + 1 (evidence) = 11
        assert score == 11

    def test_empty_facts_scores_zero(self):
        assert extract.score_facts(extract.EMPTY_FACTS) == 0


# ---------------------------------------------------------------------------
# merge_facts
# ---------------------------------------------------------------------------

class TestMergeFacts:
    def test_empty_parts_returns_empty_structure(self):
        result = extract.merge_facts([])
        assert result["program_type"] == "OTHER"
        assert result["key_numbers"] == []
        assert result["evidence"] == []

    def test_single_part_passthrough(self):
        result = extract.merge_facts([GOOD_FACTS])
        assert result["program_type"] == "REVENUE_REPORT"
        assert result["title"] == "FY2025 State General Fund Receipts"

    def test_higher_specificity_program_type_wins(self):
        parts = [
            {**extract.EMPTY_FACTS, "program_type": "OTHER"},
            {**extract.EMPTY_FACTS, "program_type": "STAR_BOND"},
            {**extract.EMPTY_FACTS, "program_type": "BUDGET"},
        ]
        result = extract.merge_facts(parts)
        assert result["program_type"] == "STAR_BOND"  # highest pref=6

    def test_first_non_empty_title_wins(self):
        parts = [
            {**extract.EMPTY_FACTS, "title": None},
            {**extract.EMPTY_FACTS, "title": "Second Title"},
            {**extract.EMPTY_FACTS, "title": "Third Title"},
        ]
        result = extract.merge_facts(parts)
        assert result["title"] == "Second Title"

    def test_key_numbers_deduplicated(self):
        kn = {"label": "Total Taxes", "value": 665620422, "unit": "USD", "year": 2025}
        parts = [
            {**extract.EMPTY_FACTS, "key_numbers": [kn]},
            {**extract.EMPTY_FACTS, "key_numbers": [kn]},  # exact duplicate
        ]
        result = extract.merge_facts(parts)
        assert len(result["key_numbers"]) == 1

    def test_key_numbers_different_values_both_kept(self):
        parts = [
            {**extract.EMPTY_FACTS, "key_numbers": [
                {"label": "Sales Tax", "value": 100, "unit": "USD", "year": 2025}
            ]},
            {**extract.EMPTY_FACTS, "key_numbers": [
                {"label": "Sales Tax", "value": 200, "unit": "USD", "year": 2025}
            ]},
        ]
        result = extract.merge_facts(parts)
        assert len(result["key_numbers"]) == 2

    def test_evidence_capped_at_eight(self):
        ev = [{"quote": f"quote {i}", "note": "p1"} for i in range(12)]
        parts = [{**extract.EMPTY_FACTS, "evidence": ev}]
        result = extract.merge_facts(parts)
        assert len(result["evidence"]) == 8

    def test_locations_deduplicated_case_insensitive(self):
        parts = [
            {**extract.EMPTY_FACTS, "locations": ["Topeka"]},
            {**extract.EMPTY_FACTS, "locations": ["topeka"]},
        ]
        result = extract.merge_facts(parts)
        assert len(result["locations"]) == 1

    def test_uncertainties_merged_unique(self):
        parts = [
            {**extract.EMPTY_FACTS, "uncertainties": ["Year ambiguous"]},
            {**extract.EMPTY_FACTS, "uncertainties": ["Year ambiguous", "No date"]},
        ]
        result = extract.merge_facts(parts)
        assert len(result["uncertainties"]) == 2

    def test_jurisdiction_set_to_kansas_when_any_part_has_it(self):
        parts = [
            {**extract.EMPTY_FACTS, "jurisdiction": None},
            {**extract.EMPTY_FACTS, "jurisdiction": "Kansas"},
        ]
        result = extract.merge_facts(parts)
        assert result["jurisdiction"] == "Kansas"


# ---------------------------------------------------------------------------
# _truncate
# ---------------------------------------------------------------------------

class TestTruncate:
    def test_short_text_unchanged(self):
        text = "hello world"
        assert extract._truncate(text) == text

    def test_long_text_truncated(self):
        # Temporarily lower the limit to make the test fast
        original = extract.MAX_TOTAL_CHARS
        extract.MAX_TOTAL_CHARS = 100
        try:
            text = "A" * 200
            result = extract._truncate(text)
            assert len(result) < len(text)
            assert "[...TRUNCATED...]" in result
        finally:
            extract.MAX_TOTAL_CHARS = original

    def test_truncation_preserves_head_and_tail(self):
        original = extract.MAX_TOTAL_CHARS
        extract.MAX_TOTAL_CHARS = 100
        try:
            text = "HEAD" + "M" * 200 + "TAIL"
            result = extract._truncate(text)
            assert result.startswith("HEAD")
            assert result.endswith("TAIL")
        finally:
            extract.MAX_TOTAL_CHARS = original

    def test_exactly_at_limit_unchanged(self):
        text = "X" * extract.MAX_TOTAL_CHARS
        assert extract._truncate(text) == text


# ---------------------------------------------------------------------------
# extract_facts — mocked API
# ---------------------------------------------------------------------------

class TestExtractFacts:
    def test_empty_text_returns_empty_facts(self):
        result = extract.extract_facts("")
        assert result == dict(extract.EMPTY_FACTS)

    def test_no_text_sentinel_returns_empty_facts(self):
        result = extract.extract_facts("[NO_TEXT_EXTRACTED]")
        assert result == dict(extract.EMPTY_FACTS)

    def test_successful_extraction(self):
        mock_response = _make_tool_use_response(GOOD_FACTS)
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response

        with patch("extract._client", return_value=mock_client):
            result = extract.extract_facts("Some Kansas revenue document text")

        assert result["program_type"] == "REVENUE_REPORT"
        assert result["title"] == "FY2025 State General Fund Receipts"
        mock_client.messages.create.assert_called_once()

    def test_tool_choice_forced(self):
        """Verify that tool_choice is passed as forced tool use."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _make_tool_use_response(
            dict(extract.EMPTY_FACTS)
        )
        with patch("extract._client", return_value=mock_client):
            extract.extract_facts("some text")

        call_kwargs = mock_client.messages.create.call_args.kwargs
        assert call_kwargs["tool_choice"] == {"type": "tool", "name": "record_facts"}

    def test_api_error_returns_empty_facts(self):
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = RuntimeError("API down")

        with patch("extract._client", return_value=mock_client):
            result = extract.extract_facts("some text")

        assert result == dict(extract.EMPTY_FACTS)

    def test_no_tool_use_block_returns_empty_facts(self):
        """Response with only a text block (no tool_use) should fall back."""
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _make_text_response("oops, plain text")

        with patch("extract._client", return_value=mock_client):
            result = extract.extract_facts("some text")

        assert result == dict(extract.EMPTY_FACTS)

    def test_evidence_capped_at_eight(self):
        facts_with_lots_of_evidence = {
            **GOOD_FACTS,
            "evidence": [{"quote": f"quote {i}", "note": "p1"} for i in range(12)],
        }
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _make_tool_use_response(
            facts_with_lots_of_evidence
        )
        with patch("extract._client", return_value=mock_client):
            result = extract.extract_facts("text")

        assert len(result["evidence"]) == 8

    def test_source_url_included_in_prompt(self):
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _make_tool_use_response(
            dict(extract.EMPTY_FACTS)
        )
        with patch("extract._client", return_value=mock_client):
            extract.extract_facts("text", source_url="https://ksrevenue.gov/report.pdf")

        prompt = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "https://ksrevenue.gov/report.pdf" in prompt


# ---------------------------------------------------------------------------
# generate_post — mocked API
# ---------------------------------------------------------------------------

class TestGeneratePost:
    URL = "https://ksrevenue.gov/report.pdf"

    def _call_with_response(self, text: str):
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _make_text_response(text)
        with patch("extract._client", return_value=mock_client):
            return extract.generate_post(GOOD_FACTS, self.URL, "report.pdf")

    def test_valid_post_returned(self):
        post_text = f'"Total Taxes were $665M in FY2025"\nSource: {self.URL}'
        result = self._call_with_response(post_text)
        assert result == post_text

    def test_missing_source_line_rejected(self):
        post_text = '"Total Taxes were $665M"\nNo source line here'
        result = self._call_with_response(post_text)
        assert result == ""

    def test_missing_quotes_rejected(self):
        post_text = f"Kansas taxes rose in FY2025\nSource: {self.URL}"
        result = self._call_with_response(post_text)
        assert result == ""

    def test_empty_response_rejected(self):
        result = self._call_with_response("")
        assert result == ""

    def test_empty_sentinel_rejected(self):
        result = self._call_with_response("EMPTY")
        assert result == ""

    def test_post_truncated_to_max_chars(self):
        long_text = f'"quote" {"x" * 2000}\nSource: {self.URL}'
        result = self._call_with_response(long_text)
        assert len(result) <= extract.MAX_POST_CHARS

    def test_api_error_returns_empty_string(self):
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = ConnectionError("timeout")
        with patch("extract._client", return_value=mock_client):
            result = extract.generate_post(GOOD_FACTS, self.URL, "report.pdf")
        assert result == ""

    def test_evidence_quotes_included_in_prompt(self):
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _make_text_response(
            f'"Total Taxes were $665M in FY2025"\nSource: {self.URL}'
        )
        with patch("extract._client", return_value=mock_client):
            extract.generate_post(GOOD_FACTS, self.URL, "report.pdf")

        prompt = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "Total Taxes were $665M in FY2025" in prompt
