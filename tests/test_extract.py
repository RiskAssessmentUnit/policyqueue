"""
tests/test_extract.py — Unit tests for extract.py

Tests cover:
  - score_facts()     — pure scoring logic, no API
  - merge_facts()     — pure dedup/merge logic, no API
  - _truncate()       — pure text truncation
  - extract_facts()   — mocked Ollama (_ollama_generate)
  - generate_post()   — mocked Ollama (_ollama_generate)
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import extract


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

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
        # Non-USD units: no USD bonus, so max from key_numbers alone is 4
        f = {
            **extract.EMPTY_FACTS,
            "program_type": "OTHER",
            "key_numbers": [{"label": f"k{i}", "value": i, "unit": "PERCENT"} for i in range(10)],
        }
        assert extract.score_facts(f) == 4

    def test_usd_key_number_adds_bonus(self):
        f = {
            **extract.EMPTY_FACTS,
            "program_type": "OTHER",
            "key_numbers": [{"label": "Revenue", "value": 1000000, "unit": "USD"}],
        }
        # 1 (key_numbers) + 1 (USD bonus) = 2
        assert extract.score_facts(f) == 2

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
        # 3 (REVENUE_REPORT) + 1 (title) + 4 (4 key_numbers) + 1 (USD bonus)
        # + 2 (2 events) + 1 (evidence) = 12
        assert score == 12

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

    def test_evidence_merged_without_cap(self):
        # merge_facts itself doesn't cap; capping happens in extract_facts()
        ev = [{"quote": f"quote {i}", "note": "p1"} for i in range(12)]
        parts = [{**extract.EMPTY_FACTS, "evidence": ev}]
        result = extract.merge_facts(parts)
        assert len(result["evidence"]) == 12

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
# extract_facts — mocked _ollama_generate
# ---------------------------------------------------------------------------

class TestExtractFacts:
    def test_empty_text_returns_empty_facts(self):
        result = extract.extract_facts("")
        assert result == dict(extract.EMPTY_FACTS)

    def test_no_text_sentinel_returns_empty_facts(self):
        result = extract.extract_facts("[NO_TEXT_EXTRACTED]")
        assert result == dict(extract.EMPTY_FACTS)

    def test_successful_extraction(self):
        with patch("extract._ollama_generate", return_value=json.dumps(GOOD_FACTS)):
            result = extract.extract_facts("Some Kansas revenue document text")
        assert result["program_type"] == "REVENUE_REPORT"
        assert result["title"] == "FY2025 State General Fund Receipts"

    def test_ollama_error_returns_empty_like_facts(self):
        # Empty response → JSON parse fails → EMPTY_FACTS structure with uncertainty note
        with patch("extract._ollama_generate", return_value=""):
            result = extract.extract_facts("some text")
        assert result["program_type"] == "OTHER"
        assert result["key_numbers"] == []
        assert result["evidence"] == []
        assert "chunk_json_parse_failed" in result["uncertainties"]

    def test_invalid_json_returns_empty_like_facts(self):
        with patch("extract._ollama_generate", return_value="not json at all"):
            result = extract.extract_facts("some text")
        assert result["program_type"] == "OTHER"
        assert result["key_numbers"] == []
        assert "chunk_json_parse_failed" in result["uncertainties"]

    def test_evidence_capped_at_eight(self):
        facts_with_lots_of_evidence = {
            **GOOD_FACTS,
            "evidence": [{"quote": f"quote {i} words here", "note": "p1"} for i in range(12)],
        }
        with patch("extract._ollama_generate", return_value=json.dumps(facts_with_lots_of_evidence)):
            result = extract.extract_facts("text")
        assert len(result["evidence"]) == 8

    def test_source_url_included_in_prompt(self):
        captured = []
        def fake_generate(prompt):
            captured.append(prompt)
            return json.dumps(dict(extract.EMPTY_FACTS))

        with patch("extract._ollama_generate", side_effect=fake_generate):
            extract.extract_facts("text", source_url="https://ksrevenue.gov/report.pdf")

        assert any("https://ksrevenue.gov/report.pdf" in p for p in captured)

    def test_markdown_fenced_json_parsed(self):
        fenced = "```json\n" + json.dumps(GOOD_FACTS) + "\n```"
        with patch("extract._ollama_generate", return_value=fenced):
            result = extract.extract_facts("text")
        assert result["program_type"] == "REVENUE_REPORT"

    def test_multi_chunk_results_merged(self):
        chunk1 = {**extract.EMPTY_FACTS, "program_type": "REVENUE_REPORT",
                  "key_numbers": [{"label": "Sales Tax", "value": 100, "unit": "USD", "year": 2025}]}
        chunk2 = {**extract.EMPTY_FACTS, "program_type": "OTHER",
                  "key_numbers": [{"label": "Income Tax", "value": 200, "unit": "USD", "year": 2025}]}
        responses = [json.dumps(chunk1), json.dumps(chunk2)]

        original = extract.MAX_CHARS_PER_CHUNK
        extract.MAX_CHARS_PER_CHUNK = 5  # force two chunks
        try:
            with patch("extract._ollama_generate", side_effect=responses):
                result = extract.extract_facts("chunk one\n\nchunk two here")
        finally:
            extract.MAX_CHARS_PER_CHUNK = original

        assert result["program_type"] == "REVENUE_REPORT"  # higher pref wins
        assert len(result["key_numbers"]) == 2


# ---------------------------------------------------------------------------
# generate_post — mocked _ollama_generate
# ---------------------------------------------------------------------------

class TestGeneratePost:
    URL = "https://ksrevenue.gov/report.pdf"

    def _call_with_response(self, text: str):
        with patch("extract._ollama_generate", return_value=text):
            return extract.generate_post(GOOD_FACTS, self.URL, "report.pdf")

    def test_valid_post_returned(self):
        post_text = f'"Total Taxes were $665M in FY2025"\nSource: {self.URL}'
        result = self._call_with_response(post_text)
        assert result == post_text

    def test_missing_source_line_rejected(self):
        result = self._call_with_response('"Total Taxes were $665M"\nNo source line here')
        assert result == ""

    def test_missing_quotes_rejected(self):
        result = self._call_with_response(f"Kansas taxes rose in FY2025\nSource: {self.URL}")
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

    def test_ollama_error_returns_empty_string(self):
        # _ollama_generate swallows errors and returns ""; simulate that
        with patch("extract._ollama_generate", return_value=""):
            result = extract.generate_post(GOOD_FACTS, self.URL, "report.pdf")
        assert result == ""

    def test_evidence_quotes_included_in_prompt(self):
        captured = []
        def fake_generate(prompt):
            captured.append(prompt)
            return f'"Total Taxes were $665M in FY2025"\nSource: {self.URL}'

        with patch("extract._ollama_generate", side_effect=fake_generate):
            extract.generate_post(GOOD_FACTS, self.URL, "report.pdf")

        assert any("Total Taxes were $665M in FY2025" in p for p in captured)
