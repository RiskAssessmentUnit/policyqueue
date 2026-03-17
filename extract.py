"""
extract.py — LLM-backed fact extraction and post generation via Claude API.

Drop-in replacement for the Ollama calls in pq.py and runner_focus_v4.py.
Uses claude-sonnet-4-6 with forced tool_use for guaranteed structured output.
"""

import json
import logging
import os
from typing import Optional

import anthropic

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLAUDE_MODEL    = os.environ.get("PQ_MODEL", "claude-sonnet-4-6")
MAX_TOTAL_CHARS = int(os.environ.get("PQ_MAX_TOTAL_CHARS", "180000"))  # ~45k tokens, well within 200k ctx
MAX_POST_CHARS  = int(os.environ.get("PQ_MAX_POST_CHARS", "900"))

# ---------------------------------------------------------------------------
# Fact extraction tool schema (unified from pq.py + runner_focus_v4.py)
# ---------------------------------------------------------------------------

_FACT_TOOL = {
    "name": "record_facts",
    "description": (
        "Record structured facts extracted from a Kansas public-policy document. "
        "Only include data explicitly supported by the text."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "program_type": {
                "type": "string",
                "enum": ["STAR_BOND", "REVENUE_REPORT", "BUDGET", "AUDIT", "BILL", "FISCAL_NOTE", "OTHER"],
            },
            "title": {"type": ["string", "null"]},
            "jurisdiction": {"type": ["string", "null"]},
            "locations": {
                "type": "array",
                "items": {"type": "string"},
            },
            "entities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "type": {
                            "type": "string",
                            "enum": ["PERSON", "ORG", "GOV_BODY", "PROJECT", "OTHER"],
                        },
                    },
                    "required": ["name", "type"],
                },
            },
            "key_numbers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string"},
                        "value": {"type": "number"},
                        "unit": {
                            "type": "string",
                            "enum": ["USD", "PERCENT", "JOBS", "YEAR", "OTHER"],
                        },
                        "year": {"type": ["integer", "null"]},
                    },
                    "required": ["label", "value", "unit"],
                },
            },
            "events": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "date": {"type": ["string", "null"]},
                        "year": {"type": ["integer", "null"]},
                        "description": {"type": "string"},
                    },
                    "required": ["description"],
                },
            },
            "evidence": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "quote": {
                            "type": "string",
                            "description": "Verbatim quote from the document, 20 words or fewer.",
                        },
                        "note": {"type": "string", "description": "Page or section reference."},
                    },
                    "required": ["quote", "note"],
                },
            },
            "uncertainties": {"type": "array", "items": {"type": "string"}},
            "recommended_next_queries": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "program_type", "title", "jurisdiction", "locations", "entities",
            "key_numbers", "events", "evidence", "uncertainties", "recommended_next_queries",
        ],
    },
}

EMPTY_FACTS: dict = {
    "program_type": "OTHER",
    "title": None,
    "jurisdiction": "Kansas",
    "locations": [],
    "entities": [],
    "key_numbers": [],
    "events": [],
    "evidence": [],
    "uncertainties": [],
    "recommended_next_queries": [],
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _client() -> anthropic.Anthropic:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY environment variable is not set. "
            "Get a key at https://console.anthropic.com/."
        )
    return anthropic.Anthropic(api_key=key)


def _truncate(text: str) -> str:
    """Keep head (65%) + tail (35%) so both the document header and closing
    summary tables are preserved when the text exceeds the safe limit."""
    if len(text) <= MAX_TOTAL_CHARS:
        return text
    head = int(MAX_TOTAL_CHARS * 0.65)
    tail = MAX_TOTAL_CHARS - head
    return text[:head] + "\n\n[...TRUNCATED...]\n\n" + text[-tail:]


def _to_text(x) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return str(x)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_facts(text: str, source_url: str = "") -> dict:
    """Extract structured facts from document text via Claude tool use.

    Claude's 200k context window handles full documents without chunking.
    Falls back to EMPTY_FACTS on any API or parsing error.
    """
    text = (text or "").strip()
    if not text or text == "[NO_TEXT_EXTRACTED]":
        return dict(EMPTY_FACTS)

    text = _truncate(text)

    url_note = f"\nSource URL: {source_url}" if source_url else ""
    user_msg = (
        "Extract Kansas public-policy facts from the document text below.\n"
        "Rules:\n"
        "- Only extract what is explicitly stated. Do not guess or infer.\n"
        "- Evidence quotes must be 20 words or fewer and verbatim.\n"
        "- Focus on Kansas tax revenue, STAR bonds, budgets, and audits."
        f"{url_note}\n\n"
        f"DOCUMENT TEXT:\n\"\"\"\n{text}\n\"\"\""
    )

    try:
        response = _client().messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,
            tools=[_FACT_TOOL],
            tool_choice={"type": "tool", "name": "record_facts"},
            messages=[{"role": "user", "content": user_msg}],
        )
    except Exception as exc:
        logging.error("extract_facts API error: %s", exc)
        return dict(EMPTY_FACTS)

    tool_use = next((b for b in response.content if b.type == "tool_use"), None)
    if tool_use is None:
        logging.warning("extract_facts: no tool_use block in response")
        return dict(EMPTY_FACTS)

    facts = dict(tool_use.input)
    # Cap evidence list (consistent with runner_focus_v4.py)
    if "evidence" in facts:
        facts["evidence"] = facts["evidence"][:8]
    return facts


def score_facts(f: dict) -> int:
    """Score extracted facts for publishing signal quality (0–10+)."""
    score = 0
    if f.get("program_type") in ("STAR_BOND", "REVENUE_REPORT", "BUDGET", "AUDIT"):
        score += 3
    if f.get("title"):
        score += 1
    score += min(4, len(f.get("key_numbers") or []))
    score += min(2, len(f.get("events") or []))
    if f.get("evidence"):
        score += 1
    return score


def generate_post(facts: dict, source_url: str, pdf_name: str) -> str:
    """Generate a publish-ready social-media post from extracted facts.

    Returns an empty string when facts lack sufficient evidence or Claude
    cannot produce a well-formed post (missing source line or evidence quotes).
    """
    ev = facts.get("evidence") or []
    e1 = ev[0].get("quote", "") if len(ev) > 0 and isinstance(ev[0], dict) else ""
    e2 = ev[1].get("quote", "") if len(ev) > 1 and isinstance(ev[1], dict) else ""

    user_msg = (
        f"Write a factual social-media post (under {MAX_POST_CHARS} characters) about Kansas "
        "tax revenue or STAR bonds based on the data below.\n\n"
        "Hard rules:\n"
        '- Include 1–2 verbatim evidence quotes enclosed in "double quotes".\n'
        f'- The last line must be exactly: Source: {source_url}\n'
        "- Neutral tone. No speculation. No accusations.\n"
        "- If there is not enough evidence, output only the single word: EMPTY\n\n"
        f"Title: {facts.get('title') or pdf_name}\n"
        f"Type: {facts.get('program_type')}\n\n"
        "Key numbers:\n"
        f"{json.dumps(facts.get('key_numbers') or [], ensure_ascii=False)}\n\n"
        "Events:\n"
        f"{json.dumps(facts.get('events') or [], ensure_ascii=False)}\n\n"
        "Allowed evidence quotes:\n"
        f'1) "{e1}"\n'
        f'2) "{e2}"\n'
    )

    try:
        response = _client().messages.create(
            model=CLAUDE_MODEL,
            max_tokens=512,
            messages=[{"role": "user", "content": user_msg}],
        )
    except Exception as exc:
        logging.error("generate_post API error: %s", exc)
        return ""

    out = (response.content[0].text if response.content else "").strip()

    if not out or out.upper() == "EMPTY":
        return ""
    if f"Source: {source_url}" not in out:
        return ""
    if '"' not in out:
        return ""

    return out[:MAX_POST_CHARS].rstrip()


def merge_facts(parts: list) -> dict:
    """Merge fact dicts from multiple text chunks into one deduplicated result.

    Still needed by pq.py when processing very large documents chunk-by-chunk.
    """
    merged = {k: (list(v) if isinstance(v, list) else v) for k, v in EMPTY_FACTS.items()}
    merged["locations"] = []
    merged["entities"] = []
    merged["key_numbers"] = []
    merged["events"] = []
    merged["evidence"] = []
    merged["uncertainties"] = []
    merged["recommended_next_queries"] = []

    # program_type: highest-specificity wins
    pref = {
        "STAR_BOND": 6, "REVENUE_REPORT": 5, "BUDGET": 4,
        "AUDIT": 4, "BILL": 3, "FISCAL_NOTE": 3, "OTHER": 1,
    }
    best = 0
    for p in parts:
        pt = p.get("program_type", "OTHER")
        s = pref.get(pt, 1)
        if s > best:
            best = s
            merged["program_type"] = pt

    for p in parts:
        if not merged["title"] and p.get("title"):
            merged["title"] = p["title"]
        if p.get("jurisdiction") == "Kansas":
            merged["jurisdiction"] = "Kansas"

    def _dedup(field: str, key_fn):
        out, seen = [], set()
        for p in parts:
            for item in (p.get(field) or []):
                k = key_fn(item)
                if k in seen:
                    continue
                seen.add(k)
                out.append(item)
        merged[field] = out

    _dedup("locations",   lambda x: _to_text(x).strip().lower())
    _dedup("entities",    lambda x: (_to_text(x.get("name")).strip().lower(),
                                     _to_text(x.get("type")).strip().lower()))
    _dedup("key_numbers", lambda x: (_to_text(x.get("label")).strip().lower(),
                                     float(x.get("value", 0)),
                                     _to_text(x.get("unit")).strip().lower(),
                                     x.get("year") or 0))
    _dedup("events",      lambda x: (_to_text(x.get("description")).strip().lower(),
                                     _to_text(x.get("date") or "").strip()))
    _dedup("evidence",    lambda x: _to_text(x.get("quote")).strip().lower())

    def _str_list(field: str):
        out, seen = [], set()
        for p in parts:
            for x in (p.get(field) or []):
                s = _to_text(x).strip()
                k = s.lower()
                if s and k not in seen:
                    seen.add(k)
                    out.append(s)
        merged[field] = out

    _str_list("uncertainties")
    _str_list("recommended_next_queries")
    merged["evidence"] = merged["evidence"][:8]
    return merged
