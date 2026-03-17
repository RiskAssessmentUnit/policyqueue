"""
extract.py — Local LLM fact extraction and post generation via Ollama.

Uses llama3.1:8b-instruct-q4_K_M (or any model configured via PQ_MODEL).
Because the local model has a limited practical context window, large documents
are split into chunks and results are merged with merge_facts().

Public API (unchanged from Claude version):
  extract_facts(text, source_url)  -> dict
  score_facts(facts)               -> int
  generate_post(facts, source_url, pdf_name) -> str
  merge_facts(parts)               -> dict
"""

import json
import logging
import os
import re
import urllib.request
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OLLAMA_BASE     = os.environ.get("PQ_OLLAMA_BASE", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL    = os.environ.get("PQ_MODEL", "llama3.1:8b-instruct-q4_K_M")
OLLAMA_TIMEOUT  = int(os.environ.get("PQ_OLLAMA_TIMEOUT", "900"))

MAX_TOTAL_CHARS     = int(os.environ.get("PQ_MAX_TOTAL_CHARS", "120000"))
MAX_CHARS_PER_CHUNK = int(os.environ.get("PQ_MAX_CHARS_PER_CHUNK", "9000"))
MAX_CHUNKS          = int(os.environ.get("PQ_MAX_CHUNKS", "80"))
MAX_POST_CHARS      = int(os.environ.get("PQ_MAX_POST_CHARS", "900"))

# ---------------------------------------------------------------------------
# Fact schema (prompt version — no tool_use with local models)
# ---------------------------------------------------------------------------

_FACT_SCHEMA = """{
  "program_type": "STAR_BOND" | "REVENUE_REPORT" | "BUDGET" | "AUDIT" | "BILL" | "FISCAL_NOTE" | "OTHER",
  "title": string | null,
  "jurisdiction": "Kansas" | null,
  "locations": [string],
  "entities": [{"name": string, "type": "PERSON"|"ORG"|"GOV_BODY"|"PROJECT"|"OTHER"}],
  "key_numbers": [{"label": string, "value": number, "unit": "USD"|"PERCENT"|"JOBS"|"YEAR"|"OTHER", "year": number|null}],
  "events": [{"date": string|null, "year": number|null, "description": string}],
  "evidence": [{"quote": string, "note": string}],
  "uncertainties": [string],
  "recommended_next_queries": [string]
}"""

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
# Ollama HTTP helper
# ---------------------------------------------------------------------------

def _ollama_generate(prompt: str) -> str:
    """POST to Ollama /api/generate, return the response string."""
    payload = json.dumps({
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{OLLAMA_BASE}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT) as r:
            resp = json.loads(r.read().decode("utf-8", errors="replace"))
            return (resp.get("response") or "").strip()
    except Exception as exc:
        logging.error("Ollama generate error: %s", exc)
        return ""


def _extract_json(raw: str) -> Optional[dict]:
    """Pull the first JSON object out of a raw LLM response string."""
    raw = raw.strip()
    # Strip markdown fences if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
    raw = re.sub(r"\s*```$", "", raw)
    # Find first { ... } block
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def _truncate(text: str) -> str:
    """Cap total chars, preserving head (65%) + tail (35%)."""
    if len(text) <= MAX_TOTAL_CHARS:
        return text
    head = int(MAX_TOTAL_CHARS * 0.65)
    tail = MAX_TOTAL_CHARS - head
    return text[:head] + "\n\n[...TRUNCATED...]\n\n" + text[-tail:]


def _chunk_text(text: str) -> list:
    """Split on paragraph boundaries into chunks <= MAX_CHARS_PER_CHUNK."""
    text = (text or "").strip()
    if len(text) <= MAX_CHARS_PER_CHUNK:
        return [text]
    parts = re.split(r"\n{2,}", text)
    chunks, cur = [], ""
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if len(cur) + len(p) + 2 <= MAX_CHARS_PER_CHUNK:
            cur = (cur + "\n\n" + p).strip()
        else:
            if cur:
                chunks.append(cur)
            cur = p[:MAX_CHARS_PER_CHUNK]
    if cur:
        chunks.append(cur)
    return chunks


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
    """Extract structured facts from document text via local Ollama model.

    Large documents are chunked; results are merged with merge_facts().
    Falls back to EMPTY_FACTS on total failure.
    """
    text = (text or "").strip()
    if not text or text == "[NO_TEXT_EXTRACTED]":
        return dict(EMPTY_FACTS)

    text = _truncate(text)
    chunks = _chunk_text(text)

    if len(chunks) > MAX_CHUNKS:
        keep_head = MAX_CHUNKS // 2
        keep_tail = MAX_CHUNKS - keep_head
        chunks = chunks[:keep_head] + chunks[-keep_tail:]

    url_note = f"\nSource URL: {source_url}" if source_url else ""
    partials = []

    for i, chunk in enumerate(chunks, start=1):
        logging.debug("extract_facts chunk %d/%d chars=%d", i, len(chunks), len(chunk))
        prompt = (
            "You are an information extraction engine.\n"
            "Output MUST be valid JSON only. No markdown fences. No extra text.\n\n"
            "Task: extract Kansas public-policy facts from the TEXT below.\n\n"
            "Rules:\n"
            "- Only extract what is explicitly stated. Do not guess or infer.\n"
            "- Evidence quotes must be 20 words or fewer and verbatim.\n"
            "- Use null for unknown fields. Use numbers for numeric values.\n"
            f"- Focus on Kansas tax revenue, STAR bonds, budgets, and audits.{url_note}\n\n"
            f"Return JSON matching this exact shape:\n{_FACT_SCHEMA}\n\n"
            f"TEXT:\n\"\"\"\n{chunk}\n\"\"\""
        )
        raw = _ollama_generate(prompt)
        obj = _extract_json(raw)
        if obj:
            partials.append(obj)
        else:
            logging.warning("extract_facts: chunk %d/%d failed JSON parse", i, len(chunks))
            partial = dict(EMPTY_FACTS)
            partial["uncertainties"] = ["chunk_json_parse_failed"]
            partials.append(partial)

    if not partials:
        return dict(EMPTY_FACTS)

    merged = merge_facts(partials)

    # Trim and sanitise evidence quotes
    clean_ev = []
    for item in (merged.get("evidence") or [])[:8]:
        if isinstance(item, dict):
            q = " ".join((item.get("quote") or "").split()[:20])
            n = (item.get("note") or "")[:140]
            if q:
                clean_ev.append({"quote": q, "note": n})
    merged["evidence"] = clean_ev

    return merged


def score_facts(f: dict) -> int:
    """Score extracted facts for publishing signal quality (0–10+)."""
    score = 0
    pt = f.get("program_type") or "OTHER"
    if pt in ("STAR_BOND", "REVENUE_REPORT", "BUDGET", "AUDIT", "BILL", "FISCAL_NOTE"):
        score += 3
    if f.get("title"):
        score += 1
    score += min(4, len(f.get("key_numbers") or []))
    score += min(2, len(f.get("events") or []))
    # Bonus for USD figures
    for kn in (f.get("key_numbers") or []):
        if isinstance(kn, dict) and kn.get("unit") == "USD":
            score += 1
            break
    if f.get("evidence"):
        score += 1
    return score


def generate_post(facts: dict, source_url: str, pdf_name: str) -> str:
    """Generate a publish-ready social-media post from extracted facts.

    Returns empty string if the model can't produce a well-formed post.
    """
    ev = facts.get("evidence") or []
    e1 = ev[0].get("quote", "") if len(ev) > 0 and isinstance(ev[0], dict) else ""
    e2 = ev[1].get("quote", "") if len(ev) > 1 and isinstance(ev[1], dict) else ""

    prompt = (
        f"You write short public-policy research posts.\n\n"
        f"Return ONLY the final post text. No markdown fences. "
        f"Keep it under {MAX_POST_CHARS} characters.\n\n"
        "Hard rules:\n"
        "- Must be clearly about Kansas public policy or public finance.\n"
        '- Must include 1–2 verbatim evidence quotes in "double quotes".\n'
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
        f'2) "{e2}"\n\n'
        f"Now write the post.\nSource: {source_url}"
    )

    out = _ollama_generate(prompt).strip()

    if not out or out.upper() == "EMPTY":
        return ""
    if f"Source: {source_url}" not in out:
        return ""
    if '"' not in out and "\u201c" not in out and "\u201d" not in out:
        return ""

    return out[:MAX_POST_CHARS].rstrip()


def merge_facts(parts: list) -> dict:
    """Merge fact dicts from multiple chunks into one deduplicated result."""
    merged = dict(EMPTY_FACTS)
    merged.update({k: [] for k in
                   ("locations", "entities", "key_numbers", "events",
                    "evidence", "uncertainties", "recommended_next_queries")})

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
    return merged
