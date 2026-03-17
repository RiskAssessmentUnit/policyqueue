import re
import time
from pathlib import Path

ROOT = Path.home() / "policyqueue"
RUNNER = ROOT / "runner.py"

NEW_FUNC = r'''
def extract_facts_fulltext(text: str, max_chars_per_chunk: int = 9000) -> dict:
    """
    V2 Summarizer:
      - Extracts structured policy intel + impact scoring + watchlist hits + money moves
      - Produces multiple scored post candidates (publish selectively)
    Output is merged JSON across chunks.

    NOTE: This keeps the old function name so the rest of runner.py doesn't need rewiring.
    """

    ANALYSIS_SCHEMA_V2 = r"""
{
  "doc": {
    "doc_type": "BILL|FISCAL_NOTE|AUDIT|BUDGET|STAR_BOND|MAP|JOURNAL|NEWS|OTHER",
    "title": "string|null",
    "jurisdiction": "Kansas",
    "agency_or_body": "string|null",
    "date": "YYYY-MM-DD|null",
    "source_hint": "string|null",
    "topic_tags": ["string", "..."]
  },
  "impact": {
    "public_impact_score": 0,
    "why_it_matters": "string|null",
    "who_is_affected": ["string", "..."],
    "actionability": "ACT_NOW|WATCH|FYI|IGNORE",
    "confidence": 0
  },
  "watch": {
    "watch_hits": [
      {
        "watch_item": "string",
        "evidence_quote": "string"
      }
    ]
  },
  "money": {
    "money_moves": [
      {
        "program": "string|null",
        "amount": "number|null",
        "amount_text": "string|null",
        "from_fund": "string|null",
        "to_fund": "string|null",
        "timeframe": "string|null",
        "evidence_quote": "string|null"
      }
    ]
  },
  "entities": {
    "people": ["string", "..."],
    "orgs": ["string", "..."],
    "places": ["string", "..."],
    "bills": ["string", "..."]
  },
  "evidence": [
    { "quote": "string", "why": "string" }
  ],
  "uncertainties": ["string", "..."],
  "recommended_next_queries": ["string", "..."],
  "post_candidates": [
    {
      "post_type": "WATCHDOG_HEADLINE|FISCAL_ALERT|QUIET_LINE_ITEM|BILL_CONSEQUENCES|STAR_BOND_WARNING|AUDIT_FINDING|FYI_SUMMARY",
      "score": 0,
      "headline": "string|null",
      "bullets": ["string", "..."],
      "cta": "string|null",
      "reason": "string|null"
    }
  ]
}
"""

    def _safe_list(x):
        return x if isinstance(x, list) else []

    def _dedupe_list(items):
        seen = set()
        out = []
        for it in items:
            key = None
            try:
                if isinstance(it, dict):
                    key = repr(sorted(it.items()))
                else:
                    key = str(it)
            except Exception:
                key = str(it)
            if key not in seen:
                seen.add(key)
                out.append(it)
        return out

    def _clamp(n, lo, hi):
        try:
            n = int(n)
        except Exception:
            n = lo
        return max(lo, min(hi, n))

    def _mk_candidates(merged: dict) -> list:
        """
        Deterministic candidate generation from merged fields.
        Model can also populate post_candidates, but we normalize and add "obvious" ones.
        """
        doc = (merged.get("doc") or {})
        impact = (merged.get("impact") or {})
        watch = (merged.get("watch") or {})
        money = (merged.get("money") or {})

        doc_type = (doc.get("doc_type") or "OTHER").upper()
        title = doc.get("title") or None
        tags = _safe_list(doc.get("topic_tags"))
        score = _clamp(impact.get("public_impact_score", 0), 0, 10)
        actionability = (impact.get("actionability") or "FYI").upper()

        watch_hits = _safe_list(watch.get("watch_hits"))
        money_moves = _safe_list(money.get("money_moves"))

        # Base candidates from model (if any)
        model_cands = []
        for c in _safe_list(merged.get("post_candidates")):
            if not isinstance(c, dict):
                continue
            c2 = dict(c)
            c2["score"] = _clamp(c2.get("score", 0), 0, 10)
            c2["post_type"] = (c2.get("post_type") or "FYI_SUMMARY").upper()
            model_cands.append(c2)

        # Add deterministic candidates
        det = []

        def add(pt, sc, headline, bullets, cta, reason):
            det.append({
                "post_type": pt,
                "score": _clamp(sc, 0, 10),
                "headline": headline,
                "bullets": bullets[:5],
                "cta": cta,
                "reason": reason
            })

        # Money move → quiet line item / fiscal alert
        if money_moves:
            amt_texts = []
            for m in money_moves[:3]:
                if isinstance(m, dict):
                    amt_texts.append(m.get("amount_text") or (str(m.get("amount")) if m.get("amount") is not None else None))
            amt_texts = [a for a in amt_texts if a]
            headline = (f"Quiet money move found in Kansas doc" + (f": {title}" if title else "")) if score >= 4 else None
            bullets = []
            for m in money_moves[:3]:
                if not isinstance(m, dict):
                    continue
                b = []
                if m.get("program"): b.append(m["program"])
                if m.get("amount_text"): b.append(m["amount_text"])
                if m.get("from_fund") or m.get("to_fund"):
                    b.append(f'{m.get("from_fund") or "?"} → {m.get("to_fund") or "?"}')
                if m.get("timeframe"): b.append(m["timeframe"])
                if b:
                    bullets.append(" • ".join(b))
            base = max(score, 5)
            add("QUIET_LINE_ITEM", base, headline, bullets or ["Money movement detected; review details."],
                "Want more like this? Follow for daily Kansas budget signals.",
                "Money movement extracted from document.")
            if base >= 6:
                add("FISCAL_ALERT", base, f"Fiscal alert: Kansas funds shifting" + (f" — {title}" if title else ""),
                    bullets or ["Potentially meaningful fund shift detected."],
                    "Check the linked source doc; this may affect budgeting/taxes.",
                    "High-scoring money move candidate.")

        # Watch hits → watchdog headline
        if watch_hits:
            wh = watch_hits[0] if isinstance(watch_hits[0], dict) else {"watch_item":"watchlist hit"}
            item = wh.get("watch_item") or "watchlist hit"
            base = max(score, 4)
            add("WATCHDOG_HEADLINE", base,
                f"Kansas watchlist hit: {item}" + (f" — {title}" if title else ""),
                [f'Watch trigger: {item}'] + ([f'Tag(s): {", ".join(tags[:3])}'] if tags else []),
                "If you care about Kansas policy money moves, keep this on your radar.",
                "Watchlist trigger(s) detected.")

        # Doc-type specializations
        if doc_type == "AUDIT":
            base = max(score, 5)
            add("AUDIT_FINDING", base,
                "Audit finding worth flagging" + (f": {title}" if title else ""),
                ["Audit document detected. Key finding(s) extracted; see bullets above if present."] if not model_cands else [],
                "Audits often hide accountability issues—worth reviewing.",
                "Doc type AUDIT.")
        if doc_type == "STAR_BOND":
            base = max(score, 6)
            add("STAR_BOND_WARNING", base,
                "STAR bond signal detected" + (f": {title}" if title else ""),
                ["STAR bond document detected; potential development financing impact."] if not model_cands else [],
                "STAR bonds can shift public risk—review details.",
                "Doc type STAR_BOND.")
        if doc_type == "BILL":
            base = max(score, 4)
            add("BILL_CONSEQUENCES", base,
                "Bill consequences snapshot" + (f": {title}" if title else ""),
                ["Bill detected; consequences/impact summarized."] if not model_cands else [],
                "Bills change policy quietly—this is your quick read.",
                "Doc type BILL.")
        if doc_type in ("BUDGET", "FISCAL_NOTE"):
            base = max(score, 5)
            add("FISCAL_ALERT", base,
                "Kansas fiscal signal" + (f": {title}" if title else ""),
                ["Budget/fiscal note detected; key numbers summarized."] if not model_cands else [],
                "Budget changes can ripple—save/share if relevant.",
                "Doc type BUDGET/FISCAL_NOTE.")

        # Fallback FYI
        if score >= 3 and not (watch_hits or money_moves):
            add("FYI_SUMMARY", score,
                "Kansas policy FYI" + (f": {title}" if title else ""),
                ["Summary extracted; lower confidence/impact than alerts."],
                None,
                "Generic FYI candidate.")

        # Combine + pick best per type
        all_cands = model_cands + det
        # normalize
        norm = []
        for c in all_cands:
            if not isinstance(c, dict):
                continue
            pt = (c.get("post_type") or "FYI_SUMMARY").upper()
            sc = _clamp(c.get("score", 0), 0, 10)
            norm.append({
                "post_type": pt,
                "score": sc,
                "headline": c.get("headline") or None,
                "bullets": _safe_list(c.get("bullets"))[:7],
                "cta": c.get("cta") or None,
                "reason": c.get("reason") or None
            })

        # Best-only per post_type
        best_by = {}
        for c in norm:
            pt = c["post_type"]
            if pt not in best_by or c["score"] > best_by[pt]["score"]:
                best_by[pt] = c
        out = list(best_by.values())
        out.sort(key=lambda x: x.get("score", 0), reverse=True)
        return out

    # ---------- Chunked extraction ----------
    chunks = chunk_text(text, max_chars=max_chars_per_chunk)
    log(f"CHUNK total_chars={len(text)} chunks={len(chunks)}")

    partials = []
    for i, ch in enumerate(chunks, start=1):
        log(f"Ollama chunk {i}/{len(chunks)} start chars={len(ch)}")

        prompt = f'''You are an information extraction engine.
Output MUST be valid JSON only. No markdown. No extra text.

Task: Extract Kansas public-policy / public-finance intelligence from the TEXT.

Rules:
- If unknown, use null.
- Use numbers for numeric fields when possible.
- Do not guess; only extract what is in the TEXT.
- Keep evidence quotes short (<= 20 words) and verbatim.
- public_impact_score is 0-10 (integer). confidence is 0-10 (integer).
- actionability must be one of: ACT_NOW, WATCH, FYI, IGNORE.
- watch_hits: include high-signal triggers like appropriations, SGF, mill levy, bond issuance, Medicaid rates, school finance, taxes, audits, procurement, etc.
- money_moves: include any dollar amounts, fund transfers, appropriations, receipts, obligations, bond amounts.
- post_candidates: propose 1-4 potential posts with scores 0-10. Keep headlines short.

SCHEMA:
{ANALYSIS_SCHEMA_V2}

TEXT:
\"\"\"{ch}\"\"\"
'''
        raw = ollama_generate(prompt)
        try:
            partials.append(json.loads(raw))
            log(f"Ollama chunk {i}/{len(chunks)} done bytes={len(raw.encode('utf-8', errors='ignore'))}")
        except Exception:
            partials.append({
                "doc": {"doc_type":"OTHER","title":None,"jurisdiction":"Kansas","agency_or_body":None,"date":None,"source_hint":None,"topic_tags":[]},
                "impact": {"public_impact_score":0,"why_it_matters":None,"who_is_affected":[],"actionability":"IGNORE","confidence":0},
                "watch": {"watch_hits":[]},
                "money": {"money_moves":[]},
                "entities": {"people":[],"orgs":[],"places":[],"bills":[]},
                "evidence": [],
                "uncertainties": ["chunk_json_parse_failed"],
                "recommended_next_queries": [],
                "post_candidates": []
            })

    # ---------- Merge ----------
    merged = {
        "doc": {"doc_type":"OTHER","title":None,"jurisdiction":"Kansas","agency_or_body":None,"date":None,"source_hint":None,"topic_tags":[]},
        "impact": {"public_impact_score":0,"why_it_matters":None,"who_is_affected":[],"actionability":"FYI","confidence":0},
        "watch": {"watch_hits":[]},
        "money": {"money_moves":[]},
        "entities": {"people":[],"orgs":[],"places":[],"bills":[]},
        "evidence": [],
        "uncertainties": [],
        "recommended_next_queries": [],
        "post_candidates": []
    }

    # Prefer doc_type by priority
    pref = {"STAR_BOND":7,"BUDGET":6,"FISCAL_NOTE":5,"BILL":4,"AUDIT":3,"NEWS":2,"JOURNAL":1,"MAP":0,"OTHER":0}
    best_doc_type = merged["doc"]["doc_type"]

    best_title = None
    best_title_len = 0
    best_impact = 0
    best_conf = 0

    for p in partials:
        if not isinstance(p, dict):
            continue

        # doc merge
        doc = p.get("doc") if isinstance(p.get("doc"), dict) else {}
        dt = (doc.get("doc_type") or "OTHER").upper()
        if pref.get(dt, 0) > pref.get(best_doc_type, 0):
            best_doc_type = dt

        title = doc.get("title")
        if isinstance(title, str) and len(title.strip()) > best_title_len:
            best_title = title.strip()
            best_title_len = len(best_title)

        # keep first non-null agency/date/source_hint if present
        for k in ("agency_or_body","date","source_hint"):
            if merged["doc"].get(k) is None and doc.get(k) not in (None, "", []):
                merged["doc"][k] = doc.get(k)

        merged["doc"]["topic_tags"] += _safe_list(doc.get("topic_tags"))

        # impact merge
        imp = p.get("impact") if isinstance(p.get("impact"), dict) else {}
        sc = _clamp(imp.get("public_impact_score", 0), 0, 10)
        cf = _clamp(imp.get("confidence", 0), 0, 10)
        if sc > best_impact:
            best_impact = sc
            # take best why/affected/actionability from best score
            merged["impact"]["why_it_matters"] = imp.get("why_it_matters") or merged["impact"]["why_it_matters"]
            merged["impact"]["actionability"] = (imp.get("actionability") or merged["impact"]["actionability"]).upper()
            merged["impact"]["who_is_affected"] += _safe_list(imp.get("who_is_affected"))
        if cf > best_conf:
            best_conf = cf

        # watch/money/evidence
        watch = p.get("watch") if isinstance(p.get("watch"), dict) else {}
        merged["watch"]["watch_hits"] += _safe_list(watch.get("watch_hits"))

        money = p.get("money") if isinstance(p.get("money"), dict) else {}
        merged["money"]["money_moves"] += _safe_list(money.get("money_moves"))

        merged["evidence"] += _safe_list(p.get("evidence"))
        merged["uncertainties"] += _safe_list(p.get("uncertainties"))
        merged["recommended_next_queries"] += _safe_list(p.get("recommended_next_queries"))

        ent = p.get("entities") if isinstance(p.get("entities"), dict) else {}
        merged["entities"]["people"] += _safe_list(ent.get("people"))
        merged["entities"]["orgs"] += _safe_list(ent.get("orgs"))
        merged["entities"]["places"] += _safe_list(ent.get("places"))
        merged["entities"]["bills"] += _safe_list(ent.get("bills"))

        merged["post_candidates"] += _safe_list(p.get("post_candidates"))

    merged["doc"]["doc_type"] = best_doc_type
    if best_title:
        merged["doc"]["title"] = best_title

    merged["impact"]["public_impact_score"] = best_impact
    merged["impact"]["confidence"] = best_conf

    # normalize / dedupe
    merged["doc"]["topic_tags"] = _dedupe_list([t for t in merged["doc"]["topic_tags"] if isinstance(t, str) and t.strip()])
    merged["impact"]["who_is_affected"] = _dedupe_list([w for w in merged["impact"]["who_is_affected"] if isinstance(w, str) and w.strip()])
    merged["watch"]["watch_hits"] = _dedupe_list([x for x in merged["watch"]["watch_hits"] if isinstance(x, dict)])
    merged["money"]["money_moves"] = _dedupe_list([x for x in merged["money"]["money_moves"] if isinstance(x, dict)])
    merged["evidence"] = _dedupe_list([x for x in merged["evidence"] if isinstance(x, dict)])
    merged["uncertainties"] = _dedupe_list([x for x in merged["uncertainties"] if isinstance(x, str) and x.strip()])
    merged["recommended_next_queries"] = _dedupe_list([x for x in merged["recommended_next_queries"] if isinstance(x, str) and x.strip()])
    for k in ("people","orgs","places","bills"):
        merged["entities"][k] = _dedupe_list([x for x in merged["entities"][k] if isinstance(x, str) and x.strip()])

    # regenerate candidates deterministically + keep best model candidates
    merged["post_candidates"] = _mk_candidates(merged)

    # convenience: best_post
    merged["best_post"] = merged["post_candidates"][0] if merged["post_candidates"] else None

    return merged
'''

def main():
    if not RUNNER.exists():
        raise SystemExit(f"ERROR: runner.py not found at {RUNNER}")

    src = RUNNER.read_text(encoding="utf-8", errors="ignore")

    m = re.search(r'(?m)^\s*def\s+extract_facts_fulltext\s*\(', src)
    if not m:
        raise SystemExit("ERROR: Could not find def extract_facts_fulltext(...) in runner.py")

    start = m.start()

    # find end: next top-level def/class after this function
    tail = src[m.start():]
    m2 = re.search(r'(?m)^\s*(def|class)\s+\w+\s*\(', tail)
    if not m2:
        raise SystemExit("ERROR: Could not locate end of extract_facts_fulltext function region")

    # The first match is extract_facts_fulltext itself; we need the second one.
    # So search again after the first match.
    m_first = re.search(r'(?m)^\s*def\s+extract_facts_fulltext\s*\(.*$', tail)
    if not m_first:
        raise SystemExit("ERROR: Unexpected: cannot re-find extract_facts_fulltext line")

    after_first = tail[m_first.end():]
    m_next = re.search(r'(?m)^\s*(def|class)\s+\w+\s*\(', after_first)
    if not m_next:
        raise SystemExit("ERROR: Could not find the next def/class after extract_facts_fulltext")

    end = m.start() + m_first.end() + m_next.start()

    before = src[:start]
    after = src[end:]

    # Ensure runner.py already has imports used by this function (json/log/chunk_text/ollama_generate).
    # We don't add imports here because your runner.py already uses them (or it wouldn't have worked before).
    patched = before + NEW_FUNC + "\n" + after

    bak = RUNNER.with_suffix(".py.bak_summarizer_v2_" + time.strftime("%Y%m%d_%H%M%S"))
    bak.write_text(src, encoding="utf-8")
    RUNNER.write_text(patched, encoding="utf-8")

    print(f"OK: Patched {RUNNER}")
    print(f"OK: Backup  {bak}")
    print("Next: restart runner via pqctl.ps1 (restart runner)")

if __name__ == "__main__":
    main()
