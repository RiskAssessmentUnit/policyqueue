import os
import os, sys, re, json, time, shutil, hashlib, random
from pathlib import Path
from urllib.parse import urljoin, urlparse
import urllib.request
import urllib.error

# ============================================================
# PolicyQueue ULTIMATE runner (single loop, low intervention)
# ============================================================

# -------------------------
# Config (env overridable)
# -------------------------
ROOT    = Path.home() / "policyqueue"
INBOX   = ROOT / "inbox"
BIGBOX  = ROOT / "big"
FACTS   = ROOT / "facts"
POSTS   = ROOT / "posts"
QUEUE   = ROOT / "queue"        # NEW: staged posts waiting for approver
JUNK    = ROOT / "junk"
ARCHIVE = ROOT / "archive"
LOGS    = ROOT / "logs"
STATE   = ROOT / "state"

RUNLOG        = LOGS / "runner.log"
SEEN_URLS     = STATE / "seen_urls.json"
SEEN_PDFS     = STATE / "seen_pdfs.json"        # url -> saved_name
SEEN_HASHES   = STATE / "seen_hashes.json"      # sha256(pdf bytes) -> saved_name
SEEN_POSTHASH = STATE / "seen_posthash.json"    # sha256(post text) -> queued filename

OLLAMA_BASE = os.environ.get("PQ_OLLAMA_BASE", "http://127.0.0.1:11434").rstrip("/")
MODEL       = os.environ.get("PQ_MODEL", "llama3.1:8b-instruct-q4_K_M")

INTERVAL_SEC          = int(os.environ.get("PQ_INTERVAL_SEC", "300"))
CRAWL_PAGES_PER_CYCLE = int(os.environ.get("PQ_CRAWL_PAGES_PER_CYCLE", "200"))
PROCESS_PER_CYCLE     = int(os.environ.get("PQ_PROCESS_PER_CYCLE", "25"))
BIG_PER_CYCLE         = int(os.environ.get("PQ_BIG_PER_CYCLE", "1"))

# Posting quality knobs
POST_SCORE_MIN        = int(os.environ.get("PQ_POST_SCORE_MIN", "7"))  # default higher quality
REQUIRE_EVIDENCE      = os.environ.get("PQ_REQUIRE_EVIDENCE", "1").strip() != "0"
MAX_POST_CHARS        = int(os.environ.get("PQ_MAX_POST_CHARS", "900"))

# PDF caps to prevent chunk disasters
MAX_TOTAL_CHARS = int(os.environ.get("PQ_MAX_TOTAL_CHARS", "350000"))
MAX_CHUNKS      = int(os.environ.get("PQ_MAX_CHUNKS", "80"))
MAX_CHARS_PER_CHUNK = int(os.environ.get("PQ_MAX_CHARS_PER_CHUNK", "9000"))

# Triage
BIG_BYTES       = int(os.environ.get("PQ_BIG_BYTES", str(8_000_000)))    # 8MB -> big lane

# HTTP
HTTP_TIMEOUT = int(os.environ.get("PQ_HTTP_TIMEOUT", "25"))
UA           = os.environ.get("PQ_UA", "Mozilla/5.0 (compatible; PolicyQueueRunner/2.0)")
MAX_RETRIES  = int(os.environ.get("PQ_MAX_RETRIES", "3"))
BACKOFF_BASE = float(os.environ.get("PQ_BACKOFF_BASE", "1.6"))

SKIP_EXT = set(x.strip().lower() for x in os.environ.get(
    "PQ_SKIP_EXT",
    ".jpg,.jpeg,.png,.gif,.webp,.svg,.css,.js,.ico,.zip,.mp4,.mp3,.wav,.mov"
).split(",") if x.strip())

DOMAINS = [d.strip().lower() for d in os.environ.get(
    "PQ_DOMAINS",
    "kslegislature.org,budget.kansas.gov,postaudit.ks.gov,ksrevisor.org"
).split(",") if d.strip()]

SEEDS = [s.strip() for s in os.environ.get(
    "PQ_SEEDS",
    "https://postaudit.ks.gov/,https://budget.kansas.gov/,https://kslegislature.org/"
).split(",") if s.strip()]

DOMAIN_COOLDOWN_SEC = int(os.environ.get("PQ_DOMAIN_COOLDOWN_SEC", "180"))
domain_cooldown_until = {}  # domain -> epoch

# -------------------------
# Utilities
# -------------------------
def now_ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str):
    LOGS.mkdir(parents=True, exist_ok=True)
    RUNLOG.open("a", encoding="utf-8").write(f"{now_ts()}  {msg}\n")

def ensure_dirs():
    for p in (ROOT, INBOX, BIGBOX, FACTS, POSTS, QUEUE, JUNK, ARCHIVE, LOGS, STATE):
        p.mkdir(parents=True, exist_ok=True)

def load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default

def save_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def sha256_text(s: str) -> str:
    return sha256_bytes((s or "").encode("utf-8", errors="ignore"))

def clean_filename(name: str) -> str:
    name = re.sub(r"[^\w\-.]+", "_", (name or "").strip())
    name = re.sub(r"_+", "_", name)
    return name[:160] if len(name) > 160 else name

def url_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def allowed_url(url: str) -> bool:
    d = url_domain(url)
    if not d:
        return False
    return any(d == dom or d.endswith("." + dom) for dom in DOMAINS)

def ext_of_url(url: str) -> str:
    path = urlparse(url).path
    m = re.search(r"\.([A-Za-z0-9]{1,6})$", path)
    return ("." + m.group(1).lower()) if m else ""

def request(url: str, method="GET", data=None, headers=None):
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    return urllib.request.Request(url, method=method, data=data, headers=h)

def fetch(url: str) -> tuple[int, bytes, dict]:
    d = url_domain(url)
    until = domain_cooldown_until.get(d, 0)
    if time.time() < until:
        raise TimeoutError(f"domain cooldown active for {d}")

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = request(url)
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                code = getattr(r, "status", 200) or 200
                body = r.read()
                headers = dict(r.headers.items()) if hasattr(r, "headers") else {}
                return code, body, headers
        except urllib.error.HTTPError as e:
            last_err = e
            code = getattr(e, "code", None)
            if code in (502, 503, 504):
                domain_cooldown_until[d] = time.time() + DOMAIN_COOLDOWN_SEC
                raise
            if attempt < MAX_RETRIES:
                time.sleep((BACKOFF_BASE ** attempt) + random.random())
                continue
            raise
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                time.sleep((BACKOFF_BASE ** attempt) + random.random())
                continue
            raise
    raise last_err if last_err else RuntimeError("fetch failed")

def is_pdf_url(url: str, headers=None) -> bool:
    ext = ext_of_url(url)
    if ext == ".pdf":
        return True
    if headers:
        ct = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
        if "application/pdf" in ct:
            return True
    return False

def extract_links(html: str, base: str) -> list[str]:
    links = []
    for m in re.finditer(r'href\s*=\s*["\']([^"\']+)["\']', html, re.I):
        href = m.group(1).strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        u = urljoin(base, href)
        links.append(u)
    return links

def save_pdf_bytes(url: str, b: bytes, seen_hashes: dict, seen_pdfs: dict) -> str:
    h = sha256_bytes(b)
    if h in seen_hashes:
        name = seen_hashes[h]
        log(f"DUP content already saved as {name}: {url}")
        seen_pdfs[url] = name
        return ""

    base = Path(urlparse(url).path).name or "download.pdf"
    if not base.lower().endswith(".pdf"):
        base += ".pdf"
    base = clean_filename(base)

    out = INBOX / base
    if out.exists():
        out = INBOX / f"{out.stem}_{h[:8]}{out.suffix}"

    out.write_bytes(b)
    seen_hashes[h] = out.name
    seen_pdfs[url] = out.name
    log(f"SAVED {out.name} ({len(b)} bytes) <- {url}")
    return out.name

# -------------------------
# Hard junk gates
# -------------------------
HARD_JUNK_PREFIX = (
    "district_map_", "district_map_h_",
    "precinct_map_", "precinct_map_h_",
    "congressional_map_", "map_index_",
)

HARD_JUNK_NAME_REGEX = [
    r"\bdistrict[_\- ]?map\b",
    r"\bprecinct[_\- ]?map\b",
    r"\bmap[_\- ]?index\b",
    r"\bcontact[_\- ]?list\b",
    r"\bdirectory\b",
]

LOW_SIGNAL_TEXT_PATTERNS = [
    r"\blegend\b", r"\bscale\b", r"\bnorth\b", r"\bprojection\b", r"\bgis\b", r"\besri\b",
    r"\bshapefile\b", r"\bboundary\b", r"\bprecinct\b", r"\bdistrict\b",
    r"\bhighway\b", r"\binterstate\b", r"\bmiles\b", r"\bkm\b",
]

POLICY_TEXT_PATTERNS = [
    r"\bfiscal\b", r"\bappropriat", r"\bbudget\b", r"\brevenue\b", r"\bexpenditure\b",
    r"\baudit\b", r"\blegislat", r"\bcommittee\b", r"\bhearing\b", r"\bbill\b",
    r"\bk\.s\.a\.\b", r"\bsection\b", r"\bsubtotal\b", r"\btotal\b", r"\bfund\b"
]

def hard_junk_reason(filename: str) -> str:
    fn = (filename or "").lower()
    for pfx in HARD_JUNK_PREFIX:
        if fn.startswith(pfx) and fn.endswith(".pdf"):
            return f"prefix:{pfx}"
    for pat in HARD_JUNK_NAME_REGEX:
        if re.search(pat, fn):
            return f"name:{pat}"
    return ""

def low_signal_reason(filename: str, text_sample: str) -> str:
    # If name isn't hard junk, do quick map-heavy sniff
    t = (text_sample or "").lower()
    map_hits = 0
    for pat in LOW_SIGNAL_TEXT_PATTERNS:
        if re.search(pat, t):
            map_hits += 1
    policy_hits = 0
    for pat in POLICY_TEXT_PATTERNS:
        if re.search(pat, t):
            policy_hits += 1
    if map_hits >= 6 and policy_hits <= 1:
        return f"text_mapheavy:map={map_hits},policy={policy_hits}"
    return ""

# -------------------------
# Ollama calls
# -------------------------
def http_post_json(url: str, payload: dict, timeout=300) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = request(url, method="POST", data=data, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))

def ollama_generate(prompt: str, timeout=600) -> str:
    resp = http_post_json(f"{OLLAMA_BASE}/api/generate", {
        "model": MODEL,
        "prompt": prompt,
        "stream": False
    }, timeout=timeout)
    return (resp.get("response") or "").strip()

def pdf_to_text(path: Path) -> str:
    try:
        import fitz  # PyMuPDF
    except Exception as e:
        raise RuntimeError("PyMuPDF not installed. Run: python -m pip install pymupdf") from e

    doc = fitz.open(str(path))
    chunks = []
    for i, page in enumerate(doc, start=1):
        txt = (page.get_text("text") or "").replace("\x00", "")
        chunks.append(f"\n\n[PAGE {i}]\n{txt}")
    out = "\n".join(chunks).strip()
    return out if out else "[NO_TEXT_EXTRACTED]"

def chunk_text(s: str, max_chars: int) -> list[str]:
    s = (s or "").strip()
    if len(s) <= max_chars:
        return [s]
    parts = re.split(r"\n{2,}", s)
    chunks, cur = [], ""
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if len(cur) + len(p) + 2 <= max_chars:
            cur = (cur + "\n\n" + p).strip()
        else:
            if cur:
                chunks.append(cur)
            cur = p[:max_chars]
    if cur:
        chunks.append(cur)
    return chunks

FACT_SCHEMA = r'''
Return JSON with exactly this top-level shape:
{
  "program_type": "STAR_BOND" | "BILL" | "FISCAL_NOTE" | "AUDIT" | "NEWS" | "OTHER",
  "title": string|null,
  "jurisdiction": "Kansas"|null,
  "locations": [string],
  "entities": [
    { "name": string, "type": "PERSON"|"ORG"|"GOV_BODY"|"PROJECT"|"OTHER" }
  ],
  "key_numbers": [
    { "label": string, "value": number, "unit": "USD"|"PERCENT"|"JOBS"|"YEAR"|"OTHER", "year": number|null }
  ],
  "events": [
    { "date": string|null, "year": number|null, "description": string }
  ],
  "evidence": [
    { "quote": string, "note": string }
  ],
  "uncertainties": [string],
  "recommended_next_queries": [string]
}
'''

def extract_facts_fulltext(text: str) -> dict:
    """
    Safe capped extractor:
    - cap total chars
    - cap chunks
    """
    if text is None:
        text = ""
    original_len = len(text)

    if original_len > MAX_TOTAL_CHARS:
        head_len = int(MAX_TOTAL_CHARS * 0.65)
        tail_len = MAX_TOTAL_CHARS - head_len
        head = text[:head_len]
        tail = text[-tail_len:] if tail_len > 0 else ""
        text = head + "\n\n[...TRUNCATED...]\n\n" + tail

    chunks = chunk_text(text, MAX_CHARS_PER_CHUNK)
    if len(chunks) > MAX_CHUNKS:
        keep_head = MAX_CHUNKS // 2
        keep_tail = MAX_CHUNKS - keep_head
        chunks = chunks[:keep_head] + ["[...CHUNKS TRUNCATED...]"] + chunks[-keep_tail:]

    log(f"CHUNK total_chars={len(text)} (orig={original_len}) chunks={len(chunks)}")

    partials = []
    for i, ch in enumerate(chunks, start=1):
        log(f"Ollama chunk {i}/{len(chunks)} start chars={len(ch)}")
        prompt = f"""You are an information extraction engine.
Output MUST be valid JSON only. No markdown. No extra text.

Task: extract Kansas public-policy / public-finance facts from the TEXT.

Rules:
- If unknown, use null.
- Use numbers for numeric fields.
- Do not guess; only extract what is in the TEXT.
- Keep evidence quotes short (<= 20 words) and verbatim.

{FACT_SCHEMA}

TEXT:
\"\"\"{ch}\"\"\"
"""
        raw = ollama_generate(prompt, timeout=900)
        try:
            obj = json.loads(raw)
            partials.append(obj)
            log(f"Ollama chunk {i}/{len(chunks)} done bytes={len(raw.encode('utf-8', errors='ignore'))}")
        except Exception:
            partials.append({
                "program_type": "OTHER",
                "title": None,
                "jurisdiction": "Kansas",
                "locations": [],
                "entities": [],
                "key_numbers": [],
                "events": [],
                "evidence": [],
                "uncertainties": ["chunk_json_parse_failed"],
                "recommended_next_queries": []
            })

    merged = {
        "program_type": "OTHER",
        "title": None,
        "jurisdiction": "Kansas",
        "locations": [],
        "entities": [],
        "key_numbers": [],
        "events": [],
        "evidence": [],
        "uncertainties": [],
        "recommended_next_queries": []
    }
    pref = {"STAR_BOND": 6, "FISCAL_NOTE": 5, "BILL": 4, "AUDIT": 3, "NEWS": 2, "OTHER": 1}

    def uniq_merge(key: str):
        seen = set()
        out = []
        for p in partials:
            vals = p.get(key) or []
            if not isinstance(vals, list):
                continue
            for v in vals:
                try:
                    sig = json.dumps(v, sort_keys=True, ensure_ascii=False)
                except Exception:
                    sig = str(v)
                if sig in seen:
                    continue
                seen.add(sig)
                out.append(v)
        merged[key] = out

    for p in partials:
        pt = (p.get("program_type") or "OTHER")
        if pref.get(pt, 1) > pref.get(merged["program_type"], 1):
            merged["program_type"] = pt

    for p in partials:
        t = p.get("title")
        if isinstance(t, str) and t.strip():
            merged["title"] = t.strip()
            break

    for k in ["locations", "entities", "key_numbers", "events", "evidence", "uncertainties", "recommended_next_queries"]:
        uniq_merge(k)

    # Evidence sanity: trim long quotes (extra safety)
    ev = []
    for item in merged.get("evidence", [])[:12]:
        if isinstance(item, dict):
            q = (item.get("quote") or "").strip()
            n = (item.get("note") or "").strip()
            if len(q.split()) > 20:
                q = " ".join(q.split()[:20])
            ev.append({"quote": q, "note": n[:140]})
    merged["evidence"] = ev

    return merged

def score_facts(f: dict) -> int:
    score = 0
    pt = f.get("program_type") or "OTHER"
    if pt in ("FISCAL_NOTE", "STAR_BOND", "BILL", "AUDIT"):
        score += 4
    if f.get("title"):
        score += 1
    score += min(4, len(f.get("key_numbers") or []))
    score += min(3, len(f.get("events") or []))
    score += min(2, len(f.get("entities") or []))

    # reward money
    for kn in (f.get("key_numbers") or []):
        if isinstance(kn, dict) and kn.get("unit") == "USD":
            score += 2
            break

    # reward having evidence quotes
    if (f.get("evidence") or []):
        score += 1

    return score

def make_post_with_model(facts: dict, source_url: str, pdf_name: str) -> str:
    """
    Writer pass:
    - MUST cite 1–2 evidence quotes verbatim
    - If it can't, it should output an empty string.
    """
    evidence = facts.get("evidence") or []
    e1 = evidence[0]["quote"] if len(evidence) > 0 and isinstance(evidence[0], dict) else ""
    e2 = evidence[1]["quote"] if len(evidence) > 1 and isinstance(evidence[1], dict) else ""

    title = facts.get("title") or pdf_name
    pt = facts.get("program_type") or "OTHER"

    prompt = f"""You write short public-policy research posts.

Return ONLY the final post text (no markdown fences). Keep it under {MAX_POST_CHARS} characters.

Hard rules:
- Must be clearly about Kansas public policy / public finance.
- Must include 1–2 short verbatim evidence quotes from EVIDENCE below, in quotation marks.
- Must include the Source URL line exactly once: "Source: <url>"
- No accusations, motives, or loaded language. Just what the document states.
- If the evidence is not usable or the content is not policy/finance relevant, output an empty string.

Context:
DocType: {pt}
Title: {title}

Key Numbers (may be empty):
{json.dumps(facts.get("key_numbers") or [], ensure_ascii=False)}

Events (may be empty):
{json.dumps(facts.get("events") or [], ensure_ascii=False)}

Entities (may be empty):
{json.dumps(facts.get("entities") or [], ensure_ascii=False)}

EVIDENCE (quotes you are allowed to use verbatim):
1) {e1}
2) {e2}

Now write the post.
Source: {source_url}
"""
    out = ollama_generate(prompt, timeout=600).strip()

    # Normalize and validate basic requirements
    if not out:
        return ""
    if "Source:" not in out:
        return ""
    if source_url not in out:
        return ""
    # must contain at least one quoted evidence string
    if '"' not in out and "“" not in out and "”" not in out:
        return ""

    # last safety trim
    out = out.strip()
    if len(out) > MAX_POST_CHARS:
        out = out[:MAX_POST_CHARS].rstrip()
    return out

# -------------------------
# Crawl / triage / process
# -------------------------
def crawl_cycle(seen_urls: set, seen_pdfs: dict, seen_hashes: dict) -> int:
    queue = list(SEEDS)
    saved_pdfs = 0
    pages = 0

    while queue and pages < CRAWL_PAGES_PER_CYCLE:
        url = queue.pop(0)
        if url in seen_urls:
            continue
        if not allowed_url(url):
            continue

        ext = ext_of_url(url)
        if ext and ext in SKIP_EXT:
            continue

        seen_urls.add(url)

        try:
            code, body, headers = fetch(url)
            pages += 1

            if is_pdf_url(url, headers):
                if url in seen_pdfs:
                    continue
                name = save_pdf_bytes(url, body, seen_hashes, seen_pdfs)
                if name:
                    saved_pdfs += 1
                continue

            ct = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
            sniff = body[:2000].decode("utf-8", errors="ignore").lower()
            if "text/html" not in ct and "<html" not in sniff:
                continue

            html = body.decode("utf-8", errors="replace")
            links = extract_links(html, url)

            pdf_links = [u for u in links if allowed_url(u) and (ext_of_url(u) == ".pdf" or ".pdf" in u.lower())]
            page_links = [u for u in links if allowed_url(u) and ext_of_url(u) not in SKIP_EXT]

            for u in page_links[:40]:
                if u not in seen_urls:
                    queue.append(u)

            for pu in pdf_links[:40]:
                if pu in seen_pdfs:
                    continue
                try:
                    c2, b2, h2 = fetch(pu)
                    if is_pdf_url(pu, h2) or pu.lower().endswith(".pdf"):
                        name = save_pdf_bytes(pu, b2, seen_hashes, seen_pdfs)
                        if name:
                            saved_pdfs += 1
                except Exception as e:
                    log(f"ERROR fetch pdf {pu}: {repr(e)}")

        except Exception as e:
            log(f"ERROR fetch {url}: {repr(e)}")

    log(f"CRAWL done pages={pages} pdfs_saved={saved_pdfs}")
    return saved_pdfs

def triage_cycle():
    moved_big = 0
    for p in INBOX.glob("*.pdf"):
        try:
            if p.stat().st_size >= BIG_BYTES:
                dst = BIGBOX / p.name
                if dst.exists():
                    dst = BIGBOX / f"{p.stem}_{int(time.time())}{p.suffix}"
                p.replace(dst)
                moved_big += 1
        except Exception:
            pass
    log(f"TRIAGE moved_big={moved_big}")
    return moved_big

def pick_files(folder: Path, limit: int) -> list:
    files = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"]
    files.sort(key=lambda p: p.stat().st_mtime)
    return files[:limit]

def archive_file(src: Path):
    ts = time.strftime("%Y%m%d-%H%M%S")
    dst = ARCHIVE / f"{src.stem}.{ts}{src.suffix}"
    shutil.move(str(src), str(dst))
    return dst

def queue_post(post_text: str, pdf_name: str, seen_posthash: dict) -> str:
    """
    Writes to QUEUE as a .post.txt; dedup by content hash.
    """
    h = sha256_text(post_text)
    if h in seen_posthash:
        return ""

    QUEUE.mkdir(parents=True, exist_ok=True)
    out = QUEUE / f"{Path(pdf_name).stem}.post.txt"
    if out.exists():
        out = QUEUE / f"{Path(pdf_name).stem}_{h[:8]}.post.txt"
    out.write_text(post_text, encoding="utf-8")
    seen_posthash[h] = out.name
    return out.name

def process_one(pdf_path: Path, source_url: str, seen_posthash: dict) -> tuple:
    log(f"PROCESS start {pdf_path.name} bytes={pdf_path.stat().st_size}")

    # HARD junk by filename
    r = hard_junk_reason(pdf_path.name)
    if r:
        try:
            JUNK.mkdir(parents=True, exist_ok=True)
            out = JUNK / pdf_path.name
            if out.exists():
                out = JUNK / f"{pdf_path.stem}_{int(time.time())}{pdf_path.suffix}"
            pdf_path.replace(out)
            log(f"PROCESS junk-hard {pdf_path.name} reason={r} -> junk")
        except Exception as e:
            log(f"PROCESS junk-hard move failed {pdf_path.name}: {e}")
        return (False, 0, "junk-hard")

    text = pdf_to_text(pdf_path)
    if not text or text.strip() in ("", "[NO_TEXT_EXTRACTED]"):
        log(f"PROCESS no-text {pdf_path.name} -> archive")
        archive_file(pdf_path)
        return (False, 0, "no-text")

    # LOW signal sniff (fast, before extractor)
    reason = low_signal_reason(pdf_path.name, (text or "")[:12000])
    if reason:
        try:
            JUNK.mkdir(parents=True, exist_ok=True)
            out = JUNK / pdf_path.name
            if out.exists():
                out = JUNK / f"{pdf_path.stem}_{int(time.time())}{pdf_path.suffix}"
            pdf_path.replace(out)
            log(f"PROCESS low-signal {pdf_path.name} reason={reason} -> junk")
        except Exception as e:
            log(f"PROCESS low-signal move failed {pdf_path.name}: {e}")
            archive_file(pdf_path)
        return (False, 0, "low-signal")

    facts = extract_facts_fulltext(text)
    score = score_facts(facts)

    (FACTS / (pdf_path.stem + ".json")).write_text(
        json.dumps(facts, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    if REQUIRE_EVIDENCE and not (facts.get("evidence") or []):
        log(f"PROCESS skip {pdf_path.name} score={score} reason=no-evidence")
        archive_file(pdf_path)
        return (False, score, "no-evidence")

    if score < POST_SCORE_MIN:
        log(f"PROCESS skip {pdf_path.name} score={score} reason=below-threshold")
        archive_file(pdf_path)
        return (False, score, "below-threshold")

    post_text = make_post_with_model(facts, source_url=source_url, pdf_name=pdf_path.name)
    if not post_text.strip():
        log(f"PROCESS skip {pdf_path.name} score={score} reason=writer-empty")
        archive_file(pdf_path)
        return (False, score, "writer-empty")

    queued_name = queue_post(post_text, pdf_path.name, seen_posthash)
    if queued_name:
        log(f"PROCESS queued {pdf_path.name} score={score} -> {queued_name}")
    else:
        log(f"PROCESS dedup {pdf_path.name} score={score} -> already queued")

    archive_file(pdf_path)
    return (bool(queued_name), score, "queued" if queued_name else "dedup")

def cycle_once(seen_urls: set, seen_pdfs: dict, seen_hashes: dict, seen_posthash: dict):
    saved = crawl_cycle(seen_urls, seen_pdfs, seen_hashes)
    triage_cycle()

    queued = 0
    skipped = 0

    # small lane
    for p in pick_files(INBOX, PROCESS_PER_CYCLE):
        try:
            url = ""
            # reverse lookup: pdf file name -> source url
            # seen_pdfs is url->name; find first match
            for u, nm in seen_pdfs.items():
                if nm == p.name:
                    url = u
                    break
            did_queue, score, why = process_one(p, url or "UNKNOWN_URL", seen_posthash)
            if did_queue:
                queued += 1
            else:
                skipped += 1
        except Exception as e:
            log(f"PROCESS ERROR {p.name}: {repr(e)}")
            try:
                archive_file(p)
            except Exception:
                pass
            skipped += 1

    # big lane
    for p in pick_files(BIGBOX, BIG_PER_CYCLE):
        try:
            url = ""
            for u, nm in seen_pdfs.items():
                if nm == p.name:
                    url = u
                    break
            did_queue, score, why = process_one(p, url or "UNKNOWN_URL", seen_posthash)
            if did_queue:
                queued += 1
            else:
                skipped += 1
        except Exception as e:
            log(f"BIG PROCESS ERROR {p.name}: {repr(e)}")
            try:
                archive_file(p)
            except Exception:
                pass
            skipped += 1

    inbox_count = len(list(INBOX.glob("*.pdf")))
    log(f"CYCLE done saved={saved} queued={queued} skipped={skipped} inbox={inbox_count}")
    return saved, queued, skipped

def main():
    ensure_dirs()

    # quick api check
    try:
        req = request(f"{OLLAMA_BASE}/api/tags", method="GET")
        with urllib.request.urlopen(req, timeout=10) as r:
            _ = json.loads(r.read().decode("utf-8", errors="replace"))
    except Exception as e:
        raise RuntimeError(f"Can't reach Ollama API at {OLLAMA_BASE}. Make sure ollama is reachable.") from e

    seen_urls_list = load_json(SEEN_URLS, [])
    seen_urls = set(seen_urls_list if isinstance(seen_urls_list, list) else [])

    seen_pdfs = load_json(SEEN_PDFS, {})
    if not isinstance(seen_pdfs, dict):
        seen_pdfs = {}

    seen_hashes = load_json(SEEN_HASHES, {})
    if not isinstance(seen_hashes, dict):
        seen_hashes = {}

    seen_posthash = load_json(SEEN_POSTHASH, {})
    if not isinstance(seen_posthash, dict):
        seen_posthash = {}

    log(f"RUNNER start domains={DOMAINS} seeds={SEEDS} model={MODEL} ollama={OLLAMA_BASE}")
    log(f"KNOBS interval={INTERVAL_SEC}s crawl_pages={CRAWL_PAGES_PER_CYCLE} process={PROCESS_PER_CYCLE} big={BIG_PER_CYCLE} post_min={POST_SCORE_MIN} big_bytes={BIG_BYTES} caps(total={MAX_TOTAL_CHARS},chunks={MAX_CHUNKS})")

    cmd = (sys.argv[1] if len(sys.argv) > 1 else "loop").lower()

    if cmd == "once":
        cycle_once(seen_urls, seen_pdfs, seen_hashes, seen_posthash)
        save_json(SEEN_URLS, sorted(seen_urls))
        save_json(SEEN_PDFS, seen_pdfs)
        save_json(SEEN_HASHES, seen_hashes)
        save_json(SEEN_POSTHASH, seen_posthash)
        return

    while True:
        cycle_once(seen_urls, seen_pdfs, seen_hashes, seen_posthash)
        save_json(SEEN_URLS, sorted(seen_urls))
        save_json(SEEN_PDFS, seen_pdfs)
        save_json(SEEN_HASHES, seen_hashes)
        save_json(SEEN_POSTHASH, seen_posthash)
        time.sleep(max(10, INTERVAL_SEC))

if __name__ == "__main__":
    main()
