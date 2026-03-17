"""
crawler.py — BFS web crawler for Kansas government PDF discovery.

Public entry point: crawl_cycle() -> int (number of new PDFs saved)
"""

import os
import re
import random
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse
import urllib.request
import urllib.error

import db

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT  = Path(__file__).resolve().parent
INBOX = ROOT / "inbox"
LOGS  = ROOT / "logs"
RUNLOG = LOGS / "runner.log"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CRAWL_PAGES_PER_CYCLE = int(os.environ.get("PQ_CRAWL_PAGES_PER_CYCLE", "120"))
HTTP_TIMEOUT          = int(os.environ.get("PQ_HTTP_TIMEOUT", "25"))
UA                    = os.environ.get("PQ_UA", "Mozilla/5.0 (compatible; PolicyQueueRunner/3.0)")
MAX_RETRIES           = int(os.environ.get("PQ_MAX_RETRIES", "3"))
BACKOFF_BASE          = float(os.environ.get("PQ_BACKOFF_BASE", "1.6"))

SKIP_EXT = set(
    x.strip().lower()
    for x in os.environ.get(
        "PQ_SKIP_EXT",
        ".jpg,.jpeg,.png,.gif,.webp,.svg,.css,.js,.ico,.zip,.mp4,.mp3,.wav,.mov",
    ).split(",")
    if x.strip()
)

DOMAINS = [
    d.strip().lower()
    for d in os.environ.get(
        "PQ_DOMAINS", "budget.kansas.gov,ksrevenue.gov,postaudit.ks.gov"
    ).split(",")
    if d.strip()
]

SEEDS = [
    s.strip()
    for s in os.environ.get(
        "PQ_SEEDS",
        "https://budget.kansas.gov/,https://ksrevenue.gov/,https://postaudit.ks.gov/",
    ).split(",")
    if s.strip()
]

# ---------------------------------------------------------------------------
# Focus gate — allow revenue + STAR bonds, block DMV / form floods
# ---------------------------------------------------------------------------

_FOCUS_ALLOW = [
    r"\brevenue\b", r"\btax\b", r"\breceipts\b", r"\bcollections\b",
    r"\bwithholding\b", r"\bsales tax\b", r"\bincome tax\b", r"\bseverance\b",
    r"\bforecast\b", r"\bestimat", r"\bstar\b", r"\bstar bond\b",
    r"\bstarbond\b", r"\bbond\b", r"\bbonds\b",
]
_FOCUS_BLOCK = [
    r"\bdmv\b", r"\bdriver\b", r"\bcdl\b", r"\bhandbook\b", r"\breciprocity\b",
    r"\bclerk\b", r"\bapprais", r"\bclasscode\b", r"\bmotor\b",
    r"\brefund\b", r"\bfein\b", r"\babat", r"\bappeal\b", r"\balcohol\b",
]

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    RUNLOG.open("a", encoding="utf-8").write(f"{ts}  {msg}\n")


def _clean_filename(name: str) -> str:
    name = re.sub(r"[^\w\-.]+", "_", (name or "").strip())
    name = re.sub(r"_+", "_", name)
    return name[:160] if len(name) > 160 else name


def _url_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _allowed_url(url: str) -> bool:
    d = _url_domain(url)
    return bool(d) and any(d == dom or d.endswith("." + dom) for dom in DOMAINS)


def _ext_of_url(url: str) -> str:
    path = urlparse(url).path
    m = re.search(r"\.([A-Za-z0-9]{1,6})$", path)
    return ("." + m.group(1).lower()) if m else ""


def _make_request(url: str, method: str = "GET", data=None, headers=None):
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    return urllib.request.Request(url, method=method, data=data, headers=h)


def _fetch(url: str) -> tuple:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = _make_request(url)
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                code = getattr(r, "status", 200) or 200
                body = r.read()
                hdrs = dict(r.headers.items()) if hasattr(r, "headers") else {}
                return code, body, hdrs
        except urllib.error.HTTPError as e:
            last_err = e
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


def _is_pdf_url(url: str, headers=None) -> bool:
    if _ext_of_url(url) == ".pdf":
        return True
    if headers:
        ct = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
        if "application/pdf" in ct:
            return True
    return False


def _extract_links(html: str, base: str) -> list:
    links = []
    for m in re.finditer(r'href\s*=\s*["\']([^"\']+)["\']', html, re.I):
        href = m.group(1).strip()
        if not href or href.startswith(("#", "mailto:", "javascript:")):
            continue
        links.append(urljoin(base, href))
    return links


def _should_save_pdf(url: str, filename: str) -> bool:
    s = (url + " " + (filename or "")).lower()
    for pat in _FOCUS_BLOCK:
        if re.search(pat, s):
            return False
    for pat in _FOCUS_ALLOW:
        if re.search(pat, s):
            return True
    return False


def _save_pdf_bytes(url: str, body: bytes) -> str:
    """Download and record a PDF. Returns filename on success, '' on skip/dup."""
    base = Path(urlparse(url).path).name or "download.pdf"
    if not base.lower().endswith(".pdf"):
        base += ".pdf"
    base = _clean_filename(base)

    if not _should_save_pdf(url, base):
        _log(f"SKIP {base} not focus")
        return ""

    h = db.sha256_bytes(body)
    existing = db.get_filename_for_hash(h)
    if existing:
        db.mark_url_seen(url)
        db.add_event("DUP", db._domain(url), url, existing)
        _log(f"DUP content already saved as {existing}: {url}")
        return ""

    INBOX.mkdir(parents=True, exist_ok=True)
    out = INBOX / base
    if out.exists():
        out = INBOX / f"{out.stem}_{h[:8]}{out.suffix}"

    out.write_bytes(body)
    db.save_pdf(h, out.name, url, len(body), str(out))
    db.add_event("SAVED", db._domain(url), url, out.name, f"bytes={len(body)}")
    _log(f"SAVED {out.name} ({len(body)} bytes) <- {url}")
    return out.name


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def crawl_cycle() -> int:
    """Run one BFS crawl cycle over SEEDS. Returns number of new PDFs saved."""
    q = list(SEEDS)
    pages = 0
    saved = 0

    while q and pages < CRAWL_PAGES_PER_CYCLE:
        url = q.pop(0)
        if db.is_url_seen(url):
            continue
        if not _allowed_url(url):
            continue
        ext = _ext_of_url(url)
        if ext and ext in SKIP_EXT:
            continue
        db.mark_url_seen(url)

        try:
            _code, body, headers = _fetch(url)
            pages += 1

            if _is_pdf_url(url, headers):
                if db.is_pdf_url_seen(url):
                    continue
                if _save_pdf_bytes(url, body):
                    saved += 1
                continue

            ct    = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
            sniff = body[:2000].decode("utf-8", errors="ignore").lower()
            if "text/html" not in ct and "<html" not in sniff:
                continue

            html  = body.decode("utf-8", errors="replace")
            links = _extract_links(html, url)

            page_links = [u for u in links if _allowed_url(u) and _ext_of_url(u) not in SKIP_EXT]
            pdf_links  = [u for u in links if _allowed_url(u) and (_ext_of_url(u) == ".pdf" or ".pdf" in u.lower())]

            for u in page_links[:30]:
                if not db.is_url_seen(u):
                    q.append(u)

            for pu in pdf_links[:30]:
                if db.is_pdf_url_seen(pu):
                    continue
                try:
                    _c2, b2, h2 = _fetch(pu)
                    if _is_pdf_url(pu, h2) or pu.lower().endswith(".pdf"):
                        if _save_pdf_bytes(pu, b2):
                            saved += 1
                except Exception as e:
                    _log(f"ERROR fetch pdf {pu}: {repr(e)}")
                    db.add_event("ERROR", db._domain(pu), pu, details=repr(e))

        except Exception as e:
            _log(f"ERROR fetch {url}: {repr(e)}")
            db.mark_url_seen(url, status="error", error=repr(e))
            db.add_event("ERROR", db._domain(url), url, details=repr(e))

    _log(f"CRAWL saved {saved} pdfs")
    return saved
