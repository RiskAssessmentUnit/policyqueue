import os, sys, time, json, hashlib, sqlite3, re
from urllib.parse import urljoin, urlparse, urldefrag
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from pathlib import Path
from html.parser import HTMLParser

ROOT = Path.home() / "policyqueue"
INBOX = ROOT / "inbox"
LOGS = ROOT / "logs"
DB_PATH = ROOT / "crawler.sqlite"
LOGFILE = LOGS / "crawler.log"

USER_AGENT = os.environ.get("PQ_UA", "PolicyQueuePDFCrawler/0.1 (+local)")
CRAWL_DELAY_SEC = float(os.environ.get("PQ_DELAY", "0.5"))   # be polite
TIMEOUT_SEC = int(os.environ.get("PQ_TIMEOUT", "20"))
MAX_PAGES = int(os.environ.get("PQ_MAX_PAGES", "8000"))
MAX_DEPTH = int(os.environ.get("PQ_MAX_DEPTH", "6"))
MAX_BYTES = int(os.environ.get("PQ_MAX_BYTES", str(50 * 1024 * 1024)))  # 50MB cap per file

class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in ("a", "link", "area"):
            href = None
            for k, v in attrs:
                if k.lower() == "href":
                    href = v
                    break
            if href:
                self.links.append(href)

def log(msg: str):
    LOGS.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    LOGFILE.open("a", encoding="utf-8").write(f"{ts}  {msg}\n")

def ensure_dirs():
    for p in (ROOT, INBOX, LOGS):
        p.mkdir(parents=True, exist_ok=True)

def db():
    con = sqlite3.connect(str(DB_PATH))
    con.execute("""create table if not exists seen_urls(
        url text primary key,
        kind text,
        status integer,
        last_seen integer
    )""")
    con.execute("""create table if not exists files(
        url text primary key,
        sha256 text,
        bytes integer,
        saved_as text,
        fetched_at integer
    )""")
    con.commit()
    return con

def normalize_url(base, href):
    u = urljoin(base, href)
    u, _frag = urldefrag(u)
    return u

def same_allowed_domain(url, allow_domains):
    host = (urlparse(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in allow_domains)

def fetch(url):
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=TIMEOUT_SEC) as r:
        status = getattr(r, "status", 200)
        ctype = (r.headers.get("Content-Type") or "").lower()
        data = r.read()
        return status, ctype, data

def sha256_bytes(b):
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def safe_filename_from_url(url):
    p = urlparse(url)
    name = Path(p.path).name or "download.pdf"
    name = re.sub(r"[^a-zA-Z0-9._-]+", "_", name)
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name

def already_downloaded(con, url):
    cur = con.execute("select 1 from files where url=? limit 1", (url,))
    return cur.fetchone() is not None

def mark_seen(con, url, kind, status):
    con.execute(
        "insert into seen_urls(url,kind,status,last_seen) values(?,?,?,?) "
        "on conflict(url) do update set kind=excluded.kind, status=excluded.status, last_seen=excluded.last_seen",
        (url, kind, int(status), int(time.time())),
    )
    con.commit()

def save_pdf(con, url, data):
    if len(data) > MAX_BYTES:
        log(f"SKIP too large ({len(data)} bytes): {url}")
        return None

    digest = sha256_bytes(data)
    # de-dupe by content hash: if hash exists, don't save again
    cur = con.execute("select saved_as from files where sha256=? limit 1", (digest,))
    row = cur.fetchone()
    if row:
        log(f"DUP content already saved as {row[0]}: {url}")
        con.execute(
            "insert or replace into files(url,sha256,bytes,saved_as,fetched_at) values(?,?,?,?,?)",
            (url, digest, len(data), row[0], int(time.time())),
        )
        con.commit()
        return row[0]

    base = safe_filename_from_url(url)
    out = INBOX / base
    # avoid name collisions
    if out.exists():
        out = INBOX / f"{out.stem}_{digest[:8]}{out.suffix}"

    out.write_bytes(data)
    con.execute(
        "insert or replace into files(url,sha256,bytes,saved_as,fetched_at) values(?,?,?,?,?)",
        (url, digest, len(data), out.name, int(time.time())),
    )
    con.commit()
    log(f"SAVED {out.name} ({len(data)} bytes) <- {url}")
    return out.name

def crawl(seeds, allow_domains):
    ensure_dirs()
    con = db()

    # BFS queue: (url, depth)
    q = [(s, 0) for s in seeds]
    seen_in_run = set()

    pages = 0
    pdfs = 0

    while q and pages < MAX_PAGES:
        url, depth = q.pop(0)
        if url in seen_in_run:
            continue
        seen_in_run.add(url)

        if not same_allowed_domain(url, allow_domains):
            continue

        # If we already downloaded this PDF URL before, skip fetch
        if url.lower().endswith(".pdf") and already_downloaded(con, url):
            continue

        # Throttle
        time.sleep(CRAWL_DELAY_SEC)

        try:
            status, ctype, data = fetch(url)
        except HTTPError as e:
            mark_seen(con, url, "error", getattr(e, "code", 0))
            log(f"HTTPError {getattr(e,'code',0)} {url}")
            continue
        except URLError as e:
            mark_seen(con, url, "error", 0)
            log(f"URLError {url}: {e}")
            continue
        except Exception as e:
            mark_seen(con, url, "error", 0)
            log(f"ERROR fetch {url}: {repr(e)}")
            continue

        # PDF by URL or content-type
        is_pdf = url.lower().endswith(".pdf") or ("application/pdf" in (ctype or ""))

        if is_pdf:
            mark_seen(con, url, "pdf", status)
            saved = save_pdf(con, url, data)
            if saved:
                pdfs += 1
            pages += 1
            continue

        mark_seen(con, url, "page", status)
        pages += 1

        if depth >= MAX_DEPTH:
            continue

        # Only parse HTML-ish
        if "text/html" not in (ctype or "") and not data.lstrip().startswith(b"<!") and b"<html" not in data[:500].lower():
            continue

        try:
            html = data.decode("utf-8", errors="replace")
        except Exception:
            continue

        parser = LinkParser()
        try:
            parser.feed(html)
        except Exception:
            continue

        for href in parser.links:
            u = normalize_url(url, href)
            if not u:
                continue
            # Keep within allowlist domains
            if not same_allowed_domain(u, allow_domains):
                continue
            # Only enqueue http(s)
            if not u.lower().startswith(("http://", "https://")):
                continue
            # Avoid giant non-html downloads (basic filter)
            if any(u.lower().endswith(ext) for ext in (".zip",".jpg",".jpeg",".png",".gif",".mp4",".mp3",".doc",".docx",".xls",".xlsx")):
                continue
            q.append((u, depth + 1))

    log(f"DONE pages={pages} pdfs_saved={pdfs} queue_remaining={len(q)}")
    print(f"Done. Crawled pages={pages}. PDFs saved this run={pdfs}. Inbox={INBOX}")

def main():
    # Usage:
    #   python crawl_pdfs.py kslegislature.org budget.kansas.gov --seeds https://...
    #
    # Domains: args before any --seeds are treated as allowed domains.
    # Seeds: if not provided, defaults to homepage of each domain.
    args = sys.argv[1:]
    if not args:
        print("Usage: python crawl_pdfs.py <domain1> [domain2...] [--seeds <url1> <url2> ...]")
        sys.exit(2)

    allow_domains = []
    seeds = []

    if "--seeds" in args:
        i = args.index("--seeds")
        allow_domains = args[:i]
        seeds = args[i+1:]
    else:
        allow_domains = args
        seeds = []

    allow_domains = [d.strip().lower().replace("https://","").replace("http://","").strip("/") for d in allow_domains if d.strip()]
    if not allow_domains:
        print("Need at least one domain.")
        sys.exit(2)

    if not seeds:
        seeds = [f"https://{d}/" for d in allow_domains]

    crawl(seeds, allow_domains)

if __name__ == "__main__":
    main()
