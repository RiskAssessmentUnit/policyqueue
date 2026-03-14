import os, re, time
from pathlib import Path

ROOT = Path(os.environ.get("PQ_ROOT", str(Path.home() / "policyqueue")))
LOGS = ROOT / "logs"
RUNNER_LOG = LOGS / "runner.log"
JUNK_LOG = LOGS / "junk.log"

INBOX   = ROOT / "inbox"
BIGPDFS = ROOT / "bigpdfs"
ARCHIVE = ROOT / "archive"
JUNK    = ROOT / "junk"

for p in (ROOT, LOGS, INBOX, BIGPDFS, ARCHIVE, JUNK):
    p.mkdir(parents=True, exist_ok=True)

# ---- Settings (override with env vars if you want) ----
RETENTION_DAYS = int(os.environ.get("PQ_JUNK_RETENTION_DAYS", "7"))
MAX_MB         = int(os.environ.get("PQ_JUNK_MAX_MB", "2048"))  # 0 disables size cap purge
LOOKBACK_HRS   = int(os.environ.get("PQ_JUNK_LOOKBACK_HRS", "24"))

# Protect anything that looks important (never junk these by name)
PROTECT_RE = re.compile(
    r"(bill|hb|sb|fisc|note|supp|committee|hearing|agenda|minutes|audit|report|budget|revenue|regulat|guidance|directive|ksa|kar|rule|ordinance)",
    re.I
)

LOW_SIGNAL_RE = re.compile(r"PROCESS low-signal\s+(.+?\.pdf)\b", re.I)

def ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str):
    try:
        with JUNK_LOG.open("a", encoding="utf-8") as f:
            f.write(f"{ts()}  {msg}\n")
    except Exception:
        pass

def read_recent_low_signal_names():
    if not RUNNER_LOG.exists():
        log("No runner.log found; nothing to do.")
        return set()

    cutoff = time.time() - (LOOKBACK_HRS * 3600)
    names = set()

    # Read tail-ish safely (runner.log can get big)
    try:
        data = RUNNER_LOG.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        log(f"ERROR reading runner.log: {e!r}")
        return set()

    # Optional: only consider lines that are "recent" by timestamp in the line
    # If parsing fails, we fall back to whole-file regex (still OK).
    # Timestamp format in your log: "YYYY-MM-DD HH:MM:SS  ..."
    line_ts = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+", re.M)
    # Find all low-signal hits and (best-effort) keep those within lookback window.
    for m in LOW_SIGNAL_RE.finditer(data):
        pdf = m.group(1).strip()
        # Try to find the timestamp on the same line (search backwards to line start)
        line_start = data.rfind("\n", 0, m.start()) + 1
        line_end = data.find("\n", m.start())
        if line_end == -1:
            line_end = len(data)
        line = data[line_start:line_end]

        keep = True
        mt = line_ts.match(line)
        if mt:
            try:
                # Convert log timestamp to epoch using local time assumptions
                t_struct = time.strptime(mt.group(1), "%Y-%m-%d %H:%M:%S")
                t_epoch = time.mktime(t_struct)
                keep = (t_epoch >= cutoff)
            except Exception:
                keep = True

        if keep and not PROTECT_RE.search(pdf):
            names.add(pdf)

    return names

def find_pdf_anywhere(name: str):
    # Search common folders only (fast)
    candidates = [
        INBOX / name,
        BIGPDFS / name,
        ARCHIVE / name,
        JUNK / name,
    ]
    for c in candidates:
        if c.exists() and c.is_file():
            return c

    # If not found, try fuzzy match by stem in archive (handles timestamped archive names)
    stem = Path(name).stem
    for p in ARCHIVE.glob(f"{stem}*.pdf"):
        if p.is_file():
            return p

    return None

def move_to_junk(path: Path, reason: str):
    if path is None or not path.exists():
        return False
    if PROTECT_RE.search(path.name):
        log(f"SKIP protect-match {path.name}")
        return False
    if path.parent.resolve() == JUNK.resolve():
        return False
    try:
        dst = JUNK / path.name
        # If collision, keep both by suffixing timestamp
        if dst.exists():
            dst = JUNK / f"{path.stem}.{int(time.time())}{path.suffix}"
        path.replace(dst)
        log(f"MOVED {path.name} -> junk  reason={reason}")
        return True
    except Exception as e:
        log(f"ERROR move {path.name} -> junk: {e!r}")
        return False

def purge_junk():
    # Time-based delete
    cutoff = time.time() - (RETENTION_DAYS * 86400)
    junk_files = [p for p in JUNK.glob("*.pdf") if p.is_file()]

    deleted = 0
    for p in junk_files:
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
                deleted += 1
                log(f"DELETED {p.name}  reason=retention>{RETENTION_DAYS}d")
        except Exception as e:
            log(f"ERROR delete {p.name}: {e!r}")

    # Size-cap delete (oldest first)
    if MAX_MB and MAX_MB > 0:
        junk_files = [p for p in JUNK.glob("*.pdf") if p.is_file()]
        total = 0
        for p in junk_files:
            try:
                total += p.stat().st_size
            except Exception:
                pass

        cap = MAX_MB * 1024 * 1024
        if total > cap:
            junk_files.sort(key=lambda p: p.stat().st_mtime)  # oldest first
            for p in junk_files:
                if total <= cap:
                    break
                try:
                    sz = p.stat().st_size
                    p.unlink()
                    total -= sz
                    deleted += 1
                    log(f"DELETED {p.name}  reason=sizecap>{MAX_MB}MB")
                except Exception as e:
                    log(f"ERROR sizecap delete {p.name}: {e!r}")

    return deleted

def main():
    names = read_recent_low_signal_names()
    if not names:
        log("No recent low-signal PDFs found (or all protected).")
    moved = 0
    for name in sorted(names):
        p = find_pdf_anywhere(name)
        if p is None:
            log(f"MISS could not locate {name} in inbox/bigpdfs/archive")
            continue
        if move_to_junk(p, reason=f"runner.low-signal lookback={LOOKBACK_HRS}h"):
            moved += 1

    deleted = purge_junk()
    log(f"DONE moved={moved} deleted={deleted} retention_days={RETENTION_DAYS} max_mb={MAX_MB} lookback_hrs={LOOKBACK_HRS}")

if __name__ == "__main__":
    main()
