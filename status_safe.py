# status_safe.py (no wmic/tasklist; low-output)
from pathlib import Path
from datetime import datetime
import re

ROOT = Path.home() / "policyqueue"
INBOX = ROOT / "inbox"
BIGPDF = ROOT / "bigpdfs"
FACTS = ROOT / "facts"
POSTS = ROOT / "posts"
APPROVED = ROOT / "approved"
ARCHIVE = ROOT / "archive"
LOGS = ROOT / "logs"
RUNNER_LOG = LOGS / "runner.log"
PQ_LOG = LOGS / "pq.log"

def human_bytes(n: int) -> str:
    try:
        f = float(n)
    except Exception:
        return "?"
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if f < 1024 or u == "TB":
            return f"{int(f)}B" if u == "B" else f"{f:.1f}{u}"
        f /= 1024.0
    return str(n)

def safe_iter_files(folder: Path, suffixes=None):
    try:
        if not folder.exists():
            return []
        out = []
        for p in folder.iterdir():
            if not p.is_file():
                continue
            if suffixes and p.suffix.lower() not in suffixes:
                continue
            out.append(p)
        return out
    except Exception:
        return []

def count_and_size(folder: Path, suffixes=None):
    files = safe_iter_files(folder, suffixes)
    total = 0
    for p in files:
        try:
            total += p.stat().st_size
        except Exception:
            pass
    return len(files), total

def newest(folder: Path, suffixes=None, n=3):
    files = safe_iter_files(folder, suffixes)
    items = []
    for p in files:
        try:
            st = p.stat()
            items.append((st.st_mtime, st.st_size, p.name))
        except Exception:
            pass
    items.sort(key=lambda t: t[0], reverse=True)
    return items[:n]

def tail(path: Path, max_bytes=128*1024):
    if not path.exists():
        return []
    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            take = min(size, max_bytes)
            f.seek(-take, 2)
            data = f.read().decode("utf-8", errors="replace")
        return data.splitlines()
    except Exception:
        return []

def runner_summary(lines):
    # pull only the most recent cycle + most recent post
    last_cycle = None
    last_post = None
    re_cycle = re.compile(r"CYCLE done .*?saved=(\d+)\s+posted=(\d+)\s+skipped=(\d+)\s+inbox=(\d+)")
    re_post = re.compile(r"PROCESS posted .*?score=\d+ -> (.+\.post\.txt)")
    re_ts = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+")

    # scan from bottom up for speed + minimal noise
    for line in reversed(lines):
        if last_cycle is None:
            m = re_cycle.search(line)
            if m:
                last_cycle = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        if last_post is None:
            m = re_post.search(line)
            if m:
                tsm = re_ts.match(line)
                ts = tsm.group(1) if tsm else "?"
                last_post = (ts, m.group(1))
        if last_cycle and last_post:
            break

    # also count errors (small)
    err = {}
    for line in lines[-600:]:
        if "HTTPError" in line:
            # ex: HTTPError 502 https://...
            parts = line.split()
            try:
                code = parts[2]
                url = parts[3]
                dom = url.split("/")[2]
                key = f"{dom} HTTP {code}"
                err[key] = err.get(key, 0) + 1
            except Exception:
                pass
        elif "URLError" in line:
            # ex: URLError https://...
            parts = line.split()
            try:
                url = parts[1]
                dom = url.split("/")[2]
                key = f"{dom} URLError"
                err[key] = err.get(key, 0) + 1
            except Exception:
                pass
        elif "timeout" in line and "ERROR fetch" in line:
            # ex: ERROR fetch https://... timeout(...)
            try:
                url = line.split()[3]
                dom = url.split("/")[2]
                key = f"{dom} timeout"
                err[key] = err.get(key, 0) + 1
            except Exception:
                pass

    top_errs = sorted(err.items(), key=lambda kv: kv[1], reverse=True)[:6]
    return last_cycle, last_post, top_errs

def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 60)
    print(f"PolicyQueue Status (SAFE) â€” {now}")
    print("=" * 60)

    for name, folder, exts in [
        ("inbox", INBOX, {".pdf"}),
        ("bigpdfs", BIGPDF, {".pdf"}),
        ("facts", FACTS, {".json", ".raw.txt"}),
        ("posts", POSTS, {".post.txt"}),
        ("approved", APPROVED, {".txt", ".post.txt"}),
        ("archive", ARCHIVE, {".pdf"}),
    ]:
        c, b = count_and_size(folder, exts)
        print(f"{name:8}: {c:6} files  ({human_bytes(b)})")

    print("-" * 60)
    print("Newest inbox PDFs:")
    for mt, sz, nm in newest(INBOX, {".pdf"}, n=3):
        print(f"  {datetime.fromtimestamp(mt).strftime('%m-%d %H:%M:%S')}  {human_bytes(sz):>8}  {nm}")

    print("Newest posts:")
    for mt, sz, nm in newest(POSTS, {".post.txt"}, n=3):
        print(f"  {datetime.fromtimestamp(mt).strftime('%m-%d %H:%M:%S')}  {human_bytes(sz):>8}  {nm}")

    print("-" * 60)
    r_lines = tail(RUNNER_LOG)
    if r_lines:
        last_cycle, last_post, top_errs = runner_summary(r_lines)
        st = RUNNER_LOG.stat()
        print(f"runner.log: {human_bytes(st.st_size)}  last_write={datetime.fromtimestamp(st.st_mtime).strftime('%m-%d %H:%M:%S')}")
        if last_cycle:
            saved, posted, skipped, inbox = last_cycle
            print(f"last cycle: saved={saved} posted={posted} skipped={skipped} inbox={inbox}")
        else:
            print("last cycle: (not found in tail)")
        if last_post:
            ts, fname = last_post
            print(f"last posted: {ts}  -> {fname}")
        else:
            print("last posted: (not found in tail)")
        if top_errs:
            print("top errors (recent tail):")
            for k, v in top_errs:
                print(f"  {v:3}  {k}")
    else:
        print("runner.log: (missing or unreadable)")

    pq_lines = tail(PQ_LOG)
    if pq_lines:
        st = PQ_LOG.stat()
        print("-" * 60)
        print(f"pq.log    : {human_bytes(st.st_size)}  last_write={datetime.fromtimestamp(st.st_mtime).strftime('%m-%d %H:%M:%S')}")
        # show last few meaningful lines
        interesting = [ln for ln in pq_lines if ("Processing" in ln or "Wrote" in ln or "ERROR" in ln or "No files" in ln)]
        for ln in interesting[-5:]:
            print("  " + ln)

    print("=" * 60)

if __name__ == "__main__":
    main()
