import os, sqlite3, time
from pathlib import Path

ROOT = Path(os.environ.get("PQ_ROOT", str(Path.home() / "policyqueue")))
DB   = Path(os.environ.get("PQ_DB", str(ROOT / "pq.sqlite")))

def fmt_bytes(n):
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024:
            return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024
    return f"{n:.1f}PB"

def qone(con, sql, args=()):
    cur = con.execute(sql, args)
    row = cur.fetchone()
    return row[0] if row else None

def main():
    if not DB.exists():
        raise SystemExit(f"DB not found: {DB} (run pq_db_init.py)")

    con = sqlite3.connect(str(DB))
    now = int(time.time())

    posts_dir = ROOT / "posts"
    post_files = list(posts_dir.glob("*.post.txt")) if posts_dir.exists() else []
    newest_post_file_ts = max((int(p.stat().st_mtime) for p in post_files), default=None)

    print("="*60)
    print(f"PolicyQueue Status (DB) — {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    # PDF stages
    stages = ["inbox","bigpdfs","posts","approved","archive","junk","unknown"]
    for st in stages:
        cnt = qone(con, "SELECT COUNT(*) FROM pdfs WHERE stage=?", (st,)) or 0
        sz  = qone(con, "SELECT COALESCE(SUM(bytes),0) FROM pdfs WHERE stage=?", (st,)) or 0
        print(f"pdfs.{st:7}: {cnt:6d} files  ({fmt_bytes(sz)})")

    print("-"*60)
    print(f"post files: {len(post_files):6d}  ({posts_dir})")

    last_posted_event_ts = qone(con, "SELECT MAX(ts) FROM events WHERE kind IN ('PROCESS_POSTED','POST_FILE_SYNCED','POST_FILE')")
    last_posted_ts = max([t for t in [last_posted_event_ts, newest_post_file_ts] if t], default=None)

    if last_posted_ts:
        age_min = (now - last_posted_ts) / 60
        print(f"last posted: {time.strftime('%m-%d %H:%M:%S', time.localtime(last_posted_ts))}  ({age_min:.1f} min ago)")
    else:
        print("last posted: (none)")

    # Throughput windows (events)
    for mins in (15, 60, 240):
        cutoff = now - mins*60
        posted = qone(con, "SELECT COUNT(*) FROM events WHERE kind IN ('PROCESS_POSTED','POST_FILE_SYNCED','POST_FILE') AND ts>=?", (cutoff,)) or 0
        saved  = qone(con, "SELECT COUNT(*) FROM events WHERE kind='SAVED' AND ts>=?", (cutoff,)) or 0
        errs   = qone(con, "SELECT COUNT(*) FROM events WHERE kind='ERROR' AND ts>=?", (cutoff,)) or 0
        print(f"last {mins:3d}m: saved={saved:4d}  posted={posted:4d}  errors={errs:4d}")

    print("-"*60)
    print("Top error domains (last 6h):")
    cutoff = now - 6*3600
    cur = con.execute("""
      SELECT COALESCE(domain,'(none)') d, COUNT(*) c
      FROM events
      WHERE kind='ERROR' AND ts>=?
      GROUP BY d
      ORDER BY c DESC
      LIMIT 10
    """, (cutoff,))
    rows = cur.fetchall()
    if not rows:
        print("  (none)")
    else:
        for d,c in rows:
            print(f"  {d:32} {c}")

    con.close()
    print("="*60)

if __name__ == "__main__":
    main()
