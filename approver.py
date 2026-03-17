# approver.py — non-destructive queue consumer
# - Reads .\queue\*.post.txt
# - Sends to Discord/Telegram (optional)
# - Moves files instead of deleting:
#     sent -> .\sent\
#     duplicates -> .\sent_dupes\
#     empty -> .\skip\empty_posts\

import os, time, json, urllib.request
from pathlib import Path

import db

ROOT = Path(__file__).resolve().parent
QUEUE = ROOT / "queue"
LOGS  = ROOT / "logs"
LOG   = LOGS / "approver.log"

SENT_DIR        = ROOT / "sent"
SENT_DUPES_DIR  = ROOT / "sent_dupes"
EMPTY_DIR       = ROOT / "skip" / "empty_posts"

INTERVAL_SEC = int(os.environ.get("PQ_APPROVER_INTERVAL", "10"))

DISCORD_WEBHOOK = os.environ.get("PQ_DISCORD_WEBHOOK", "").strip()
TELEGRAM_BOT    = os.environ.get("PQ_TELEGRAM_BOT", "").strip()
TELEGRAM_CHAT   = os.environ.get("PQ_TELEGRAM_CHAT", "").strip()


def log(msg: str):
    LOGS.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts}  {msg}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def http_post(url: str, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        _ = r.read()


def send_discord(text: str):
    if not DISCORD_WEBHOOK:
        return
    http_post(DISCORD_WEBHOOK, {"content": text[:1900]})


def send_telegram(text: str):
    if not (TELEGRAM_BOT and TELEGRAM_CHAT):
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT}/sendMessage"
    http_post(url, {"chat_id": TELEGRAM_CHAT, "text": text[:3900]})


def safe_move(src: Path, dst_dir: Path) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name

    # if name exists, add a timestamp suffix
    if dst.exists():
        stamp = time.strftime("%Y%m%d-%H%M%S")
        dst = dst_dir / f"{src.stem}.{stamp}{src.suffix}"

    src.replace(dst)
    return dst


def main():
    QUEUE.mkdir(parents=True, exist_ok=True)
    SENT_DIR.mkdir(parents=True, exist_ok=True)
    SENT_DUPES_DIR.mkdir(parents=True, exist_ok=True)
    EMPTY_DIR.mkdir(parents=True, exist_ok=True)

    db.init(ROOT / "pq.sqlite")
    log("APPROVER start (non-destructive)")

    while True:
        try:
            files = sorted(
                [p for p in QUEUE.glob("*.post.txt") if p.is_file()],
                key=lambda p: p.stat().st_mtime,
            )

            if not files:
                time.sleep(max(1, INTERVAL_SEC))
                continue

            for p in files:
                txt = p.read_text(encoding="utf-8", errors="ignore").strip()

                # Empty file -> move aside (never delete)
                if not txt:
                    moved = safe_move(p, EMPTY_DIR)
                    log(f"EMPTY -> {moved.name}")
                    continue

                h = db.sha256_text(txt)

                # Duplicate -> move aside (never delete)
                if db.is_post_hash_seen(h):
                    moved = safe_move(p, SENT_DUPES_DIR)
                    log(f"DUPE  -> {moved.name}")
                    continue

                # Send
                send_discord(txt)
                send_telegram(txt)

                # Record in DB
                db.save_post_hash(h, p.name)
                db.add_event("APPROVER_SENT", pdf_name=p.name)

                # Move to sent (never delete)
                moved = safe_move(p, SENT_DIR)
                log(f"SENT  -> {moved.name}")

        except Exception as e:
            log(f"ERROR {type(e).__name__}: {e}")
            time.sleep(2)


if __name__ == "__main__":
    main()