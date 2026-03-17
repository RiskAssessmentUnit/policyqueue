"""
runner.py — Thin orchestrator: crawl → triage → process → repeat.

Usage:
    python runner.py          # continuous loop (default)
    python runner.py once     # single cycle then exit
"""

import os
import sys
import time
from pathlib import Path

import db
import crawler
import processor
import extract as extract_mod

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT  = Path(__file__).resolve().parent
LOGS  = ROOT / "logs"
PIDS  = ROOT / "pids"
STATE = ROOT / "state"
RUNLOG = LOGS / "runner.log"
LOCKFILE = PIDS / "runner.pid"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INTERVAL_SEC      = int(os.environ.get("PQ_INTERVAL_SEC", "300"))
PROCESS_PER_CYCLE = int(os.environ.get("PQ_PROCESS_PER_CYCLE", "15"))
BIG_PER_CYCLE     = int(os.environ.get("PQ_BIG_PER_CYCLE", "1"))
BIG_BYTES         = int(os.environ.get("PQ_BIG_BYTES", str(8_000_000)))

# ---------------------------------------------------------------------------
# PID lock
# ---------------------------------------------------------------------------

def _acquire_lock() -> None:
    PIDS.mkdir(parents=True, exist_ok=True)
    if LOCKFILE.exists():
        old = (LOCKFILE.read_text(encoding="utf-8", errors="ignore").strip() or "")
        if old.isdigit():
            try:
                os.kill(int(old), 0)
                raise SystemExit(f"runner already running pid={old}")
            except OSError:
                pass
    LOCKFILE.write_text(str(os.getpid()), encoding="utf-8")


def _release_lock() -> None:
    try:
        if LOCKFILE.exists():
            LOCKFILE.unlink()
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    RUNLOG.open("a", encoding="utf-8").write(f"{ts}  {msg}\n")

# ---------------------------------------------------------------------------
# Ensure directories
# ---------------------------------------------------------------------------

def _ensure_dirs() -> None:
    for p in (
        crawler.INBOX, processor.BIGBOX, processor.FACTS,
        processor.QUEUE, processor.ARCHIVE, LOGS, STATE, PIDS,
    ):
        p.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# One cycle
# ---------------------------------------------------------------------------

def cycle_once() -> None:
    crawler.crawl_cycle()

    # Move oversized PDFs out of inbox before processing
    for p in list(crawler.INBOX.glob("*.pdf")):
        try:
            if p.stat().st_size >= BIG_BYTES:
                p.replace(processor.BIGBOX / p.name)
        except Exception:
            pass

    # Process normal-sized PDFs
    for p in processor.pick_files(crawler.INBOX, PROCESS_PER_CYCLE):
        processor.process_one(p, db.get_url_for_filename(p.name))

    # Process big PDFs (one per cycle to avoid long stalls)
    for p in processor.pick_files(processor.BIGBOX, BIG_PER_CYCLE):
        processor.process_one(p, db.get_url_for_filename(p.name))

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    _ensure_dirs()
    _acquire_lock()
    try:
        db.init(ROOT / "pq.sqlite")
        _log(
            f"RUNNER start domains={crawler.DOMAINS} seeds={crawler.SEEDS} "
            f"model={extract_mod.OLLAMA_MODEL} ollama={extract_mod.OLLAMA_BASE}"
        )

        cmd = (sys.argv[1] if len(sys.argv) > 1 else "loop").lower()
        if cmd == "once":
            cycle_once()
            return

        while True:
            cycle_once()
            time.sleep(max(10, INTERVAL_SEC))
    finally:
        _release_lock()


if __name__ == "__main__":
    main()
