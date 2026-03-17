from __future__ import annotations
from pathlib import Path
import re, shutil, time

RP = Path.home() / "policyqueue" / "runner.py"
if not RP.exists():
    raise SystemExit(f"ERROR: runner.py not found: {RP}")

src = RP.read_text(encoding="utf-8", errors="ignore")

if "PQ_BIGPDF_GATE_V1" in src:
    print("OK: PQ_BIGPDF_GATE_V1 already present.")
    raise SystemExit(0)

# We expect PQ_HARDJUNK_GATE_V1 block exists (you confirmed it)
needle = "# PQ_HARDJUNK_GATE_V1"
ix = src.find(needle)
if ix < 0:
    raise SystemExit("ERROR: Could not find # PQ_HARDJUNK_GATE_V1 in runner.py")

# Insert immediately AFTER the hardjunk gate block.
# Find the next blank line after the hardjunk gate starts, then insert our block there.
after = src.find("\n\n", ix)
if after < 0:
    after = ix

insert = r'''
    # PQ_BIGPDF_GATE_V1
    # If a PDF is too large, move it aside BEFORE any scoring / model work.
    # Tune threshold as you like.
    BIGPDF_BYTES = int(os.getenv("PQ_BIGPDF_BYTES", "8000000"))  # 8MB default
    try:
        sz = int(p.stat().st_size)
    except Exception:
        sz = 0
    if sz >= BIGPDF_BYTES:
        try:
            BIGPDFS.mkdir(parents=True, exist_ok=True)
            out = BIGPDFS / p.name
            if out.exists():
                out = BIGPDFS / f"{p.stem}_{int(time.time())}{p.suffix}"
            p.replace(out)
            log(f"PROCESS bigpdf {p.name} bytes={sz} -> {out.name}")
        except Exception as e:
            log(f"PROCESS bigpdf move failed {getattr(p,'name',str(p))}: {e}")
        return
'''

# Safety: ensure os is imported (runner.py may or may not)
if not re.search(r"(?m)^\s*import\s+os\s*$", src) and "import os" not in src:
    # insert after first import block
    src = re.sub(r"(?m)^(import .+\r?\n)", r"\1import os\n", src, count=1)

src2 = src[:after+2] + insert + src[after+2:]

bak = RP.with_suffix(f".py.bak.{time.strftime('%Y%m%d-%H%M%S')}")
shutil.copy2(RP, bak)
RP.write_text(src2, encoding="utf-8")

print("OK: inserted PQ_BIGPDF_GATE_V1 (default 8MB).")
print("Backup:", bak)
