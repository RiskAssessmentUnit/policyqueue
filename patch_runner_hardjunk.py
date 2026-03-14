from __future__ import annotations
from pathlib import Path
import re, shutil, time

ROOT = Path.home() / "policyqueue"
RP = ROOT / "runner.py"
if not RP.exists():
    raise SystemExit(f"ERROR: runner.py not found at {RP}")

src = RP.read_text(encoding="utf-8", errors="ignore")

# ----------------------------
# 1) Ensure JUNK path exists
# ----------------------------
if re.search(r"(?m)^\s*JUNK\s*=\s*", src) is None:
    insert_after_patterns = [
        r"(?m)^\s*POSTS\s*=\s*.*$",
        r"(?m)^\s*ROOT\s*=\s*.*$",
    ]
    inserted = False
    for pat in insert_after_patterns:
        m = re.search(pat, src)
        if m:
            pos = src.find("\n", m.end())
            if pos == -1:
                pos = len(src)
            block = "\nJUNK = ROOT / \"junk\"\n"
            src = src[:pos] + block + src[pos:]
            inserted = True
            break

    if not inserted:
        m = re.search(r"(?ms)^(?:\s*(?:from|import)\s+[^\n]+\n)+", src)
        if m:
            pos = m.end()
            src = src[:pos] + "\nJUNK = ROOT / \"junk\"\n" + src[pos:]
        else:
            src = "JUNK = ROOT / \"junk\"\n" + src

# ----------------------------------------
# 2) Ensure hard-junk helper fn exists
# ----------------------------------------
marker_fn = "def pq_is_hard_junk_filename("
if marker_fn not in src:
    helper = r'''
def pq_is_hard_junk_filename(name: str) -> bool:
    """
    Fast deterministic junk filter BEFORE any scoring / model work.
    Add patterns here as you discover recurring garbage PDFs.
    """
    n = (name or "").strip().lower()

    # Known bad family: Kansas district map tiles
    if n.startswith("district_map_") and n.endswith(".pdf"):
        return True

    return False
'''.lstrip("\n")

    m = re.search(r"(?ms)^(?:\s*(?:from|import)\s+[^\n]+\n)+", src)
    if m:
        pos = m.end()
        src = src[:pos] + "\n" + helper + "\n" + src[pos:]
    else:
        src = helper + "\n" + src

# ---------------------------------------------------
# 3) Insert gate after *any* PROCESS start log line
# ---------------------------------------------------
gate_tag = "PQ_HARDJUNK_GATE_V1"
if gate_tag not in src:
    m = re.search(r"(?m)^(?P<indent>[ \t]*).*(PROCESS start).*$", src)
    if not m:
        raise SystemExit("ERROR: Could not find any line containing 'PROCESS start' in runner.py")

    indent = m.group("indent")
    line_end = src.find("\n", m.end())
    if line_end == -1:
        line_end = len(src)

    # NOTE: double braces {{ }} are REQUIRED so this patcher doesn't try to evaluate p.*
    gate = f"""\n{indent}# {gate_tag}
{indent}# Hard-junk filter BEFORE any scoring / model work
{indent}if pq_is_hard_junk_filename(getattr(p, "name", str(p))):
{indent}    try:
{indent}        JUNK.mkdir(parents=True, exist_ok=True)
{indent}        out = JUNK / p.name
{indent}        if out.exists():
{indent}            out = JUNK / f"{{{{p.stem}}}}_{{{{int(time.time())}}}}{{{{p.suffix}}}}"
{indent}        p.replace(out)
{indent}        log(f"PROCESS junk-hard {{p.name}} -> junk")
{indent}    except Exception as e:
{indent}        log(f"PROCESS junk-hard {{getattr(p,'name',p)}} -> junk FAILED: {{e}}")
{indent}    continue
"""

    src = src[:line_end] + gate + src[line_end:]

# ----------------------------
# 4) Backup + write
# ----------------------------
bak = RP.with_suffix(f".py.bak.{time.strftime('%Y%m%d-%H%M%S')}")
shutil.copy2(RP, bak)
RP.write_text(src, encoding="utf-8")
print("OK: patched runner.py")
print("Backup:", bak)
