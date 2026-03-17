from __future__ import annotations
from pathlib import Path
import re, shutil, time

RP = Path.home() / "policyqueue" / "runner.py"
if not RP.exists():
    raise SystemExit(f"ERROR: runner.py not found at {RP}")

src = RP.read_text(encoding="utf-8", errors="ignore")

m = re.search(r"(?ms)def\s+pq_is_hard_junk_filename\s*\(.*?\)\s*:\s*\n(.*?)(?=\n\S|\Z)", src)
if not m:
    raise SystemExit("ERROR: pq_is_hard_junk_filename() not found in runner.py")

fn_block = m.group(0)

# Replace the district-map section inside the function (or add it if missing)
if "district_map" in fn_block:
    fn_block2 = re.sub(
        r"(?ms)# Known bad family:.*?return True\s*\n",
        "# Known bad family: Kansas district map tiles\n"
        "    if (n.startswith(\"district_map_\") or n.startswith(\"district_map_h_\")) and n.endswith(\".pdf\"):\n"
        "        return True\n\n",
        fn_block
    )
else:
    # inject rule just before final return False
    fn_block2 = re.sub(
        r"(?m)^\s*return False\s*$",
        "    # Known bad family: Kansas district map tiles\n"
        "    if (n.startswith(\"district_map_\") or n.startswith(\"district_map_h_\")) and n.endswith(\".pdf\"):\n"
        "        return True\n\n"
        "    return False",
        fn_block
    )

src2 = src[:m.start()] + fn_block2 + src[m.end():]

bak = RP.with_suffix(f".py.bak.{time.strftime('%Y%m%d-%H%M%S')}")
shutil.copy2(RP, bak)
RP.write_text(src2, encoding="utf-8")

print("OK: updated pq_is_hard_junk_filename() to match district_map_h_*.pdf")
print("Backup:", bak)
