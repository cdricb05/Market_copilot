"""scripts/check_ui_js.py — validate every inline <script> block in the operator UI.

Extracts each inline JavaScript ``<script>`` block from ``api/ui/index.html``
(skipping external ``src=`` scripts and non-JS ``type=`` blocks such as
``application/json``) and runs ``node --check`` on it. Reports the number of
checked blocks and the number of syntax errors. Exit code 0 only when at least
one block was checked and there were zero errors — this is the JS-syntax gate the
Slice 2 handoff ``validate.ps1`` relies on (no fragile inline node one-liners).

Usage:
    python scripts/check_ui_js.py [path/to/index.html]
"""
from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

_SCRIPT_RE = re.compile(r"(?is)<script([^>]*)>(.*?)</script>")
# A script block is inline JavaScript unless it has a src= (external) or a
# non-JavaScript type= (e.g. application/json, text/template).
_SRC_RE = re.compile(r"(?i)\bsrc\s*=")
_TYPE_RE = re.compile(r"""(?i)\btype\s*=\s*['"]?([^'">\s]+)""")
_JS_TYPES = {"", "text/javascript", "application/javascript", "module",
             "text/ecmascript", "application/ecmascript"}


def _inline_js_blocks(html: str) -> list[str]:
    blocks: list[str] = []
    for m in _SCRIPT_RE.finditer(html):
        attrs, body = m.group(1) or "", m.group(2) or ""
        if _SRC_RE.search(attrs):
            continue
        tm = _TYPE_RE.search(attrs)
        if tm and tm.group(1).lower() not in _JS_TYPES:
            continue
        if body.strip():
            blocks.append(body)
    return blocks


def main() -> int:
    ui = Path(sys.argv[1]) if len(sys.argv) > 1 else (
        Path(__file__).resolve().parent.parent / "api" / "ui" / "index.html")
    html = ui.read_text(encoding="utf-8")
    blocks = _inline_js_blocks(html)
    errors = 0
    with tempfile.TemporaryDirectory() as td:
        for i, body in enumerate(blocks):
            f = Path(td) / ("block_%03d.js" % i)
            f.write_text(body, encoding="utf-8")
            proc = subprocess.run(["node", "--check", str(f)],
                                  capture_output=True, text=True)
            if proc.returncode != 0:
                errors += 1
                print("JS SYNTAX ERROR in inline block #%d:" % i)
                print((proc.stderr or proc.stdout).strip()[:2000])
    print("checked_blocks=%d errors=%d" % (len(blocks), errors))
    if len(blocks) == 0:
        print("NO_SCRIPT_BLOCKS_FOUND")
        return 2
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
