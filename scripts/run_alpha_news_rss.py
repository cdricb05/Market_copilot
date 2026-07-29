"""
scripts/run_alpha_news_rss.py — Alpha Agent Stage 3.5 News/RSS-Atom CLI.

Deterministic driver for the generalized News/RSS layer. Loads the feed
registry, audits/collects official RSS/Atom feeds with conditional polling,
archives raw objects immutably, normalizes into the shared PIT record contract,
clusters the same event across RSS + Stage 2 sources (read-only) and writes an
immutable run package. Never calls a model API or the prediction service, never
connects to PostgreSQL, never mutates Paper Trader data, never prints secrets.

Modes:
    --mode audit        probe + validate enabled feeds; no normalized persistence
    --mode collect      bounded real collection; publishes latest.json on success
    --mode incremental  resume from checkpoints; NO_NEW_RSS_DATA when nothing new
    --mode verify       validate the latest run; no network, writes nothing

Terminal lines (exactly one):
    ALPHA_AGENT_STAGE3_5_READY
    ALPHA_AGENT_STAGE3_5_PARTIAL — <blocked or missing feed summary>
    NO_NEW_RSS_DATA
    ALPHA_AGENT_STAGE3_5_VERIFIED
    ALPHA_AGENT_STAGE3_5_BLOCKED — <exact reason>

Example (Windows PowerShell):
    C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      scripts\\run_alpha_news_rss.py `
      --config configs\\alpha_agent\\stage3_5_news_rss.json `
      --output-root D:\\Stock_Prediction_app_data\\alpha_agent\\news_rss `
      --mode collect `
      --as-of latest `
      --json
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import feed_registry as fr  # noqa: E402


def _git(args: list[str]) -> str:
    try:
        out = subprocess.run(["git", "-C", str(_REPO)] + args,
                             capture_output=True, text=True, timeout=15)
        if out.returncode == 0:
            return out.stdout.strip()
    except Exception:  # noqa: BLE001
        pass
    return ""


def _resolve_feeds_path(config: dict, config_path: Path) -> Path:
    ref = config.get("feed_registry_config")
    if not ref:
        raise ValueError("config missing feed_registry_config")
    p = Path(ref)
    if p.is_absolute() and p.exists():
        return p
    for base in (_REPO, config_path.parent, Path.cwd()):
        cand = base / ref
        if cand.exists():
            return cand
    return _REPO / ref


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Alpha Agent Stage 3.5 News/RSS-Atom collection layer.")
    ap.add_argument("--config", required=True, help="Stage 3.5 config JSON path.")
    ap.add_argument("--output-root", required=True,
                    help="News/RSS output root (state/raw/normalized/clusters/runs).")
    ap.add_argument("--mode", default="collect",
                    choices=["audit", "collect", "incremental", "verify"])
    ap.add_argument("--as-of", default="latest", dest="as_of",
                    help="Business date YYYY-MM-DD or 'latest'.")
    ap.add_argument("--json", action="store_true",
                    help="Emit a JSON result summary to stdout.")
    args = ap.parse_args(argv)

    cfg_path = Path(args.config)
    try:
        config = json.loads(cfg_path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        print("%s — cannot read config %s: %s" % (fr.BLOCKED, cfg_path, exc))
        return 2

    feeds_config: dict = {}
    if args.mode != "verify":
        try:
            feeds_path = _resolve_feeds_path(config, cfg_path)
            feeds_config = json.loads(feeds_path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError) as exc:
            print("%s — cannot read feed registry: %s" % (fr.BLOCKED, exc))
            return 2

    git_commit = _git(["rev-parse", "HEAD"]) or "UNKNOWN"
    contact_email = _git(["config", "user.email"]) or None

    try:
        result = fr.run_news_rss(
            config=config, feeds_config=feeds_config, output_root=args.output_root,
            mode=args.mode, as_of=args.as_of, git_commit=git_commit,
            contact_email=contact_email)
    except Exception as exc:  # noqa: BLE001 — one clean BLOCKED line
        print("%s — %s: %s" % (fr.BLOCKED, type(exc).__name__, exc))
        return 1

    if args.json:
        summary = {k: result.get(k) for k in
                   ("status", "terminal", "run_id", "run_dir", "as_of", "reason",
                    "counts", "enabled_feeds", "healthy_feeds", "failed_feeds",
                    "already_existed", "ledger_unchanged", "dq_failures")}
        print(json.dumps(summary, indent=1, default=str))

    status = result.get("status")
    if status == fr.READY:
        print(fr.READY)
        return 0
    if status == fr.PARTIAL:
        print(result.get("terminal")
              or "%s — partial coverage" % fr.PARTIAL)
        return 0
    if status == fr.NO_NEW:
        print(fr.NO_NEW)
        return 0
    if status == fr.VERIFIED:
        print(fr.VERIFIED)
        return 0
    print("%s — %s" % (fr.BLOCKED, result.get("reason", "unknown")))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
