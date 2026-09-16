"""READ-ONLY probe of the live estate for the S25 prospective re-arm.

Answers exactly four questions and writes NOTHING:
  1. the frozen book's identity (spec hash, inception, membership, marks);
  2. how many shadow books the Stage-8 registry holds, and how many are ACTIVE
     (``advance_shadow_books`` advances every ACTIVE book, so the blast radius
     of re-parenting that producer has to be known before it is wired);
  3. what the owned trailing panel can price, and on which sessions;
  4. whether any canonical forward registration already names this identity.

No mark is produced. The registry is opened ``mode=ro``.
"""
import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

ESTATE = Path(r"D:\Stock_Prediction_app_data")
STAGE8 = ESTATE / "alpha_agent" / "stage8"
REGISTRY = STAGE8 / "tournament.sqlite"
SHADOW_ROOT = STAGE8 / "shadow_books"
PANEL = ESTATE / "phase25_multi_horizon_alpha" / "_inputs" / "current_trailing_prices.csv"
REG_DIR = ESTATE / "forward_challenger_registry" / "registrations"
CAND = "c9_qualityprofi_e490533606"
BOOK_ID = "sb_%s" % CAND
INCEPTION = "2026-08-16"
EXPECT_HASH = "67f0314106f9ce56806170669719a8cc3b035cd16469e984939c11c42956245e"

print("=" * 66)
print("1. THE FROZEN BOOK")
print("=" * 66)
bp = SHADOW_ROOT / BOOK_ID / "shadow_book.json"
print("path exists          =", bp.exists())
book = json.loads(bp.read_text(encoding="utf-8")) if bp.exists() else {}
inc = (book.get("inception") or {})
spec = inc.get("spec") or {}
members = inc.get("membership") or []
print("candidate_id         =", book.get("candidate_id"))
print("shadow_book_id       =", book.get("shadow_book_id"))
print("inception date       =", inc.get("date"), "(expect %s)" % INCEPTION)
print("spec_hash            =", spec.get("spec_hash"))
print("spec_hash matches    =", spec.get("spec_hash") == EXPECT_HASH)
print("horizon_days         =", spec.get("horizon_days"))
print("membership           =", len(members))
print("  LONG               =", sum(1 for m in members if m.get("leg") == "LONG"))
print("  SHORT              =", sum(1 for m in members if m.get("leg") == "SHORT"))
print("  sum(weight)        =", sum(float(m.get("weight") or 0) for m in members))
print("MARKS                =", len(book.get("marks") or []))
print("benchmark            =", inc.get("benchmark"))
print("cost_bps             =", inc.get("cost_bps"))
print("notional             =", inc.get("notional"))
print("read_only            =", book.get("read_only"))
print("operating_portfolio  =", book.get("operating_portfolio"))

print()
print("=" * 66)
print("2. THE STAGE-8 REGISTRY - blast radius of the mark producer")
print("=" * 66)
print("registry exists      =", REGISTRY.exists())
if REGISTRY.exists():
    con = sqlite3.connect("file:%s?mode=ro" % REGISTRY.as_posix(), uri=True)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT shadow_book_id, candidate_id, inception_date, status "
        "FROM shadow_books ORDER BY shadow_book_id").fetchall()
    print("shadow_books rows    =", len(rows))
    for r in rows:
        mp = SHADOW_ROOT / str(r["shadow_book_id"]) / "shadow_book.json"
        nm = 0
        if mp.exists():
            nm = len((json.loads(mp.read_text(encoding="utf-8"))
                      .get("marks") or []))
        print("   %-34s %-10s inception=%s marks=%d"
              % (r["shadow_book_id"], r["status"], r["inception_date"], nm))
    n_active = sum(1 for r in rows if str(r["status"]).upper() == "ACTIVE")
    print("ACTIVE books         =", n_active)
    ch = con.execute("SELECT COUNT(*) c, MAX(recorded_at) m FROM changes").fetchone()
    print("changes rows         =", ch["c"], " last write =", ch["m"])
    con.close()

print()
print("=" * 66)
print("3. THE OWNED TRAILING PANEL")
print("=" * 66)
print("panel exists         =", PANEL.exists())
if PANEL.exists() and members:
    symbols = {str(m["symbol"]).upper() for m in members}
    by_sym = defaultdict(set)
    all_dates = set()
    with PANEL.open(newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            d = row.get("date")
            all_dates.add(d)
            by_sym[(row.get("ticker") or "").upper()].add(d)
    sd = sorted(all_dates)
    print("panel sessions       =", len(sd), " range", sd[0], "..", sd[-1])
    print("SPY present          =", "SPY" in by_sym)
    print("membership in panel  = %d / %d" % (len(symbols & set(by_sym)),
                                              len(symbols)))
    fwd = [d for d in sd if d > INCEPTION]
    print("sessions > inception =", len(fwd), (fwd[0] if fwd else ""),
          "..", (fwd[-1] if fwd else ""))
    for d in sd[-6:]:
        priced = sum(1 for s in symbols if d in by_sym.get(s, ()))
        print("   %s priced %3d/100  spy=%s"
              % (d, priced, d in by_sym.get("SPY", ())))

print()
print("=" * 66)
print("4. CANONICAL FORWARD REGISTRATIONS")
print("=" * 66)
print("registry dir exists  =", REG_DIR.exists())
if REG_DIR.exists():
    files = sorted(REG_DIR.glob("*.json"))
    print("registrations        =", len(files))
    for f in files:
        d = json.loads(f.read_text(encoding="utf-8"))
        print("   %-46s %s" % (str(d.get("challenger_id"))[:46],
                               d.get("originating_release")))
    print("names S25            =",
          any(CAND in json.dumps(json.loads(f.read_text(encoding="utf-8")))
              for f in files))
