"""
alpha_agent/collectors/sec_edgar.py — SOURCE 3: SEC EDGAR (official public interfaces).

* Official endpoints only (www.sec.gov files + daily index).
* Compliant User-Agent: "<product> <contact-email>", with the contact resolved
  from git config user.email by the CLI and passed in via config runtime — the
  full email is NEVER printed in logs/health (masked form only).
* If no contact email can be resolved the source is marked
  BLOCKED_MISSING_USER_AGENT_CONTACT and Stage 2 continues.
* Conservative client-side rate limit (min_interval_seconds, well below SEC's
  10 req/s guidance) and a tiny bounded request count.
* Collects: the ticker/CIK identity map + a bounded recent daily-index filing
  window (metadata + official links; filing text NOT stored by default).
* The daily index provides the FILING DATE only. Acceptance and publication
  timestamps are NOT in the index and are left null with explicit quality
  warnings — never fabricated, and period-end is never substituted.
"""
from __future__ import annotations

import datetime as _dt
import json
import re
import xml.etree.ElementTree as _ET
from pathlib import Path
from typing import Optional

from ..source_contracts import (
    EM_AMBIGUOUS, EM_MATCHED_EXACT, EM_UNMATCHED, RT_EARNINGS_EVENT,
    RT_FILING_EVENT, RT_FUNDAMENTAL_FACT, RT_INSIDER_FILING,
    RT_SECURITY_IDENTITY, SH_BLOCKED_CONFIGURATION, SH_DEGRADED, SH_FAILED,
    SH_HEALTHY, build_normalized_record,
)
from .base import BaseCollector

# Default bounded us-gaap concept allowlist for the companyfacts / companyconcept
# lanes (kept small so record volume stays bounded; overridable via config).
_DEFAULT_FACT_CONCEPTS = (
    "Revenues", "NetIncomeLoss", "Assets", "Liabilities",
    "StockholdersEquity", "EarningsPerShareDiluted",
    "CashAndCashEquivalentsAtCarryingValue",
    "OperatingIncomeLoss")

BLOCKED_MISSING_USER_AGENT_CONTACT = "BLOCKED_MISSING_USER_AGENT_CONTACT"


def _mask_email(email: str) -> str:
    if "@" not in email:
        return "***"
    local, _, domain = email.partition("@")
    return "%s***@%s***" % (local[:1], domain[:1])


def _business_days_back(as_of: str, count: int) -> list[str]:
    """The last `count` weekdays ending at as_of (inclusive when a weekday)."""
    out: list[str] = []
    day = _dt.date.fromisoformat(as_of)
    while len(out) < count:
        if day.weekday() < 5:
            out.append(day.isoformat())
        day -= _dt.timedelta(days=1)
    return out


# --------------------------------------------------------------------------- #
# Form 4 (ownership) XML parsing — official SEC filing documents only.
# --------------------------------------------------------------------------- #
def _ln(tag: str) -> str:
    """Local (namespace-stripped) element name."""
    return tag.rsplit("}", 1)[-1]


def _child(el, name):
    if el is None:
        return None
    for c in list(el):
        if _ln(c.tag) == name:
            return c
    return None


def _child_text(el, *path):
    cur = el
    for name in path:
        cur = _child(cur, name)
        if cur is None:
            return None
    text = (cur.text or "").strip()
    return text or None


def _value(el, name):
    """A Form 4 leaf like <transactionShares><value>N</value></transactionShares>."""
    return _child_text(el, name, "value")


def _bool01(v) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("1", "true", "yes"):
        return True
    if s in ("0", "false", "no"):
        return False
    return None


def extract_ownership_xml(body: bytes) -> Optional[bytes]:
    """Extract the raw ``<ownershipDocument>...</ownershipDocument>`` block from a
    full SEC submission ``.txt`` (the SGML wrapper embeds the Form 4 XML between
    <XML> tags). Returns the XML bytes, or None if no ownership block is present.
    This is robust to the primaryDocument being an HTML rendering / xsl wrapper."""
    try:
        text = body.decode("utf-8", "replace")
    except Exception:  # noqa: BLE001
        text = body.decode("latin-1", "replace")
    start = text.find("<ownershipDocument")
    end = text.rfind("</ownershipDocument>")
    if start == -1 or end == -1 or end < start:
        return None
    return text[start:end + len("</ownershipDocument>")].encode("utf-8")


def parse_form4_xml(body: bytes) -> Optional[dict]:
    """Parse an official Form 3/4/5 ownership XML document into normalized issuer,
    reporting-owner and transaction fields. Accepts either a bare ownership XML or
    a full submission .txt (the embedded ownership block is extracted). Returns
    None if the payload is not an ownership document (never guesses)."""
    try:
        root = _ET.fromstring(body)
    except _ET.ParseError:
        extracted = extract_ownership_xml(body)
        if extracted is None:
            return None
        try:
            root = _ET.fromstring(extracted)
        except _ET.ParseError:
            return None
    if _ln(root.tag) != "ownershipDocument":
        # A full submission .txt parsed as its outer element — pull the block out.
        extracted = extract_ownership_xml(body)
        if extracted is None:
            return None
        try:
            root = _ET.fromstring(extracted)
        except _ET.ParseError:
            return None
        if _ln(root.tag) != "ownershipDocument":
            return None
    issuer = _child(root, "issuer")
    owner = _child(root, "reportingOwner")
    owner_id = _child(owner, "reportingOwnerId")
    rel = _child(owner, "reportingOwnerRelationship")
    out = {
        "document_type": _child_text(root, "documentType"),
        "period_of_report": _child_text(root, "periodOfReport"),
        "not_subject_to_section16": _child_text(root, "notSubjectToSection16"),
        "issuer": {
            "cik": _child_text(issuer, "issuerCik"),
            "name": _child_text(issuer, "issuerName"),
            "trading_symbol": _child_text(issuer, "issuerTradingSymbol")},
        "reporting_owner": {
            "cik": _child_text(owner_id, "rptOwnerCik"),
            "name": _child_text(owner_id, "rptOwnerName"),
            "is_director": _bool01(_child_text(rel, "isDirector")),
            "is_officer": _bool01(_child_text(rel, "isOfficer")),
            "is_ten_percent_owner": _bool01(_child_text(rel, "isTenPercentOwner")),
            "is_other": _bool01(_child_text(rel, "isOther")),
            "officer_title": _child_text(rel, "officerTitle")},
        "transactions": [],
    }
    for table, is_deriv in (("nonDerivativeTable", False),
                            ("derivativeTable", True)):
        tbl = _child(root, table)
        if tbl is None:
            continue
        txn_tag = "derivativeTransaction" if is_deriv else "nonDerivativeTransaction"
        for txn in [c for c in list(tbl) if _ln(c.tag) == txn_tag]:
            coding = _child(txn, "transactionCoding")
            amounts = _child(txn, "transactionAmounts")
            post = _child(txn, "postTransactionAmounts")
            nature = _child(txn, "ownershipNature")
            out["transactions"].append({
                "is_derivative": is_deriv,
                "security_title": _value(txn, "securityTitle"),
                "transaction_date": _value(txn, "transactionDate"),
                "transaction_form_type": _child_text(coding, "transactionFormType"),
                "transaction_code": _child_text(coding, "transactionCode"),
                "equity_swap_involved": _child_text(coding, "equitySwapInvolved"),
                "shares": _value(amounts, "transactionShares"),
                "price_per_share": _value(amounts, "transactionPricePerShare"),
                "acquired_disposed": _value(amounts,
                                            "transactionAcquiredDisposedCode"),
                "shares_owned_following":
                    _value(post, "sharesOwnedFollowingTransaction"),
                "direct_or_indirect":
                    _value(nature, "directOrIndirectOwnership"),
                "conversion_or_exercise_price":
                    _value(txn, "conversionOrExercisePrice") if is_deriv else None,
            })
    return out


# --------------------------------------------------------------------------- #
# 8-K Item 2.02 (results of operations) — official filing text only.
# --------------------------------------------------------------------------- #
_ITEM_202_RE = re.compile(r"item[\s ]*2\.02", re.IGNORECASE)
_EPS_RE = re.compile(
    r"(?:diluted\s+)?(?:earnings|eps|net\s+income)[^\n\r]{0,60}?"
    r"\$?\(?\s*([0-9]+\.[0-9]{1,2})\s*\)?\s*per\s+(?:diluted\s+)?share",
    re.IGNORECASE)
_REVENUE_RE = re.compile(
    r"(?:total\s+)?(?:net\s+)?revenue[s]?[^\n\r]{0,40}?\$?\s*"
    r"([0-9]{1,3}(?:[,\.][0-9]{3})*(?:\.[0-9]+)?)\s*(billion|million|thousand)?",
    re.IGNORECASE)


# Two header forms carry the contemporaneous SIC: the tag form
# (``<ASSIGNED-SIC>3571</ASSIGNED-SIC>`` / ``ASSIGNED-SIC: 3571``) and the modern
# human-readable form (``STANDARD INDUSTRIAL CLASSIFICATION: ELECTRONIC COMPUTERS
# [3571]``). The bracketed code is the reliable modern one.
_ASSIGNED_SIC_RE = re.compile(r"ASSIGNED-SIC[:>\s]*?(\d{2,4})", re.IGNORECASE)
_SIC_BRACKET_RE = re.compile(
    r"STANDARD INDUSTRIAL CLASSIFICATION[^\[\n\r]*\[(\d{2,4})\]", re.IGNORECASE)


def extract_assigned_sic(body: bytes) -> Optional[str]:
    """Extract the contemporaneous SIC from a filing's SGML header (the region
    before the first <DOCUMENT>). This is the SIC AT the filing's acceptance time
    — the point-in-time-correct classification, unlike the current-only
    submissions ``sic`` field. Supports both the ``ASSIGNED-SIC`` tag form and the
    modern ``STANDARD INDUSTRIAL CLASSIFICATION: NAME [NNNN]`` form. Returns None
    when absent (never guessed)."""
    try:
        text = body.decode("utf-8", "replace")
    except Exception:  # noqa: BLE001
        text = body.decode("latin-1", "replace")
    header = text.split("<DOCUMENT>", 1)[0][:40000]
    m = _SIC_BRACKET_RE.search(header) or _ASSIGNED_SIC_RE.search(header)
    return m.group(1) if m else None


def _strip_tags(html: str) -> str:
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = (text.replace("&nbsp;", " ").replace("&amp;", "&")
            .replace("&#160;", " ").replace("&#8217;", "'"))
    return re.sub(r"[ \t ]+", " ", text)


def extract_item_202(text_or_html: bytes) -> dict:
    """Detect an 8-K Item 2.02 (Results of Operations) disclosure and safely
    extract EPS / revenue figures WHEN PRESENT in the filing text. Never
    fabricates a value from later data — a value absent from THIS document stays
    None."""
    try:
        raw = text_or_html.decode("utf-8", "replace")
    except Exception:  # noqa: BLE001
        raw = text_or_html.decode("latin-1", "replace")
    text = _strip_tags(raw)
    has_item_202 = bool(_ITEM_202_RE.search(text))
    eps_m = _EPS_RE.search(text) if has_item_202 else None
    rev_m = _REVENUE_RE.search(text) if has_item_202 else None
    eps = None
    if eps_m:
        try:
            eps = float(eps_m.group(1))
        except ValueError:
            eps = None
    revenue_raw = rev_m.group(0).strip()[:120] if rev_m else None
    return {"has_item_202": has_item_202,
            "eps_actual": eps,
            "revenue_text": revenue_raw,
            "guidance_present": bool(re.search(
                r"guidance|outlook|expects|anticipat", text, re.IGNORECASE))
            if has_item_202 else False}


class SecEdgarCollector(BaseCollector):
    source_id = "sec_edgar"
    requires_credential = False

    def _contact(self) -> Optional[str]:
        return (self.ctx.config.get("_runtime", {}) or {}).get("contact_email")

    def _ua(self, contact: str) -> str:
        product = self.ctx.config.get("user_agent", {}).get(
            "product", "paper-trader-alpha-agent/2.0")
        return "%s %s" % (product, contact)

    def _blocked_no_contact(self) -> dict:
        return self.blocked_result(
            SH_BLOCKED_CONFIGURATION,
            "%s: no contact email resolvable from git config user.email; SEC "
            "requires a User-Agent with contact information"
            % BLOCKED_MISSING_USER_AGENT_CONTACT)

    # ------------------------------------------------------------------ #
    def _fetch_ticker_map(self, headers: dict, as_of: str) -> Optional[dict]:
        url = self.ctx.source_cfg.get("base_url_www", "https://www.sec.gov").rstrip("/") \
            + self.ctx.source_cfg.get("ticker_map_path", "/files/company_tickers.json")
        res = self.fetch(url, headers=headers, expect="text", extension="json",
                         business_date=as_of, native_id="company_tickers",
                         content_type="application/json")
        if not res["ok"]:
            return None
        try:
            obj = json.loads(res["body"].decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            self.record_error("PARSE_ERROR", "company_tickers.json not parseable JSON")
            return None
        count = 0
        cik_to_tickers: dict[str, list[str]] = {}
        for row in (obj.values() if isinstance(obj, dict) else []):
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "").upper()
            cik = str(row.get("cik_str") or "")
            if not ticker or not cik:
                continue
            self.ctx.identity.register_cik(ticker, cik, "sec_company_tickers")
            cik_to_tickers.setdefault(cik.lstrip("0") or "0", []).append(ticker)
            count += 1
        self.inventory["ticker_map_entries"] = count
        return {"raw": res["raw"], "cik_to_tickers": cik_to_tickers}

    def _parse_master_idx(self, body: bytes) -> list[dict]:
        rows: list[dict] = []
        try:
            text = body.decode("latin-1")
        except Exception:  # noqa: BLE001
            return rows
        in_table = False
        for line in text.splitlines():
            if not in_table:
                if line.upper().startswith("CIK|"):
                    in_table = True
                continue
            if set(line.strip()) <= {"-"}:
                continue
            parts = line.split("|")
            if len(parts) != 5:
                continue
            cik, company, form, date_filed, filename = (p.strip() for p in parts)
            rows.append({"cik": cik, "company_name": company, "form_type": form,
                         "date_filed": date_filed, "filename": filename})
        return rows

    @staticmethod
    def _accession_from_filename(filename: str) -> str:
        tail = filename.rsplit("/", 1)[-1]
        return tail[:-4] if tail.lower().endswith(".txt") else tail

    @staticmethod
    def _iso_date(value: str) -> str:
        """Normalize the index's date form (compact YYYYMMDD or dashed) to
        dashed ISO. Unrecognized values pass through unchanged (never guessed)."""
        text = (value or "").strip()
        if len(text) == 8 and text.isdigit():
            return "%s-%s-%s" % (text[:4], text[4:6], text[6:8])
        return text

    @staticmethod
    def _cik10(cik: str) -> str:
        """Zero-pad a CIK to the 10-digit form data.sec.gov paths require."""
        digits = "".join(ch for ch in str(cik) if ch.isdigit())
        return digits.zfill(10) if digits else ""

    def _bounded_ciks(self, cik_to_tickers: dict, as_of: str) -> list:
        """Resolve the pre-registered fundamentals sample tickers to a small,
        deterministic list of (cik10, ticker) pairs for the data.sec.gov lanes.

        The SEC source config carries its OWN ``facts_sample_symbols`` so the
        structured lanes work when SEC is collected as a single source (the
        durable-queue path), not only when the eodhd config happens to be
        present in the same run."""
        srcs = self.ctx.config.get("sources", {})
        sample = [s.split(".")[0].upper() for s in
                  (self.ctx.source_cfg.get("facts_sample_symbols")
                   or srcs.get("eodhd", {}).get("fundamentals_sample_symbols")
                   or srcs.get("eodhd", {}).get("sample_symbols") or [])]
        limit = int(self.ctx.source_cfg.get("facts_symbol_limit", 4))
        out, seen = [], set()
        for ticker in sample:
            resolved = self.ctx.identity.resolve(ticker)
            cik = resolved.get("cik")
            if cik and cik not in seen:
                seen.add(cik)
                out.append((self._cik10(cik), ticker))
            if len(out) >= limit:
                break
        return out

    def _collect_submissions(self, headers: dict, ciks: list, as_of: str,
                             retrieved: str) -> int:
        """Per-CIK filing history from data.sec.gov/submissions — carries the
        acceptanceDateTime PIT anchor for EVERY form (incl. Form 4 + 8-K), which
        the bare daily index lacks. Bounded to the recent filing slice."""
        base = self.ctx.source_cfg.get("base_url_data", "https://data.sec.gov")
        cap = int(self.ctx.source_cfg.get("submissions_recent_cap", 40))
        ok = 0
        for cik10, ticker in ciks:
            res = self.fetch("%s/submissions/CIK%s.json" % (base, cik10),
                             headers=headers, expect="text", extension="json",
                             business_date=as_of,
                             native_id="submissions|%s" % cik10,
                             content_type="application/json")
            if not res["ok"]:
                continue
            try:
                obj = json.loads(res["body"].decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                self.record_error("PARSE_ERROR", "submissions %s not JSON" % cik10)
                continue
            recent = ((obj.get("filings") or {}).get("recent") or {})
            forms = recent.get("form") or []
            acc = recent.get("accessionNumber") or []
            fdate = recent.get("filingDate") or []
            adt = recent.get("acceptanceDateTime") or []
            rdate = recent.get("reportDate") or []
            pdoc = recent.get("primaryDocument") or []
            n = min(len(forms), len(acc), len(fdate))
            emitted = 0
            for i in range(n):
                if emitted >= cap:
                    break
                form = str(forms[i])
                accession = str(acc[i])
                filing_date = str(fdate[i])[:10]
                acceptance = str(adt[i]) if i < len(adt) and adt[i] else None
                report_date = str(rdate[i])[:10] if i < len(rdate) and rdate[i] \
                    else None
                rt = RT_INSIDER_FILING if form.startswith("4") else RT_FILING_EVENT
                is_earn_8k = form.startswith("8-K")
                primary_document = pdoc[i] if i < len(pdoc) else None
                if form.startswith("4"):
                    self._form4_candidates.append({
                        "cik10": cik10, "ticker": ticker, "accession": accession,
                        "primary_document": primary_document,
                        "acceptance": acceptance, "form": form})
                if is_earn_8k:
                    self._earn8k_candidates.append({
                        "cik10": cik10, "ticker": ticker, "accession": accession,
                        "primary_document": primary_document,
                        "acceptance": acceptance, "report_date": report_date,
                        "form": form})
                payload = {
                    "cik": cik10.lstrip("0") or "0", "ticker": ticker,
                    "form_type": form, "accession_number": accession,
                    "filing_date": filing_date,
                    "acceptance_datetime": acceptance,
                    "report_date": report_date,
                    "primary_document": pdoc[i] if i < len(pdoc) else None,
                    "official_link": "%s/Archives/edgar/data/%s/%s/%s" % (
                        base.replace("data.", "www."), cik10.lstrip("0"),
                        accession.replace("-", ""),
                        pdoc[i] if i < len(pdoc) else ""),
                    "is_form4_insider": form.startswith("4"),
                    "is_8k": is_earn_8k,
                    "item_202_note": ("8-K Item 2.02 (results of operations) "
                                      "sub-classification requires the primary "
                                      "document body; captured as an 8-K filing "
                                      "event with its PIT acceptance timestamp"
                                      if is_earn_8k else None),
                }
                self.records.append(build_normalized_record(
                    record_type=rt, source_id=self.source_id,
                    source_native_id="sub|%s|%s" % (cik10, accession),
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved,
                    observed_at=report_date or filing_date,
                    effective_at=filing_date,
                    available_at=acceptance,  # true PIT anchor
                    ticker=ticker, company_id=cik10.lstrip("0") or "0",
                    event_type=form, payload=payload,
                    entity_mapping_confidence=EM_MATCHED_EXACT,
                    provenance="SEC EDGAR submissions API (official)",
                    quality_warnings=(
                        [] if acceptance else
                        ["ACCEPTANCE_TIME_UNKNOWN: submissions row lacked "
                         "acceptanceDateTime; available_at left null"])))
                self.note_event_time(acceptance or filing_date)
                emitted += 1
            self.inventory.setdefault("submissions_by_cik", {})[cik10] = emitted
            ok += 1
        return ok

    def _archive_url(self, cik10: str, accession: str, doc: str) -> str:
        base = self.ctx.source_cfg.get("base_url_www",
                                       "https://www.sec.gov").rstrip("/")
        return "%s/Archives/edgar/data/%s/%s/%s" % (
            base, cik10.lstrip("0") or "0", accession.replace("-", ""), doc)

    def _emit_assigned_sic(self, body: bytes, *, cik: str, ticker,
                           acceptance, form: str, accession: str, raw_id: str,
                           retrieved: str) -> None:
        """WS5: emit a point-in-time ASSIGNED-SIC observation (RT_SECURITY_IDENTITY)
        from a filing header. availability = SEC acceptance time (contemporaneous;
        no classification look-ahead). Feeds the leakage-safe PIT sector series."""
        sic = extract_assigned_sic(body)
        if not sic:
            return
        cik_key = str(cik).lstrip("0") or "0"
        self.records.append(build_normalized_record(
            record_type=RT_SECURITY_IDENTITY, source_id=self.source_id,
            source_native_id="assigned_sic|%s|%s" % (cik_key, accession),
            raw_object_id=raw_id, retrieved_at=retrieved,
            observed_at=str(acceptance)[:10] if acceptance else None,
            effective_at=str(acceptance)[:10] if acceptance else None,
            available_at=acceptance, ticker=ticker, company_id=cik_key,
            event_type="ASSIGNED_SIC_PIT",
            payload={"cik": cik_key, "ticker": ticker, "assigned_sic": sic,
                     "acceptance_time": acceptance, "source_form": form,
                     "accession_number": accession,
                     "note": "contemporaneous ASSIGNED-SIC from filing header; "
                             "point-in-time classification (no look-ahead)"},
            entity_mapping_confidence=EM_MATCHED_EXACT,
            provenance="SEC EDGAR filing header ASSIGNED-SIC (official, PIT)"))
        self.inventory["assigned_sic_pit"] = \
            self.inventory.get("assigned_sic_pit", 0) + 1

    def _collect_form4_transactions(self, headers: dict, as_of: str,
                                    retrieved: str) -> int:
        """WS2: fetch the OFFICIAL Form 4 ownership XML for a bounded set of the
        Form 4 filings identified in the submissions lane and normalize the
        transaction table (issuer, reporting owner + relationship, transaction
        code, shares, price, acquired/disposed, direct/indirect, post-transaction
        holdings, derivative flag). PIT availability = SEC acceptance time.
        Amendments (4/A) are emitted as distinct append-only records (their
        accession differs) — an amendment never overwrites the original."""
        cap = int(self.ctx.source_cfg.get("form4_xml_cap", 8))
        candidates = self._form4_candidates[:cap]
        txns = 0
        issuers = set()
        filings_parsed = 0
        for cand in candidates:
            # Fetch the FULL official submission text (the SGML wrapper embeds the
            # raw ownershipDocument XML). Robust to the primaryDocument being an
            # HTML rendering / xsl wrapper.
            txt_doc = "%s.txt" % cand["accession"]
            doc = txt_doc
            res = self.fetch(
                self._archive_url(cand["cik10"], cand["accession"], txt_doc),
                headers=headers, expect="html", extension="txt",
                business_date=as_of,
                native_id="form4|%s" % cand["accession"],
                content_type="text/plain", allow_404=True)
            if res.get("not_found") or not res["ok"]:
                continue
            parsed = parse_form4_xml(res["body"])
            if not parsed:
                self.inventory.setdefault("form4_no_ownership_block", 0)
                self.inventory["form4_no_ownership_block"] += 1
                continue
            filings_parsed += 1
            is_amend = str(cand.get("form", "")).endswith("/A")
            issuer = parsed.get("issuer") or {}
            owner = parsed.get("reporting_owner") or {}
            issuer_cik = str(issuer.get("cik") or cand["cik10"]).lstrip("0") or "0"
            ticker = issuer.get("trading_symbol") or cand.get("ticker")
            acceptance = cand.get("acceptance")
            issuers.add(issuer_cik)
            # WS5: contemporaneous ASSIGNED-SIC (the issuer's, from the header).
            self._emit_assigned_sic(res["body"], cik=issuer_cik, ticker=ticker,
                                    acceptance=acceptance, form=cand.get("form"),
                                    accession=cand["accession"],
                                    raw_id=res["raw"]["raw_object_id"],
                                    retrieved=retrieved)
            for idx, t in enumerate(parsed.get("transactions") or []):
                txn_date = t.get("transaction_date")
                payload = {
                    "issuer_cik": issuer_cik,
                    "issuer_name": issuer.get("name"),
                    "issuer_trading_symbol": issuer.get("trading_symbol"),
                    "reporting_owner_cik": owner.get("cik"),
                    "reporting_owner_name": owner.get("name"),
                    "is_director": owner.get("is_director"),
                    "is_officer": owner.get("is_officer"),
                    "is_ten_percent_owner": owner.get("is_ten_percent_owner"),
                    "is_other": owner.get("is_other"),
                    "officer_title": owner.get("officer_title"),
                    "security_title": t.get("security_title"),
                    "transaction_date": txn_date,
                    "transaction_code": t.get("transaction_code"),
                    "transaction_form_type": t.get("transaction_form_type"),
                    "acquired_disposed": t.get("acquired_disposed"),
                    "shares": t.get("shares"),
                    "price_per_share": t.get("price_per_share"),
                    "shares_owned_following": t.get("shares_owned_following"),
                    "direct_or_indirect": t.get("direct_or_indirect"),
                    "is_derivative": t.get("is_derivative"),
                    "conversion_or_exercise_price":
                        t.get("conversion_or_exercise_price"),
                    "accession_number": cand["accession"],
                    "document_type": parsed.get("document_type"),
                    "is_amendment": is_amend,
                    "acceptance_datetime": acceptance,
                    "official_link": self._archive_url(cand["cik10"],
                                                       cand["accession"], doc),
                }
                self.records.append(build_normalized_record(
                    record_type=RT_INSIDER_FILING, source_id=self.source_id,
                    source_native_id="form4txn|%s|%d" % (cand["accession"], idx),
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=txn_date,
                    effective_at=txn_date or (str(acceptance)[:10]
                                             if acceptance else None),
                    available_at=acceptance, ticker=ticker,
                    company_id=issuer_cik,
                    event_type=t.get("transaction_code") or "FORM4_TXN",
                    payload=payload,
                    entity_mapping_confidence=(EM_MATCHED_EXACT
                                               if issuer.get("cik") else EM_UNMATCHED),
                    provenance=("SEC EDGAR Form 4 ownership XML (official, "
                                "transaction-level%s)" % (" — AMENDMENT"
                                                          if is_amend else "")),
                    quality_warnings=(
                        [] if acceptance else
                        ["ACCEPTANCE_TIME_UNKNOWN: no acceptanceDateTime on the "
                         "submissions row; available_at left null"])))
                self.note_event_time(acceptance or txn_date)
                txns += 1
        self.inventory["form4_filings_parsed"] = filings_parsed
        self.inventory["form4_transactions"] = txns
        self.inventory["form4_distinct_issuers"] = len(issuers)
        self.inventory["form4_candidates_seen"] = len(self._form4_candidates)
        return txns

    def _collect_form8k_earnings(self, headers: dict, as_of: str,
                                 retrieved: str) -> int:
        """WS3: for a bounded set of the 8-K filings identified in the submissions
        lane, fetch the FULL official submission text (which incorporates the
        EX-99 earnings-release exhibit) and extract an Item 2.02 (Results of
        Operations) disclosure with any inline EPS / revenue figures and guidance
        indicator. PIT availability = SEC acceptance time. Figures absent from the
        document stay null — never fabricated from later data."""
        cap = int(self.ctx.source_cfg.get("form8k_doc_cap", 6))
        candidates = self._earn8k_candidates[:cap]
        emitted = 0
        item202_hits = 0
        for cand in candidates:
            accession = cand["accession"]
            txt_doc = "%s.txt" % accession
            res = self.fetch(self._archive_url(cand["cik10"], accession, txt_doc),
                             headers=headers, expect="html", extension="txt",
                             business_date=as_of,
                             native_id="form8k|%s" % accession,
                             content_type="text/plain", allow_404=True)
            if res.get("not_found") or not res["ok"]:
                continue
            acceptance = cand.get("acceptance")
            report_date = cand.get("report_date")
            # WS5: contemporaneous ASSIGNED-SIC of the 8-K filer (the company).
            self._emit_assigned_sic(res["body"], cik=cand["cik10"],
                                    ticker=cand.get("ticker"),
                                    acceptance=acceptance, form=cand.get("form"),
                                    accession=accession,
                                    raw_id=res["raw"]["raw_object_id"],
                                    retrieved=retrieved)
            info = extract_item_202(res["body"])
            if not info.get("has_item_202"):
                continue
            item202_hits += 1
            payload = {
                "cik": cand["cik10"].lstrip("0") or "0", "ticker": cand.get("ticker"),
                "form_type": cand.get("form"), "accession_number": accession,
                "item": "2.02", "item_2_02_results_of_operations": True,
                "eps_actual": info.get("eps_actual"),
                "revenue_text": info.get("revenue_text"),
                "guidance_present": info.get("guidance_present"),
                "report_date": report_date,
                "acceptance_datetime": acceptance,
                "official_link": self._archive_url(cand["cik10"], accession,
                                                   txt_doc),
                "extraction_note": ("EPS/revenue extracted from filing text where "
                                    "inline; figures embedded only in a binary/"
                                    "structured EX-99 exhibit remain null (not "
                                    "fabricated)"),
            }
            self.records.append(build_normalized_record(
                record_type=RT_EARNINGS_EVENT, source_id=self.source_id,
                source_native_id="8k202|%s" % accession,
                raw_object_id=res["raw"]["raw_object_id"], retrieved_at=retrieved,
                observed_at=report_date or (str(acceptance)[:10]
                                            if acceptance else None),
                effective_at=report_date or (str(acceptance)[:10]
                                             if acceptance else None),
                available_at=acceptance, ticker=cand.get("ticker"),
                company_id=cand["cik10"].lstrip("0") or "0",
                event_type="8-K_ITEM_2.02", payload=payload,
                entity_mapping_confidence=EM_MATCHED_EXACT,
                provenance="SEC EDGAR 8-K Item 2.02 / EX-99 (official filing text)",
                quality_warnings=(
                    [] if acceptance else
                    ["ACCEPTANCE_TIME_UNKNOWN: available_at left null"])))
            self.note_event_time(acceptance or report_date)
            emitted += 1
        self.inventory["form8k_item202_hits"] = item202_hits
        self.inventory["form8k_candidates_seen"] = len(self._earn8k_candidates)
        self.inventory["form8k_records_emitted"] = emitted
        return emitted

    def _collect_companyfacts(self, headers: dict, ciks: list, as_of: str,
                              retrieved: str) -> int:
        """Per-CIK XBRL company facts (data.sec.gov/api/xbrl/companyfacts).
        Emits a bounded set of us-gaap concepts as TRUE point-in-time fundamental
        facts: each fact carries its ``filed`` date (availability) and period
        ``end`` — no snapshot look-ahead (unlike vendor snapshot fundamentals)."""
        base = self.ctx.source_cfg.get("base_url_data", "https://data.sec.gov")
        concepts = set(self.ctx.source_cfg.get("companyfacts_concepts")
                       or _DEFAULT_FACT_CONCEPTS)
        per_concept = int(self.ctx.source_cfg.get("facts_per_concept_cap", 24))
        ok = 0
        for cik10, ticker in ciks:
            res = self.fetch(
                "%s/api/xbrl/companyfacts/CIK%s.json" % (base, cik10),
                headers=headers, expect="text", extension="json",
                business_date=as_of, native_id="companyfacts|%s" % cik10,
                content_type="application/json")
            if not res["ok"]:
                continue
            try:
                obj = json.loads(res["body"].decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                self.record_error("PARSE_ERROR", "companyfacts %s not JSON" % cik10)
                continue
            gaap = ((obj.get("facts") or {}).get("us-gaap") or {})
            emitted = 0
            for concept in sorted(concepts):
                cdata = gaap.get(concept)
                if not isinstance(cdata, dict):
                    continue
                for unit, facts in (cdata.get("units") or {}).items():
                    if not isinstance(facts, list):
                        continue
                    for fact in facts[-per_concept:]:
                        if not isinstance(fact, dict) or fact.get("val") is None:
                            continue
                        end = str(fact.get("end") or "")[:10]
                        filed = str(fact.get("filed") or "")[:10] or None
                        payload = {
                            "cik": cik10.lstrip("0") or "0", "ticker": ticker,
                            "concept": concept, "unit": unit,
                            "value": fact.get("val"),
                            "period_start": fact.get("start"),
                            "period_end": end, "filed": filed,
                            "form": fact.get("form"), "fy": fact.get("fy"),
                            "fp": fact.get("fp"), "frame": fact.get("frame"),
                        }
                        self.records.append(build_normalized_record(
                            record_type=RT_FUNDAMENTAL_FACT,
                            source_id=self.source_id,
                            source_native_id="facts|%s|%s|%s|%s|%s" % (
                                cik10, concept, unit, end,
                                fact.get("accn") or fact.get("form")),
                            raw_object_id=res["raw"]["raw_object_id"],
                            retrieved_at=retrieved, observed_at=end,
                            effective_at=end, available_at=filed,
                            ticker=ticker, company_id=cik10.lstrip("0") or "0",
                            event_type="XBRL_FACT", payload=payload,
                            entity_mapping_confidence=EM_MATCHED_EXACT,
                            provenance="SEC EDGAR companyfacts XBRL (official)",
                            quality_warnings=(
                                [] if filed else
                                ["FILED_DATE_MISSING: XBRL fact lacked 'filed'; "
                                 "available_at left null"])))
                        self.note_event_time(filed or end)
                        emitted += 1
            self.inventory.setdefault("companyfacts_by_cik", {})[cik10] = emitted
            ok += 1
        return ok

    def _collect_companyconcept(self, headers: dict, ciks: list, as_of: str,
                                retrieved: str) -> int:
        """Targeted single-concept history (data.sec.gov/api/xbrl/companyconcept)
        for the FIRST allowlisted concept — the lighter, targeted sibling of
        companyfacts. Proves the lane independently; facts carry filed-date PIT."""
        base = self.ctx.source_cfg.get("base_url_data", "https://data.sec.gov")
        concept = (self.ctx.source_cfg.get("companyconcept_concept")
                   or (self.ctx.source_cfg.get("companyfacts_concepts")
                       or list(_DEFAULT_FACT_CONCEPTS))[0])
        taxonomy = self.ctx.source_cfg.get("companyconcept_taxonomy", "us-gaap")
        per = int(self.ctx.source_cfg.get("facts_per_concept_cap", 24))
        ok = 0
        for cik10, ticker in ciks[:1]:  # bounded: one representative CIK
            res = self.fetch(
                "%s/api/xbrl/companyconcept/CIK%s/%s/%s.json" % (
                    base, cik10, taxonomy, concept),
                headers=headers, expect="text", extension="json",
                business_date=as_of,
                native_id="companyconcept|%s|%s" % (cik10, concept),
                content_type="application/json", allow_404=True)
            if res.get("not_found") or not res["ok"]:
                continue
            try:
                obj = json.loads(res["body"].decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                self.record_error("PARSE_ERROR",
                                  "companyconcept %s not JSON" % cik10)
                continue
            emitted = 0
            for unit, facts in ((obj.get("units") or {}).items()):
                if not isinstance(facts, list):
                    continue
                for fact in facts[-per:]:
                    if not isinstance(fact, dict) or fact.get("val") is None:
                        continue
                    end = str(fact.get("end") or "")[:10]
                    filed = str(fact.get("filed") or "")[:10] or None
                    self.records.append(build_normalized_record(
                        record_type=RT_FUNDAMENTAL_FACT, source_id=self.source_id,
                        source_native_id="concept|%s|%s|%s|%s" % (
                            cik10, concept, unit, end),
                        raw_object_id=res["raw"]["raw_object_id"],
                        retrieved_at=retrieved, observed_at=end,
                        effective_at=end, available_at=filed, ticker=ticker,
                        company_id=cik10.lstrip("0") or "0",
                        event_type="XBRL_CONCEPT",
                        payload={"cik": cik10.lstrip("0") or "0",
                                 "ticker": ticker, "concept": concept,
                                 "taxonomy": taxonomy, "unit": unit,
                                 "value": fact.get("val"), "period_end": end,
                                 "filed": filed, "form": fact.get("form"),
                                 "fy": fact.get("fy"), "fp": fact.get("fp")},
                        entity_mapping_confidence=EM_MATCHED_EXACT,
                        provenance="SEC EDGAR companyconcept XBRL (official)"))
                    self.note_event_time(filed or end)
                    emitted += 1
            self.inventory["companyconcept_%s" % concept] = emitted
            ok += 1
        return ok

    def _collect_full_index(self, headers: dict, as_of: str,
                            retrieved: str) -> int:
        """Bounded quarterly full-index (BULK) lane: fetch + archive the current
        quarter's master.idx and emit a bounded, capped sample of filing events.
        A REAL acquisition of the official bulk filing index (the bulk-backfill
        sibling of the incremental daily index). If the quarterly file exceeds the
        raw-object size cap it is recorded as a size gap rather than crashing —
        never fabricated."""
        base = self.ctx.source_cfg.get("base_url_www",
                                       "https://www.sec.gov").rstrip("/")
        d = _dt.date.fromisoformat(as_of)
        quarter = (d.month - 1) // 3 + 1
        url = "%s/Archives/edgar/full-index/%d/QTR%d/master.idx" % (
            base, d.year, quarter)
        res = self.fetch(url, headers=headers, expect="text", extension="idx",
                         business_date=as_of,
                         native_id="full_index|%d|Q%d" % (d.year, quarter),
                         content_type="text/plain", allow_404=True)
        if not res["ok"]:
            self.inventory["full_index_status"] = (
                "size_exceeds_raw_object_cap"
                if res.get("rejected_reason") == "RAW_OBJECT_TOO_LARGE"
                else "unavailable_or_gap")
            return 0
        rows = self._parse_master_idx(res["body"])
        self.inventory["full_index_rows_total"] = len(rows)
        self.inventory["full_index_quarter"] = "%d-Q%d" % (d.year, quarter)
        forms = set(self.ctx.source_cfg.get("forms_of_interest", []))
        cap = int(self.ctx.source_cfg.get("bulk_index_record_cap", 150))
        emitted = 0
        for row in rows:
            if emitted >= cap:
                break
            if forms and row["form_type"] not in forms:
                continue
            accession = self._accession_from_filename(row["filename"])
            date_filed = self._iso_date(row["date_filed"])
            cik_key = row["cik"].lstrip("0") or "0"
            rt = RT_INSIDER_FILING if row["form_type"].startswith("4") \
                else RT_FILING_EVENT
            self.records.append(build_normalized_record(
                record_type=rt, source_id=self.source_id,
                source_native_id="bulk|%s" % accession,
                raw_object_id=res["raw"]["raw_object_id"],
                retrieved_at=retrieved, observed_at=date_filed,
                effective_at=date_filed, available_at=None,
                company_id=cik_key, event_type=row["form_type"],
                payload={"cik": cik_key, "company_name": row["company_name"],
                         "form_type": row["form_type"], "date_filed": date_filed,
                         "accession_number": accession,
                         "official_link": "%s/Archives/%s" % (base,
                                                              row["filename"]),
                         "source_lane": "full_index_bulk"},
                entity_mapping_confidence=EM_UNMATCHED,
                provenance="SEC EDGAR quarterly full-index (bulk, official)",
                quality_warnings=[
                    "ACCEPTANCE_TIME_UNKNOWN: full index provides filing date "
                    "only; acceptance timestamp left null (see submissions lane "
                    "for the PIT acceptance anchor)"]))
            self.note_event_time(date_filed)
            emitted += 1
        self.inventory["full_index_records_emitted"] = emitted
        return emitted

    def _probe_bulk_archives(self, headers: dict, as_of: str) -> int:
        """WS4: HEAD-probe the OFFICIAL SEC bulk archives (companyfacts.zip,
        submissions.zip) and classify EACH lane honestly and separately (never
        conflated with the in-cycle full-index lane). An archive whose measured
        Content-Length fits the bounded raw-object cap is downloaded resumably
        (ranged GET + checkpoint + SHA-256 checksum); an archive that exceeds it
        is a PRECISE, evidence-based blocker — the real measured byte size — with
        the honest note that the per-CIK companyfacts/submissions APIs already
        supply the same data point-in-time-correctly in-cycle."""
        base = self.ctx.source_cfg.get("base_url_www",
                                       "https://www.sec.gov").rstrip("/")
        manage_cap = int(self.ctx.source_cfg.get("bulk_manageable_bytes",
                                                 33554432))
        archives = self.ctx.source_cfg.get("bulk_archives", {})
        timeout = float(self.ctx.limits.get("http_timeout_seconds", 30))
        out: dict = {}
        for name, path in archives.items():
            url = base + path
            lane = ("SEC_COMPANYFACTS_BULK" if name == "companyfacts"
                    else "SEC_SUBMISSIONS_BULK")
            try:
                resp = self.ctx.transport(
                    {"method": "HEAD", "url": url, "headers": headers}, timeout)
            except Exception as exc:  # noqa: BLE001 — probe must never raise up
                out[name] = {"lane": lane, "disposition": "PROBE_ERROR",
                             "detail": self._redact("%s" % exc)[:120]}
                continue
            status = resp.get("status")
            hdrs = {str(k).lower(): v
                    for k, v in (resp.get("headers") or {}).items()}
            clen = hdrs.get("content-length")
            size = int(clen) if clen and str(clen).isdigit() else None
            manageable = size is not None and 0 < size <= manage_cap
            if manageable:
                disposition, blocker = "RESUMABLE_DOWNLOAD", None
            elif size:
                disposition = "OUT_OF_BAND_BULK_EXCEEDS_CAP"
                blocker = ("multi-GB archive (%s bytes) exceeds the bounded raw-"
                           "object cap (%s bytes); the in-cycle per-CIK "
                           "companyfacts/submissions APIs already yield the same "
                           "data point-in-time-correctly, so a full bulk mirror is "
                           "an out-of-band batch job, not an in-cycle lane"
                           % (size, manage_cap))
            else:
                disposition = "SIZE_UNKNOWN_HEAD_UNAVAILABLE"
                blocker = "HEAD returned no Content-Length; size undetermined"
            entry = {
                "lane": lane, "request": self._fingerprint("HEAD", url),
                "http_status": status, "content_length_bytes": size,
                "manageable_cap_bytes": manage_cap,
                "manageable_within_cap": bool(manageable),
                "disposition": disposition, "blocker": blocker,
            }
            if manageable:
                entry["download"] = self._resumable_bulk_download(
                    url, headers, name, as_of)
            out[name] = entry
        self.inventory["bulk_archive_probe"] = out
        return len(out)

    def _resumable_bulk_download(self, url: str, headers: dict, name: str,
                                 as_of: str) -> dict:
        """Resumable ranged download of a bounded bulk archive to D: with a
        persistent byte-offset checkpoint and a full-file SHA-256 checksum on
        completion. Resumes from the last stored offset via a Range header. Only
        reached when the archive fits the manageable cap."""
        import hashlib
        root = Path(self.ctx.archive.output_root) / "bulk" / "sec_edgar"
        root.mkdir(parents=True, exist_ok=True)
        dest = root / ("%s.zip" % name)
        ckpt = root / ("%s.checkpoint.json" % name)
        chunk = int(self.ctx.source_cfg.get("bulk_chunk_bytes", 1 << 20))
        cap = int(self.ctx.source_cfg.get("bulk_manageable_bytes", 33554432))
        timeout = float(self.ctx.limits.get("http_timeout_seconds", 30))
        offset = 0
        if ckpt.exists() and dest.exists():
            try:
                offset = int(json.loads(ckpt.read_text(
                    encoding="utf-8")).get("bytes", 0))
            except (OSError, ValueError):
                offset = 0
        requests = 0
        mode = "ab" if offset and dest.exists() else "wb"
        with dest.open(mode) as fh:
            while requests < 4096:
                rng = {"Range": "bytes=%d-%d" % (offset, offset + chunk - 1)}
                resp = self.ctx.transport(
                    {"method": "GET", "url": url, "headers": {**headers, **rng}},
                    timeout)
                requests += 1
                body = resp.get("body") or b""
                if resp.get("status") not in (200, 206) or not body:
                    break
                fh.write(body)
                offset += len(body)
                ckpt.write_text(json.dumps({"bytes": offset, "url":
                                self._fingerprint("GET", url)}), encoding="utf-8")
                if len(body) < chunk or offset >= cap:
                    break
        checksum = None
        if dest.exists():
            h = hashlib.sha256()
            with dest.open("rb") as fh:
                for blk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(blk)
            checksum = h.hexdigest()
        return {"bytes_downloaded": offset, "requests": requests,
                "storage_path": str(dest), "checkpoint": str(ckpt),
                "sha256": checksum, "resumable": True}

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        contact = self._contact()
        if not contact:
            return self._blocked_no_contact()
        headers = {"User-Agent": self._ua(contact),
                   "Accept-Encoding": "identity"}
        as_of = _dt.date.today().isoformat()
        result = self._fetch_ticker_map(headers, as_of)
        self.inventory["user_agent_contact_masked"] = _mask_email(contact)
        state = SH_HEALTHY if result else SH_FAILED
        return self.result(overall_state=state,
                           entitlement_summary="official public data; UA contact present (masked %s)"
                                               % _mask_email(contact))

    def collect(self, as_of: str) -> dict:
        contact = self._contact()
        if not contact:
            return self._blocked_no_contact()
        cfg = self.ctx.source_cfg
        headers = {"User-Agent": self._ua(contact), "Accept-Encoding": "identity"}
        self.inventory["user_agent_contact_masked"] = _mask_email(contact)
        retrieved = self.ctx.now_iso()
        # WS2/WS3 candidate accumulators, populated by the submissions lane.
        self._form4_candidates: list[dict] = []
        self._earn8k_candidates: list[dict] = []

        map_result = self._fetch_ticker_map(headers, as_of)
        cik_to_tickers: dict[str, list[str]] = (map_result or {}).get("cik_to_tickers", {})

        # Bounded identity records for the pre-registered sample only (the full
        # map lives in the archived raw object + identity resolver).
        sample_tickers = {s.split(".")[0].upper()
                          for s in self.ctx.config.get("sources", {})
                          .get("norgate_local", {}).get("sample_symbols", [])}
        sample_tickers |= {s.split(".")[0].upper()
                           for s in self.ctx.config.get("sources", {})
                           .get("eodhd", {}).get("sample_symbols", [])}
        if map_result:
            for ticker in sorted(sample_tickers):
                resolved = self.ctx.identity.resolve(ticker)
                if resolved["cik"] is None and not resolved["all_ciks"]:
                    continue
                self.records.append(build_normalized_record(
                    record_type=RT_SECURITY_IDENTITY, source_id=self.source_id,
                    source_native_id="cikmap|%s" % ticker,
                    raw_object_id=map_result["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=retrieved,
                    effective_at=as_of, available_at=retrieved, ticker=ticker,
                    company_id=resolved["cik"],
                    event_type="TICKER_CIK_MAP",
                    payload={"all_ciks": resolved["all_ciks"]},
                    entity_mapping_confidence=resolved["state"],
                    provenance="SEC company_tickers.json (official)"))

        window = int(cfg.get("filing_window_business_days", 3))
        forms = set(cfg.get("forms_of_interest", []))
        cap = int(cfg.get("max_filings_per_day", 6000))
        base = cfg.get("base_url_www", "https://www.sec.gov").rstrip("/")
        days_hit = 0
        for day in _business_days_back(as_of, window):
            d = _dt.date.fromisoformat(day)
            quarter = (d.month - 1) // 3 + 1
            url = "%s/Archives/edgar/daily-index/%d/QTR%d/master.%s.idx" % (
                base, d.year, quarter, day.replace("-", ""))
            res = self.fetch(url, headers=headers, expect="text", extension="idx",
                             business_date=day, native_id="master_idx|%s" % day,
                             content_type="text/plain", allow_404=True)
            if res["not_found"]:
                self.inventory.setdefault("index_days_unavailable", []).append(day)
                continue
            if not res["ok"]:
                continue
            days_hit += 1
            rows = self._parse_master_idx(res["body"])
            self.inventory.setdefault("index_rows_by_day", {})[day] = len(rows)
            emitted = 0
            for row in rows:
                if row["form_type"] not in forms:
                    continue
                if emitted >= cap:
                    self.inventory.setdefault("index_days_capped", []).append(day)
                    break
                accession = self._accession_from_filename(row["filename"])
                date_filed = self._iso_date(row["date_filed"])
                cik_key = row["cik"].lstrip("0") or "0"
                tickers = cik_to_tickers.get(cik_key, [])
                if len(tickers) == 1:
                    ticker, mapping = tickers[0], EM_MATCHED_EXACT
                elif len(tickers) > 1:
                    ticker, mapping = None, EM_AMBIGUOUS
                else:
                    ticker, mapping = None, EM_UNMATCHED
                record_type = RT_INSIDER_FILING if row["form_type"].startswith("4") \
                    else RT_FILING_EVENT
                payload = {
                    "cik": cik_key, "company_name": row["company_name"],
                    "form_type": row["form_type"], "date_filed": date_filed,
                    "date_filed_raw": row["date_filed"],
                    "accession_number": accession,
                    "official_link": "%s/Archives/%s" % (base, row["filename"]),
                    "event_time": date_filed,
                    "publication_time": None, "acceptance_time": None,
                    "period_end": None,
                    "all_tickers_for_cik": tickers,
                }
                self.records.append(build_normalized_record(
                    record_type=record_type, source_id=self.source_id,
                    source_native_id=accession,
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=date_filed,
                    effective_at=date_filed, available_at=None,
                    ticker=ticker, company_id=cik_key,
                    event_type=row["form_type"], payload=payload,
                    entity_mapping_confidence=mapping,
                    provenance="SEC EDGAR daily index (official)",
                    quality_warnings=[
                        "ACCEPTANCE_TIME_UNKNOWN: daily index provides filing date "
                        "only; acceptance timestamp left null (not fabricated)",
                        "PUBLICATION_TIME_UNKNOWN: left null; filing date is "
                        "date-precision"]))
                emitted += 1
                self.note_event_time(date_filed)

        # ---- data.sec.gov structured lanes (submissions + XBRL facts) --------
        # These are the "ACCESSIBLE_AFTER_REPAIR" lanes: real, official,
        # credential-free JSON APIs. submissions supplies the acceptanceDateTime
        # PIT anchor for every form (incl. Form 4 + 8-K); companyfacts/
        # companyconcept supply TRUE point-in-time XBRL fundamentals (each fact
        # carries its filed date). Each lane is bounded and gated by config.
        ciks = self._bounded_ciks(cik_to_tickers, as_of) if map_result else []
        lanes = {}
        if cfg.get("collect_submissions") and ciks:
            lanes["submissions"] = self._collect_submissions(
                headers, ciks, as_of, retrieved)
        if cfg.get("collect_companyfacts") and ciks:
            lanes["companyfacts"] = self._collect_companyfacts(
                headers, ciks, as_of, retrieved)
        if cfg.get("collect_companyconcept") and ciks:
            lanes["companyconcept"] = self._collect_companyconcept(
                headers, ciks, as_of, retrieved)
        # WS2/WS3: transaction-level Form 4 XML + 8-K Item 2.02 extraction, drawn
        # from the Form 4 / 8-K candidates the submissions lane accumulated.
        if cfg.get("collect_form4_transactions") and self._form4_candidates:
            lanes["form4_transactions"] = self._collect_form4_transactions(
                headers, as_of, retrieved)
        if cfg.get("collect_form8k_earnings") and self._earn8k_candidates:
            lanes["form8k_item202"] = self._collect_form8k_earnings(
                headers, as_of, retrieved)
        if cfg.get("collect_full_index"):
            lanes["full_index_bulk"] = self._collect_full_index(
                headers, as_of, retrieved)
        if cfg.get("probe_bulk_archives"):
            lanes["bulk_archive_probe"] = self._probe_bulk_archives(
                headers, as_of)
        if lanes:
            self.inventory["structured_lanes_ciks_ok"] = lanes

        self.cursor = {"last_collected_as_of": as_of,
                       "index_window_business_days": window,
                       "structured_lanes": lanes,
                       "newest_filing_date": self.newest_source_event_time}
        if map_result and days_hit > 0 and not self.http_errors:
            state = SH_HEALTHY
        elif self.records:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state,
                           entitlement_summary="official public data; UA contact present (masked %s)"
                                               % _mask_email(contact))
