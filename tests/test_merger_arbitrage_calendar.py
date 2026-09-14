r"""The merger-arbitrage deal calendar: terms and identity from the filings themselves.

Synthetic snippets only - no network, no vendor, no price. Each case is a shape measured on
real EDGAR documents before this test was written (the Coty partial tender, the Ultimate
Software cash merger, the no-shop and option cash-out boilerplate that a bare phrase match
mistook for stock consideration, the Airborne and Digene mixed deals) or a trap the
preregistration names (a counterparty's symbol; a later document can never rescue an
excluded episode).
"""
from __future__ import annotations

from paper_trader.alpha_agent.alpha_recovery import merger_arb_data as MD
from paper_trader.alpha_agent.alpha_recovery import merger_arb_events as ME

HEADER_SC_TO_T = """<SEC-DOCUMENT>0000000000-19-000001.txt : 20190213
<SEC-HEADER>0000000000-19-000001.hdr.sgml : 20190213
<ACCEPTANCE-DATETIME>20190213171122
ACCESSION NUMBER:\t\t0000000000-19-000001
CONFORMED SUBMISSION TYPE:\tSC TO-T
FILED AS OF DATE:\t\t20190213
SUBJECT COMPANY:

\tCOMPANY DATA:
\t\tCOMPANY CONFORMED NAME:\t\t\tTARGET CO INC
\t\tCENTRAL INDEX KEY:\t\t\t0001234567

FILED BY:

\tCOMPANY DATA:
\t\tCOMPANY CONFORMED NAME:\t\t\tBIDDER HOLDINGS
\t\tCENTRAL INDEX KEY:\t\t\t0007654321
</SEC-HEADER>
<DOCUMENT><TEXT><html><body><p>Offer to purchase all outstanding shares of common stock of Target Co
at a price of $24.50 per Share, net to the seller in cash, without interest.</p>
<p>The Shares are listed on the NASDAQ and trade under the symbol &#8220;TGCO.&#8221;</p>
<p>222070203 (CUSIP Number of Class of Securities)</p></body></html>"""


def test_01_the_sec_header_names_the_subject_and_the_filer():
    h = MD.parse_header(HEADER_SC_TO_T)
    assert h["form"] == "SC TO-T"
    assert h["filed_as_of"] == "2019-02-13"
    assert h["acceptance_local"] == "2019-02-13T17:11:22"
    assert h["subject"] == {"name": "TARGET CO INC", "cik": "1234567"}
    assert h["filed_by"] == {"name": "BIDDER HOLDINGS", "cik": "7654321"}


def test_02_a_cash_tender_for_all_shares_is_read_with_its_symbol():
    t = MD.extract_terms(MD.flatten(HEADER_SC_TO_T))
    assert t["distinct_cash_prices"] == [24.5]
    assert t["all_shares"] is True
    assert t["partial_offer"] is False and t["stock_consideration"] is False
    assert {"ticker": "TGCO", "context": "SELF"} in [
        {"ticker": x["ticker"], "context": x["context"]} for x in t["tickers"]]


def test_03_a_cash_merger_proxy_is_read():
    flat = ("each share of our common stock (other than certain shares) will be converted into the right "
            "to receive $331.50 per share in cash, without interest. Market Price of the Company Common "
            "Stock The Company common stock is listed on the NASDAQ under the symbol “ULTI.”")
    t = MD.extract_terms(flat)
    assert 331.5 in t["distinct_cash_prices"]
    assert t["all_shares"] is True
    assert t["stock_consideration"] is False
    assert [x["ticker"] for x in t["tickers"] if x["context"] == "SELF"] == ["ULTI"]


def test_04_a_partial_tender_is_flagged_and_a_proxy_option_grant_is_not():
    coty = ("Offer to Purchase for Cash Up to 150,000,000 Shares of Class A Common Stock at $11.65 per "
            "Share, net to the seller in cash")
    assert MD.extract_terms(coty)["partial_offer"] is True
    proxy = ("directors may receive an option grant for up to 5,000 shares of common stock, and the "
             "investors may purchase from the acquisition vehicle up to 12,954,478 shares")
    assert MD.extract_terms(proxy)["partial_offer"] is False


def test_05_true_stock_election_and_cvr_consideration_are_flagged():
    stock = ("each share will be converted into the right to receive 0.5 shares of Parent common stock "
             "and $10.00 in cash")
    assert MD.extract_terms(stock)["stock_consideration"] is True
    airborne = ("you will receive written instructions for exchanging your stock certificates for your "
                "cash payment and shares of ABX Air common stock")
    assert MD.extract_terms(airborne)["stock_consideration"] is True
    digene = "Cytyc Corporation will acquire Digene Corporation in a stock and cash tender offer transaction"
    assert MD.extract_terms(digene)["stock_consideration"] is True
    assert MD.extract_terms("holders may elect to receive cash or stock")["election"] is True
    cvr = "the right to receive $12.00 per share in cash plus one contingent value right"
    assert MD.extract_terms(cvr)["contingent_value_right"] is True


def test_05b_boilerplate_inside_cash_deals_is_not_stock_consideration():
    for text in (
        "we do not convey a statement in respect of a tender offer or exchange offer by a third party",
        "the holder of that option will be entitled to receive a one-time cash payment equal to the number "
        "of shares of common stock subject to the option",
        "each of your shares of our common stock will be converted into the right to receive $20.75 in cash, "
        "without interest, and each share of our preferred stock will be cancelled",
        "Dr. Smith indicated that the other bidder would not amend the exchange ratio in its merger agreement",
        "restricted stock and cash awards will vest",
    ):
        assert MD.extract_terms(text)["stock_consideration"] is False, text


def test_05c_mixed_consideration_by_its_measured_shapes():
    # the classification audit's misses (DIRECTV, Alterra) and the included mixed deals they exposed
    for text in (
        "you will be entitled to receive for each share of DIRECTV common stock an amount equal to $28.50 in cash "
        "plus a number of shares of AT&T common stock equal to the exchange ratio",
        "will be converted into the right to receive (i) $43.11 in cash without interest and (ii) 0.6494 shares of "
        "Conagra common stock",
        "will be converted into (a) $27.50 in cash, without interest, plus (b) a fraction of a share of Exact "
        "Sciences common stock",
        "the right to receive (i) $6.00 in cash, without interest and subject to applicable withholding tax, plus "
        "(ii) 0.1553 of a share of the common stock of Trulia",
        "(1) 0.04315, which we refer to as the exchange ratio, validly issued, fully paid and nonassessable shares "
        "of Markel voting common stock, together with any cash paid in lieu of fractional shares and (2) $10.00 "
        "in cash, without interest",
        "the right to receive (i) 0.4125 of one ICON ordinary share, which number is referred to as the exchange "
        "ratio, and (ii) $80.00 in cash, without interest",
    ):
        assert MD.extract_terms(text)["stock_consideration"] is True, text


def test_05d_option_cash_outs_and_filing_fee_arithmetic_are_not_mixed_consideration():
    for text in (
        "each option will be converted into the right to receive a cash payment equal to the product of (x) the "
        "number of shares of common stock underlying such option and (y) the excess, if any, of $25.00 over the "
        "exercise price",
        "The filing fee was determined based upon the sum of (A) 17,864,680 shares of Common Stock multiplied by "
        "$0.85 per share plus (B) $1,130,610 expected to be paid to holders of warrants",
        "calculated by adding (1) the product of (a) $5.50, the per share tender price, and (b) 5,197,182 shares "
        "of common stock",
        "will receive $331.50 per share in cash, without interest, and each share of our preferred stock will be "
        "cancelled",
    ):
        assert MD.extract_terms(text)["stock_consideration"] is False, text


def test_05e_a_percentage_partial_tender_is_flagged_and_background_percentages_are_not():
    supervalu = ("This Schedule TO relates to the offer by Purchaser to purchase up to 30% of the outstanding shares "
                 "of common stock at a purchase price of $4.00 per Share, net to the seller in cash")
    assert MD.extract_terms(supervalu)["partial_offer"] is True
    assert MD.extract_terms("Offer to Purchase for Cash Up to 30% of the Outstanding Shares of Common Stock "
                            "at $4.00 Net Per Share in Cash")["partial_offer"] is True
    for text in (
        "Opto Circuits changed its proposal to be a tender offer for up to 100% of the outstanding shares",
        "Sage has an irrevocable option to purchase up to 19.9% of our outstanding shares",
        "a proposed amendment to allow Mill Road to purchase up to 10% of our outstanding common stock",
        "We are offering to purchase up to 100% of the issued and outstanding common stock",
    ):
        assert MD.extract_terms(text)["partial_offer"] is False, text


def test_05f_registered_securities_exclude_and_a_stray_prospectus_mention_does_not():
    kimball = ("ABOUT THIS PROXY STATEMENT/PROSPECTUS This document, which forms part of a registration statement "
               "on Form S-4 filed with the SEC by HNI, constitutes a prospectus of HNI with respect to the shares of "
               "HNI common stock to be issued to Kimball shareholders. You will receive $9.00 in cash, without "
               "interest.")
    t = MD.extract_terms(kimball)
    assert t["securities_registered"] is True and t["stock_consideration"] is True
    lsi = ("you will be entitled to receive $11.15 in cash, without interest. RESOLVED, that the compensation, as "
           "disclosed in the section of the joint proxy statement/prospectus statement entitled 'The Merger', is "
           "approved.")
    t = MD.extract_terms(lsi)
    assert t["securities_registered"] is False and t["stock_consideration"] is False
    no_prospectus = "Parent filed a registration statement on Form S-4 for an unrelated offering in 2010."
    assert MD.extract_terms(no_prospectus)["stock_consideration"] is False


def test_05g_contingent_consideration_under_other_names_and_not_in_background():
    for text in (
        "to purchase all outstanding shares at a price of $4.50 per Share, net to the seller in cash (less any "
        "required withholding taxes and without interest), plus contractual rights to receive up to an "
        "additional $3.00 per Share in contingent cash consideration payments",
        "holders will be entitled to receive (1) $8.55 in cash, without interest and less applicable withholding "
        "taxes, and (2) one contingent payment right, or CPR",
        "you will be entitled to receive $4.20 in cash (the Closing Date Consideration) and one PPP Loan "
        "Forgiveness Right representing the contingent right to receive up to $0.25",
    ):
        assert MD.extract_terms(text)["contingent_value_right"] is True, text
    for text in (
        "Welsh Carson revised its offer to $20.50 per share in cash, plus a $0.50 per share contingent payment "
        "if proposed reimbursement rates were adjusted",
        "Mr. Cornelius proposed a revised offer of $11.00 in cash plus contingent payment rights to pay up to "
        "$4.00 in cash",
        "each unvested option will be converted into a contingent right to receive an amount in cash equal to "
        "the merger consideration",
        "you will be entitled to receive $11.15 in cash, without interest, and a copy of the proxy card",
    ):
        assert MD.extract_terms(text)["contingent_value_right"] is False, text


def test_06_a_counterparty_ticker_is_never_the_target():
    flat = ("each outstanding share of the Company's common stock will be converted into the right to receive "
            "$40.00 per share in cash. Parent's common stock is listed on the NYSE under the symbol “PFE.”")
    t = MD.extract_terms(flat)
    assert [x["context"] for x in t["tickers"] if x["ticker"] == "PFE"] == ["COUNTERPARTY"]


UNIVERSE = {
    "TGCO": [{"symbol": "TGCO-201905", "base": "TGCO", "status": "DELISTED", "first_quoted": "2001-01-02",
              "last_quoted": "2019-05-20", "name": "Target Co Inc Common", "exchange": "NASDAQ",
              "subtype": "Equity"}],
    "DUAL": [{"symbol": "DUAL-200301", "base": "DUAL", "status": "DELISTED", "first_quoted": "1990-01-02",
              "last_quoted": "2003-01-10", "name": "Old Dual", "exchange": "NYSE", "subtype": "Equity"},
             {"symbol": "DUAL", "base": "DUAL", "status": "ACTIVE", "first_quoted": "2010-01-04",
              "last_quoted": "2026-09-11", "name": "New Dual", "exchange": "NYSE", "subtype": "Equity"}],
    "BIG": [{"symbol": "BIG", "base": "BIG", "status": "ACTIVE", "first_quoted": "1980-01-02",
             "last_quoted": "2026-09-11", "name": "Bigacquirer Pharmaceuticals", "exchange": "NYSE",
             "subtype": "Equity"}],
}


def _doc(filed, form, company="TARGET CO INC", **terms):
    base = {"cash_prices": [], "all_shares": False, "partial_offer": False, "stock_consideration": False,
            "election": False, "contingent_value_right": False, "tickers": []}
    base.update(terms)
    return {"accession": "%s%s" % (form, filed), "form": form, "filed": filed, "company": company,
            "acceptance_local": None, "terms": base}


def _tk(t, ctx="SELF"):
    return {"ticker": t, "context": ctx, "kind": "SYMBOL_STATEMENT", "at": 5}


def test_07_the_deal_is_established_only_when_the_filings_say_so():
    ep = {"target_cik": "1234567", "documents": [
        _doc("2019-02-01", "PREM14A", cash_prices=[{"price": 24.5, "at": 10}]),
        _doc("2019-02-13", "SC 14D9", all_shares=True, tickers=[_tk("TGCO")]),
    ]}
    res = ME.establish(ep, UNIVERSE)
    assert res["state"] == ME.ST_INCLUDED
    assert res["established_on"] == "2019-02-13"            # never the first filing's date
    assert res["symbol"] == "TGCO-201905" and res["cash_price"] == 24.5
    assert res["name_guard_tokens"] == ["TARGET"]


def test_08_a_later_document_cannot_rescue_an_excluded_episode():
    ep = {"target_cik": "1", "documents": [
        _doc("2019-02-01", "PREM14A", cash_prices=[{"price": 10.0, "at": 1}], all_shares=True,
             stock_consideration=True),
        _doc("2019-03-01", "DEFM14A", cash_prices=[{"price": 10.0, "at": 1}], all_shares=True,
             tickers=[_tk("TGCO")]),
    ]}
    assert ME.establish(ep, UNIVERSE)["state"] == ME.ST_NOT_CASH


def test_09_identity_is_the_security_quoted_on_the_establishing_date():
    ep = {"target_cik": "2", "documents": [
        _doc("2012-06-01", "DEFM14A", company="DUAL CORP", cash_prices=[{"price": 5.0, "at": 1}],
             all_shares=True, tickers=[_tk("DUAL")])]}
    assert ME.establish(ep, UNIVERSE)["symbol"] == "DUAL"   # the 2003 security is not quoted in 2012


def test_10_no_resolvable_symbol_leaves_the_episode_unresolved():
    ep = {"target_cik": "3", "documents": [
        _doc("2012-06-01", "DEFM14A", cash_prices=[{"price": 5.0, "at": 1}], all_shares=True,
             tickers=[_tk("NOPE")])]}
    assert ME.establish(ep, UNIVERSE)["state"] == ME.ST_UNRESOLVED


def test_10b_the_name_guard_refuses_an_acquirers_symbol_and_never_adds_one():
    ep = {"target_cik": "4", "documents": [
        _doc("2012-06-01", "DEFM14A", cash_prices=[{"price": 5.0, "at": 1}], all_shares=True,
             tickers=[_tk("BIG", "UNTAGGED")])]}
    res = ME.establish(ep, UNIVERSE)
    assert res["state"] == ME.ST_UNRESOLVED and res["reason"] == "SYMBOL_REFUSED_BY_NAME_GUARD"
    # with the target's own symbol also stated, the guard leaves exactly the target
    ep["documents"][0]["terms"]["tickers"].append(_tk("TGCO", "UNTAGGED"))
    assert ME.establish(ep, UNIVERSE)["symbol"] == "TGCO-201905"


def test_11_a_bidders_copy_of_a_tender_index_row_is_not_a_target_document():
    idx = [{"cik": "7654321", "company": "BIDDER", "form": "SC TO-T", "date": "2019-02-13", "path": "p",
            "accession": "A1"},
           {"cik": "1234567", "company": "TARGET CO INC", "form": "SC TO-T", "date": "2019-02-13",
            "path": "p", "accession": "A1"}]
    parsed = {"A1": {"filed_as_of": "2019-02-13", "subject": {"cik": "1234567"}, "terms": {}}}
    docs = ME.documents_by_target(idx, parsed)
    assert list(docs) == ["1234567"]


def test_11b_symbols_are_searched_beyond_the_terms_window_and_their_venue_is_tagged():
    filler = "x " * (MD.TEXT_WINDOW // 2 + 10)
    flat = ("each share of our common stock will be converted into the right to receive $9.00 per share in "
            "cash, without interest. " + filler + " Our common stock is quoted on the OTC Bulletin Board "
            "under the symbol “WNMP.OB.”")
    t = MD.extract_terms(flat)
    assert t["distinct_cash_prices"] == [9.0]
    assert [(x["ticker"], x["venue"]) for x in t["tickers"]] == [("WNMP", "OTC")]


def test_11c_an_over_the_counter_target_is_outside_the_universe_not_unresolved():
    otc = dict(_tk("WNMP"), venue="OTC")
    ep = {"target_cik": "5", "documents": [
        _doc("2002-08-23", "PREM14A", company="WESTWOOD CORP", cash_prices=[{"price": 9.0, "at": 1}],
             all_shares=True, tickers=[otc])]}
    assert ME.establish(ep, UNIVERSE)["state"] == ME.ST_OTC
    listed = dict(_tk("NOPE"), venue="LISTED_OR_UNSTATED")
    ep["documents"][0]["terms"]["tickers"] = [otc, listed]
    assert ME.establish(ep, UNIVERSE)["state"] == ME.ST_UNRESOLVED


def test_12_episodes_split_after_a_year_of_silence():
    docs = {"9": [_doc("2010-01-01", "DEFM14A"), _doc("2010-03-01", "DEFM14A"), _doc("2012-01-01", "PREM14A")]}
    eps = ME.episodes(docs)
    assert [len(e["documents"]) for e in eps] == [2, 1]
