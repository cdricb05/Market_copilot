# Prediction Service Audit v1 (READ-ONLY)

- **Date:** 2026-06-15
- **Auditor scope:** Read-only inspection of the remote GCP prediction service. No remote files were modified, no services restarted, no migrations run, no packages installed.
- **Audited host:** GCP project `stock-prediction-app-466420`, VM `stock-prediction-vm-new` (zone `us-central1-a`), service `stock-api.service`, remote user `binisti`.
- **Service code commit:** `8751e35` ("perf: fast-only mode, kill CmdStan on timeout, SPY fast models, DB warmup, series cache, skip yfinance on weekends"), repo `github.com/cdricb05/Stock_Prediction_app`.
- **What Paper Trader sees:** the service through local tunnel `http://127.0.0.1:9000` (remote `0.0.0.0:8000`).

> All credentials, tokens, and DB passwords observed during this audit are intentionally **masked** in this document.

---

## 1. Executive summary

The remote "prediction service" is a **single-file FastAPI app** (`api_server.py`, ~850 lines, FastAPI title "Stock Prediction API" v1.7.1) that, on each request, **trains a small suite of classical time-series / ML point-forecast models from scratch** on a single price series (adjusted close), averages them into a 5-business-day price forecast, and emits a heuristic BUY/SELL/HOLD recommendation with a heuristic "confidence" and "agreement" score.

What is genuinely real today:

- 5 lightweight forecasters run in production (Drift, LinearTrend, XGBoost-on-returns, Naive, SMA), trained **per request** on up to 365 days of daily adjusted close.
- A robust ensemble (median of in-sample-accurate models), a residual-alpha-vs-SPY calculation, a z-score of the move, and a threshold-based recommendation.
- Data comes from a local Postgres `stock_prices` table (~302k rows, 491 tickers, 2020-01-02 -> 2026-06-15), with a yfinance fallback/refresh.

What is **not** real today (do not claim otherwise):

- **No** news, sentiment, macro, seasonality, fundamentals, sector, beta, regime, or liquidity inputs. The models see **price only** (adjusted close). OHLC and volume exist in the DB but are **never read** by the model code.
- **No** prediction intervals (the `lo`/`hi` fields are always `null`).
- **No** walk-forward / out-of-sample validation. The only "metrics" are **in-sample** fitted error, used merely as a filter; this is **not** a backtest.
- **No** statistical confidence calibration. "Confidence" is `100 - coefficient_of_variation%` of the models' day-5 forecasts (i.e. forecast dispersion), and "agreement" is the bull/bear vote split. Both are heuristics, not probabilities.
- **No** transaction-cost, slippage, position-sizing, or downside/drawdown model. **No** model versioning, monitoring, or persisted performance history for the request path. The universe is a **hardcoded static S&P 500 list** (survivorship / look-ahead membership bias).

Bottom line: this is a **reasonable, fast, price-only momentum/trend ensemble** suitable for first-pass screening and rough direction. It is **not** quant-grade for ranking, position monitoring, or (disabled) automation, primarily due to the absence of out-of-sample validation, calibrated uncertainty, risk modeling, and any non-price information.

---

## 2. Current architecture (text diagram)

```
                          Paper Trader (local, Windows, 127.0.0.1:8001)
                                       |
                 POST {"ticker": "AAPL"}  -> /predict_all_models/
                                       |
                          local tunnel 127.0.0.1:9000
                                       v
   GCP VM stock-prediction-vm-new (us-central1-a)
   systemd: stock-api.service
     ExecStart: /home/binisti/venv/bin/uvicorn api_server:app --host 0.0.0.0 --port 8000 --workers 1
     WorkingDir: /home/binisti/Stock_Prediction_app
     EnvironmentFile: api.env   (OMP/OPENBLAS threads pinned to 1)
                                       |
                                       v
   api_server.py  (FastAPI, Python 3.11.2, venv /home/binisti/venv)
     |- get_fresh_series(ticker)            <- Postgres stock_prices (adj_close), 30-min cache
     |     |- if missing/stale(>=3 biz days) -> yfinance download -> upsert stock_prices
     |- run_model_suite()  (per request, FAST_ONLY=1):
     |     Drift, LinearTrend, XGBoost(returns), Naive, SMA   [trained at request time]
     |     (Prophet, ARIMA, ETS exist but are DORMANT under FAST_ONLY; LSTM off)
     |     each model: 5-business-day point path + in-sample MAE%/RMSE%
     |- ensemble: robust median of models with in-sample error <= 5%
     |- agreement + confidence (heuristic), residual alpha vs SPY, z-score
     |- make_recommendation() -> Strong Buy/Buy/Hold/Sell/Strong Sell
                                       |
                                       v
   Postgres (same VM, DB "stock_data"):
     stock_prices(id,ticker,date,open,high,low,close,adj_close,volume)  -- only adj_close is read
     model_runs / predictions  -- written by alerts_service.py (separate Telegram bot), NOT by the request path
     alerts_log

   Side process (NOT on Paper Trader's request path):
     alerts_service.py  -> scheduled scans -> writes model_runs + predictions, sends Telegram alerts
   Cron (weekdays 23:30 UTC): load_stock.py -> refresh stock_prices from yfinance
```

Key architectural facts:

- **Single uvicorn worker** (`--workers 1`), BLAS/OMP threads pinned to 1. Models are run in **subprocesses with per-model timeouts** (`MODEL_TIMEOUT_SEC=10`) under a total budget (`TOTAL_BUDGET_SEC=20`).
- The `model_runs` / `predictions` artifact tables are populated by the **separate `alerts_service.py` bot**, not by `/predict_all_models/`. **A Paper Trader prediction request persists no artifact** of itself.

---

## 3. API contract

### Endpoints (from `api_server.py`)

| Method | Path | Purpose |
|---|---|---|
| GET | `/ping` | Liveness; returns `{"status":"ok"}` |
| GET | `/healthz` | `{"status","db_ok","fast_only"}` |
| GET | `/config` | Echoes runtime flags/thresholds |
| POST | `/predict_all_models/` | Main prediction; body `{"ticker": "<SYM>"}` |
| GET | `/predict/{ticker}` | Thin wrapper that calls `predict_all(TickerRequest(ticker))` |

### Request shape

`POST /predict_all_models/` body is `TickerRequest`:

```json
{ "ticker": "AAPL" }
```

**Confirmed: Paper Trader sends only the ticker.** No features, no history, no context are sent from Paper Trader (see local `engine/prediction_client.py`, which POSTs `{"ticker": ticker}` only). The ticker is upper-cased server-side.

### Response shape (success)

A large flat dict. The fields Paper Trader actually consumes are marked **[USED]** (per local `engine/prediction_client.py::normalize_prediction_response_with_error`):

| Field | Type | Notes |
|---|---|---|
| `ticker` | str | **[USED]** |
| `current_price` | float | **[USED]** last DB close (ALLOW_LIVE_PRICE=0) |
| `yesterday_close`, `price_source`, `price_as_of` | mixed | provenance metadata |
| `ensemble_day1`, `ensemble_day5` | float | **[USED: ensemble_day5 -> forecast_price_5d]** |
| `d1_change_pct`, `d5_change_pct` | float | **[USED: d5_change_pct -> expected_return_pct]** |
| `agreement` | float or null | 0..1 vote split (heuristic) |
| `confidence` | float or null | **[USED]** 0..100; Paper Trader divides by 100 |
| `recommendation` | str | **[USED]** "Strong Buy/Buy/Hold/Sell/Strong Sell" |
| `rationale` | list[str] | **[USED -> reason]** human-readable reasons |
| `per_model_summary` | list[{model,d1,d5,d5_change_pct,direction}] | **[USED -> model_consensus]** |
| `results` | dict[model -> 5 floats] | raw per-model forecast paths |
| `metrics` | dict[model -> [mae%, rmse%]] | **in-sample** fitted error |
| `ran_models`, `skipped_models`, `model_errors` | lists/dict | execution diagnostics |
| `eligibility` | dict | consensus eligibility detail |
| `alpha_by_day_pct`, `alpha_day5_bp`, `residual_alpha_bp` | list/int | residual alpha vs SPY |
| `zscore` | float | abs z-score of the 5-day move |
| `avg_forecast`, `robust_forecast`, `spy_avg_forecast` | lists | ensemble series |
| `predictions` | list[{model,horizon_day,point,lo,hi}] | **`lo`/`hi` always null** |
| `table_rows` | list | per-day table for UI |
| `spy_current_price`, `spy_price_source`, `spy_price_as_of` | mixed | SPY provenance |

Error response: `{"error": "<message>", ...}` (HTTP 200 with an `error` key, not a non-2xx status, for model/data failures).

---

## 4. Data source audit

- **Primary store:** Postgres DB `stock_data` on the same VM, table `stock_prices`.
  - Columns: `id, ticker, date, open, high, low, close, adj_close, volume`.
  - Coverage observed: **302,715 rows**, **491 distinct tickers**, dates **2020-01-02 -> 2026-06-15**.
- **What the model code actually reads:** **only `adj_close`** (`get_data_from_db` selects `date, adj_close`). **`open/high/low/close/volume` are present in the DB but never read** by the prediction models. So:
  - **Volume:** available in storage, **not used**.
  - **OHLC:** available in storage, **not used**.
  - **Adjusted close vs close:** uses **adjusted close** (`auto_adjust=False`, prefers the "Adj Close" column, falls back to "Close").
- **Live vs cached vs DB vs file:**
  - Per request: `get_fresh_series` reads the DB (30-minute in-process series cache, TTL 1800s).
  - **Staleness refresh:** if the latest DB date is `>= STALE_DAYS (3)` business days behind the last business day, the service fetches from **yfinance** and upserts into `stock_prices`. So a serving request **can write prices** to the DB (price refresh only).
  - **Live intraday price:** controlled by `ALLOW_LIVE_PRICE`. In production it is **0 (off)**, so `current_price` = last DB close; no intraday yfinance call. (When on, it would pull `fast_info`/1-minute close during market hours only.)
  - Weekday cron `load_stock.py` (23:30 UTC) bulk-refreshes prices from yfinance.
- **Lookback window:** `LOOKBACK_DAYS=365` (env override; code default 180). Each model trains on up to the last 365 daily rows.
- **Fundamentals / macro / sentiment / news / seasonality:** **none present.** No data source, table, or API for any of these exists in the service. (Prophet would model weekly/yearly seasonality, but Prophet is dormant under `FAST_ONLY=1` — see Section 5.)
- **Data-quality checks / fallbacks:** numeric coercion + `dropna`; DB-miss -> yfinance fallback with 3 retry windows (explicit start/end, then `period=2y`, then `1y`); empty series -> `{"error":"No data available"}`. There is **no outlier/spike detection, no corporate-action sanity check, no minimum-history gate** beyond "non-empty".

---

## 5. Model inventory

Active set is governed by `FAST_ONLY` (production `=1`). Under fast mode the suite is `[Drift, LinearTrend, XGBoost, Naive, SMA]`. Prophet/ARIMA/ETS only run when `FAST_ONLY=0`; LSTM only when `ENABLE_LSTM=1` (unset/off) **and** not fast.

| Model | Type | Train timing | Target | Features / inputs | Horizon | Output | Failure handling | Active in prod? |
|---|---|---|---|---|---|---|---|---|
| Drift | Geometric random walk w/ drift | per request | adj_close level | mean of last 20 daily log returns | 5 biz days | 5-pt path | try/except -> empty | **YES** |
| LinearTrend | OLS on time index | per request | adj_close level | `t = 0..n` vs price | 5 biz days | 5-pt path | try/except -> empty | **YES** |
| XGBoost | Gradient boosting on **returns** | per request | next-day log return | lagged returns r1,r2,r3, ma5, ma10, vol20 (iterated 5x; per-step clip +/-15%) | 5 biz days | 5-pt path | try/except -> empty | **YES** |
| Naive | Last-value | per request | adj_close level | last close repeated | 5 biz days | 5-pt path | try/except -> empty | **YES** |
| SMA | 5-day SMA + drift | per request | adj_close level | 5-day mean seeded, drifted by mean log return | 5 biz days | 5-pt path | try/except -> empty | **YES** |
| Prophet | Additive trend + seasonality | per request | adj_close level | date + price (default Prophet) | 5 biz days | 5-pt path | try/except -> empty | No (dormant; FAST_ONLY) |
| ARIMA | ARIMA(1,1,0) | per request | adj_close level | price series | 5 biz days | 5-pt path | try/except -> empty | No (dormant; FAST_ONLY) |
| ETS | Holt exponential smoothing (additive trend) | per request | adj_close level | price series | 5 biz days | 5-pt path | try/except -> empty | No (dormant; FAST_ONLY) |
| LSTM | 2-layer LSTM (32 units) | per request | scaled adj_close | 60-step window, 2 epochs | 5 biz days | 5-pt path | gated off; try/except | No (ENABLE_LSTM off) |

Notes:

- **All models are trained at request time. There are no pre-trained/persisted model artifacts** (no `.pkl`/`.joblib`/`.h5`/`.pt` files exist in the service directory). Each call re-fits from scratch.
- **Dead/dormant code:** Prophet, ARIMA, ETS (require `FAST_ONLY=0`), and LSTM (requires `ENABLE_LSTM=1`) are present but **not exercised in production**. The `api.env` also pins `XGB_EST=120`, `MODEL_TIMEOUT_SEC=10`, `TOTAL_BUDGET_SEC=20`, `LOOKBACK_DAYS=365`, `ALLOW_LIVE_PRICE=0`.
- **SPY path:** for the alpha calculation, SPY is forecast with only `[Drift, LinearTrend, Naive]` (Prophet/ARIMA never run on SPY by design).
- **Per-model timeout & isolation:** each model runs in a `multiprocessing.Process` with a 10s kill (psutil/`SIGKILL`), under a 20s total budget; models that exceed budget are skipped.

---

## 6. Current scoring / recommendation logic

1. **Per-model 5-day path.** Each active model outputs 5 daily price points.
2. **In-sample error filter.** For each model, `[MAE%, RMSE%]` are computed against **in-sample fitted values** (not out-of-sample). Models with both `<= WARN_ERR (5.0%)` are "eligible".
3. **Robust ensemble.** `robust_forecast` = element-wise **median** across eligible models; if none are eligible, falls back to the **mean** across all models. `ensemble_day5` = last point of that series.
4. **Expected return.** `d5_change_pct = (ensemble_day5 / current_price - 1) * 100`. (`current_price` = last DB close in production.)
5. **Agreement (heuristic).** Among models whose day-5 move exceeds the flat band `EPS_DIRECTION (0.30%)`, count bullish vs bearish; `agreement = max(bull,bear)/(bull+bear)`, needs `>= 2` directional models, else `null`.
6. **Confidence (heuristic, NOT calibrated).** If `>= 3` directional models: `confidence = clamp(0,100, 100 - CV%)` where `CV% = std(day5 values)/mean(day5 values)*100`. This measures **forecast dispersion/consensus tightness**, not probability of being correct. Default for HOLD downstream is 0.50.
7. **Residual alpha vs SPY.** Per-day `(ticker_return - SPY_return)`, reported as `alpha_by_day_pct` / `alpha_day5_bp`. **Computed and returned but NOT used in the recommendation rule.**
8. **Z-score.** `abs((ensemble_day5 - current_price) / std(last 60 closes))` — reported, not gating.
9. **Recommendation rule** (`make_recommendation`, thresholds are code defaults, not overridden in `api.env`):
   - Gate: only act if `agreement >= REC_MIN_AGREEMENT (0.55)` **and** `confidence >= REC_MIN_CONF (30)`.
   - Then on `rel = ensemble_day5/current_price - 1`:
     - `rel >= +2.0%` -> **Strong Buy**; `rel >= +0.5%` -> **Buy**
     - `rel <= -2.0%` -> **Strong Sell**; `rel <= -0.5%` -> **Sell**
   - Otherwise -> **Hold** (also Hold whenever the agreement/confidence gate fails).

So the final decision is driven **only by ensemble expected return, gated by a heuristic agreement+dispersion check**. Alpha vs SPY, z-score, and any risk measure do **not** influence BUY/SELL/HOLD.

---

## 7. Quant quality assessment

| Property | Status | Evidence |
|---|---|---|
| Point-in-time safe | **Partial / No** | Live serving uses last DB close (fine), but uses **adjusted close** (retroactively adjusted) and a **static universe** -> not point-in-time for any historical/backtest use. |
| Leakage safe | **No (for evaluation)** | "metrics" are **in-sample** fitted error; some fits (ARIMA/ETS/Prophet fittedvalues, OLS in-sample) see the whole window. Fine for live forecasting, invalid as a skill estimate. |
| Calibrated | **No** | "confidence" = `100 - CV%` of forecasts; no probability calibration, no reliability curve. |
| Backtested | **No** | No out-of-sample backtest harness exists; only in-sample error. |
| Walk-forward validated | **No** | None. |
| Robust to missing data | **Partial** | DB-miss -> yfinance fallback; empty -> error; per-model try/except + timeouts. But **no outlier/corporate-action checks**, no min-history gate. |
| Robust to regime changes | **No** | No volatility/regime/structural-break handling; short windows + drift assume local stationarity. |
| Appropriate for ranking S&P 500 candidates | **Limited** | Gives a fast directional/return estimate, but no cross-sectional normalization, no risk adjustment, no calibrated score; survivorship-biased universe. |
| Appropriate for position monitoring | **Limited** | 5-day price-only point forecast with no downside/interval; usable as a soft signal, not a risk monitor. |
| Appropriate for automated trading | **No** | Even with automation disabled: no costs, slippage, sizing, calibrated edge, or validation. Not safe to automate. |

---

## 8. What is real today vs what is missing

**Real (code-proven):**

- Postgres price store with ~6.5y daily history for ~491 tickers; yfinance refresh/fallback.
- 5 active per-request forecasters (Drift, LinearTrend, XGBoost-on-returns, Naive, SMA) + dormant Prophet/ARIMA/ETS/LSTM.
- Robust-median ensemble with in-sample error gate; 5-business-day horizon.
- Heuristic agreement + dispersion "confidence"; residual alpha vs SPY; z-score.
- Threshold-based Strong Buy/Buy/Hold/Sell/Strong Sell.
- Subprocess timeouts + total budget; single uvicorn worker.
- Separate `alerts_service.py` bot persists `model_runs`/`predictions` (357k runs / ~8.0M prediction rows) and sends Telegram alerts — **independent of Paper Trader's request path**.

**Missing (do NOT claim these exist):**

- Any **volume/OHLC** usage (present in DB, unused), **fundamentals, macro, sentiment, news, seasonality** (none).
- **Prediction intervals** (`lo`/`hi` always null), **downside/drawdown** estimate.
- **Out-of-sample backtest** and **walk-forward** validation; **confidence calibration**.
- **Transaction-cost, slippage, liquidity, position-sizing, sector, beta, regime** models.
- **Model versioning, monitoring, drift detection, benchmark-relative performance history** for the request path.
- **Point-in-time universe** (uses a hardcoded static S&P 500 list -> survivorship / membership look-ahead).
- **Artifact capture for Paper Trader's own calls** (its predictions are not logged anywhere).

---

## 9. Prioritized improvement roadmap

**Phase 1 - Make the service observable and auditable (local-only, no remote change).**
- Treat this document as the source of truth; surface its facts in the Paper Trader "Quant Model Methodology" panel.
- Add local-side request/response logging of every prediction Paper Trader receives (latency, ran/skipped models, ensemble, confidence, recommendation). No remote change required.
- Add a read-only health/version probe of `/healthz` + `/config` so the UI shows the live remote flags (FAST_ONLY, lookback, thresholds).

**Phase 2 - Capture prediction-run artifacts.**
- Persist each Paper Trader prediction (inputs sent, raw response, normalized fields, model_version=commit hash, timestamp) in the local Paper Trader DB. This creates the dataset needed for later calibration/backtesting without touching GCP.
- Optionally reconcile against the remote `model_runs`/`predictions` tables (already populated by the alerts bot) once a read path is agreed.

**Phase 3 - Improve the local pre-screen features.**
- The local S&P 500 pre-screen is price-only today; add point-in-time, sourced, testable features (liquidity/volume from the existing DB columns, volatility regime, cross-sectional momentum/relative-strength normalization). No faked data.

**Phase 4 - Improve remote prediction features / modeling.**
- Use the **already-stored volume/OHLC** before adding any external feed. Then consider point-in-time fundamentals/sector/regime — each only if sourced and timestamped.
- Re-evaluate the dormant models (Prophet seasonality, ARIMA, ETS) vs. cost; remove or formally retire dead code.

**Phase 5 - Add backtesting / walk-forward validation.**
- Build an out-of-sample, walk-forward harness with a frozen universe-as-of-date (fix survivorship) and adjusted-close handling that avoids look-ahead. Replace in-sample "metrics" with true out-of-sample skill, and calibrate "confidence" against realized hit-rate.

**Phase 6 - Add calibrated, risk-aware portfolio construction.**
- Add prediction intervals/downside estimates, transaction-cost and liquidity models, and position sizing. Only after Phases 2 and 5 produce a calibrated, validated edge. (Automation remains out of scope / disabled.)

---

## 10. Concrete recommended next coding task

**Implement Phase 2 locally: a Paper Trader "prediction run capture" store + read-only viewer.**

- On every prediction Paper Trader fetches, write a row to a new **local** table (e.g. `prediction_runs`) capturing: ticker, request payload, raw response JSON, normalized `{ensemble_day5, d5_change_pct, confidence, recommendation, per_model_summary}`, ran/skipped models, latency, and `model_version` (the remote commit hash from `/config` or a pinned constant). 
- Add a read-only `GET /v1/model/prediction-runs` endpoint + a compact Audit/Advanced UI table (no orders, no automation, no remote writes).
- This is fully local, preview-safe, requires no GCP change, and produces the labeled dataset that Phases 5-6 (calibration, backtesting, risk) depend on.

> Rationale: the single biggest gap is that **Paper Trader's own predictions are never recorded**, so today there is no way to measure whether the remote model is actually right. Capturing them is the prerequisite for every downstream quant improvement and is the lowest-risk next step.

---

### Appendix - audit method (read-only)

- `gcloud compute ssh --tunnel-through-iap` (read-only commands) for: `systemctl cat stock-api.service`, directory/file inventory, `git remote/log`, masked `api.env` keys, non-secret predictor flags, and `psql` schema/coverage introspection.
- `gcloud compute scp` pulled `api_server.py` and `load_sp500_to_db.py` into local `.tmp/prediction_service_audit/` for line-level reading (uncommitted; not part of the repo).
- No remote file was edited; no service restarted; no migration/package/env change performed.
</content>
</invoke>
