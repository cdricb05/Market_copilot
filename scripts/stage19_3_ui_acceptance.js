/**
 * scripts/stage19_3_ui_acceptance.js — Stage 19.3 HERMETIC browser acceptance.
 *
 * Loads the REAL api/ui/index.html in Chromium and drives the five required operator
 * scenarios at 1920x1080 and 1440x900.
 *
 * HERMETIC BY CONSTRUCTION — it never touches live production state:
 *   * the page is served from a throwaway local static server (never the live 8001);
 *   * EVERY /v1/** request is intercepted and answered from generated fixtures;
 *   * any non-GET request is FAILED and recorded as a violation (a page load must
 *     never write), so an accidental mutation cannot reach a backend at all;
 *   * no live desk / ledger / plan store is opened, and no provider or prediction
 *     service is contacted.
 *
 * Usage:
 *   node scripts/stage19_3_ui_acceptance.js <index.html> <fixtures.json> <out.json> [shotDir]
 */
'use strict';

const fs = require('fs');
const http = require('http');
const path = require('path');
const { chromium } = require('playwright');

const UI = process.argv[2];
const FIXTURES = process.argv[3];
const OUT = process.argv[4];
const SHOTS = process.argv[5] || null;

const fixtures = JSON.parse(fs.readFileSync(FIXTURES, 'utf8'));
const html = fs.readFileSync(UI, 'utf8');
const VIEWPORTS = [{ w: 1920, h: 1080 }, { w: 1440, h: 900 }];

// ---- throwaway static server (never the live backend) ------------------------ #
function serve() {
  return new Promise((resolve) => {
    const assetDir = path.dirname(UI);
    const srv = http.createServer((req, res) => {
      const p = (req.url || '').split('?')[0];
      // Sibling static assets (pt_charts.js, analytics.html) must be served for real —
      // returning the HTML shell for a .js request produces a spurious SyntaxError.
      const m = /^\/ui\/([A-Za-z0-9_.-]+)$/.exec(p);
      if (m) {
        const f = path.join(assetDir, m[1]);
        if (fs.existsSync(f)) {
          const ct = f.endsWith('.js') ? 'application/javascript; charset=utf-8'
            : f.endsWith('.css') ? 'text/css; charset=utf-8' : 'text/html; charset=utf-8';
          res.writeHead(200, { 'Content-Type': ct });
          res.end(fs.readFileSync(f));
          return;
        }
      }
      if (p.startsWith('/ui')) {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(html);
        return;
      }
      res.writeHead(404).end('not found');
    });
    srv.listen(0, '127.0.0.1', () => resolve(srv));
  });
}

function pathOf(url) {
  try { return new URL(url).pathname; } catch (e) { return url; }
}

async function runScenario(browser, name, scen, vp) {
  const ctx = await browser.newContext({ viewport: { width: vp.w, height: vp.h } });
  const violations = [];
  const consoleErrors = [];
  const dialogs = [];
  const nonGet = [];

  const page = await ctx.newPage();
  page.on('console', (m) => {
    if (m.type() === 'error') consoleErrors.push(m.text().slice(0, 400));
  });
  page.on('pageerror', (e) => consoleErrors.push('pageerror: ' + String(e).slice(0, 400)));
  // A native browser dialog is a hard failure of the project UI contract.
  page.on('dialog', async (d) => { dialogs.push(d.type() + ': ' + d.message()); await d.dismiss(); });

  await ctx.addInitScript(() => {
    try { localStorage.setItem('apiKey', 'HERMETIC_TEST_KEY'); } catch (e) {}
  });

  // Intercept EVERY backend call. GET -> fixture (or a benign empty payload).
  // Anything else is aborted and recorded: a page load must never write.
  await page.route('**/v1/**', async (route) => {
    const req = route.request();
    const p = pathOf(req.url());
    if (req.method() !== 'GET') {
      nonGet.push(req.method() + ' ' + p);
      await route.abort();
      return;
    }
    // Fixtures where the scenario defines one; otherwise a benign shape that satisfies
    // the generic panel loaders so an UNMOCKED panel cannot masquerade as a UI defect.
    const body = Object.prototype.hasOwnProperty.call(scen, p)
      ? scen[p]
      : p === '/v1/review/workflow-status'
        ? { status: 'OK', hermetic_stub: true,
            review_candidates: { total: 0, approved_for_signal: 0 },
            review_created_signals: { total: 0, received: 0 },
            review_created_trade_decisions: { total: 0, order_eligible: 0 },
            orders: { total: 0, filled: 0, pending: 0 } }
        : { status: 'OK', hermetic_stub: true, rows: [], items: [], warnings: [],
            indicators: [], placeholders: [], series: [], holdings: [], orders: [],
            fills: [], entries: [], events: [], top25: [], top50: [] };
    await route.fulfill({
      status: 200, contentType: 'application/json', body: JSON.stringify(body),
    });
  });

  await page.goto(`${global.__BASE__}/ui/`, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(2600);

  // The current-rebalance lineage strip lives on the Portfolio Manager surface, so the
  // acceptance must actually OPEN it — verifying the counts are visible to the
  // operator, not merely present in the DOM.
  await page.evaluate(() => {
    try { if (typeof navigateToRoute === 'function') navigateToRoute('portfolio-manager'); } catch (e) {}
  });
  await page.waitForTimeout(1400);

  const probe = await page.evaluate(() => {
    const q = (sel) => document.querySelector(sel);
    const txt = (sel) => { const e = q(sel); return e ? (e.textContent || '').trim() : null; };
    const vis = (e) => {
      if (!e) return false;
      const cs = getComputedStyle(e);
      if (cs.display === 'none' || cs.visibility === 'hidden' || Number(cs.opacity) === 0) return false;
      const r = e.getBoundingClientRect();
      return r.width > 0 && r.height > 0;
    };
    const inCollapsed = (e) => {
      for (let n = e; n; n = n.parentElement) {
        if (n.tagName === 'DETAILS' && !n.open) return true;
      }
      return false;
    };
    const cmd = q('#operator-command');
    const cmdBtn = q('#opc-primary-btn');

    // Every VISIBLE, ENABLED control that is not inside a collapsed area.
    const visibleButtons = Array.from(document.querySelectorAll('button, a[role="button"]'))
      .filter((b) => vis(b) && !b.disabled && !inCollapsed(b))
      .map((b) => ({
        text: (b.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 80),
        id: b.id || null,
        inCommandBar: !!(cmd && cmd.contains(b)),
        isRailMirror: b.id === 'right-primary-action-btn',
      }))
      .filter((b) => b.text);

    const bodyText = document.body.innerText || '';
    return {
      command: cmd ? {
        present: true,
        visible: vis(cmd),
        state: cmd.getAttribute('data-op-state'),
        passive: cmd.getAttribute('data-op-passive'),
        actionAvailable: cmd.getAttribute('data-op-action-available'),
        task: txt('#operator-command .opc-task'),
        why: txt('#operator-command .opc-why'),
        next: txt('#opc-no-action'),
        supporting: txt('#opc-supporting'),
        ctaLabel: cmdBtn ? (cmdBtn.textContent || '').trim() : null,
        ctaVisible: vis(cmdBtn),
        rect: (() => { const r = cmd.getBoundingClientRect(); return { top: r.top, h: r.height, w: r.width }; })(),
      } : { present: false },
      currentRebalance: {
        rowVisible: vis(q('#pm-lc-current')),
        submitted: txt('#pm-lc-cur-submitted'),
        filled: txt('#pm-lc-cur-filled'),
        cancelled: txt('#pm-lc-cur-cancelled'),
        buys: txt('#pm-lc-cur-buys'),
        sells: txt('#pm-lc-cur-sells'),
        plan: txt('#pm-lc-cur-plan'),
        approved: txt('#pm-lc-cur-approved'),
        note: txt('#pm-lc-cur-next'),
        historicalFills: txt('#pm-lc-histfills'),
      },
      deskMaintenance: (() => {
        const d = q('#pd-maintenance');
        const b = q('#pd-act-refresh');
        return {
          detailsPresent: !!d,
          detailsOpen: d ? !!d.open : null,
          refreshLabel: b ? (b.textContent || '').trim() : null,
          refreshVisibleInNormalPath: vis(b) && !inCollapsed(b),
        };
      })(),
      rightRail: { next: txt('#right-next-action'), btnVisible: vis(q('#right-primary-action-btn')),
                   btnLabel: txt('#right-primary-action-btn') },
      visibleButtons,
      bodyHasRefreshAfterMarketClose: bodyText.indexOf('Refresh After Market Close') !== -1,
      horizontalOverflow: document.documentElement.scrollWidth > window.innerWidth + 1,
      scrollWidth: document.documentElement.scrollWidth,
      innerWidth: window.innerWidth,
      brokerControls: Array.from(document.querySelectorAll('button'))
        .filter((b) => vis(b) && /broker|live order|place order|automation on|enable automation/i.test(b.textContent || ''))
        .map((b) => (b.textContent || '').trim()),
      auditReachable: !!q('#pm-adv-exec') && !!q('#pm-order-fill-history'),
    };
  });

  if (SHOTS) {
    fs.mkdirSync(SHOTS, { recursive: true });
    await page.screenshot({ path: path.join(SHOTS, `${name}-${vp.w}x${vp.h}.png`) });
  }

  // ---- assertions (the acceptance contract) ---------------------------------- #
  const cmd = probe.command;
  if (!cmd.present || !cmd.visible) violations.push('operator command bar not visible');
  if (cmd.present && cmd.rect.top > 400) violations.push('command bar is not near the top');

  // ONE normal-path mutation control, and only when the backend allows it.
  const mutationVerb = /^(run|confirm|approve|submit|execute|refresh desk|create|apply)\b/i;
  const enabledMutations = probe.visibleButtons.filter(
    (b) => mutationVerb.test(b.text) && !/^refresh view$/i.test(b.text));
  if (cmd.actionAvailable === '0') {
    if (cmd.ctaVisible) violations.push('passive state still renders a command CTA');
    if (enabledMutations.length) {
      violations.push('passive state exposes mutation controls: '
        + enabledMutations.map((b) => b.text).join(' | '));
    }
    if (probe.rightRail.btnVisible) violations.push('right rail shows a CTA in a passive state');
  } else {
    if (!cmd.ctaVisible) violations.push('actionable state renders no command CTA');
    // Exactly ONE execution surface (the command bar) plus its ONE sanctioned mirror
    // (the right action rail), which must carry the IDENTICAL label. Anything else is
    // a competing control — the defect this slice removes.
    const outside = enabledMutations.filter((b) => !b.inCommandBar && !b.isRailMirror);
    if (outside.length) {
      violations.push('competing mutation controls outside the command bar: '
        + outside.map((b) => `${b.text}${b.id ? ' #' + b.id : ''}`).join(' | '));
    }
    const mirror = enabledMutations.find((b) => b.isRailMirror);
    if (mirror && cmd.ctaLabel && mirror.text !== cmd.ctaLabel) {
      violations.push(`right rail reinterprets the command ("${mirror.text}" vs "${cmd.ctaLabel}")`);
    }
    const inBar = enabledMutations.filter((b) => b.inCommandBar);
    if (inBar.length > 1) {
      violations.push('more than one action inside the command bar: '
        + inBar.map((b) => b.text).join(' | '));
    }
  }
  if (probe.bodyHasRefreshAfterMarketClose) violations.push('normal path shows "Refresh After Market Close"');
  if (probe.deskMaintenance.refreshVisibleInNormalPath) {
    violations.push('desk refresh is exposed in the normal operator path');
  }
  if (probe.deskMaintenance.detailsOpen === true) violations.push('maintenance area is expanded by default');
  if (probe.horizontalOverflow) violations.push(`horizontal body overflow (${probe.scrollWidth} > ${probe.innerWidth})`);
  if (dialogs.length) violations.push('native browser dialog(s): ' + dialogs.join(' | '));
  if (nonGet.length) violations.push('non-GET request on page load: ' + nonGet.join(' | '));
  if (consoleErrors.length) violations.push('console error(s): ' + consoleErrors.slice(0, 3).join(' | '));
  if (probe.brokerControls.length) violations.push('broker/automation control(s): ' + probe.brokerControls.join(' | '));
  if (!probe.auditReachable) violations.push('raw audit detail is not reachable');

  // Lineage: the CURRENT rebalance is stated on its own, and the book's historical
  // implementation fills are labelled SEPARATELY — never mixed into one count.
  const cr = probe.currentRebalance;
  if (scen.expect_current_rebalance) {
    const e = scen.expect_current_rebalance;
    if (!cr.rowVisible) violations.push('current-rebalance row is not visible on Portfolio Manager');
    for (const [k, want] of Object.entries(e)) {
      if (String(cr[k]) !== String(want)) {
        violations.push(`current rebalance ${k}: expected ${want}, saw ${cr[k]}`);
      }
    }
    if (String(cr.historicalFills) === String(cr.filled)) {
      violations.push('historical implementation fills are indistinguishable from current-plan fills');
    }
    if (cr.note && !/Current rebalance: /.test(cr.note)) {
      violations.push('current-rebalance note is not lineage-labelled');
    }
  }

  await ctx.close();
  return { scenario: name, title: scen.title, viewport: `${vp.w}x${vp.h}`,
           probe, consoleErrors, dialogs, nonGet, violations, pass: violations.length === 0 };
}

(async () => {
  const srv = await serve();
  global.__BASE__ = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch();
  const results = [];
  try {
    for (const [name, scen] of Object.entries(fixtures)) {
      for (const vp of VIEWPORTS) {
        results.push(await runScenario(browser, name, scen, vp));
      }
    }
  } finally {
    await browser.close();
    srv.close();
  }
  const failed = results.filter((r) => !r.pass);
  const report = {
    generated_at: new Date().toISOString(),
    hermetic: true, live_backend_contacted: false, non_get_requests_blocked: true,
    viewports: VIEWPORTS.map((v) => `${v.w}x${v.h}`),
    total: results.length, passed: results.length - failed.length, failed: failed.length,
    results,
  };
  fs.writeFileSync(OUT, JSON.stringify(report, null, 1));
  for (const r of results) {
    console.log(`${r.pass ? 'PASS' : 'FAIL'}  ${r.scenario} @ ${r.viewport}`);
    for (const v of r.violations) console.log('        - ' + v);
  }
  console.log(`\n${report.passed}/${report.total} scenario runs passed`);
  process.exit(failed.length ? 1 : 0);
})().catch((e) => { console.error(e && e.stack || String(e)); process.exit(2); });
