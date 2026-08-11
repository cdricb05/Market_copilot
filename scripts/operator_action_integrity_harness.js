/* scripts/operator_action_integrity_harness.js — Operator Action Integrity slice.
 *
 * Executes the ACTUAL canonical-action UI code from api/ui/index.html (the ONE
 * dispatcher, the Today hero, the right Action/Safety panel, the Daily-Close
 * secondary panels and the real execution handlers) against a minimal in-process
 * DOM shim, one fresh sandbox per canonical workflow state. It proves — by
 * running the real code, not by inspecting source — that:
 *
 *   * the canonical primary action is never a dead control: an executable primary
 *     POSTs through its EXISTING owner (paper-desk refresh / Daily Research Cycle
 *     / Daily Close) and a non-executable primary navigates;
 *   * double submission is prevented;
 *   * a secondary Daily-Close panel can never execute (or even offer) a close the
 *     canonical workflow forbids;
 *   * after completion the canonical state says "no action required" and the
 *     legacy stage row is re-framed as optional review, not required work.
 *
 * Depends only on Node's built-in `vm` module (no jsdom / npm install). Used by
 * tests/test_operator_action_integrity.py.
 *
 * Usage:  node scripts/operator_action_integrity_harness.js <index.html> <payload.json>
 *   payload.json = { "states": { name: { "ws": <workflow-state payload>,
 *                                        "dc": <daily-close payload or null>,
 *                                        "cc": <command-center data or null> } } }
 * Prints a JSON report to stdout: { states: { name: <observations> } }.
 */
'use strict';
const fs = require('fs');
const vm = require('vm');

const UI = process.argv[2];
const PAYLOAD = process.argv[3];
const src = fs.readFileSync(UI, 'utf8');
const payload = JSON.parse(fs.readFileSync(PAYLOAD, 'utf8'));

// ---- string/comment-aware balanced-brace extractor --------------------------- #
function extractFrom(startIdx) {
  let i = src.indexOf('{', startIdx);
  let depth = 0, inStr = null, line = false, block = false;
  for (; i < src.length; i++) {
    const c = src[i], n = src[i + 1];
    if (line) { if (c === '\n') line = false; continue; }
    if (block) { if (c === '*' && n === '/') { block = false; i++; } continue; }
    if (inStr) { if (c === '\\') { i++; continue; } if (c === inStr) inStr = null; continue; }
    if (c === '/' && n === '/') { line = true; i++; continue; }
    if (c === '/' && n === '*') { block = true; i++; continue; }
    if (c === '"' || c === "'" || c === '`') { inStr = c; continue; }
    if (c === '{') depth++;
    else if (c === '}') { depth--; if (depth === 0) return src.slice(startIdx, i + 1); }
  }
  throw new Error('unbalanced from ' + startIdx);
}
function grabFn(name) {
  const idx = src.indexOf('function ' + name + '(');
  if (idx === -1) throw new Error('function not found: ' + name);
  // Include a leading `async ` so async handlers stay async when re-evaluated.
  const asyncIdx = src.lastIndexOf('async ', idx);
  const start = (asyncIdx !== -1 && asyncIdx + 6 === idx) ? asyncIdx : idx;
  return src.slice(start, idx) + extractFrom(idx);
}
function grabAssign(lhs) {
  const idx = src.indexOf(lhs);
  if (idx === -1) throw new Error('assignment not found: ' + lhs);
  return extractFrom(idx) + ';';
}

// The REAL canonical-action code under test (everything else is a shim).
const FUNCS = [
  '_wsIsCanonicalNode', '_wsOwnSet', '_wsOwnHtml', '_wsGuardedSet',
  '_wsSevColor', '_wsRoute', '_wsBannerHtml', 'renderWorkflowState',
  '_wsApplyRightPanel', '_wsRenderTodayHero',
  'dispatchCanonicalPrimaryAction', '_wsExecuteOwnedDataRefresh',
  'wsExecConfirmYes', 'wsExecConfirmNo', 'runDailyResearchCycle',
  '_wsDailyCloseGate', '_wsIsNoOpState', '_dcIsLegacyCompat', '_dcApplyHeadline',
  'renderDailyClose', 'renderDailyClosePm', 'renderDailyClosePerf',
  '_dcRunButtons', '_dcFmtElapsed', '_dcShowRunOutcome', '_dcSet',
  '_ccApplyWorkflowStages', '_obStageFraming', '_obReframeStageStatuses',
  'pdDoAction', 'runDailyClose',
];
let code = grabAssign('window.WS_CANONICAL_NODES =') + '\n';
code += grabAssign('var _WS_VIEW_LABEL =') + '\n';
code += grabAssign('var _PD_ACTIONS =') + '\n';
for (const f of FUNCS) code += grabFn(f) + '\n';

// ---- minimal DOM shim -------------------------------------------------------- #
function makeNode(id) {
  const attrs = {};
  const style = {};
  return {
    id, _t: '', _h: '', style, onclick: null, attrs, disabled: false, open: false,
    className: '', title: '',
    get textContent() { return this._t; }, set textContent(v) { this._t = (v == null ? '' : String(v)); },
    get innerHTML() { return this._h; }, set innerHTML(v) { this._h = (v == null ? '' : String(v)); },
    setAttribute(k, v) { attrs[k] = String(v); },
    getAttribute(k) { return (k in attrs) ? attrs[k] : null; },
    removeAttribute(k) { delete attrs[k]; },
    closest() { return null; },
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
}

function _esc(s) {
  s = (s == null) ? '' : String(s);
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function buildSandbox(rec, responses) {
  const registry = {};
  const document = { getElementById(id) { return (registry[id] || (registry[id] = makeNode(id))); } };
  const sandbox = {};
  sandbox.window = sandbox;
  sandbox.document = document;
  sandbox._registry = registry;
  sandbox._wsEsc = _esc;
  sandbox.escapeHtml = _esc;
  sandbox.console = console;
  sandbox.setTimeout = setTimeout;
  sandbox.clearTimeout = clearTimeout;
  sandbox.setInterval = setInterval;
  sandbox.clearInterval = clearInterval;
  sandbox.Date = Date;
  sandbox.Promise = Promise;
  // ---- recorders (the observable contract) ----
  sandbox.call = function (method, path, body) {
    rec.posts.push({ method, path, body: body || null });
    const resp = (responses && responses[path]) || {};
    return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(resp) });
  };
  sandbox.navigateToRoute = function (route) { rec.navs.push(route); };
  sandbox.toast = function (msg, kind) { rec.toasts.push({ msg: String(msg), kind: kind || '' }); };
  sandbox.renderWorkflowStage = function (id, text, cls, sub) {
    rec.stages[id] = { text, cls, sub };
  };
  sandbox.loadWorkflowState = function () { rec.loads.push('workflow-state'); return Promise.resolve(); };
  sandbox.loadDailyResearchCycle = function () { rec.loads.push('drc'); return Promise.resolve(); };
  sandbox.loadDataFreshness = function () { rec.loads.push('freshness'); return Promise.resolve(); };
  sandbox.loadPaperDesk = function () { rec.loads.push('paper-desk'); return Promise.resolve(); };
  sandbox.loadOperationalBook = function () { rec.loads.push('operational-book'); return Promise.resolve(); };
  sandbox.loadDailyAlphaStatus = function () { rec.loads.push('daily-alpha'); return Promise.resolve(); };
  sandbox.pmOpenAdvExec = function () { rec.loads.push('pm-open-adv'); };
  // ---- display-only helpers outside the contract under test ----
  sandbox._wsApplyAssessmentFraming = function () {};
  sandbox._wsApplyEvidence = function () {};
  sandbox.renderDailyActionGate = function () {};
  sandbox._dcSevColor = function () { return '#888'; };
  sandbox._dcMoney = function () { return ''; };
  sandbox._dcPct = function () { return ''; };
  sandbox._fePp = function () { return ''; };
  sandbox._dcReadinessText = function () { return ''; };
  sandbox._dcCycleHtml = function (stages) { rec.cycle_stages = stages || []; return ''; };
  sandbox._dcProposalHtml = function () { return ''; };
  sandbox.renderChecksPerformed = function () {};
  sandbox._dcRenderRunSteps = function () {};
  sandbox.recoverForwardEvidence = function () {};
  sandbox._DC_RUN_BTN_IDS = ['cc-dc-btn', 'dw-dc-btn', 'dc-perf-btn', 'pm-dc-btn'];
  sandbox._dcRunInFlight = false;
  sandbox._drcRunning = false;
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

function flush(ms) { return new Promise((res) => setTimeout(res, ms || 25)); }

function snapNode(sb, id) {
  const n = sb._registry[id];
  if (!n) return null;
  return { text: n.textContent, html: n.innerHTML, display: n.style.display,
           disabled: !!n.disabled, attrs: Object.assign({}, n.attrs), className: n.className };
}

async function runState(name, st) {
  const rec = { posts: [], navs: [], toasts: [], loads: [], stages: {}, cycle_stages: null };
  const sb = buildSandbox(rec, st.responses || {});
  if (st.cc) sb._commandCenter = { data: st.cc };
  // Deliberately load the SECONDARY daily-close payload FIRST (the historically
  // dangerous order), then the canonical workflow payload — the final visible
  // state must obey the canonical gate by ownership, not timing.
  if (st.dc) { sb._dcData = st.dc; sb.renderDailyClose(st.dc); }
  const preWs = { dcBtn: snapNode(sb, 'cc-dc-btn') };
  sb._wsData = st.ws;
  sb.renderWorkflowState(st.ws);
  await flush();

  const out = { pre_ws: preWs };
  out.hero = snapNode(sb, 'today-hero');
  out.right_task = snapNode(sb, 'right-current-task');
  out.right_next = snapNode(sb, 'right-next-action');
  out.right_btn = snapNode(sb, 'right-primary-action-btn');
  out.banner_dw = snapNode(sb, 'wf-dw');
  out.dc = {
    cc_btn: snapNode(sb, 'cc-dc-btn'), dw_btn: snapNode(sb, 'dw-dc-btn'),
    pm_btn: snapNode(sb, 'pm-dc-btn'), perf_btn: snapNode(sb, 'dc-perf-btn'),
    cc_headline: snapNode(sb, 'cc-dc-headline'), dw_headline: snapNode(sb, 'dw-dc-headline'),
    cc_badge: snapNode(sb, 'cc-dc-badge'), pm_badge: snapNode(sb, 'pm-dc-badge'),
  };
  out.stages = JSON.parse(JSON.stringify(rec.stages));
  // Defect 4: how a lifecycle NEEDS_ACTION stage (e.g. the manual model-target
  // review) is framed under THIS canonical state.
  out.ob_needs_action_framing = sb._obStageFraming('NEEDS_ACTION');
  // Defect 4: what the five-stage close cycle actually rendered (post-mapping).
  out.rendered_cycle_stages = rec.cycle_stages;

  // ---- primary-action click through the ONE canonical dispatcher ----
  const fakeBtn = sb.document.getElementById('harness-primary-btn');
  fakeBtn.textContent = (st.ws.primary_action && st.ws.primary_action.label) || '';
  sb.dispatchCanonicalPrimaryAction(fakeBtn);
  out.confirm_panel_after_click = snapNode(sb, 'ws-exec-confirm');
  const panelShown = !!(out.confirm_panel_after_click && out.confirm_panel_after_click.display === '');
  if (panelShown) {
    out.confirm_title = snapNode(sb, 'ws-exec-confirm-title');
    out.confirm_phrase = snapNode(sb, 'ws-exec-confirm-phrase');
    sb.wsExecConfirmYes();
  }
  await flush(40);
  out.posts_after_click = rec.posts.slice();
  out.navs_after_click = rec.navs.slice();
  out.loads_after_click = rec.loads.slice();

  // ---- double-submit probe: two immediate dispatches must yield ONE new POST ----
  rec.posts.length = 0; rec.navs.length = 0;
  sb.dispatchCanonicalPrimaryAction(fakeBtn);
  sb.dispatchCanonicalPrimaryAction(fakeBtn);
  const panel2 = sb._registry['ws-exec-confirm'];
  if (panel2 && panel2.style.display === '') { sb.wsExecConfirmYes(); sb.wsExecConfirmYes(); }
  await flush(40);
  out.posts_after_double = rec.posts.slice();

  // ---- forbidden-close probe: a direct (stale-button) close attempt ----
  rec.posts.length = 0; rec.toasts.length = 0;
  sb.runDailyClose(null);
  await flush(40);
  out.posts_after_close_attempt = rec.posts.slice();
  out.toasts_after_close_attempt = rec.toasts.slice();

  return out;
}

(async () => {
  const report = { states: {} };
  for (const [name, st] of Object.entries(payload.states || {})) {
    report.states[name] = await runState(name, st);
  }
  process.stdout.write(JSON.stringify(report));
})().catch((e) => { console.error(e && e.stack || String(e)); process.exit(1); });
