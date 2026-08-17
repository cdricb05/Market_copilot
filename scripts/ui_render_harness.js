/* scripts/ui_render_harness.js — Slice 2 (Phase 29C.1) async-order-independence harness.
 *
 * Extracts the ACTUAL canonical render functions from api/ui/index.html and runs
 * them against a minimal in-process DOM shim in several completion orders, then
 * dumps the final visible text of every primary operator-interpretation node. It
 * proves — by executing the real code, not by inspecting source — that the final
 * visible state is identical whatever order the specialized loaders complete in,
 * because the canonical value wins by OWNERSHIP (static guard + data-wf-owned
 * stamp), not by timing.
 *
 * Depends only on Node's built-in `vm` module (no jsdom / npm install). Used by
 * tests/test_slice2_ui_hard_cutover.py and the handoff restart_smoke.ps1.
 *
 * Usage:  node scripts/ui_render_harness.js <index.html> <payload.json>
 *   payload.json = { "ws": <workflow-state dict>, "gate": <daily-action-gate dict> }
 * Prints a JSON report to stdout: { orders: {name: dump}, all_equal, canonical_keys }.
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
  return extractFrom(idx);
}
function grabAssign(lhs) {
  const idx = src.indexOf(lhs);
  if (idx === -1) throw new Error('assignment not found: ' + lhs);
  return extractFrom(idx) + ';';
}

const FUNCS = [
  '_wsIsCanonicalNode', '_wsOwnSet', '_wsOwnHtml', '_wsGuardedSet',
  '_wsSevColor', '_wsRoute', '_wsBannerHtml',
  'dispatchCanonicalPrimaryAction', '_wsDailyCloseGate',
  'renderWorkflowState', '_wsApplyRightPanel', '_wsApplyAssessmentFraming', '_wsApplyEvidence',
  'renderDailyActionGate', '_dagApplySafeFraming',
  '_dagSet', '_dcSet', '_obSet', '_dagSevColor', '_dagPct', '_dagChangesHtml', '_dagTstate',
  // Release 29 UI consolidation: the opportunity-cost COUNT summary is rendered from
  // the same assessment_presentation the canonical framing owns, so it runs here too
  // and the ownership assertions cover it.
  '_r29RenderHocCounts',
];
// _wsEsc / escapeHtml contain a regex char-class with a quote (/[&<>"]/g) that the
// lightweight extractor cannot brace-match; they only escape HTML, so the harness
// provides equivalent stubs (the ownership assertions read textContent, not markup).
let code = grabAssign('window.WS_CANONICAL_NODES =') + '\n';
// Operator Action Integrity: _wsBannerHtml renders the navigational view-link
// labels from this top-level map — extract it so the real code runs unmodified.
code += grabAssign('var _WS_VIEW_LABEL =') + '\n';
for (const f of FUNCS) code += grabFn(f) + '\n';

// ---- minimal DOM shim -------------------------------------------------------- #
let registry = {};
function makeNode(id) {
  const attrs = {};
  const style = {};
  return {
    id, _t: '', _h: '', style, onclick: null, attrs,
    get textContent() { return this._t; }, set textContent(v) { this._t = (v == null ? '' : String(v)); },
    get innerHTML() { return this._h; }, set innerHTML(v) { this._h = (v == null ? '' : String(v)); },
    setAttribute(k, v) { attrs[k] = String(v); },
    getAttribute(k) { return (k in attrs) ? attrs[k] : null; },
    closest() { return null; },
  };
}
const document = { getElementById(id) { return (registry[id] || (registry[id] = makeNode(id))); } };

function _esc(s) {
  s = (s == null) ? '' : String(s);
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
const sandbox = {};
sandbox.window = sandbox;
sandbox.document = document;
sandbox._wsEsc = _esc;
sandbox.escapeHtml = _esc;
sandbox.navigateToRoute = function () {};      // canonical destination nav (not fired here)
sandbox.renderChecksPerformed = function () {}; // detail renderer (out of scope for ownership)
sandbox.renderOtr = function () {};
sandbox.console = console;
vm.createContext(sandbox);
vm.runInContext(code, sandbox);

// ---- scenario engine --------------------------------------------------------- #
const CANON_RIGHT = ['right-current-task', 'right-next-action', 'right-primary-action-btn',
  'right-dc-badge', 'right-dag-badge'];
const CANON_DAG = [];
for (const p of ['cc', 'dw', 'pm']) for (const s of ['title', 'badge', 'headline', 'explanation'])
  CANON_DAG.push(p + '-dag-' + s);
const CANON = CANON_RIGHT.concat(CANON_DAG);
const DETAIL = ['cc-dag-market', 'cc-dag-review', 'pm-dag-eval', 'pm-dag-turnover', 'pm-dag-cost'];

function step(name) {
  const ws = payload.ws, gate = payload.gate;
  if (name === 'ws') sandbox.window._wsData = ws, sandbox.renderWorkflowState(ws);
  else if (name === 'gate') sandbox.window._dagData = gate, sandbox.renderDailyActionGate(gate);
  else if (name === 'guarded_pre') sandbox._dagApplySafeFraming(gate);
  else if (name === 'dc_attack') CANON.forEach(function (id) { sandbox._dcSet(id, 'ATTACK_DC'); });
  else if (name === 'ob_attack') CANON.forEach(function (id) { sandbox._obSet(id, 'ATTACK_OB'); });
  else if (name === 'dag_attack') CANON.forEach(function (id) { sandbox._dagSet(id, 'ATTACK_DAG'); });
}
function dump() {
  const out = { canonical: {}, owned: {}, detail: {}, evidence: '' };
  CANON.forEach(function (id) {
    const el = registry[id]; out.canonical[id] = el ? el.textContent : null;
    out.owned[id] = el ? el.getAttribute('data-wf-owned') : null;
  });
  DETAIL.forEach(function (id) { const el = registry[id]; out.detail[id] = el ? el.textContent : null; });
  const ev = registry['wf-evidence']; out.evidence = ev ? ev.innerHTML : '';
  const wf = registry['wf-cc']; out.banner = wf ? wf.innerHTML : '';
  return out;
}
function run(steps) { registry = {}; sandbox.window._wsData = null; sandbox.window._dagData = null; steps.forEach(step); return dump(); }

const orders = {
  ws_then_gate: run(['ws', 'gate']),
  gate_then_ws: run(['gate', 'ws']),
  guardedpre_then_ws: run(['guarded_pre', 'ws']),
  ws_then_gate_then_attacks: run(['ws', 'gate', 'dc_attack', 'ob_attack', 'dag_attack']),
  gate_then_ws_then_attacks: run(['gate', 'ws', 'dc_attack', 'ob_attack', 'dag_attack']),
  ws_only: run(['ws']),
};
// Equality of the CANONICAL node texts across every order (the required proof).
const ref = JSON.stringify(orders.ws_then_gate.canonical);
let all_equal = true;
for (const k of Object.keys(orders)) {
  if (k === 'ws_only') continue;               // ws_only lacks gate detail; canonical must still match
  if (JSON.stringify(orders[k].canonical) !== ref) all_equal = false;
}
// ws_only canonical must also equal (gate detail is separate from canonical framing).
if (JSON.stringify(orders.ws_only.canonical) !== ref) all_equal = false;

console.log(JSON.stringify({ orders, all_equal, canonical_keys: CANON }, null, 1));
