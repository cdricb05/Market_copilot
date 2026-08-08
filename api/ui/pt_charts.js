/* ============================================================================
   Phase 29J.3 — PTC: interactive Portfolio chart toolkit (shared)
   ----------------------------------------------------------------------------
   Hand-rolled, dependency-free, theme-aware interactive charts used by BOTH the
   Portfolio page (inline cards) and the dedicated drill-down detail view
   (analytics.html). NO external chart library.

   Every chart:
     - renders backend series VERBATIM (the browser performs no portfolio /
       P&L / benchmark / return math; it only lays out pixels),
     - supports a hover CROSSHAIR + floating TOOLTIP with the EXACT date + value,
     - degrades to an explicit empty state when data is unavailable,
     - never mutates state, creates a signal, or places an order.

   Self-contained: defines its own formatters + a single shared tooltip element,
   so it works standalone in analytics.html without the index.html inline helpers.
   ========================================================================== */
(function (global) {
  'use strict';

  var PTC = {};

  PTC.COLORS = {
    book: '#3b82f6', spy: '#8aa0c0', pos: '#22c55e', neg: '#ef4444',
    warn: '#f59e0b', info: '#3b82f6', muted: '#7a8fb0', muted2: '#556080',
    grid: 'rgba(122,143,176,.16)', zero: 'rgba(122,143,176,.45)',
    crosshair: 'rgba(168,184,216,.55)'
  };
  // A small qualitative palette for donut/sector segments (colour-by-series).
  PTC.PALETTE = ['#3b82f6', '#22c55e', '#f59e0b', '#a855f7', '#ec4899',
    '#14b8a6', '#8aa0c0', '#ef4444', '#eab308', '#60a5fa', '#34d399', '#f472b6'];

  /* ---- formatters (self-contained) ---------------------------------------- */
  function isNum(v) { return v !== null && v !== undefined && isFinite(Number(v)); }
  PTC.money = function (v) {
    if (!isNum(v)) return '—';
    var n = Number(v);
    return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString('en-US', { maximumFractionDigits: 0 });
  };
  PTC.moneyc = function (v) {
    if (!isNum(v)) return '—';
    var n = Number(v);
    return (n < 0 ? '-$' : '$') + Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  PTC.pct = function (v, nd) {            // v already in percent units (e.g. 1.5 => +1.50%)
    if (!isNum(v)) return '—';
    nd = (nd == null ? 2 : nd); v = Number(v);
    return (v >= 0 ? '+' : '') + v.toFixed(nd) + '%';
  };
  PTC.pctU = function (v, nd) {           // unsigned percent
    if (!isNum(v)) return '—';
    return Number(v).toFixed(nd == null ? 1 : nd) + '%';
  };
  PTC.wpct = function (frac, nd) {        // weight fraction 0..1 => percent
    if (!isNum(frac)) return '—';
    return (Number(frac) * 100).toFixed(nd == null ? 1 : nd) + '%';
  };
  PTC.num = function (v, nd) { return isNum(v) ? Number(v).toFixed(nd == null ? 2 : nd) : '—'; };
  PTC.shortDate = function (s) {
    if (!s) return '';
    s = String(s).slice(0, 10);
    try {
      var p = s.split('-');
      if (p.length === 3) {
        var mo = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][Number(p[1]) - 1];
        if (mo) return mo + ' ' + Number(p[2]) + ", " + p[0];
      }
    } catch (e) {}
    return s;
  };
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  PTC.esc = esc;

  /* ---- shared floating tooltip ------------------------------------------- */
  function tipEl() {
    var el = document.getElementById('ptc-tooltip');
    if (!el) {
      el = document.createElement('div');
      el.id = 'ptc-tooltip';
      el.setAttribute('role', 'tooltip');
      el.style.cssText = 'position:fixed;z-index:99999;pointer-events:none;display:none;'
        + 'background:#0b1424;border:1px solid #2c4a78;border-radius:6px;padding:7px 9px;'
        + 'font:12px/1.5 -apple-system,Segoe UI,Roboto,sans-serif;color:#e0e0ff;'
        + 'box-shadow:0 6px 22px rgba(0,0,0,.5);max-width:280px;';
      document.body.appendChild(el);
    }
    return el;
  }
  function showTip(html, x, y) {
    var el = tipEl();
    el.innerHTML = html;
    el.style.display = 'block';
    var w = el.offsetWidth, h = el.offsetHeight;
    var vx = x + 14, vy = y + 14;
    if (vx + w > window.innerWidth - 8) vx = x - w - 14;
    if (vy + h > window.innerHeight - 8) vy = y - h - 14;
    el.style.left = Math.max(4, vx) + 'px';
    el.style.top = Math.max(4, vy) + 'px';
  }
  function hideTip() { var el = document.getElementById('ptc-tooltip'); if (el) el.style.display = 'none'; }
  PTC.hideTip = hideTip;
  PTC.swatch = function (color) {
    return '<span style="display:inline-block;width:9px;height:9px;border-radius:2px;background:'
      + color + ';margin-right:6px;vertical-align:middle;"></span>';
  };

  /* ---- render registry (re-render on resize / becoming visible) ---------- */
  PTC._RO = null;
  function ensureRO() {
    if (PTC._RO || typeof ResizeObserver === 'undefined') return;
    PTC._RO = new ResizeObserver(function (entries) {
      entries.forEach(function (en) {
        var fn = en.target.__ptcRender;
        if (fn && en.contentRect && en.contentRect.width > 0) {
          try { fn(); } catch (e) {}
        }
      });
    });
  }
  function register(container, renderFn) {
    container.__ptcRender = renderFn;
    ensureRO();
    if (PTC._RO) { try { PTC._RO.observe(container); } catch (e) {} }
  }
  function empty(container, msg) {
    container.innerHTML = '<div class="ptc-empty">' + esc(msg || 'No data available.') + '</div>';
  }
  PTC.empty = empty;

  function widthOf(container, fallback) {
    var w = container.clientWidth || (container.getBoundingClientRect().width | 0);
    return w > 40 ? w : (fallback || 560);
  }
  function svgEl(w, h) {
    return '<svg class="ptc-svg" width="' + w + '" height="' + h + '" viewBox="0 0 ' + w + ' ' + h
      + '" style="display:block;width:100%;height:' + h + 'px;">';
  }

  /* ======================================================================== */
  /* TIME SERIES (line/area) with crosshair + tooltip                          */
  /* cfg = { dates:[str], series:[{name,color,values:[num|null],dash,fill}],    */
  /*         height, yFmt(v), valFmt(v), baseline(0|null), expandKey }          */
  /* ======================================================================== */
  PTC.timeSeries = function (container, cfg) {
    if (typeof container === 'string') container = document.getElementById(container);
    if (!container) return;
    cfg = cfg || {};
    var dates = cfg.dates || [];
    var series = (cfg.series || []).filter(function (s) { return s && s.values; });
    var height = cfg.height || 200;
    var yFmt = cfg.yFmt || function (v) { return PTC.num(v, 2); };
    var valFmt = cfg.valFmt || yFmt;

    function render() {
      var n = dates.length;
      var anyVal = series.some(function (s) { return s.values.some(isNum); });
      if (!n || !anyVal) { empty(container, cfg.emptyMsg); return; }
      var W = widthOf(container, cfg.fallbackWidth), H = height;
      var padL = 48, padR = 12, padT = 12, padB = 22;
      var plotW = Math.max(10, W - padL - padR), plotH = Math.max(10, H - padT - padB);

      var all = [];
      series.forEach(function (s) { s.values.forEach(function (v) { if (isNum(v)) all.push(Number(v)); }); });
      var lo = Math.min.apply(null, all), hi = Math.max.apply(null, all);
      if (cfg.baseline != null) { lo = Math.min(lo, cfg.baseline); hi = Math.max(hi, cfg.baseline); }
      if (hi === lo) { hi = lo + 1; lo = lo - 1; }
      var pad = (hi - lo) * 0.08; lo -= pad; hi += pad;

      var xAt = function (i) { return padL + (n <= 1 ? plotW / 2 : i * plotW / (n - 1)); };
      var yAt = function (v) { return padT + plotH * (1 - (v - lo) / (hi - lo)); };

      var svg = svgEl(W, H);
      // y gridlines + labels
      var ticks = 4;
      for (var t = 0; t <= ticks; t++) {
        var yv = lo + (hi - lo) * t / ticks, yy = yAt(yv);
        svg += '<line x1="' + padL + '" y1="' + yy.toFixed(1) + '" x2="' + (W - padR) + '" y2="' + yy.toFixed(1)
          + '" stroke="' + PTC.COLORS.grid + '" stroke-width="1"/>';
        svg += '<text x="' + (padL - 6) + '" y="' + (yy + 3).toFixed(1) + '" text-anchor="end" '
          + 'font-size="9" fill="' + PTC.COLORS.muted + '">' + esc(yFmt(yv)) + '</text>';
      }
      // baseline (zero) emphasised
      if (cfg.baseline != null && cfg.baseline > lo && cfg.baseline < hi) {
        var zy = yAt(cfg.baseline);
        svg += '<line x1="' + padL + '" y1="' + zy.toFixed(1) + '" x2="' + (W - padR) + '" y2="' + zy.toFixed(1)
          + '" stroke="' + PTC.COLORS.zero + '" stroke-width="1" stroke-dasharray="4,3"/>';
      }
      // x labels (first / mid / last)
      var xi = n === 1 ? [0] : [0, Math.floor((n - 1) / 2), n - 1];
      xi.forEach(function (i, k) {
        var anchor = k === 0 ? 'start' : (k === xi.length - 1 ? 'end' : 'middle');
        svg += '<text x="' + xAt(i).toFixed(1) + '" y="' + (H - 6) + '" text-anchor="' + anchor
          + '" font-size="9" fill="' + PTC.COLORS.muted + '">' + esc(PTC.shortDate(dates[i])) + '</text>';
      });
      // series areas + lines
      series.forEach(function (s) {
        var pts = [], first = null, last = null;
        s.values.forEach(function (v, i) {
          if (isNum(v)) { var p = xAt(i).toFixed(1) + ',' + yAt(Number(v)).toFixed(1); pts.push(p); if (first == null) first = i; last = i; }
        });
        if (!pts.length) return;
        if (s.fill && cfg.baseline != null) {
          svg += '<polygon points="' + xAt(first).toFixed(1) + ',' + yAt(cfg.baseline).toFixed(1) + ' '
            + pts.join(' ') + ' ' + xAt(last).toFixed(1) + ',' + yAt(cfg.baseline).toFixed(1) + '" fill="'
            + s.color + '" fill-opacity="0.10"/>';
        }
        svg += '<polyline points="' + pts.join(' ') + '" fill="none" stroke="' + s.color + '" stroke-width="'
          + (s.width || 2) + '" stroke-linejoin="round" stroke-linecap="round"'
          + (s.dash ? ' stroke-dasharray="6,4"' : '') + '/>';
      });
      // crosshair overlay group (updated on hover)
      svg += '<g class="ptc-cross" style="display:none;">'
        + '<line x1="0" y1="' + padT + '" x2="0" y2="' + (padT + plotH) + '" stroke="' + PTC.COLORS.crosshair
        + '" stroke-width="1" stroke-dasharray="3,3"/>';
      series.forEach(function (s) { svg += '<circle r="3.5" fill="' + s.color + '" stroke="#0b1424" stroke-width="1.5" cx="-99" cy="-99"/>'; });
      svg += '</g>';
      // transparent hover capture rect
      svg += '<rect class="ptc-hit" x="' + padL + '" y="' + padT + '" width="' + plotW + '" height="' + plotH
        + '" fill="transparent" style="cursor:' + (cfg.expandKey ? 'pointer' : 'crosshair') + ';"/>';
      svg += '</svg>';
      container.innerHTML = svg;

      var svgNode = container.querySelector('svg');
      var hit = container.querySelector('.ptc-hit');
      var cross = container.querySelector('.ptc-cross');
      var crossLine = cross.querySelector('line');
      var dots = cross.querySelectorAll('circle');

      function idxFromEvent(ev) {
        var rect = svgNode.getBoundingClientRect();
        var scale = W / rect.width;                 // viewBox unit per screen px
        var xv = (ev.clientX - rect.left) * scale;
        var frac = (xv - padL) / plotW;
        var i = Math.round(frac * (n - 1));
        return Math.max(0, Math.min(n - 1, i));
      }
      function move(ev) {
        var i = idxFromEvent(ev);
        var cx = xAt(i);
        cross.style.display = '';
        crossLine.setAttribute('x1', cx.toFixed(1));
        crossLine.setAttribute('x2', cx.toFixed(1));
        var rows = '';
        series.forEach(function (s, si) {
          var v = s.values[i];
          if (isNum(v)) { dots[si].setAttribute('cx', cx.toFixed(1)); dots[si].setAttribute('cy', yAt(Number(v)).toFixed(1)); }
          else { dots[si].setAttribute('cx', '-99'); dots[si].setAttribute('cy', '-99'); }
          rows += '<div style="display:flex;justify-content:space-between;gap:14px;">'
            + '<span>' + PTC.swatch(s.color) + esc(s.name) + '</span>'
            + '<span style="font-variant-numeric:tabular-nums;font-weight:700;">' + esc(isNum(v) ? valFmt(Number(v)) : '—') + '</span></div>';
        });
        var html = '<div style="font-weight:800;margin-bottom:4px;">' + esc(PTC.shortDate(dates[i])) + '</div>' + rows;
        showTip(html, ev.clientX, ev.clientY);
      }
      hit.addEventListener('mousemove', move);
      hit.addEventListener('mouseenter', move);
      hit.addEventListener('mouseleave', function () { cross.style.display = 'none'; hideTip(); });
      if (cfg.expandKey) hit.addEventListener('click', function () { PTC.openDetail(cfg.expandKey); });
    }
    register(container, render);
    render();
  };

  /* ======================================================================== */
  /* VERTICAL BARS (daily P&L) with hover                                      */
  /* cfg = { dates:[str], values:[num|null], height, valFmt(v), expandKey }     */
  /* ======================================================================== */
  PTC.bars = function (container, cfg) {
    if (typeof container === 'string') container = document.getElementById(container);
    if (!container) return;
    cfg = cfg || {};
    var dates = cfg.dates || [], values = cfg.values || [];
    var height = cfg.height || 130;
    var valFmt = cfg.valFmt || function (v) { return PTC.money(v); };

    function render() {
      var fin = values.filter(isNum);
      if (!dates.length || !fin.length) { empty(container, cfg.emptyMsg); return; }
      var W = widthOf(container, cfg.fallbackWidth), H = height;
      var padL = 48, padR = 12, padT = 10, padB = 20;
      var plotW = Math.max(10, W - padL - padR), plotH = Math.max(10, H - padT - padB);
      var maxAbs = Math.max.apply(null, fin.map(function (v) { return Math.abs(Number(v)); })) || 1;
      var n = values.length, midY = padT + plotH / 2;
      var slot = plotW / n, bw = Math.max(1, slot * 0.66);
      var xAt = function (i) { return padL + i * slot + (slot - bw) / 2; };

      var svg = svgEl(W, H);
      // y labels (+max / 0 / -max)
      [maxAbs, 0, -maxAbs].forEach(function (yv) {
        var yy = midY - (yv / maxAbs) * (plotH / 2);
        svg += '<text x="' + (padL - 6) + '" y="' + (yy + 3).toFixed(1) + '" text-anchor="end" font-size="9" fill="'
          + PTC.COLORS.muted + '">' + esc(PTC.money(yv)) + '</text>';
      });
      var bars = '';
      values.forEach(function (v, i) {
        if (!isNum(v)) return;
        var h = Math.abs(Number(v)) / maxAbs * (plotH / 2);
        var y = Number(v) >= 0 ? (midY - h) : midY;
        bars += '<rect class="ptc-bar" data-i="' + i + '" x="' + xAt(i).toFixed(1) + '" y="' + y.toFixed(1)
          + '" width="' + bw.toFixed(1) + '" height="' + Math.max(0.8, h).toFixed(1) + '" rx="1" fill="'
          + (Number(v) >= 0 ? PTC.COLORS.pos : PTC.COLORS.neg) + '" style="cursor:'
          + (cfg.expandKey ? 'pointer' : 'default') + ';"/>';
      });
      svg += bars;
      svg += '<line x1="' + padL + '" y1="' + midY.toFixed(1) + '" x2="' + (W - padR) + '" y2="' + midY.toFixed(1)
        + '" stroke="' + PTC.COLORS.zero + '" stroke-width="1"/>';
      // x labels
      [0, n - 1].forEach(function (i, k) {
        svg += '<text x="' + (xAt(i) + bw / 2).toFixed(1) + '" y="' + (H - 5) + '" text-anchor="' + (k === 0 ? 'start' : 'end')
          + '" font-size="9" fill="' + PTC.COLORS.muted + '">' + esc(PTC.shortDate(dates[i])) + '</text>';
      });
      svg += '</svg>';
      container.innerHTML = svg;

      Array.prototype.forEach.call(container.querySelectorAll('.ptc-bar'), function (r) {
        r.addEventListener('mousemove', function (ev) {
          var i = Number(r.getAttribute('data-i')), v = values[i];
          r.setAttribute('fill-opacity', '0.75');
          showTip('<div style="font-weight:800;margin-bottom:3px;">' + esc(PTC.shortDate(dates[i])) + '</div>'
            + '<div style="display:flex;justify-content:space-between;gap:14px;"><span>Daily P&amp;L</span>'
            + '<span style="font-weight:700;color:' + (Number(v) >= 0 ? PTC.COLORS.pos : PTC.COLORS.neg) + ';">'
            + esc(valFmt(v)) + '</span></div>', ev.clientX, ev.clientY);
        });
        r.addEventListener('mouseleave', function () { r.removeAttribute('fill-opacity'); hideTip(); });
        if (cfg.expandKey) r.addEventListener('click', function () { PTC.openDetail(cfg.expandKey); });
      });
    }
    register(container, render);
    render();
  };

  /* ======================================================================== */
  /* HORIZONTAL BARS (allocation / top holdings) with hover                    */
  /* cfg = { items:[{name,value,valTxt,sub,color}], fmt(v), color, expandKey }  */
  /* ======================================================================== */
  PTC.hbars = function (container, cfg) {
    if (typeof container === 'string') container = document.getElementById(container);
    if (!container) return;
    cfg = cfg || {};
    var items = (cfg.items || []).filter(function (it) { return it && isNum(it.value); });

    function render() {
      if (!items.length) { empty(container, cfg.emptyMsg); return; }
      var max = cfg.max != null ? cfg.max : Math.max.apply(null, items.map(function (it) { return Math.abs(Number(it.value)); }));
      if (!(max > 0)) max = 1;
      var html = '<div class="ptc-hbars">';
      items.forEach(function (it, i) {
        var v = Number(it.value), pct = Math.max(2, Math.abs(v) / max * 100);
        var color = it.color || cfg.color || PTC.COLORS.info;
        var valTxt = it.valTxt != null ? it.valTxt : (cfg.fmt ? cfg.fmt(v) : String(v));
        html += '<div class="ptc-hbar" data-i="' + i + '">'
          + '<span class="ptc-hbar-name">' + esc(it.name) + '</span>'
          + '<span class="ptc-hbar-track"><span class="ptc-hbar-fill" style="width:' + pct.toFixed(1) + '%;background:' + color + ';"></span></span>'
          + '<span class="ptc-hbar-val">' + esc(valTxt) + '</span></div>';
      });
      html += '</div>';
      container.innerHTML = html;
      Array.prototype.forEach.call(container.querySelectorAll('.ptc-hbar'), function (row) {
        row.addEventListener('mousemove', function (ev) {
          var it = items[Number(row.getAttribute('data-i'))];
          var extra = it.sub ? ('<div style="color:' + PTC.COLORS.muted + ';font-size:11px;">' + esc(it.sub) + '</div>') : '';
          var valTxt = it.valTxt != null ? it.valTxt : (cfg.fmt ? cfg.fmt(Number(it.value)) : String(it.value));
          showTip('<div style="font-weight:800;">' + esc(it.name) + '</div>'
            + '<div style="display:flex;justify-content:space-between;gap:14px;"><span>' + esc(cfg.label || 'Value')
            + '</span><span style="font-weight:700;">' + esc(valTxt) + '</span></div>' + extra, ev.clientX, ev.clientY);
        });
        row.addEventListener('mouseleave', hideTip);
        if (cfg.expandKey) { row.style.cursor = 'pointer'; row.addEventListener('click', function () { PTC.openDetail(cfg.expandKey); }); }
      });
    }
    register(container, render);
    render();
  };

  /* ======================================================================== */
  /* DIVERGING HORIZONTAL BARS (contributors / drift) with hover               */
  /* cfg = { items:[{name,value,valTxt,sub}], maxAbs, posColor,negColor, fmt }  */
  /* ======================================================================== */
  PTC.diverging = function (container, cfg) {
    if (typeof container === 'string') container = document.getElementById(container);
    if (!container) return;
    cfg = cfg || {};
    var items = (cfg.items || []).filter(function (it) { return it && isNum(it.value); });

    function render() {
      if (!items.length) { empty(container, cfg.emptyMsg); return; }
      var maxAbs = cfg.maxAbs != null ? cfg.maxAbs : Math.max.apply(null, items.map(function (it) { return Math.abs(Number(it.value)); }));
      if (!(maxAbs > 0)) maxAbs = 1;
      var pos = cfg.posColor || PTC.COLORS.pos, neg = cfg.negColor || PTC.COLORS.neg;
      var html = '<div class="ptc-hbars">';
      items.forEach(function (it, i) {
        var v = Number(it.value), half = Math.abs(v) / maxAbs * 50;
        var style = v >= 0 ? ('left:50%;width:' + half.toFixed(1) + '%;') : ('left:' + (50 - half).toFixed(1) + '%;width:' + half.toFixed(1) + '%;');
        var valTxt = it.valTxt != null ? it.valTxt : (cfg.fmt ? cfg.fmt(v) : String(v));
        html += '<div class="ptc-dbar" data-i="' + i + '">'
          + '<span class="ptc-hbar-name">' + esc(it.name) + '</span>'
          + '<span class="ptc-dbar-track"><span class="ptc-dbar-mid"></span>'
          + '<span class="ptc-dbar-fill" style="' + style + 'background:' + (v >= 0 ? pos : neg) + ';"></span></span>'
          + '<span class="ptc-hbar-val" style="color:' + (v >= 0 ? pos : neg) + ';">' + esc(valTxt) + '</span></div>';
      });
      html += '</div>';
      container.innerHTML = html;
      Array.prototype.forEach.call(container.querySelectorAll('.ptc-dbar'), function (row) {
        row.addEventListener('mousemove', function (ev) {
          var it = items[Number(row.getAttribute('data-i'))];
          var lines = (it.lines || []).map(function (ln) {
            return '<div style="display:flex;justify-content:space-between;gap:14px;"><span style="color:' + PTC.COLORS.muted + ';">'
              + esc(ln[0]) + '</span><span style="font-weight:700;">' + esc(ln[1]) + '</span></div>';
          }).join('');
          var valTxt = it.valTxt != null ? it.valTxt : (cfg.fmt ? cfg.fmt(Number(it.value)) : String(it.value));
          var body = lines || ('<div style="display:flex;justify-content:space-between;gap:14px;"><span>' + esc(cfg.label || 'Value')
            + '</span><span style="font-weight:700;">' + esc(valTxt) + '</span></div>');
          showTip('<div style="font-weight:800;margin-bottom:3px;">' + esc(it.name)
            + (it.sub ? ' <span style="color:' + PTC.COLORS.muted + ';font-weight:400;">' + esc(it.sub) + '</span>' : '')
            + '</div>' + body, ev.clientX, ev.clientY);
        });
        row.addEventListener('mouseleave', hideTip);
        if (cfg.expandKey) { row.style.cursor = 'pointer'; row.addEventListener('click', function () { PTC.openDetail(cfg.expandKey); }); }
      });
    }
    register(container, render);
    render();
  };

  /* ======================================================================== */
  /* DONUT (cash vs invested / sector) with hover                              */
  /* cfg = { segments:[{label,value,color,sub}], centerTop, centerBottom }      */
  /* ======================================================================== */
  PTC.donut = function (container, cfg) {
    if (typeof container === 'string') container = document.getElementById(container);
    if (!container) return;
    cfg = cfg || {};
    var segs = (cfg.segments || []).filter(function (s) { return s && isNum(s.value) && Number(s.value) > 0; });

    function render() {
      if (!segs.length) { empty(container, cfg.emptyMsg); return; }
      var total = segs.reduce(function (a, s) { return a + Number(s.value); }, 0) || 1;
      var W = widthOf(container, 320), H = cfg.height || 150;
      var cx = Math.min(W * 0.28, 90), cy = H / 2, R = Math.min(cy - 8, 56), r = R * 0.6;
      var svg = svgEl(W, H), a0 = -Math.PI / 2;
      segs.forEach(function (s, i) {
        var frac = Number(s.value) / total, a1 = a0 + frac * Math.PI * 2;
        var large = (a1 - a0) > Math.PI ? 1 : 0;
        var x0 = cx + R * Math.cos(a0), y0 = cy + R * Math.sin(a0);
        var x1 = cx + R * Math.cos(a1), y1 = cy + R * Math.sin(a1);
        var xi0 = cx + r * Math.cos(a1), yi0 = cy + r * Math.sin(a1);
        var xi1 = cx + r * Math.cos(a0), yi1 = cy + r * Math.sin(a0);
        var color = s.color || PTC.PALETTE[i % PTC.PALETTE.length];
        svg += '<path class="ptc-arc" data-i="' + i + '" d="M' + x0.toFixed(1) + ',' + y0.toFixed(1)
          + ' A' + R + ',' + R + ' 0 ' + large + ',1 ' + x1.toFixed(1) + ',' + y1.toFixed(1)
          + ' L' + xi0.toFixed(1) + ',' + yi0.toFixed(1) + ' A' + r + ',' + r + ' 0 ' + large + ',0 '
          + xi1.toFixed(1) + ',' + yi1.toFixed(1) + ' Z" fill="' + color + '" stroke="#0b1424" stroke-width="1"/>';
        a0 = a1;
      });
      if (cfg.centerTop != null) {
        svg += '<text x="' + cx + '" y="' + (cy - 2) + '" text-anchor="middle" font-size="13" font-weight="800" fill="#e0e0ff">' + esc(cfg.centerTop) + '</text>';
        if (cfg.centerBottom != null)
          svg += '<text x="' + cx + '" y="' + (cy + 13) + '" text-anchor="middle" font-size="9" fill="' + PTC.COLORS.muted + '">' + esc(cfg.centerBottom) + '</text>';
      }
      // legend to the right
      var lx = cx + R + 22, ly = cy - segs.length * 9 + 6;
      segs.forEach(function (s, i) {
        var color = s.color || PTC.PALETTE[i % PTC.PALETTE.length];
        var pctTxt = (Number(s.value) / total * 100).toFixed(1) + '%';
        svg += '<rect x="' + lx + '" y="' + (ly + i * 18 - 8) + '" width="9" height="9" rx="2" fill="' + color + '"/>';
        svg += '<text x="' + (lx + 14) + '" y="' + (ly + i * 18) + '" font-size="10" fill="#a8b8d8">'
          + esc(s.label) + ' <tspan fill="' + PTC.COLORS.muted + '">' + pctTxt + '</tspan></text>';
      });
      svg += '</svg>';
      container.innerHTML = svg;
      Array.prototype.forEach.call(container.querySelectorAll('.ptc-arc'), function (p) {
        p.addEventListener('mousemove', function (ev) {
          var s = segs[Number(p.getAttribute('data-i'))];
          p.setAttribute('fill-opacity', '0.8');
          var pctTxt = (Number(s.value) / total * 100).toFixed(1) + '%';
          showTip('<div style="font-weight:800;">' + esc(s.label) + '</div>'
            + '<div style="display:flex;justify-content:space-between;gap:14px;"><span>Weight</span><span style="font-weight:700;">' + pctTxt + '</span></div>'
            + (s.sub ? '<div style="color:' + PTC.COLORS.muted + ';">' + esc(s.sub) + '</div>' : ''), ev.clientX, ev.clientY);
        });
        p.addEventListener('mouseleave', function () { p.removeAttribute('fill-opacity'); hideTip(); });
      });
    }
    register(container, render);
    render();
  };

  /* ---- drill-down: open the dedicated detail view in a new tab/window ----- */
  PTC.detailBase = 'analytics.html';   // resolved relative to /ui/
  PTC.openDetail = function (chartKey, params) {
    var q = 'chart=' + encodeURIComponent(chartKey || 'performance');
    if (params) { Object.keys(params).forEach(function (k) { q += '&' + k + '=' + encodeURIComponent(params[k]); }); }
    var base = (global.PT_ANALYTICS_URL || PTC.detailBase);
    try {
      window.open(base + '?' + q, 'ptAnalytics',
        'noopener,width=1280,height=880,menubar=no,toolbar=no,location=yes,resizable=yes,scrollbars=yes');
    } catch (e) { window.open(base + '?' + q, '_blank'); }
  };

  global.PTC = PTC;
})(window);
