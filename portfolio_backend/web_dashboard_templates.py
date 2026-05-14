from __future__ import annotations


BASE_CSS = """
:root{color-scheme:dark;--bg:#090d0b;--panel:#121a16;--panel2:#18231e;--line:#2b3a32;--muted:#aab7ad;--text:#eef7ef;--accent:#48d0bd;--accent2:#7ddf8a;--warn:#f5b84c;--bad:#ff6b6b;--good:#72dd7d;--shadow:0 12px 40px rgba(0,0,0,.24)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
a{color:var(--accent)}button,input{font:inherit}.login{max-width:520px;margin:14vh auto;padding:32px;background:var(--panel);border:1px solid var(--line);border-radius:8px;box-shadow:var(--shadow)}
.login h1{margin:0 0 8px;font-size:32px}.login p{color:var(--muted)}.login input{width:100%;padding:13px 14px;background:#0c120f;color:var(--text);border:1px solid var(--line);border-radius:8px;margin:12px 0}
.login button,.primary{background:var(--accent);color:#06201b;border:0;border-radius:8px;padding:12px 16px;font-weight:750;cursor:pointer}.secondary{background:var(--panel2);border:1px solid var(--line);color:var(--text);border-radius:8px;padding:10px 13px;cursor:pointer}
.signin-block{margin:18px 0}.google-login-button{display:inline-flex;align-items:center;gap:12px;background:#fff;color:#202124;border:1px solid #dadce0;border-radius:4px;padding:10px 16px;font-weight:700;text-decoration:none;box-shadow:0 1px 2px rgba(0,0,0,.18)}.google-login-icon{display:inline-grid;place-items:center;width:20px;height:20px;font-weight:900;color:#4285f4}.fallback-login{margin-top:18px;border-top:1px solid var(--line);padding-top:14px}.fallback-login summary{cursor:pointer;color:var(--muted);font-weight:750}.auth-user{color:var(--muted);font-size:13px;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.error{color:var(--bad);font-weight:700}.error-panel{border-color:#5d2a2d;background:#211113}
"""

GOOGLE_REDIRECT_CALLBACK_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Options ROI</title><style>__BASE_CSS__</style></head>
<body><main class="login"><h1>Signing in</h1><p>Completing Google sign-in...</p><p class="error" id="error"></p></main>
<script>
(async () => {
  const params = new URLSearchParams(window.location.hash.slice(1) || window.location.search.slice(1));
  const credential = params.get("id_token");
  const state = params.get("state");
  if (!credential) {
    document.getElementById("error").textContent = "Google sign-in did not return a credential.";
    return;
  }
  const body = new URLSearchParams();
  body.set("credential", credential);
  if (state) body.set("state", state);
  const response = await fetch("/auth/google", {
    method: "POST",
    credentials: "same-origin",
    headers: {"Content-Type": "application/x-www-form-urlencoded"},
    body
  });
  if (response.redirected) {
    window.location.replace(response.url);
    return;
  }
  const text = await response.text();
  document.open();
  document.write(text);
  document.close();
})();
</script></body></html>""".replace("__BASE_CSS__", BASE_CSS)

LOGIN_TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Options ROI</title><style>__BASE_CSS__</style></head>
<body><main class="login"><h1>Options ROI Dashboard</h1><p>Sign in with your allowed Google account. This browser will stay signed in.</p>
<p class="error">__ERROR__</p>__GOOGLE_SIGNIN__
__FALLBACK_LOGIN__</main></body></html>"""

DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Options ROI Dashboard</title>
<style>
__BASE_CSS__
:root{--bg2:#0c1114;--panel3:#10161a;--panel4:#151f23;--line2:#26383b;--blue:#7aa7ff;--amber:#f6c25b;--red:#ff6f78;--green:#7ee092;--teal:#45d2c5}
body{background:#080c0f;color:var(--text)}
.topbar{position:sticky;top:0;z-index:40;background:rgba(8,12,15,.94);backdrop-filter:blur(14px);border-bottom:1px solid var(--line2)}
.topbar-inner{max-width:1420px;margin:0 auto;padding:9px 18px;display:flex;gap:16px;align-items:center;justify-content:space-between}
.brand{font-size:18px;font-weight:850;letter-spacing:.01em;white-space:nowrap}.brand small{display:block;color:var(--muted);font-size:11px;font-weight:650;margin-top:-2px}
.nav{display:flex;gap:2px;flex:1;justify-content:flex-start;min-width:0;overflow:auto;scrollbar-width:none;border-left:1px solid var(--line2);padding-left:12px}.nav::-webkit-scrollbar{display:none}.nav button{background:transparent;color:var(--muted);border:0;border-bottom:2px solid transparent;border-radius:0;padding:9px 9px 7px;cursor:pointer;font-weight:750;white-space:nowrap}.nav button.active{color:var(--text);border-bottom-color:var(--accent)}
.segmented button,.segmented a{background:transparent;color:var(--muted);border:1px solid transparent;border-radius:8px;padding:8px 10px;cursor:pointer;font-weight:750;white-space:nowrap}.segmented button.active,.segmented a.active{color:#061a18;background:var(--accent);border-color:transparent}
.actions{display:flex;gap:7px;align-items:center;flex:0 0 auto;border-left:1px solid var(--line2);padding-left:12px}.actions form{margin:0}.actions .primary,.actions .secondary{padding:9px 11px}.actions button[disabled]{opacity:.72;cursor:wait}.auth-user{display:none;color:var(--muted);font-size:12px;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.shell{max-width:1420px;margin:0 auto;padding:22px 18px 64px}.hero{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:18px;align-items:end;margin:4px 0 16px}
.hero h1{font-size:34px;line-height:1.05;margin:0 0 8px}.sub{color:var(--muted);font-size:14px}.status-strip{display:flex;gap:8px;flex-wrap:wrap;align-items:center;justify-content:flex-end}
.badge{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line2);background:#111a1d;border-radius:999px;padding:5px 9px;color:var(--muted);font-size:12px;font-weight:750;white-space:nowrap}.badge.good{color:#c6f6ce;border-color:#2f6941}.badge.warn{color:#ffe3a3;border-color:#73551d}.badge.bad{color:#ffc0c5;border-color:#70303b}.badge.blue{color:#cfe0ff;border-color:#355189}
.basis-control{display:flex;justify-content:flex-end;margin-top:8px}.basis-control .segmented{border:1px solid var(--line2);border-radius:8px;padding:4px;background:#0d1417}.basis-control a,.range-panel a{display:inline-flex;align-items:center;text-decoration:none}
.control-label{color:var(--muted);font-size:12px;text-transform:uppercase;font-weight:800;letter-spacing:.06em}.segmented{display:flex;gap:4px;flex-wrap:wrap}.range-panel{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:0 0 12px;padding:10px 12px;background:#0d1417;border:1px solid var(--line2);border-radius:8px}.range-panel .segmented button{padding:7px 10px}
.grid{display:grid;gap:12px}.grid>*{min-width:0}.metrics{grid-template-columns:repeat(4,minmax(180px,1fr))}.two{grid-template-columns:minmax(0,1.08fr) minmax(360px,.92fr)}.two-even{grid-template-columns:repeat(2,minmax(0,1fr))}.three{grid-template-columns:repeat(3,minmax(0,1fr))}
.card,.panel{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;box-shadow:0 10px 28px rgba(0,0,0,.18)}.card{padding:14px}.panel{padding:16px}
.metric-label{color:var(--muted);font-size:12px;font-weight:800;text-transform:uppercase;letter-spacing:.04em}.metric-value{font-size:25px;font-weight:900;margin-top:5px;line-height:1.08}.metric-note{color:var(--muted);font-size:12px;margin-top:5px}.pos{color:var(--green)}.neg{color:var(--red)}.muted{color:var(--muted)}.warn-text{color:var(--amber)}.mono{font-variant-numeric:tabular-nums}
h2{font-size:21px;margin:22px 0 10px}h3{font-size:16px;margin:0 0 10px}.section{display:none}.section.active{display:block}.section-head{display:flex;align-items:end;justify-content:space-between;gap:14px;margin:20px 0 10px}.section-head h2{margin:0}.section-note{color:var(--muted);font-size:13px;margin-top:4px}
.toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:0 0 10px}.toolbar input,.toolbar select,.section-head select{background:#0b1113;color:var(--text);border:1px solid var(--line2);border-radius:8px;padding:9px 11px;min-height:38px}.toolbar input{min-width:260px}.toolbar .segmented button{padding:7px 10px}
.table-card{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;overflow:hidden}.table-title{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;padding:12px 14px;border-bottom:1px solid var(--line2);background:#11191c}.table-title strong{font-size:15px}.table-title span{display:block;color:var(--muted);font-size:12px;margin-top:2px}
.table-scroll{overflow:auto;max-height:620px}table{width:100%;border-collapse:separate;border-spacing:0;font-size:13px;min-width:820px}th,td{text-align:left;padding:8px 10px;border-bottom:1px solid #213034;vertical-align:middle;white-space:nowrap}tbody tr:nth-child(even) td{background:rgba(255,255,255,.012)}th{position:sticky;top:0;z-index:2;background:#0f171a;color:#c4d0cc;font-size:11.5px;text-transform:uppercase;letter-spacing:.04em}th button{all:unset;cursor:pointer}td.num,th.num{text-align:right}.small-table table{min-width:620px}.wide-table table{min-width:1120px}.compact-table table{min-width:100%;table-layout:auto}.col-compact{width:1%;max-width:96px}.empty{padding:18px;color:var(--muted)}
.risk-row-itm td{background:rgba(255,111,120,.12)}.risk-row-near td{background:rgba(246,194,91,.11)}.risk-row-clear td{background:rgba(69,210,197,.07)}.risk-dot{display:inline-block;width:9px;height:9px;border-radius:999px;margin-right:6px;vertical-align:middle}.dot-bad{background:var(--red)}.dot-warn{background:var(--amber)}.dot-good{background:var(--green)}.dot-blue{background:var(--blue)}
.risk-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px}.risk-card{background:#10181b;border:1px solid var(--line2);border-radius:8px;padding:12px}.risk-head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}.risk-title{font-weight:900}.risk-meta{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:4px;margin-top:8px;color:var(--muted);font-size:12px}.pill{display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;max-width:150px;min-height:22px;line-height:1.05;font-size:11px;border-radius:999px;padding:4px 8px;background:#1c2a2d;color:var(--muted);font-weight:850;white-space:nowrap}.pill.bad{background:#3a171d;color:#ffc4c9}.pill.warn{background:#352714;color:#ffe1a0}.pill.good{background:#143420;color:#bff3c7}.pill.blue{background:#17243c;color:#cadcff}
.chart-card{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;padding:14px;min-width:0;overflow:hidden}.chart-title{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:8px}.chart-title strong{font-size:16px}.chart-title .muted{font-size:13px}.chart-wrap{height:310px;position:relative;min-width:0}.chart-canvas{width:100%!important;height:100%!important}.chart{width:100%;height:285px;display:block}.chart text{fill:#c0cbc7;font-size:15px;font-weight:650}.axis{stroke:#42585d;stroke-width:1.2}.grid-line{stroke:#26383c;stroke-width:1}.line{fill:none;stroke-width:3}.bar-pos{fill:#66d37a}.bar-neg{fill:#ff7078}.bar-label{fill:#d7e3de;font-size:12px;font-weight:750}.legend{display:flex;gap:10px;flex-wrap:wrap;margin-top:8px}.legend-item{font-size:13px;color:var(--muted);display:inline-flex;gap:6px;align-items:center}.legend-swatch{width:11px;height:11px;border-radius:2px}.chart-legend{display:flex;gap:14px;flex-wrap:wrap;margin-top:10px;color:#c0cbc7!important;font-size:13px;font-weight:700}.chart-legend span{display:inline-flex;align-items:center;gap:6px;color:#c0cbc7!important}.chart-legend i{display:inline-block;width:12px;height:12px;border-radius:2px}
.note-list{display:grid;gap:8px}.note{border-left:3px solid var(--accent);background:var(--panel3);border-radius:8px;padding:10px 12px}.note strong{display:block}details{border:1px solid var(--line2);border-radius:8px;padding:12px;background:var(--panel3)}summary{cursor:pointer;font-weight:850}.footnote{font-size:12px;color:var(--muted);margin-top:8px}
.loading-panel{min-height:220px;display:grid;place-items:center;text-align:center}.loading-panel h2{margin:0 0 8px}.loading-panel p{margin:0;color:var(--muted)}.retry-load{margin-top:14px}
.view-updating{position:fixed;right:18px;bottom:18px;z-index:90;background:#11201d;border:1px solid #2d4b46;color:#c8fff6;border-radius:999px;padding:8px 12px;font-weight:850;box-shadow:0 10px 28px rgba(0,0,0,.28);opacity:0;transform:translateY(8px);transition:opacity .12s ease,transform .12s ease;pointer-events:none}body.is-rendering .view-updating,body.is-loading .view-updating{opacity:1;transform:translateY(0)}
@media(max-width:1160px){.topbar-inner{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center}.nav{grid-column:1/-1;grid-row:2;width:100%;justify-content:flex-start}.hero{grid-template-columns:1fr}.hero h1{font-size:28px}.brand small{display:none}.nav button{padding:8px}.actions .secondary,.actions .primary{padding:8px 10px}}
@media(max-width:980px){.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.two,.two-even,.three{grid-template-columns:1fr}.chart,.chart-wrap{height:280px}}
@media(max-width:760px){.topbar-inner{display:flex;flex-direction:column;align-items:stretch;gap:10px;padding:10px 12px}.brand{width:100%;font-size:18px;line-height:1.1;overflow:visible}.brand small{display:none}.actions{order:2;width:100%;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}.actions form{margin:0;min-width:0}.actions .primary,.actions .secondary{width:100%;padding:8px 7px;font-size:14px}.nav{order:3;grid-column:auto;grid-row:auto;width:100%;justify-content:flex-start;gap:6px}.nav button{padding:8px 10px}.shell{padding:18px 12px 64px}.hero{margin-top:6px}.status-strip{justify-content:flex-start}.basis-control{justify-content:flex-start}.metrics{grid-template-columns:1fr}.toolbar input{min-width:100%;width:100%}.metric-value{font-size:22px}.chart,.chart-wrap{height:260px}}
</style>
</head>
<body>
<div class="view-updating" id="viewUpdating" aria-live="polite">Updating view...</div>
<div class="topbar">
  <div class="topbar-inner">
    <div class="brand">Options ROI<small>IBKR dashboard</small></div>
    <nav class="nav" id="nav"></nav>
    <div class="actions">
      <span class="auth-user">__AUTH_USER__</span>
      <form id="refreshForm" method="post" action="/refresh"><button class="primary" type="submit">Refresh data</button></form>
      <button class="secondary" type="button" id="reloadApp">Reload app</button>
      <form method="post" action="/logout"><button class="secondary" type="submit">Logout</button></form>
    </div>
  </div>
</div>
<main class="shell">
  <section class="hero">
    <div>
      <h1 id="heroTitle">Dashboard</h1>
      <div class="sub" id="subtitle"></div>
    </div>
    <div>
      <div class="status-strip" id="statusStrip"></div>
      <div class="basis-control" id="basisControl"></div>
    </div>
  </section>
  <section id="dashboard" class="section active"></section>
  <section id="monthly" class="section"></section>
  <section id="tickers" class="section"></section>
  <section id="performance" class="section"></section>
  <section id="settings" class="section"></section>
  <section id="diagnostics" class="section"></section>
  <section id="methodology" class="section"></section>
</main>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.9/dist/chart.umd.min.js"></script>
<script id="dashboard-data" type="application/json">__DASHBOARD_DATA__</script>
<script>
let data = JSON.parse(document.getElementById("dashboard-data").textContent);
const queryParams = new URLSearchParams(window.location.search);
function queryNumber(name, fallback){
  const raw = queryParams.get(name);
  const value = raw === null ? null : Number(raw);
  return Number.isFinite(value) ? value : fallback;
}
let dashboardLoaded = data.loading !== true;
const appState = {
  active: "dashboard",
  range: "YTD",
  includeUnrealized: data.web?.include_unrealized !== false,
  targetReturn: Number(data.web?.target_return ?? data.monthly?.target_return ?? 0.015),
  targetFloor: queryNumber("target_floor", 0.01),
  openRisk: "all",
  openType: "all",
  openSearch: "",
  tickerSearch: "",
  tickerYear: "all",
  expectancyYear: "all",
  sort: {},
  renderTimer: null
};
const sections = [
  ["dashboard","Dashboard"], ["performance","Performance"], ["monthly","Monthly"], ["tickers","Tickers"],
  ["settings","Settings"], ["diagnostics","Diagnostics"],
  ["methodology","Methodology"]
];
const pageTitles = {
  dashboard: "Dashboard",
  performance: "Performance",
  monthly: "Monthly",
  tickers: "Tickers",
  settings: "Settings",
  diagnostics: "Diagnostics",
  methodology: "Methodology"
};
const rangeOptions = ["3M","6M","YTD","1Y","Since inception"];
const colors = ["#45d2c5","#7aa7ff","#f6c25b","#b99cff","#ff8e96","#7ee092"];
let chartSeq = 0;
let pendingCharts = [];
let chartInstances = [];
const $ = (id) => document.getElementById(id);
const safe = (v) => String(v ?? "n/a").replace(/[&<>"']/g, (ch) => ({"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#39;"}[ch]));
const numeric = (v) => v === null || v === undefined || v === "" || Number.isNaN(Number(v)) ? null : Number(v);
const fmtMoney = (v, digits=0) => {
  const places = Number.isInteger(digits) ? digits : 0;
  return numeric(v) === null ? "n/a" : new Intl.NumberFormat("en-US",{style:"currency",currency:"USD",minimumFractionDigits:places,maximumFractionDigits:places}).format(Number(v));
};
const fmtCompactMoney = (v) => {
  if (numeric(v) === null) return "n/a";
  const n = Number(v);
  const sign = n < 0 ? "-" : "";
  const abs = Math.abs(n);
  if (abs >= 1000000) return `${sign}$${(abs/1000000).toFixed(1)}m`;
  if (abs >= 1000) return `${sign}$${(abs/1000).toFixed(abs >= 10000 ? 0 : 1)}k`;
  return `${sign}$${abs.toFixed(0)}`;
};
const fmtPct = (v) => numeric(v) === null ? "n/a" : new Intl.NumberFormat("en-US",{style:"percent",minimumFractionDigits:1,maximumFractionDigits:1}).format(Number(v));
const fmtPctNumber = (v) => numeric(v) === null ? "" : (Number(v) * 100).toFixed(2).replace(/\\.?0+$/,"");
const fmtNum = (v) => numeric(v) === null ? "n/a" : new Intl.NumberFormat("en-US",{maximumFractionDigits:0}).format(Number(v));
const fmtDec = (v,d=2) => numeric(v) === null ? "n/a" : Number(v).toFixed(d);
const fmtDate = (v) => v ? String(v).slice(0,10) : "n/a";
const fmtLocalTime = (v) => {
  if (!v) return "n/a";
  const d = new Date(String(v));
  return Number.isNaN(d.getTime()) ? "n/a" : d.toLocaleTimeString([], {hour:"2-digit", minute:"2-digit", second:"2-digit"});
};
const fmtLocalDateTime = (v) => {
  if (!v) return "n/a";
  const d = new Date(String(v));
  return Number.isNaN(d.getTime()) ? "n/a" : d.toLocaleString([], {year:"numeric", month:"short", day:"2-digit", hour:"2-digit", minute:"2-digit"});
};
const monthName = (v) => {
  if (!v) return "n/a";
  const d = new Date(String(v).slice(0,10) + "T00:00:00Z");
  return Number.isNaN(d.getTime()) ? fmtDate(v) : d.toLocaleDateString("en-US",{month:"short",year:"numeric",timeZone:"UTC"});
};
const cls = (v) => numeric(v) === null ? "" : Number(v) < 0 ? "neg" : Number(v) > 0 ? "pos" : "";
const moneynessCls = (v) => numeric(v) === null ? "" : Number(v) > 0 ? "neg" : Number(v) < 0 ? "pos" : "";
const get = (obj, key) => key.split(".").reduce((acc, part) => acc == null ? undefined : acc[part], obj);
function labelize(v){
  return safe(String(v ?? "n/a").replaceAll("_"," ").replace(/\\b\\w/g, ch => ch.toUpperCase()));
}
function badge(text, type=""){ return `<span class="badge ${type}">${safe(text)}</span>`; }
function card(label, value, note="", klass=""){
  return `<div class="card"><div class="metric-label">${safe(label)}</div><div class="metric-value mono ${klass}">${value}</div>${note ? `<div class="metric-note">${note}</div>` : ""}</div>`;
}
function statusPill(value, row={}){
  const text = labelize(value || "n/a");
  const projected = displayMonthReturn(row);
  const target = targetReturn();
  let tone = "warn";
  if (String(value || "").toLowerCase().includes("beat") || (projected !== null && target !== null && projected >= target)) tone = "good";
  if (String(value || "").toLowerCase().includes("below") || (projected !== null && target !== null && projected < target)) tone = "bad";
  return `<span class="pill ${tone}">${text}</span>`;
}
function sectionHead(title, note="", right=""){
  return `<div class="section-head"><div><h2>${safe(title)}</h2>${note ? `<div class="section-note">${note}</div>` : ""}</div>${right}</div>`;
}
function segmentedControl(control, options, selected){
  const labels = {all:"All", itm:"ITM", near:"Near", clear:"Clear", puts:"Puts", calls:"Calls"};
  return `<div class="segmented" data-open-control="${control}">${options.map(o => `<button type="button" data-value="${safe(o)}" class="${o === selected ? "active" : ""}">${safe(labels[o] || o)}</button>`).join("")}</div>`;
}
function rangePicker(){
  return `<div class="range-panel"><div class="control-label">Period</div><div class="segmented rangeControl">${rangeOptions.map(o => `<button type="button" data-value="${safe(o)}" class="${o === appState.range ? "active" : ""}">${safe(o)}</button>`).join("")}</div></div>`;
}
function updateUrlState(){
  const url = new URL(window.location.href);
  url.searchParams.set("section", appState.active);
  url.searchParams.set("include_unrealized", appState.includeUnrealized ? "1" : "0");
  url.searchParams.set("target_return", Number(appState.targetReturn || 0).toFixed(6));
  url.searchParams.set("target_floor", Number(appState.targetFloor || 0).toFixed(6));
  url.searchParams.delete("target_good");
  url.searchParams.delete("refreshed");
  window.history.replaceState(null, "", `${url.pathname}${url.search}`);
}
function apiDashboardUrl(){
  const url = new URL("/api/dashboard", window.location.origin);
  url.searchParams.set("include_unrealized", appState.includeUnrealized ? "1" : "0");
  url.searchParams.set("target_return", Number(appState.targetReturn || 0).toFixed(6));
  return url;
}
function loadingPanel(message="Loading portfolio data...", detail="The dashboard shell is ready while Cloud Run builds or reads the latest IBKR snapshot.", error=""){
  return `<div class="panel loading-panel"><div><h2>${safe(message)}</h2><p>${safe(detail)}</p>${error ? `<p class="error retry-load">${safe(error)}</p><button class="secondary retry-load" type="button" id="retryDashboardLoad">Retry</button>` : ""}</div></div>`;
}
async function loadDashboardData(){
  setUpdating(true, "Loading portfolio data...");
  try {
    const response = await fetch(apiDashboardUrl().toString(), {
      credentials: "same-origin",
      headers: {"Accept": "application/json"}
    });
    if (response.status === 401) {
      window.location.replace("/login");
      return;
    }
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text.slice(0, 400) || `HTTP ${response.status}`);
    }
    data = await response.json();
    dashboardLoaded = true;
    appState.includeUnrealized = data.web?.include_unrealized !== false;
    appState.targetReturn = Number(data.web?.target_return ?? appState.targetReturn ?? 0.015);
    render();
  } catch (err) {
    dashboardLoaded = false;
    renderLoadingError(err && (err.message || err.stack) || String(err));
  } finally {
    setUpdating(false);
  }
}
function renderLoadingError(message){
  renderHeader();
  [...$("nav").children].forEach(b => b.classList.toggle("active", b.dataset.section === appState.active));
  document.querySelectorAll(".section").forEach(s => s.classList.toggle("active", s.id === appState.active));
  const target = $(appState.active) || $("dashboard");
  if (target) target.innerHTML = loadingPanel("Dashboard data failed to load", "The service returned an error while building the portfolio payload.", message);
  const retry = $("retryDashboardLoad");
  if (retry) retry.addEventListener("click", loadDashboardData);
  bindControls();
}
function renderBasisControl(){
  const target = $("basisControl");
  if (!["dashboard"].includes(appState.active)) {
    target.innerHTML = "";
    return;
  }
  const include = appState.includeUnrealized;
  target.innerHTML = `<div class="segmented" data-basis-control><button type="button" data-value="1" class="${include ? "active" : ""}">With unrealized</button><button type="button" data-value="0" class="${!include ? "active" : ""}">Realized only</button></div>`;
}
function currentSnapshot(){
  const snapshots = data.views?.snapshots || {};
  return appState.includeUnrealized
    ? (snapshots.with_unrealized || data.dashboard.snapshot || {})
    : (snapshots.realized_only || data.dashboard.snapshot || {});
}
function yearlyRows(){
  const rows = data.views?.yearly || {};
  return appState.includeUnrealized
    ? (rows.with_unrealized || data.yearly.years || [])
    : (rows.realized_only || data.yearly.years || []);
}
function realizedYearlyRows(){
  return data.views?.yearly?.realized_only || data.yearly.years || [];
}
function riskTone(row){
  const band = String(row.moneyness_band || "").toLowerCase();
  const m = numeric(row.moneyness);
  if (band === "in_the_money" || (m !== null && m >= 0)) return "bad";
  if (band === "at_strike" || band === "near_the_money" || (m !== null && m >= -0.05)) return "warn";
  if (m !== null && m < -0.10) return "blue";
  return "good";
}
function rowRiskClass(row){
  const tone = riskTone(row);
  if (tone === "bad") return "risk-row-itm";
  if (tone === "warn") return "risk-row-near";
  if (tone === "blue") return "risk-row-clear";
  return "";
}
function dot(row){
  const tone = riskTone(row);
  return `<span class="risk-dot ${tone === "bad" ? "dot-bad" : tone === "warn" ? "dot-warn" : tone === "blue" ? "dot-blue" : "dot-good"}"></span>`;
}
function compareValues(a,b){
  const na = numeric(a), nb = numeric(b);
  if (na !== null && nb !== null) return na - nb;
  const da = Date.parse(a), db = Date.parse(b);
  if (!Number.isNaN(da) && !Number.isNaN(db)) return da - db;
  return String(a ?? "").localeCompare(String(b ?? ""));
}
function sortedRows(tableId, rows, columns){
  const state = appState.sort[tableId];
  if (!state) return rows;
  const col = columns.find(c => c.key === state.key);
  if (!col) return rows;
  return [...rows].sort((a,b) => {
    const av = col.value ? col.value(a) : get(a, col.key);
    const bv = col.value ? col.value(b) : get(b, col.key);
    return compareValues(av,bv) * (state.dir === "desc" ? -1 : 1);
  });
}
function dataTable(tableId, rows, columns, opts={}){
  const tableRows = sortedRows(tableId, rows || [], columns);
  const title = opts.title ? `<div class="table-title"><div><strong>${safe(opts.title)}</strong>${opts.subtitle ? `<span>${safe(opts.subtitle)}</span>` : ""}</div>${opts.count === false ? "" : badge(`${fmtNum(tableRows.length)} rows`,"blue")}</div>` : "";
  if (!tableRows.length) return `<div class="table-card ${opts.small ? "small-table" : ""}">${title}<div class="empty">No rows.</div></div>`;
  const head = `<tr>${columns.map(c => {
    const thClass = [c.num ? "num" : "", c.compact ? "col-compact" : "", c.thClass || ""].filter(Boolean).join(" ");
    return `<th class="${thClass}"><button type="button" onclick="sortTable('${tableId}','${c.key}')">${safe(c.label)}</button></th>`;
  }).join("")}</tr>`;
  const body = tableRows.map(row => `<tr class="${opts.rowClass ? opts.rowClass(row) : ""}">${columns.map(c => {
    const raw = c.value ? c.value(row) : get(row, c.key);
    const value = c.format ? c.format(raw,row) : safe(raw);
    const cellClass = [
      c.num ? "num" : "",
      c.compact ? "col-compact" : "",
      c.cellClass || "",
      c.className ? c.className(raw,row) : ""
    ].filter(Boolean).join(" ");
    return `<td class="${cellClass}">${value}</td>`;
  }).join("")}</tr>`).join("");
  return `<div class="table-card ${opts.small ? "small-table" : ""} ${opts.wide ? "wide-table" : ""} ${opts.compact ? "compact-table" : ""}">${title}<div class="table-scroll" style="${opts.maxHeight ? `max-height:${opts.maxHeight}px` : ""}"><table><thead>${head}</thead><tbody>${body}</tbody></table></div></div>`;
}
function sortTable(tableId,key){
  const current = appState.sort[tableId] || {};
  appState.sort[tableId] = {key, dir: current.key === key && current.dir !== "desc" ? "desc" : "asc"};
  render();
}
function parseDate(v){ const d = new Date(fmtDate(v) + "T00:00:00Z"); return Number.isNaN(d.getTime()) ? null : d; }
function addMonths(date, months){ const d = new Date(date); d.setUTCMonth(d.getUTCMonth() + months); return d; }
function monthIndex(date){ return date.getUTCFullYear() * 12 + date.getUTCMonth(); }
function rangeFiltered(rows, dateKey){
  const range = appState.range;
  if (range === "Since inception") return rows || [];
  const asOf = parseDate(data.dashboard.request?.as_of || data.generated_at) || new Date();
  if (dateKey === "month") {
    const asOfMonth = monthIndex(asOf);
    let startMonth = null;
    if (range === "3M") startMonth = asOfMonth - 2;
    if (range === "6M") startMonth = asOfMonth - 5;
    if (range === "1Y") startMonth = asOfMonth - 11;
    if (range === "YTD") startMonth = asOf.getUTCFullYear() * 12;
    return (rows || []).filter(row => {
      const d = parseDate(get(row,dateKey));
      if (!d) return false;
      const m = monthIndex(d);
      return (startMonth === null || m >= startMonth) && m <= asOfMonth;
    });
  }
  let start = null;
  if (range === "3M") start = addMonths(asOf, -3);
  if (range === "6M") start = addMonths(asOf, -6);
  if (range === "1Y") start = addMonths(asOf, -12);
  if (range === "YTD") start = new Date(Date.UTC(asOf.getUTCFullYear(),0,1));
  return (rows || []).filter(row => {
    const d = parseDate(get(row,dateKey));
    return !start || (d && d >= start && d <= asOf);
  });
}
function dateLabel(value, mode="auto"){
  const d = parseDate(value);
  if (!d) return safe(value);
  const month = d.toLocaleDateString("en-US",{month:"short",timeZone:"UTC"});
  const year = d.getUTCFullYear();
  if (mode === "year") return String(year);
  if (mode === "day-month") return `${d.getUTCDate()}-${month}-${year}`;
  if (mode === "month-year") return `${month} ${year}`;
  return `${month} ${year}`;
}
function chartRegister(title, subtitle, config, footer=""){
  const id = `chart-${++chartSeq}`;
  pendingCharts.push({id, config});
  return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong>${subtitle ? `<span class="muted">${safe(subtitle)}</span>` : ""}</div><div class="chart-wrap"><canvas id="${id}" class="chart-canvas"></canvas></div>${footer}</div>`;
}
function chartCommonOptions(valueFormatter){
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {mode: "index", intersect: false},
    animation: false,
    plugins: {
      legend: {
        display: true,
        position: "bottom",
        align: "start",
        labels: {color: "#c0cbc7", boxWidth: 12, boxHeight: 12, padding: 14, font: {size: 13, weight: 650}}
      },
      tooltip: {
        backgroundColor: "#11191c",
        borderColor: "#365257",
        borderWidth: 1,
        titleColor: "#f2f7f2",
        bodyColor: "#d8e3df",
        padding: 10,
        callbacks: {
          title(items){ return items?.[0]?.label || ""; },
          label(item){
            const label = item.dataset.label ? `${item.dataset.label}: ` : "";
            return `${label}${valueFormatter(item.parsed.y)}`;
          }
        }
      }
    },
    scales: {
      x: {
        offset: false,
        grid: {display: false},
        ticks: {
          color: "#c0cbc7",
          font: {size: 12, weight: 650},
          maxRotation: 0,
          minRotation: 0,
          autoSkip: true,
          maxTicksLimit: window.innerWidth < 760 ? 4 : 7,
          callback(value){
            const label = this.getLabelForValue(value);
            return label.includes(" ") ? label.split(" ") : label;
          }
        }
      },
      y: {
        grid: {color: "#26383c"},
        border: {color: "#42585d"},
        ticks: {
          color: "#c0cbc7",
          font: {size: 12, weight: 650},
          callback: (value) => valueFormatter(value)
        }
      }
    }
  };
}
function initCharts(){
  chartInstances.forEach(chart => chart.destroy());
  chartInstances = [];
  if (!pendingCharts.length) return;
  if (!window.Chart) {
    pendingCharts.forEach(({id}) => {
      const canvas = $(id);
      if (canvas) canvas.outerHTML = `<div class="empty">Interactive chart library did not load. Reload the app.</div>`;
    });
    pendingCharts = [];
    return;
  }
  window.Chart.defaults.color = "#c0cbc7";
  window.Chart.defaults.font.family = "-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif";
  pendingCharts.forEach(({id, config}) => {
    const canvas = $(id);
    if (!canvas) return;
    chartInstances.push(new window.Chart(canvas, config));
  });
  pendingCharts = [];
}
function targetReturn(){ return numeric(appState.targetReturn) ?? numeric(data.monthly?.target_return) ?? 0.015; }
function targetFloor(){ return Math.min(targetReturn(), numeric(appState.targetFloor) ?? 0.01); }
function targetCapital(row){
  const rowTarget = numeric(row.target_return ?? data.monthly?.target_return);
  const targetPnl = numeric(row.target_pnl);
  if (numeric(row.avg_capital) !== null) return numeric(row.avg_capital);
  if (numeric(row.average_capital) !== null) return numeric(row.average_capital);
  if (rowTarget && targetPnl !== null) return targetPnl / rowTarget;
  return null;
}
function displayMonthPnl(row){
  const explicit = numeric(row.risk_adjusted_projected_month_pnl);
  if (explicit !== null) return explicit;
  const openNet = numeric(row.open_expiring_option_unrealized_pnl);
  if (openNet !== null) {
    const realized = numeric(row.realized_month_pnl ?? row.total_realized_pnl) || 0;
    return realized + openNet;
  }
  return numeric(row.projected_month_pnl) ?? numeric(row.total_realized_pnl);
}
function displayMonthReturn(row){
  const capital = targetCapital(row);
  const pnl = displayMonthPnl(row);
  if (capital && pnl !== null) return pnl / capital;
  return numeric(row.risk_adjusted_projected_return_roac) ?? numeric(row.projected_return_roac) ?? numeric(row.return_roac);
}
function displayTargetPnl(row){
  const capital = targetCapital(row);
  return capital === null ? numeric(row.target_pnl) : capital * targetReturn();
}
function displayRemainingToTarget(row){
  const targetPnl = displayTargetPnl(row);
  const pnl = displayMonthPnl(row);
  if (targetPnl === null || pnl === null) return numeric(row.risk_adjusted_projected_remaining_pnl ?? row.projected_remaining_pnl);
  return Math.max(targetPnl - pnl, 0);
}
function displayTargetStatus(row){
  const ret = displayMonthReturn(row);
  if (ret === null) return row.monthly_target_status || "n/a";
  return ret >= targetReturn() ? "Beat Target" : "Below Target";
}
function lineChart(title, rows, xKey, yKey, seriesKey, yFormat=fmtDec){
  let clean = (rows || []).filter(r => numeric(get(r,yKey)) !== null && get(r,xKey));
  if (clean.length < 2) return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong></div><div class="empty">Chart unavailable for the selected range.</div></div>`;
  const dates=[...new Set(clean.map(r => fmtDate(get(r,xKey))))].sort();
  const groups={}; clean.forEach(r => { const name = get(r,seriesKey) || "Series"; (groups[name] ||= []).push(r); });
  const labels = dates.map(d => dateLabel(d, "day-month"));
  const datasets = Object.entries(groups).map(([name, vals], i) => {
    const byDate = new Map(vals.map(r => [fmtDate(get(r,xKey)), numeric(get(r,yKey))]));
    return {
      label: name,
      data: dates.map(d => byDate.has(d) ? byDate.get(d) : null),
      borderColor: colors[i % colors.length],
      backgroundColor: colors[i % colors.length],
      borderWidth: 3,
      pointRadius: 3,
      pointHoverRadius: 6,
      tension: 0.22,
      spanGaps: true
    };
  });
  const options = chartCommonOptions(yFormat);
  options.scales.x.ticks.autoSkip = labels.length > 7;
  options.scales.x.ticks.maxTicksLimit = appState.range === "Since inception" ? (window.innerWidth < 760 ? 4 : 6) : 7;
  return chartRegister(title, appState.range, {type: "line", data: {labels, datasets}, options});
}
function barChart(title, rows, xKey, yKey){
  const clean=(rows || []).filter(r=>numeric(get(r,yKey))!==null);
  if (!clean.length) return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong></div><div class="empty">Chart unavailable for the selected range.</div></div>`;
  const labels = clean.map(r => dateLabel(get(r,xKey)));
  const values = clean.map(r => numeric(get(r,yKey)) || 0);
  const options = chartCommonOptions(fmtCompactMoney);
  options.plugins.legend.display = false;
  options.scales.x.offset = true;
  options.scales.x.ticks.autoSkip = labels.length > 7;
  options.scales.x.ticks.maxTicksLimit = 7;
  return chartRegister(title, appState.range, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: "P&L",
        data: values,
        backgroundColor: values.map(v => v >= 0 ? "#66d37a" : "#ff7078"),
        borderWidth: 0,
        borderRadius: 2,
        categoryPercentage: 0.72,
        barPercentage: 0.9,
        maxBarThickness: 96
      }]
    },
    options
  });
}
function monthlyPnlBarChart(title, rows){
  const clean=(rows || []).filter(r=>displayMonthPnl(r)!==null);
  if (!clean.length) return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong></div><div class="empty">Chart unavailable for the selected range.</div></div>`;
  const labels = clean.map(r => dateLabel(r.month || r.Date));
  const values = clean.map(r => displayMonthPnl(r) || 0);
  const options = chartCommonOptions(fmtCompactMoney);
  options.plugins.legend.display = false;
  options.scales.x.offset = true;
  options.scales.x.ticks.autoSkip = labels.length > 7;
  options.scales.x.ticks.maxTicksLimit = 7;
  return chartRegister(title, appState.range, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: "P&L",
        data: values,
        backgroundColor: values.map(v => v >= 0 ? "#66d37a" : "#ff7078"),
        borderWidth: 0,
        borderRadius: 2,
        categoryPercentage: 0.72,
        barPercentage: 0.9,
        maxBarThickness: 96
      }]
    },
    options
  });
}
function monthlyReturnTargetChart(title, rows){
  const clean=(rows || []).filter(r=>displayMonthReturn(r)!==null);
  if (!clean.length) return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong></div><div class="empty">Chart unavailable for the selected range.</div></div>`;
  const labels = clean.map(r => dateLabel(r.month || r.Date));
  const values = clean.map(r => displayMonthReturn(r) || 0);
  const target = targetReturn();
  const floor = targetFloor();
  const bandColor = (value) => value < 0 ? "#ff7078" : value < floor ? "#f6c25b" : value <= target ? "#7ee092" : "#6da8ff";
  const targetBandPlugin = {
    id: "targetBands",
    beforeDatasetsDraw(chart, args, opts) {
      const {ctx, chartArea, scales} = chart;
      if (!chartArea || !scales.y) return;
      const y = scales.y;
      const yTarget = y.getPixelForValue(opts.target);
      const yFloor = y.getPixelForValue(opts.floor);
      const yZero = y.getPixelForValue(0);
      const clamp = (value) => Math.max(chartArea.top, Math.min(chartArea.bottom, value));
      const band = (top, bottom, fill) => {
        const y1 = clamp(top);
        const y2 = clamp(bottom);
        const height = y2 - y1;
        if (height <= 0) return;
        ctx.fillStyle = fill;
        ctx.fillRect(chartArea.left, y1, chartArea.right - chartArea.left, height);
      };
      ctx.save();
      band(chartArea.top, yTarget, "rgba(109,168,255,.12)");
      band(yTarget, yFloor, "rgba(126,224,146,.11)");
      band(yFloor, yZero, "rgba(246,194,91,.13)");
      band(yZero, chartArea.bottom, "rgba(255,111,120,.10)");
      ctx.strokeStyle = "#d6e1a1";
      ctx.lineWidth = 2;
      ctx.setLineDash([7, 5]);
      ctx.beginPath();
      ctx.moveTo(chartArea.left, yTarget);
      ctx.lineTo(chartArea.right, yTarget);
      ctx.stroke();
      ctx.strokeStyle = "#f6c25b";
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(chartArea.left, yFloor);
      ctx.lineTo(chartArea.right, yFloor);
      ctx.stroke();
      ctx.restore();
    }
  };
  const options = chartCommonOptions(fmtPct);
  options.scales.x.offset = true;
  options.scales.x.ticks.autoSkip = labels.length > 7;
  options.scales.x.ticks.maxTicksLimit = 7;
  options.plugins.targetBands = {floor, target};
  options.plugins.legend.display = false;
  const footer = `<div class="chart-legend"><span><i style="background:#ff7078"></i>Negative</span><span><i style="background:#f6c25b"></i>0-${safe(fmtPct(floor))}</span><span><i style="background:#7ee092"></i>Target band ${safe(fmtPct(floor))}-${safe(fmtPct(target))}</span><span><i style="background:#6da8ff"></i>Above target</span></div>`;
  return chartRegister(title, appState.range, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: "Monthly return",
        data: values,
        backgroundColor: values.map(v => bandColor(v)),
        borderWidth: 0,
        borderRadius: 2,
        categoryPercentage: 0.72,
        barPercentage: 0.9,
        maxBarThickness: 96
      }]
    },
    options,
    plugins: [targetBandPlugin]
  }, footer);
}
function growthFromReturns(rows){
  let growth=1;
  return rangeFiltered(rows || [],"month").sort((a,b)=>String(a.month).localeCompare(String(b.month))).map(row => {
    growth *= (1 + (numeric(row.return) || 0));
    return {month: row.month, Series: "Strategy", Growth: growth};
  });
}
function openShortRows(){
  const rows = data.positions.open_option_shorts || data.open_shorts.items || [];
  const q = appState.openSearch.trim().toUpperCase();
  return rows.filter(row => {
    const m = numeric(row.moneyness);
    const tone = riskTone(row);
    const riskOk = appState.openRisk === "all"
      || (appState.openRisk === "itm" && tone === "bad")
      || (appState.openRisk === "near" && tone === "warn")
      || (appState.openRisk === "clear" && (tone === "good" || tone === "blue"));
    const optionType = String(row.option_type || "").toLowerCase();
    const typeOk = appState.openType === "all"
      || (appState.openType === "puts" && optionType.includes("put"))
      || (appState.openType === "calls" && optionType.includes("call"));
    const text = `${row.ticker || ""} ${row.option_type || ""} ${row.strike || ""} ${row.expiration || ""}`.toUpperCase();
    return riskOk && typeOk && (!q || text.includes(q)) && m !== null;
  });
}
function openShortProjectedPnl(row){
  if (!row || row.missing_price) return null;
  const strike = numeric(row.strike);
  const current = numeric(row.current_price);
  const qty = Math.abs(numeric(row.quantity) || 0);
  const premium = numeric(row.display_premium_collected ?? row.roll_adjusted_premium_collected ?? row.premium_collected) || 0;
  if (strike === null || current === null || !qty) return premium;
  const type = String(row.option_type || "").toLowerCase();
  const intrinsic = type.includes("call")
    ? Math.max(current - strike, 0) * 100 * qty
    : Math.max(strike - current, 0) * 100 * qty;
  return premium - intrinsic;
}
function openShortColumns(){
  return [
    {key:"ticker",label:"Ticker",format:(v,r)=>`${dot(r)}<strong>${safe(v)}</strong>`},
    {key:"option_type",label:"Type"},
    {key:"strike",label:"Strike",format:v=>fmtMoney(v,2),num:true},
    {key:"expiration",label:"Expiry",format:fmtDate},
    {key:"days_to_expiration",label:"DTE",num:true},
    {key:"current_price",label:"Current",format:v=>fmtMoney(v,2),num:true},
    {key:"moneyness",label:"Moneyness",format:fmtPct,num:true,className:moneynessCls},
    {key:"quantity",label:"Qty",num:true},
    {key:"display_premium_collected",label:"Premium",value:r=>numeric(r.display_premium_collected ?? r.roll_adjusted_premium_collected ?? r.premium_collected) || 0,format:v=>fmtMoney(v,2),num:true,className:cls},
    {key:"projected_pnl",label:"Projected P&L",value:openShortProjectedPnl,format:v=>fmtMoney(v,2),num:true,className:cls},
    {key:"covered_status",label:"Backing",format:labelize}
  ];
}
function openShortToolbar(){
  return `<div class="toolbar">
    ${segmentedControl("risk",["all","itm","near","clear"],appState.openRisk)}
    ${segmentedControl("type",["all","puts","calls"],appState.openType)}
    <input data-open-search value="${safe(appState.openSearch)}" placeholder="Filter ticker, strike, expiry">
  </div>`;
}
function riskPill(row){
  const tone = riskTone(row);
  if (tone === "bad") return "ITM";
  if (tone === "warn") return "Near";
  if (tone === "blue") return "Deep OTM";
  return "OK";
}
function riskCards(rows, limit=null){
  const top = limit === null ? (rows || []) : (rows || []).slice(0, limit);
  if (!top.length) return `<div class="panel muted">No open shorts match the filters.</div>`;
  return `<div class="risk-grid">${top.map(r => {
    const tone = riskTone(r);
    const premium = numeric(r.display_premium_collected ?? r.roll_adjusted_premium_collected ?? r.premium_collected) || 0;
    return `<div class="risk-card"><div class="risk-head"><div><div class="risk-title">${safe(r.ticker)} ${safe(r.option_type)} ${safe(fmtDec(r.strike,2))}</div><div class="muted">${safe(fmtDate(r.expiration))} - ${safe(r.days_to_expiration)} DTE</div></div><span class="pill ${tone}">${safe(riskPill(r))}</span></div><div class="risk-meta"><span>Current ${safe(fmtMoney(r.current_price,2))}</span><span>Moneyness ${safe(fmtPct(r.moneyness))}</span><span>Qty ${safe(r.quantity)}</span><span>${labelize(r.covered_status)}</span><span>Premium ${safe(fmtMoney(premium,2))}</span><span>Opened ${safe(fmtDate(r.opened))}</span></div></div>`;
  }).join("")}</div>`;
}
function monthlyRows(){
  const cycles = new Map((data.tables.monthly_cycles || []).map(row => [fmtDate(row.month), row]));
  return (data.monthly.months || []).map(row => ({...(cycles.get(fmtDate(row.month)) || {}), ...row}));
}
function inventoryColumns(){
  return [
    {key:"ticker",label:"Ticker",format:v=>`<strong>${safe(v)}</strong>`,compact:true},
    {key:"buy_date",label:"Buy date",format:fmtDate},
    {key:"shares",label:"Shares",num:true},
    {key:"cost_per_share",label:"Cost/share",format:v=>fmtMoney(v,2),num:true},
    {key:"current_price",label:"Current",format:v=>fmtMoney(v,2),num:true},
    {key:"covered_shares",label:"Covered shares",num:true},
    {key:"covered_strike",label:"Covered strike",format:v=>fmtMoney(v,2),num:true},
    {key:"unrealized_pnl",label:"Unrealized",format:fmtMoney,num:true,className:cls}
  ];
}
function renderHeader(){
  const d=data.dashboard || {}, freshness=d.data_freshness || {}, price=freshness.price_coverage || {}, issue=d.issue_summary || {};
  const priced=price.priced_count ?? price.stocks_fetched ?? price.fetched ?? 0;
  const required=price.required_count ?? price.stocks_requested ?? price.requested ?? 0;
  const missing=price.missing_count ?? Math.max(required-priced,0);
  $("heroTitle").textContent = pageTitles[appState.active] || "Dashboard";
  $("subtitle").textContent = `${data.source?.label || "IBKR Flex"} - ${dashboardLoaded ? `portfolio as of ${fmtDate(d.request?.as_of)}` : "loading portfolio data"}`;
  $("statusStrip").innerHTML = [
    dashboardLoaded ? badge(`${priced}/${required} priced`, missing > 0 ? "bad" : "good") : badge("Loading", "blue"),
    badge(`${issue.total_count ?? 0} actionable issues`, (issue.total_count || 0) ? "bad" : "good"),
    dashboardLoaded ? badge(`Prices updated ${fmtLocalTime(freshness.prices_updated_at)}`) : badge("IBKR Flex")
  ].join("");
  renderBasisControl();
}
function renderDashboard(){
  const snap=currentSnapshot(), mt=data.dashboard.monthly_target || {}, shorts=openShortRows();
  const riskMonthPnl = numeric(mt.risk_adjusted_projected_month_pnl) !== null ? mt.risk_adjusted_projected_month_pnl : mt.projected_month_pnl;
  const riskMonthReturn = displayMonthReturn(mt);
  const riskRemaining = displayRemainingToTarget(mt);
  const riskStatus = displayTargetStatus(mt);
  const target = targetReturn();
  $("dashboard").innerHTML = `
    <div class="grid metrics">
      ${card("YTD total P&L", fmtMoney(snap.ytd_total_pnl), snap.unrealized_adjusted ? "Realized YTD + current unrealized" : "Realized P&L only", cls(snap.ytd_total_pnl))}
      ${card("YTD realized P&L", fmtMoney(snap.ytd_realized_pnl), "Options, stock P&L, and dividends", cls(snap.ytd_realized_pnl))}
      ${card("Current unrealized", fmtMoney(snap.current_unrealized_pnl), `Options net ${safe(fmtMoney(snap.current_option_unrealized_pnl))}${numeric(snap.current_put_assignment_unrealized_pnl) ? ` (premium ${safe(fmtMoney(snap.current_option_premium_unrealized_pnl))}, ITM put gap ${safe(fmtMoney(snap.current_put_assignment_unrealized_pnl))})` : ""} / Stock ${safe(fmtMoney(snap.current_stock_unrealized_pnl))}`, cls(snap.current_unrealized_pnl))}
      ${card("YTD annualized TWR", fmtPct(snap.ytd_annualized_twr), snap.unrealized_adjusted ? "Unrealized-adjusted" : "Realized only", cls(snap.ytd_annualized_twr))}
    </div>
    ${sectionHead("Current Month")}
    <div class="grid metrics">
      ${card("Risk-adjusted month P&L", fmtMoney(riskMonthPnl), `Realized ${safe(fmtMoney(mt.realized_month_pnl))} + open option net ${safe(fmtMoney(mt.open_expiring_option_unrealized_pnl ?? mt.current_unrealized_pnl))}`, cls(riskMonthPnl))}
      ${card("Risk-adjusted return", `${safe(fmtPct(riskMonthReturn))} RoAC`, `Target ${safe(fmtPct(target))} - ${labelize(riskStatus)}`, cls((numeric(riskMonthReturn)||0) - target))}
      ${card("Remaining to target", fmtMoney(riskRemaining), "Based on risk-adjusted monthly target", cls(-1*(numeric(riskRemaining)||0)))}
      ${card("ITM put cash required", fmtMoney(snap.itm_put_cash_required), `${safe(snap.itm_put_contracts || 0)} ITM puts`, cls(-1*(numeric(snap.itm_put_cash_required)||0)))}
    </div>
    ${sectionHead("Assigned Holdings and Exposure")}
    ${dataTable("dashboard-inventory", data.positions.inventory || [], inventoryColumns(), {title:"Assigned holdings", compact:true})}
    ${sectionHead("Open Option Shorts", `${shorts.length} rows after filters.`)}
    ${openShortToolbar()}
    ${dataTable("dashboard-open-shorts", shorts, openShortColumns(), {title:"Open option shorts", rowClass:rowRiskClass, wide:true})}
  `;
}
function renderMonthly(){
  const rows = monthlyRows();
  const filtered = rangeFiltered(rows, "month");
  const future = data.monthly.future_months || [];
  $("monthly").innerHTML = `
    ${sectionHead("Monthly Performance")}
    ${rangePicker()}
    <div class="grid two-even">
      ${monthlyReturnTargetChart("Monthly Return vs Target", filtered)}
      ${monthlyPnlBarChart("Monthly P&L", filtered)}
    </div>
    <div style="height:12px"></div>
    ${dataTable("monthly-table", rows, [
      {key:"month",label:"Month",format:monthName},
      {key:"realized_options_pnl",label:"Options P&L",format:fmtMoney,num:true,className:cls},
      {key:"realized_stock_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"dividends",label:"Dividends",format:fmtMoney,num:true},
      {key:"total_realized_pnl",label:"Total realized",format:fmtMoney,num:true,className:cls},
      {key:"avg_capital",label:"Avg capital",format:fmtMoney,num:true},
      {key:"peak_capital",label:"Peak capital",format:fmtMoney,num:true},
      {key:"return_roac",label:"RoAC",format:fmtPct,num:true,className:cls},
      {key:"return_ropc",label:"RoPC",format:fmtPct,num:true,className:cls},
      {key:"open_expiring_incremental_premium",label:"Open premium",format:fmtMoney,num:true,className:cls},
      {key:"open_expiring_intrinsic_value_gap",label:"ITM put gap",format:fmtMoney,num:true,className:cls},
      {key:"open_expiring_option_unrealized_pnl",label:"Open option net",format:fmtMoney,num:true,className:cls},
      {key:"risk_adjusted_projected_month_pnl",label:"Target-view P&L",value:displayMonthPnl,format:fmtMoney,num:true,className:cls},
      {key:"risk_adjusted_projected_return_roac",label:"Target-view RoAC",value:displayMonthReturn,format:fmtPct,num:true,className:cls},
      {key:"risk_adjusted_projected_remaining_pnl",label:"Remaining",value:displayRemainingToTarget,format:fmtMoney,num:true,className:(v)=>numeric(v)>0?"neg":"pos"},
      {key:"monthly_target_status",label:"Target status",value:displayTargetStatus,format:statusPill}
    ], {title:"Monthly table", wide:true})}
    ${sectionHead("Future Open Expiry Months")}
    ${dataTable("future-months", future, [
      {key:"month",label:"Month",format:monthName},
      {key:"open_option_count",label:"Open options",num:true},
      {key:"open_expiring_incremental_premium",label:"Incremental premium",format:fmtMoney,num:true,className:cls},
      {key:"open_expiring_roll_adjusted_premium",label:"Roll-adjusted premium",format:fmtMoney,num:true,className:cls},
      {key:"open_expiring_intrinsic_value_gap",label:"ITM put gap",format:fmtMoney,num:true,className:cls},
      {key:"open_expiring_option_unrealized_pnl",label:"Open option net",format:fmtMoney,num:true,className:cls},
      {key:"projected_month_pnl",label:"Target-view P&L",value:displayMonthPnl,format:fmtMoney,num:true,className:cls},
      {key:"projected_return_roac",label:"Target-view RoAC",value:displayMonthReturn,format:fmtPct,num:true},
      {key:"projection_basis",label:"Basis"}
    ], {title:"Future expiry months", small:true})}
  `;
}
function renderTickers(){
  const rows = data.tickers.items || data.tables.per_ticker_totals || [];
  const years = ["all", ...[...new Set((data.tables.per_ticker_yearly || []).map(r => String(r.year)))].sort()];
  const q = appState.tickerSearch.trim().toUpperCase();
  const filtered = rows.filter(r => !q || String(r.ticker || "").toUpperCase().includes(q));
  const yearly = (data.tables.per_ticker_yearly || []).filter(r => (appState.tickerYear === "all" || String(r.year) === appState.tickerYear) && (!q || String(r.ticker || "").toUpperCase().includes(q)));
  $("tickers").innerHTML = `
    ${sectionHead("Per-Ticker P&L", "Ticker totals include realized options, realized stock, dividends, unrealized snapshot, and total P&L.")}
    <div class="toolbar"><input id="tickerSearch" value="${safe(appState.tickerSearch)}" placeholder="Filter ticker"></div>
    ${dataTable("ticker-totals", filtered, [
      {key:"ticker",label:"Ticker",format:v=>`<strong>${safe(v)}</strong>`,compact:true},
      {key:"realized_options_pnl",label:"Options P&L",format:fmtMoney,num:true,className:cls},
      {key:"realized_stock_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"dividends",label:"Dividends",format:fmtMoney,num:true,className:cls},
      {key:"combined_realized_pnl",label:"Realized",format:fmtMoney,num:true,className:cls},
      {key:"unrealized_pnl",label:"Unrealized",format:fmtMoney,num:true,className:cls},
      {key:"total_pnl",label:"Total P&L",format:fmtMoney,num:true,className:cls},
      {key:"current_price",label:"Price",format:v=>fmtMoney(v,2),num:true},
      {key:"open_option_count",label:"Open options",num:true,compact:true},
      {key:"inventory_share_count",label:"Shares",num:true,compact:true}
    ], {title:"Ticker totals", subtitle:"Cumulative ticker results.", compact:true})}
    ${sectionHead("Per-Year Ticker Realized P&L", "Year filter applies to this table.", `<select id="tickerYear">${years.map(y=>`<option value="${safe(y)}" ${y===appState.tickerYear?"selected":""}>${safe(y==="all"?"All years":y)}</option>`).join("")}</select>`)}
    ${dataTable("ticker-yearly", yearly, [
      {key:"year",label:"Year",num:true,compact:true},
      {key:"ticker",label:"Ticker",format:v=>`<strong>${safe(v)}</strong>`,compact:true},
      {key:"options_pnl",label:"Options P&L",format:fmtMoney,num:true,className:cls},
      {key:"stock_realized_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"combined_realized",label:"Total realized",format:fmtMoney,num:true,className:cls}
    ], {title:"Per-year realized P&L", compact:true})}
  `;
}
function expectancyByYearRows(){
  return (data.tables.expectancy_by_year || []).filter(r => appState.expectancyYear === "all" || String(r.Year) === appState.expectancyYear);
}
function maxDrawdownFromReturns(rows){
  let growth = 1, peak = 1, maxDd = 0;
  (rows || []).filter(r => numeric(r.return) !== null).sort((a,b)=>String(a.month).localeCompare(String(b.month))).forEach(r => {
    growth *= (1 + (numeric(r.return) || 0));
    peak = Math.max(peak, growth);
    maxDd = Math.min(maxDd, growth / peak - 1);
  });
  return maxDd;
}
function benchmarkMetricRows(){
  const rows = (data.tables.benchmark_metrics || []).map(r => ({...r}));
  const strategy = rows.find(r => String(r.Series) === "My Strategy");
  if (strategy) strategy["Max Drawdown"] = maxDrawdownFromReturns(data.charts.monthly_returns || []);
  return rows;
}
function expectancyTrendChart(){
  const rows = data.tables.expectancy_by_year || [];
  const clean = rows.filter(r => numeric(r["Win rate"]) !== null && r.Year && r.Category);
  if (!clean.length) return `<div class="chart-card"><div class="chart-title"><strong>Win Rate by Year</strong></div><div class="empty">Chart unavailable.</div></div>`;
  const years=[...new Set(clean.map(r=>String(r.Year)))].sort();
  const cats=[...new Set(clean.map(r=>String(r.Category)))].sort();
  const datasets = cats.map((cat, i) => {
    const byYear = new Map(clean.filter(r => String(r.Category) === cat).map(r => [String(r.Year), numeric(r["Win rate"])]));
    return {
      label: cat,
      data: years.map(year => byYear.has(year) ? byYear.get(year) : null),
      borderColor: colors[i % colors.length],
      backgroundColor: colors[i % colors.length],
      borderWidth: 3,
      pointRadius: 3,
      pointHoverRadius: 6,
      tension: 0.2,
      spanGaps: true
    };
  });
  const options = chartCommonOptions(fmtPct);
  options.scales.x.ticks.autoSkip = false;
  options.scales.y.min = 0;
  options.scales.y.max = 1;
  return chartRegister("Win Rate by Year", "percentage of profitable trades", {type: "line", data: {labels: years, datasets}, options});
}
function renderPerformance(){
  const benchmark = (data.charts.benchmark_growth_by_range || {})[appState.range] || data.charts.benchmark_growth || [];
  const pnlRows = rangeFiltered(data.tables.options_cycle_pnl || [], "Date");
  const expectancyYears = ["all", ...[...new Set((data.tables.expectancy_by_year || []).map(r => String(r.Year)))].sort()];
  const expectancyRows = expectancyByYearRows();
  $("performance").innerHTML = `
    ${sectionHead("Yearly Performance", "Includes active-month TWR so inactive periods do not dilute trading-period performance.")}
    ${dataTable("yearly-mobile", realizedYearlyRows(), [
      {key:"year",label:"Year",num:true},
      {key:"realized_options_pnl",label:"Options P&L",format:fmtMoney,num:true,className:cls},
      {key:"realized_stock_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"dividends",label:"Dividends",format:fmtMoney,num:true},
      {key:"total_realized_pnl",label:"Realized",format:fmtMoney,num:true,className:cls},
      {key:"avg_capital",label:"Avg capital",format:fmtMoney,num:true},
      {key:"peak_capital",label:"Peak capital",format:fmtMoney,num:true},
      {key:"roac_year",label:"RoAC",format:fmtPct,num:true,className:cls},
      {key:"ropc_year",label:"RoPC",format:fmtPct,num:true,className:cls},
      {key:"annualized_twr",label:"Ann. TWR",format:fmtPct,num:true,className:cls},
      {key:"annualized_twr_active",label:"Ann. TWR active",format:fmtPct,num:true,className:cls}
    ], {title:"Yearly performance", wide:true})}
    <div style="height:12px"></div>
    ${sectionHead("Performance Charts")}
    ${rangePicker()}
    <div class="grid two-even">${lineChart("Cumulative Growth vs Benchmarks", benchmark, "Date", "Growth", "Series", v=>fmtDec(v,2)+"x")}${barChart("P&L by Options Cycle", pnlRows, "Date", "pnl")}</div>
    ${sectionHead("Benchmark Metrics")}
    ${dataTable("benchmark-metrics", benchmarkMetricRows(), [
      {key:"Series",label:"Series"},
      {key:"CAGR",label:"CAGR",format:fmtPct,num:true},
      {key:"Volatility",label:"Volatility",format:fmtPct,num:true},
      {key:"Sharpe",label:"Sharpe",format:v=>fmtDec(v,2),num:true},
      {key:"Sortino",label:"Sortino",format:v=>fmtDec(v,2),num:true},
      {key:"Max Drawdown",label:"Max DD",format:fmtPct,num:true},
      {key:"Return 3M",label:"3M",format:fmtPct,num:true},
      {key:"Return 6M",label:"6M",format:fmtPct,num:true},
      {key:"Return YTD",label:"YTD",format:fmtPct,num:true},
      {key:"Return 1Y",label:"1Y",format:fmtPct,num:true},
      {key:"Return SI",label:"Since inception",format:fmtPct,num:true}
    ], {title:"Key performance metrics versus benchmarks", wide:true})}
    ${sectionHead("Expectancy Analysis", "", `<select id="expectancyYear">${expectancyYears.map(y=>`<option value="${safe(y)}" ${y===appState.expectancyYear?"selected":""}>${safe(y==="all"?"All years":y)}</option>`).join("")}</select>`)}
    <div class="grid two-even">
      ${dataTable("expectancy", data.tables.expectancy || [], [
        {key:"Category",label:"Category"},
        {key:"Count",label:"Count",num:true},
        {key:"Win rate",label:"Win rate",format:fmtPct,num:true},
        {key:"Avg win",label:"Avg win",format:fmtMoney,num:true,className:cls},
        {key:"Avg loss",label:"Avg loss",format:fmtMoney,num:true,className:cls},
        {key:"Expectancy",label:"Expectancy",format:fmtMoney,num:true,className:cls},
        {key:"Total P&L",label:"Total P&L",format:fmtMoney,num:true,className:cls}
      ], {title:"Overall expectancy", small:true})}
      ${expectancyTrendChart()}
    </div>
    <div style="height:12px"></div>
    ${dataTable("expectancy-by-year", expectancyRows, [
      {key:"Year",label:"Year",num:true},
      {key:"Category",label:"Category"},
      {key:"Count",label:"Count",num:true},
      {key:"Win rate",label:"Win rate",format:fmtPct,num:true},
      {key:"Avg win",label:"Avg win",format:fmtMoney,num:true,className:cls},
      {key:"Avg loss",label:"Avg loss",format:fmtMoney,num:true,className:cls},
      {key:"Expectancy",label:"Expectancy",format:fmtMoney,num:true,className:cls},
      {key:"Total P&L",label:"Total P&L",format:fmtMoney,num:true,className:cls}
    ], {title:"Expectancy by year", wide:true})}
  `;
}
function renderDiagnostics(){
  const iss=data.issues || {}, sum=iss.summary || {}, aud=iss.audit_summary || {}, fresh=data.dashboard.data_freshness || {}, coverage=fresh.price_coverage || {};
  const priced=coverage.priced_count ?? coverage.stocks_fetched ?? coverage.fetched ?? 0;
  const required=coverage.required_count ?? coverage.stocks_requested ?? coverage.requested ?? 0;
  $("diagnostics").innerHTML = `
    ${sectionHead("Data Health", "Actionable issues are separated from expected IBKR audit notes.")}
    <div class="grid metrics">
      ${card("Actionable issues", fmtNum(sum.total_count || 0), "Warnings/errors requiring attention", (sum.total_count || 0) ? "neg" : "pos")}
      ${card("Audit notes", fmtNum(aud.total_count || 0), "Expected classification notes")}
      ${card("Current prices", `${safe(priced)}/${safe(required)}`, "Required pricing coverage", (required && priced < required) ? "neg" : "pos")}
      ${card("Source rows", fmtNum(data.source.row_count), data.source.kind || "source")}
      ${card("Prices updated", fmtLocalDateTime(fresh.prices_updated_at), "Local browser time")}
      ${card("Payload generated", fmtLocalDateTime(data.generated_at), "Local browser time")}
      ${card("App revision", String(data.app?.revision || "local").replace(/^options-roi-web-/,""), "Cloud Run revision")}
    </div>
    ${sectionHead("Actionable Issues")}
    ${dataTable("issues", iss.issues || [], [
      {key:"severity",label:"Severity"},
      {key:"category",label:"Category"},
      {key:"message",label:"Message"}
    ], {title:"Warnings and errors", wide:true})}
    ${sectionHead("Source and Coverage")}
    <div class="grid two-even">
      ${dataTable("sheet-counts", data.source.sheet_counts || [], [
        {key:"source_sheet",label:"Source",value:r=>r.source_sheet || r.sheet || r.name},
        {key:"rows",label:"Rows",num:true,value:r=>r.rows}
      ], {title:"Loaded rows by source", small:true})}
      ${dataTable("stock-prices", data.tables.stock_prices || [], [
        {key:"ticker",label:"Ticker"},
        {key:"price",label:"Price",format:v=>fmtMoney(v,2),num:true}
      ], {title:"Stock prices used", small:true, maxHeight:360})}
    </div>
    ${sectionHead("Unrealized and Cashflow Detail")}
    <div class="grid two-even">
      ${dataTable("unrealized-by-ticker", data.tables.unrealized_by_ticker || [], [
        {key:"ticker",label:"Ticker"},
        {key:"unrealized_pnl",label:"Unrealized P&L",format:fmtMoney,num:true,className:cls}
      ], {title:"Unrealized by ticker", small:true, maxHeight:360})}
      ${dataTable("dividends", data.tables.dividends || [], [
        {key:"ex_date",label:"Ex/pay date",format:fmtDate},
        {key:"ticker",label:"Ticker"},
        {key:"shares",label:"Shares",num:true},
        {key:"per_share",label:"Per share",format:v=>fmtMoney(v,2),num:true},
        {key:"cash",label:"Cash",format:fmtMoney,num:true,className:cls}
      ], {title:"Dividends", small:true, maxHeight:360})}
    </div>
    ${sectionHead("Reconciliation Notes")}
    <div class="note-list">${(data.reconciliation_notes || []).map(n=>`<div class="note"><strong>${safe(n.case)} - ${safe(n.status)}</strong><span class="muted">${safe(n.detail)}</span></div>`).join("")}</div>
    ${sectionHead("Audit Notes")}
    <details><summary>Show ${fmtNum(aud.total_count || 0)} audit notes</summary><div style="height:10px"></div>${dataTable("audit-notes", (iss.audit_notes || []).slice(0,600), [
      {key:"category",label:"Category"},
      {key:"severity",label:"Severity"},
      {key:"message",label:"Message"}
    ], {title:"Wheel audit notes", subtitle:"Expected non-wheel exclusions and classification notes.", wide:true})}</details>
    ${sectionHead("Capital Tail")}
    ${dataTable("capital-tail", data.tables.capital_daily_tail || [], [
      {key:"date",label:"Date",format:fmtDate},
      {key:"total",label:"Total capital",format:fmtMoney,num:true},
      {key:"shares_invested",label:"Shares invested",format:fmtMoney,num:true},
      {key:"puts_reserve",label:"Put reserve",format:fmtMoney,num:true}
    ], {title:"Capital daily tail", wide:true})}
  `;
}
function renderSettings(){
  const targetPct = fmtPctNumber(appState.targetReturn);
  const floorPct = fmtPctNumber(appState.targetFloor);
  $("settings").innerHTML = `
    ${sectionHead("Settings")}
    <div class="grid two-even">
      <div class="panel">
        <h3>Monthly Target Band</h3>
        <p class="muted">Negative months are red, positive months below the lower limit are yellow, months inside the target band are green, and months above the upper limit are blue. The upper limit is also used as the dashboard target.</p>
        <form id="targetSettingsForm" class="toolbar" style="align-items:end">
          <label>
            <span class="control-label">Lower band %</span>
            <input name="target_floor_pct" type="number" min="0" max="100" step="0.05" value="${safe(floorPct)}" style="min-width:150px">
          </label>
          <label>
            <span class="control-label">Upper band / target %</span>
            <input name="target_return_pct" type="number" min="0" max="100" step="0.05" value="${safe(targetPct)}" style="min-width:160px">
          </label>
          <button class="primary" type="submit">Apply</button>
        </form>
        <div class="footnote">Current target band: ${safe(fmtPct(targetFloor()))}-${safe(fmtPct(targetReturn()))}. Negative is red; 0-${safe(fmtPct(targetFloor()))} is yellow; above ${safe(fmtPct(targetReturn()))} is blue.</div>
      </div>
      <div class="panel">
        <h3>Display Basis</h3>
        <p class="muted">The realized/unrealized switch is used only on Dashboard where the top-level snapshot changes meaning.</p>
        <div class="segmented" data-basis-control><button type="button" data-value="1" class="${appState.includeUnrealized ? "active" : ""}">With unrealized</button><button type="button" data-value="0" class="${!appState.includeUnrealized ? "active" : ""}">Realized only</button></div>
      </div>
    </div>
  `;
}
function renderMethodology(){
  $("methodology").innerHTML = `
    ${sectionHead("Methodology", "Same backend accounting as iOS, with web-only diagnostic breadth.")}
    <div class="grid two-even">
      <div class="panel"><h3>Source</h3><p>Production web and iOS read imported IBKR Flex data from Firestore. Streamlit remains the Google Sheets backup/control dashboard.</p><h3>Wheel scope</h3><p>Wheel P&L starts with assigned puts. Covered calls are included when backed by assignment-derived shares or valid covered-call roll replacements. Expected non-wheel exclusions are audit notes, not actionable issues.</p><h3>Monthly projections</h3><p>The dashboard current-month target uses realized month P&L plus current-month open option net exposure. Premium-only and roll-adjusted premium fields remain available in the monthly table for reconciliation.</p></div>
      <div class="panel"><h3>Unrealized snapshot</h3><p>Current unrealized values are monitoring snapshots, not complete option mark-to-market accounting. Missing required prices suppress affected unrealized fields.</p><h3>Benchmarks</h3><p>Return metrics compare monthly strategy returns with aligned benchmark monthly series when coverage is complete.</p><h3>Refresh</h3><p>Refresh checks whether the IBKR source changed. If not, it updates current prices only and keeps the existing accounting pipeline.</p></div>
    </div>
  `;
}
function renderPreservingInput(event, selector, updateState){
  const value = event.target.value;
  const selectionStart = event.target.selectionStart;
  const selectionEnd = event.target.selectionEnd;
  updateState(value);
  render();
  window.requestAnimationFrame(() => {
    const scope = document.querySelector(".section.active") || document;
    const next = scope.querySelector(selector) || document.querySelector(selector);
    if (!next) return;
    next.focus({preventScroll:true});
    if (typeof next.setSelectionRange === "function" && selectionStart !== null && selectionEnd !== null) {
      const start = Math.min(selectionStart, next.value.length);
      const end = Math.min(selectionEnd, next.value.length);
      next.setSelectionRange(start, end);
    }
  });
}
function bindControls(){
  const reloadApp = $("reloadApp");
  if (reloadApp && !reloadApp.dataset.bound) {
    reloadApp.dataset.bound = "1";
    reloadApp.addEventListener("click", () => {
      reloadApp.disabled = true;
      reloadApp.textContent = "Reloading...";
      setUpdating(true, "Reloading app...");
      const url = new URL(window.location.href);
      url.searchParams.set("v", Date.now().toString());
      window.location.replace(url.toString());
    });
  }
  const refreshForm = $("refreshForm");
  if (refreshForm) {
    refreshForm.action = (
      `/refresh?include_unrealized=${appState.includeUnrealized ? "1" : "0"}`
      + `&target_return=${encodeURIComponent(Number(appState.targetReturn || 0).toFixed(6))}`
      + `&section=${encodeURIComponent(appState.active)}`
    );
    if (!refreshForm.dataset.bound) {
      refreshForm.dataset.bound = "1";
      refreshForm.addEventListener("submit", () => {
        const button = refreshForm.querySelector("button");
        if (button) {
          button.disabled = true;
          button.textContent = "Refreshing...";
        }
        setUpdating(true, "Refreshing data...");
      });
    }
  }
  document.querySelectorAll("[data-basis-control] button").forEach(btn => btn.addEventListener("click", () => {
    const next = btn.dataset.value !== "0";
    if (appState.includeUnrealized === next) return;
    appState.includeUnrealized = next;
    btn.closest("[data-basis-control]")?.querySelectorAll("button").forEach(button => {
      button.disabled = true;
      button.classList.toggle("active", (button.dataset.value !== "0") === next);
    });
    updateUrlState();
    scheduleRender();
  }));
  document.querySelectorAll(".rangeControl button").forEach(btn => btn.addEventListener("click", () => { appState.range = btn.dataset.value; render(); }));
  document.querySelectorAll('[data-open-control="risk"] button').forEach(btn => btn.addEventListener("click", () => { appState.openRisk = btn.dataset.value; render(); }));
  document.querySelectorAll('[data-open-control="type"] button').forEach(btn => btn.addEventListener("click", () => { appState.openType = btn.dataset.value; render(); }));
  document.querySelectorAll("[data-open-search]").forEach(input => input.addEventListener("input", (e) => {
    renderPreservingInput(e, "[data-open-search]", (value) => { appState.openSearch = value; });
  }));
  const tickerSearch = $("tickerSearch"); if (tickerSearch) tickerSearch.addEventListener("input", (e) => {
    renderPreservingInput(e, "#tickerSearch", (value) => { appState.tickerSearch = value; });
  });
  const tickerYear = $("tickerYear"); if (tickerYear) tickerYear.addEventListener("change", (e) => { appState.tickerYear = e.target.value; render(); });
  const expectancyYear = $("expectancyYear"); if (expectancyYear) expectancyYear.addEventListener("change", (e) => { appState.expectancyYear = e.target.value; render(); });
  const targetSettingsForm = $("targetSettingsForm");
  if (targetSettingsForm) targetSettingsForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const form = new FormData(targetSettingsForm);
    const nextTarget = Number(form.get("target_return_pct")) / 100;
    const nextFloor = Number(form.get("target_floor_pct")) / 100;
    if (Number.isFinite(nextTarget)) appState.targetReturn = Math.max(nextTarget, 0);
    if (Number.isFinite(nextFloor)) appState.targetFloor = Math.max(nextFloor, 0);
    if (appState.targetReturn < appState.targetFloor) appState.targetReturn = appState.targetFloor;
    updateUrlState();
    render();
  });
}
const sectionRenderers = {
  dashboard: renderDashboard,
  monthly: renderMonthly,
  tickers: renderTickers,
  performance: renderPerformance,
  settings: renderSettings,
  diagnostics: renderDiagnostics,
  methodology: renderMethodology
};
function setUpdating(isUpdating, message="Updating view..."){
  const indicator = $("viewUpdating");
  if (indicator) indicator.textContent = message;
  document.body.classList.toggle("is-rendering", Boolean(isUpdating));
  document.body.classList.toggle("is-loading", Boolean(isUpdating) && message !== "Updating view...");
}
function scheduleRender(){
  window.clearTimeout(appState.renderTimer);
  setUpdating(true);
  appState.renderTimer = window.setTimeout(() => {
    render();
    setUpdating(false);
  }, 20);
}
function render(){
  const renderStarted = performance.now();
  pendingCharts = [];
  try {
    renderHeader();
    [...$("nav").children].forEach(b => b.classList.toggle("active", b.dataset.section === appState.active));
    if (!dashboardLoaded) {
      const target = $(appState.active) || $("dashboard");
      if (target) target.innerHTML = loadingPanel();
      document.querySelectorAll(".section").forEach(s => s.classList.toggle("active", s.id === appState.active));
      bindControls();
      initCharts();
      return;
    }
    const renderer = sectionRenderers[appState.active];
    if (renderer) renderSection(appState.active, renderer);
    document.querySelectorAll(".section").forEach(s => s.classList.toggle("active", s.id === appState.active));
    bindControls();
    initCharts();
  } catch (err) {
    const target = $(appState.active);
    if (target) {
      target.innerHTML = `<div class="panel"><h2>Section failed to render</h2><p class="error">${safe(err && (err.stack || err.message) || err)}</p></div>`;
    }
    console.error(err);
  } finally {
    document.documentElement.dataset.lastRenderMs = String(Math.round(performance.now() - renderStarted));
  }
}
function renderSection(id, fn){
  try {
    fn();
  } catch (err) {
    const target = $(id);
    if (target) {
      target.innerHTML = `<div class="panel"><h2>${safe(id)} failed to render</h2><p class="error">${safe(err && (err.stack || err.message) || err)}</p></div>`;
    }
    console.error(err);
  }
}
function initNav(){
  $("nav").innerHTML = sections.map(([id,label]) => `<button type="button" data-section="${id}" class="${id === appState.active ? "active" : ""}">${safe(label)}</button>`).join("");
  [...$("nav").children].forEach(btn => btn.addEventListener("click", () => {
    appState.active = btn.dataset.section;
    updateUrlState();
    render();
    window.scrollTo({top:0,behavior:"instant"});
  }));
}
const initialSection = new URLSearchParams(window.location.search).get("section");
if (initialSection === "positions") {
  appState.active = "dashboard";
} else if (initialSection && sections.some(([id]) => id === initialSection)) {
  appState.active = initialSection;
}
initNav();
render();
loadDashboardData();
</script>
</body>
</html>""".replace(
    "__BASE_CSS__", BASE_CSS
)
