from __future__ import annotations

from portfolio_backend.web_dashboard_templates import BASE_CSS


DECISION_LAB_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Options ROI Decision Lab</title>
<style>
__BASE_CSS__
:root{--bg2:#080c0f;--panel3:#10171a;--panel4:#142024;--line2:#26383b;--blue:#6da8ff;--amber:#f6c25b;--red:#ff6f78;--green:#7ee092;--teal:#45d2c5}
body{background:#080c0f;color:var(--text)}
.shell{max-width:1440px;margin:0 auto;padding:24px 18px 80px}.top{display:flex;justify-content:space-between;gap:18px;align-items:flex-start;margin-bottom:20px}
h1{font-size:38px;line-height:1.05;margin:0 0 8px}h2{font-size:24px;margin:28px 0 10px}h3{font-size:17px;margin:0 0 8px}.sub{color:var(--muted)}
.nav{position:sticky;top:0;z-index:20;background:rgba(8,12,15,.94);backdrop-filter:blur(12px);border-bottom:1px solid var(--line2)}.nav-inner{max-width:1440px;margin:0 auto;padding:10px 18px;display:flex;gap:8px;align-items:center;overflow:auto}.nav a{color:var(--muted);text-decoration:none;font-weight:800;padding:8px 10px;border-radius:8px}.nav a:hover{background:#121c20;color:var(--text)}.nav .back{background:var(--accent);color:#06201b}
.grid{display:grid;gap:12px}.metrics{grid-template-columns:repeat(5,minmax(160px,1fr))}.two{grid-template-columns:1.12fr .88fr}.two-even{grid-template-columns:repeat(2,minmax(0,1fr))}.three{grid-template-columns:repeat(3,minmax(0,1fr))}
.card,.panel,.table-card{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;box-shadow:0 10px 28px rgba(0,0,0,.18)}.card,.panel{padding:15px}.metric-label{color:var(--muted);font-size:12px;font-weight:850;text-transform:uppercase;letter-spacing:.05em}.metric-value{font-size:28px;font-weight:950;line-height:1.05;margin-top:6px}.metric-note{color:var(--muted);font-size:12px;margin-top:6px}.pos{color:var(--green)}.neg{color:var(--red)}.warn{color:var(--amber)}.blue{color:var(--blue)}.muted{color:var(--muted)}.mono{font-variant-numeric:tabular-nums}
.section-head{display:flex;justify-content:space-between;align-items:end;gap:14px;margin:30px 0 10px}.section-head h2{margin:0}.section-note{color:var(--muted);font-size:13px;margin-top:4px}.pill{display:inline-flex;align-items:center;border-radius:999px;padding:4px 8px;font-size:12px;font-weight:850;background:#1c2a2d;color:var(--muted);white-space:nowrap}.pill.high,.pill.Avoid{background:#3a171d;color:#ffc4c9}.pill.medium,.pill.Review{background:#352714;color:#ffe1a0}.pill.low,.pill.Watch{background:#17243c;color:#cadcff}.pill.Preferred{background:#143420;color:#bff3c7}
.table-card{overflow:hidden}.table-title{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;padding:12px 14px;border-bottom:1px solid var(--line2);background:#11191c}.table-title strong{font-size:15px}.table-title span{display:block;color:var(--muted);font-size:12px;margin-top:2px}.table-scroll{overflow:auto;max-height:560px}table{width:100%;border-collapse:separate;border-spacing:0;font-size:13px;min-width:760px}th,td{text-align:left;padding:8px 10px;border-bottom:1px solid #213034;vertical-align:middle;white-space:nowrap}th{position:sticky;top:0;background:#0f171a;color:#c4d0cc;font-size:11.5px;text-transform:uppercase;letter-spacing:.04em}td.num,th.num{text-align:right}tbody tr:nth-child(even) td{background:rgba(255,255,255,.012)}.wide table{min-width:1100px}.compact table{min-width:100%}
.chart-card{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;padding:14px;min-width:0;overflow:hidden}.chart-title{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:8px}.chart-wrap{height:310px;position:relative}.chart-canvas{width:100%!important;height:100%!important}.note{border-left:3px solid var(--accent);background:#10181b;border-radius:8px;padding:10px 12px;color:var(--muted)}.waterfall{display:grid;gap:8px}.water-row{display:grid;grid-template-columns:170px minmax(120px,1fr) 110px;gap:10px;align-items:center}.bar-track{height:18px;border-radius:999px;background:#1b282b;overflow:hidden}.bar{height:100%;border-radius:999px}.bar.pos{background:var(--green)}.bar.neg{background:var(--red)}.bar.warn{background:var(--amber)}.empty{padding:18px;color:var(--muted)}
@media(max-width:1050px){.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.two,.two-even,.three{grid-template-columns:1fr}.top{display:block}.chart-wrap{height:280px}}
@media(max-width:720px){.shell{padding:18px 12px 64px}h1{font-size:30px}.metrics{grid-template-columns:1fr}.nav-inner{padding:8px 12px}.water-row{grid-template-columns:1fr}.chart-wrap{height:260px}}
</style>
</head>
<body>
<div class="nav"><div class="nav-inner">
  <a class="back" href="/">Current dashboard</a>
  <a href="#actions">Actions</a><a href="#monthly">Month</a><a href="#strikes">Strike quality</a><a href="#tickers">Tickers</a><a href="#positions">Positions</a><a href="#performance">Performance</a>
</div></div>
<main class="shell">
  <div class="top">
    <div>
      <h1>Decision Dashboard Lab</h1>
      <div class="sub">Prototype analytics from live IBKR dashboard data. Existing dashboard and mobile app are unchanged.</div>
    </div>
    <div class="sub mono" id="generated"></div>
  </div>
  <section id="content"><div class="panel"><h2>Loading real portfolio data...</h2><p class="sub">Building decision analytics from the current backend payload.</p></div></section>
</main>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.9/dist/chart.umd.min.js"></script>
<script>
const $ = (id) => document.getElementById(id);
const safe = (v) => String(v ?? "n/a").replace(/[&<>"']/g, (ch) => ({"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#39;"}[ch]));
const num = (v) => v === null || v === undefined || v === "" || Number.isNaN(Number(v)) ? null : Number(v);
const cls = (v) => num(v) === null ? "" : Number(v) < 0 ? "neg" : Number(v) > 0 ? "pos" : "";
const fmtMoney = (v, d=0) => {
  const digits = Number.isInteger(d) ? d : 0;
  return num(v) === null ? "n/a" : new Intl.NumberFormat("en-US",{style:"currency",currency:"USD",minimumFractionDigits:digits,maximumFractionDigits:digits}).format(Number(v));
};
const fmtPct = (v) => num(v) === null ? "n/a" : `${(Number(v)*100).toFixed(1)}%`;
const fmtNum = (v) => num(v) === null ? "n/a" : new Intl.NumberFormat("en-US",{maximumFractionDigits:0}).format(Number(v));
const fmtDate = (v) => !v ? "" : String(v).slice(0,10);
const metric = (label, value, note="", tone="") => `<div class="card"><div class="metric-label">${safe(label)}</div><div class="metric-value mono ${tone}">${value}</div>${note ? `<div class="metric-note">${safe(note)}</div>` : ""}</div>`;
const head = (id, title, note="") => `<div id="${safe(id)}" class="section-head"><div><h2>${safe(title)}</h2>${note ? `<div class="section-note">${safe(note)}</div>` : ""}</div></div>`;
function table(title, rows, cols, opts={}){
  if (!rows || !rows.length) return `<div class="table-card"><div class="table-title"><strong>${safe(title)}</strong><span>0 rows</span></div><div class="empty">No rows.</div></div>`;
  return `<div class="table-card ${opts.wide ? "wide" : ""} ${opts.compact ? "compact" : ""}"><div class="table-title"><div><strong>${safe(title)}</strong>${opts.note ? `<span>${safe(opts.note)}</span>` : ""}</div><span class="pill">${rows.length} rows</span></div><div class="table-scroll"><table><thead><tr>${cols.map(c=>`<th class="${c.num?"num":""}">${safe(c.label)}</th>`).join("")}</tr></thead><tbody>${rows.map(r=>`<tr>${cols.map(c=>{const raw = c.value ? c.value(r) : r[c.key]; const val = c.format ? c.format(raw,r) : safe(raw); const tone = c.className ? c.className(raw,r) : ""; return `<td class="${c.num?"num":""} ${tone}">${val}</td>`}).join("")}</tr>`).join("")}</tbody></table></div></div>`;
}
function chartCommon(formatter){
  return {responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:"#c0cbc7",font:{weight:"700"}},position:"bottom"},tooltip:{callbacks:{label:(ctx)=>`${ctx.dataset.label}: ${formatter(ctx.parsed.y)}`}}},scales:{x:{ticks:{color:"#c0cbc7",font:{weight:"700"},maxRotation:0},grid:{display:false}},y:{ticks:{color:"#c0cbc7",font:{weight:"700"},callback:(v)=>formatter(v)},grid:{color:"#26383c"}}}};
}
let chartSeq = 0; const pending = [];
function chartCard(title, subtitle, config){
  const id = `chart-${++chartSeq}`; pending.push([id, config]);
  return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong><span class="muted">${safe(subtitle || "")}</span></div><div class="chart-wrap"><canvas id="${id}" class="chart-canvas"></canvas></div></div>`;
}
function renderCharts(){
  for (const [id, config] of pending.splice(0)) {
    const el = $(id); if (el) new Chart(el, config);
  }
}
function actionQueue(data){
  return table("Action queue", data.action_queue || [], [
    {key:"priority",label:"Priority",format:v=>`<span class="pill ${safe(v)}">${safe(v)}</span>`},
    {key:"ticker",label:"Ticker"},
    {key:"reason",label:"Reason"},
    {key:"impact",label:"Impact",format:fmtMoney,num:true,className:cls},
    {key:"expiry",label:"Expiry",format:fmtDate},
    {key:"dte",label:"DTE",num:true},
    {key:"suggested_action",label:"Suggested action"},
    {key:"source",label:"Source"}
  ], {wide:true});
}
function monthlyBlock(data){
  const m = data.monthly_decision || {};
  const parts = [
    ["Realized P&L", m.realized_pnl],
    ["Open option net", m.open_option_net],
    ["Projected P&L", m.projected_pnl],
    ["Target P&L", m.target_pnl],
    ["Remaining", m.remaining_to_target]
  ];
  const max = Math.max(...parts.map(p => Math.abs(num(p[1]) || 0)), 1);
  return `<div class="grid two"><div class="panel"><h3>Current month target bridge</h3><div class="waterfall">${parts.map(([label,value])=>`<div class="water-row"><strong>${safe(label)}</strong><div class="bar-track"><div class="bar ${(num(value)||0)<0?"neg":label==="Remaining"?"warn":"pos"}" style="width:${Math.max(4,Math.abs(num(value)||0)/max*100)}%"></div></div><div class="mono ${cls(value)}">${fmtMoney(value)}</div></div>`).join("")}</div></div><div class="grid metrics" style="grid-template-columns:1fr">${metric("Projected RoAC",fmtPct(m.projected_return_roac),`Target ${fmtPct(m.target_return)}`,cls((num(m.projected_return_roac)||0)-(num(m.target_return)||0)))}${metric("Premium component",fmtMoney(m.premium_component),"Open current-month premium",cls(m.premium_component))}${metric("Intrinsic gap",fmtMoney(m.intrinsic_gap),"ITM option drag",cls(m.intrinsic_gap))}${metric("ITM put cash",fmtMoney(m.itm_put_cash_required),`${fmtNum(m.itm_put_contracts)} ITM puts`,"neg")}</div></div>`;
}
function strikeCharts(data){
  const s = data.strike_selection || {};
  const buckets = s.bucket_summary || [];
  const years = s.year_summary || [];
  return `<div class="grid two-even">
    ${chartCard("Risk bucket coverage", "historical sheet probability", {type:"bar",data:{labels:buckets.map(r=>r.bucket),datasets:[{label:"Trades",data:buckets.map(r=>r.count||0),backgroundColor:"#45d2c5"}]},options:chartCommon(v=>fmtNum(v))})}
    ${chartCard("Average assignment-risk proxy", "1 - Profit probability", {type:"line",data:{labels:years.map(r=>String(r.year)),datasets:[{label:"Risk proxy",data:years.map(r=>r.avg_assignment_risk_proxy),borderColor:"#f6c25b",backgroundColor:"#f6c25b",borderWidth:3,pointRadius:4,tension:.2}]},options:chartCommon(fmtPct)})}
  </div>
  <div style="height:12px"></div>
  ${table("Strike selection by risk bucket", buckets, [
    {key:"bucket",label:"Risk bucket"},
    {key:"count",label:"Trades",num:true},
    {key:"avg_profit_probability",label:"Avg profit prob.",format:fmtPct,num:true},
    {key:"avg_assignment_risk_proxy",label:"Avg risk proxy",format:fmtPct,num:true},
    {key:"total_opening_premium",label:"Opening premium",format:fmtMoney,num:true,className:cls},
    {key:"avg_premium_to_capital",label:"Premium/capital",format:fmtPct,num:true},
    {key:"top_tickers",label:"Top tickers"}
  ], {wide:true, note:s.note})}`;
}
function tickerScorecard(data){
  return table("Ticker quality scorecard", data.ticker_scorecard || [], [
    {key:"ticker",label:"Ticker"},
    {key:"score",label:"Score",num:true},
    {key:"status",label:"Status",format:v=>`<span class="pill ${safe(v)}">${safe(v)}</span>`},
    {key:"total_pnl",label:"Total P&L",format:fmtMoney,num:true,className:cls},
    {key:"realized_options_pnl",label:"Options P&L",format:fmtMoney,num:true,className:cls},
    {key:"unrealized_pnl",label:"Unrealized",format:fmtMoney,num:true,className:cls},
    {key:"open_options",label:"Open opts",num:true},
    {key:"itm_open_options",label:"ITM open",num:true},
    {key:"assigned_lots",label:"Assigned lots",num:true},
    {key:"capital_tied",label:"Capital tied",format:fmtMoney,num:true}
  ], {wide:true});
}
function positions(data){
  const p = data.open_positions || {};
  return `<div class="grid two-even">
    ${table("Assigned holdings", p.assigned_holdings || [], [
      {key:"ticker",label:"Ticker"},
      {key:"shares",label:"Shares",num:true},
      {key:"cost_per_share",label:"Avg cost",format:v=>fmtMoney(v,2),num:true},
      {key:"current_price",label:"Current",format:v=>fmtMoney(v,2),num:true},
      {key:"unrealized_pnl",label:"Unrealized",format:fmtMoney,num:true,className:cls},
      {key:"covered_shares",label:"Covered",num:true},
      {key:"uncovered_shares",label:"Uncovered",num:true},
      {key:"decision",label:"Decision"}
    ], {wide:true})}
    ${table("Open option shorts", p.open_shorts || [], [
      {key:"ticker",label:"Ticker"},
      {key:"option_type",label:"Type"},
      {key:"strike",label:"Strike",format:v=>fmtMoney(v,2),num:true},
      {key:"expiration",label:"Expiry",format:fmtDate},
      {key:"days_to_expiration",label:"DTE",num:true},
      {key:"current_price",label:"Current",format:v=>fmtMoney(v,2),num:true},
      {key:"display_premium_collected",label:"Premium",format:fmtMoney,num:true,className:cls},
      {key:"intrinsic_gap",label:"Intrinsic gap",format:fmtMoney,num:true,className:cls},
      {key:"projected_pnl",label:"Projected P&L",format:fmtMoney,num:true,className:cls},
      {key:"cash_required_if_assigned",label:"Cash if assigned",format:fmtMoney,num:true},
      {key:"decision",label:"Decision"}
    ], {wide:true})}
  </div>`;
}
function performance(data){
  const p = data.performance_insights || {};
  return `<div class="grid two-even">
    ${table("Largest negative months", p.largest_negative_months || [], [
      {key:"month",label:"Month",format:fmtDate},
      {key:"total_realized_pnl",label:"Total realized",format:fmtMoney,num:true,className:cls},
      {key:"options_pnl",label:"Options",format:fmtMoney,num:true,className:cls},
      {key:"stock_pnl",label:"Stock",format:fmtMoney,num:true,className:cls}
    ])}
    ${table("Worst ticker contributors", p.worst_tickers || [], [
      {key:"ticker",label:"Ticker"},
      {key:"total_pnl",label:"Total P&L",format:fmtMoney,num:true,className:cls},
      {key:"options_pnl",label:"Options",format:fmtMoney,num:true,className:cls},
      {key:"unrealized_pnl",label:"Unrealized",format:fmtMoney,num:true,className:cls}
    ])}
  </div>`;
}
function render(data){
  $("generated").textContent = `Generated ${new Date(data.generated_at).toLocaleString()} · ${data.source?.label || "IBKR Flex"}`;
  const s = data.summary || {};
  $("content").innerHTML = `
    <div class="grid metrics">
      ${metric("YTD total P&L",fmtMoney(s.ytd_total_pnl),"Realized + current unrealized",cls(s.ytd_total_pnl))}
      ${metric("Current unrealized",fmtMoney(s.current_unrealized_pnl),"Open stock/options snapshot",cls(s.current_unrealized_pnl))}
      ${metric("Action items",fmtNum((data.action_queue||[]).length),"Ranked by priority",(data.action_queue||[]).length?"warn":"pos")}
      ${metric("Open shorts",fmtNum(s.open_short_count),"Current option shorts")}
      ${metric("Probability coverage",fmtPct(s.probability_coverage_rate),`${fmtNum(s.probability_match_count)}/${fmtNum(s.probability_trade_count)} trades`)}
    </div>
    ${head("actions","1. Action Needed","Only rows that may require a decision.")}
    ${actionQueue(data)}
    ${head("monthly","2. Monthly Target","Current month with open option risk included.")}
    ${monthlyBlock(data)}
    ${head("strikes","3. Strike Selection Quality","Historical Google Sheet probability history, matched to IBKR opening trades.")}
    ${strikeCharts(data)}
    ${head("tickers","4. Ticker Quality Scorecard","Rank tickers by decision quality, not only realized premium.")}
    ${tickerScorecard(data)}
    ${head("positions","5. Open Positions","Operational decision tables for assigned holdings and open shorts.")}
    ${positions(data)}
    ${head("performance","6. Performance Diagnostics","Where the strategy has historically lost money.")}
    ${performance(data)}
    ${head("coverage","Coverage Notes")}
    <div class="grid">${(data.coverage_notes||[]).map(n=>`<div class="note"><strong>${safe(n.severity)}</strong> ${safe(n.message)}</div>`).join("")}</div>
  `;
  renderCharts();
}
fetch("/api/decision-lab" + window.location.search, {credentials:"same-origin"})
  .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
  .then(render)
  .catch(err => {$("content").innerHTML = `<div class="panel"><h2>Decision lab failed to load</h2><p class="error">${safe(err.message || err)}</p></div>`});
</script>
</body>
</html>""".replace("__BASE_CSS__", BASE_CSS)
