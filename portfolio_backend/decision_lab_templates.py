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
:root{--bg2:#080c0f;--panel3:#10171a;--panel4:#142024;--line2:#26383b;--blue:#6da8ff;--amber:#f6c25b;--red:#ff6f78;--green:#7ee092;--teal:#45d2c5;--purple:#b59bff}
body{background:#080c0f;color:var(--text)}
.shell{max-width:1440px;margin:0 auto;padding:24px 18px 80px}.top{display:flex;justify-content:space-between;gap:18px;align-items:flex-start;margin-bottom:20px}
h1{font-size:38px;line-height:1.05;margin:0 0 8px}h2{font-size:24px;margin:28px 0 10px}h3{font-size:17px;margin:0 0 8px}.sub{color:var(--muted)}
.nav{position:sticky;top:0;z-index:20;background:rgba(8,12,15,.94);backdrop-filter:blur(12px);border-bottom:1px solid var(--line2)}.nav-inner{max-width:1440px;margin:0 auto;padding:10px 18px;display:flex;gap:8px;align-items:center;overflow:auto}.nav a{color:var(--muted);text-decoration:none;font-weight:800;padding:8px 10px;border-radius:8px}.nav a:hover{background:#121c20;color:var(--text)}.nav .back{background:var(--accent);color:#06201b}
.grid{display:grid;gap:12px}.metrics{grid-template-columns:repeat(5,minmax(160px,1fr))}.two{grid-template-columns:1.12fr .88fr}.two-even{grid-template-columns:repeat(2,minmax(0,1fr))}.three{grid-template-columns:repeat(3,minmax(0,1fr))}
.card,.panel,.table-card{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;box-shadow:0 10px 28px rgba(0,0,0,.18)}.card,.panel{padding:15px}.metric-label{color:var(--muted);font-size:12px;font-weight:850;text-transform:uppercase;letter-spacing:.05em}.metric-value{font-size:28px;font-weight:950;line-height:1.05;margin-top:6px}.metric-note{color:var(--muted);font-size:12px;margin-top:6px}.pos{color:var(--green)}.neg{color:var(--red)}.warn{color:var(--amber)}.blue{color:var(--blue)}.muted{color:var(--muted)}.mono{font-variant-numeric:tabular-nums}
.section-head{display:flex;justify-content:space-between;align-items:end;gap:14px;margin:30px 0 10px}.section-head h2{margin:0}.section-note{color:var(--muted);font-size:13px;margin-top:4px}.pill{display:inline-flex;align-items:center;border-radius:999px;padding:4px 8px;font-size:12px;font-weight:850;background:#1c2a2d;color:var(--muted);white-space:nowrap}.pill.high{background:#3a171d;color:#ffc4c9}.pill.medium{background:#352714;color:#ffe1a0}.pill.low{background:#17243c;color:#cadcff}.pill.sim{background:#143234;color:#bff7f0}.pill.good{background:#143420;color:#bff3c7}
.table-card{overflow:hidden}.table-title{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;padding:12px 14px;border-bottom:1px solid var(--line2);background:#11191c}.table-title strong{font-size:15px}.table-title span{display:block;color:var(--muted);font-size:12px;margin-top:2px}.table-scroll{overflow:auto;max-height:520px}table{width:100%;border-collapse:separate;border-spacing:0;font-size:13px;min-width:760px}th,td{text-align:left;padding:8px 10px;border-bottom:1px solid #213034;vertical-align:middle;white-space:nowrap}th{position:sticky;top:0;background:#0f171a;color:#c4d0cc;font-size:11.5px;text-transform:uppercase;letter-spacing:.04em}td.num,th.num{text-align:right}tbody tr:nth-child(even) td{background:rgba(255,255,255,.012)}.wide table{min-width:1100px}.compact table{min-width:100%}
.chart-card{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;padding:14px;min-width:0;overflow:hidden}.chart-title{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:8px}.chart-wrap{height:300px;position:relative}.chart-canvas{width:100%!important;height:100%!important}
.note{border-left:3px solid var(--accent);background:#10181b;border-radius:8px;padding:10px 12px;color:var(--muted)}.waterfall{display:grid;gap:8px}.water-row{display:grid;grid-template-columns:190px minmax(120px,1fr) 115px;gap:10px;align-items:center}.bar-track{height:18px;border-radius:999px;background:#1b282b;overflow:hidden}.bar{height:100%;border-radius:999px}.bar.pos{background:var(--green)}.bar.neg{background:var(--red)}.bar.warn{background:var(--amber)}
.candidate-card{padding:14px;border:1px solid var(--line2);border-radius:8px;background:#11191c}.candidate-head{display:flex;justify-content:space-between;gap:12px;align-items:start}.candidate-action{font-size:18px;font-weight:950}.candidate-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin:12px 0}.candidate-mini{padding:8px;border-radius:7px;background:#0d1417}.candidate-mini b{display:block;font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}.candidate-alt{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}.alt{border:1px solid var(--line2);background:#0d1417;border-radius:7px;padding:7px 9px;color:var(--muted)}
.empty{padding:18px;color:var(--muted)}
@media(max-width:1050px){.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.two,.two-even,.three{grid-template-columns:1fr}.top{display:block}.chart-wrap{height:280px}.candidate-grid{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:720px){.shell{padding:18px 12px 64px}h1{font-size:30px}.metrics{grid-template-columns:1fr}.nav-inner{padding:8px 12px}.water-row{grid-template-columns:1fr}.chart-wrap{height:260px}.candidate-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
</style>
</head>
<body>
<div class="nav"><div class="nav-inner">
  <a class="back" href="/">Current dashboard</a>
  <a href="#actions">Actions</a><a href="#cycle">Active cycle</a><a href="#candidates">Candidates</a><a href="#strikes">Strike quality</a><a href="#coverage">Coverage</a>
</div></div>
<main class="shell">
  <div class="top">
    <div>
      <h1>Decision Dashboard Lab</h1>
      <div class="sub">Prototype decision layer from live IBKR dashboard data. Existing dashboard and mobile app are unchanged.</div>
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
const fmtMoney = (v, d=0) => { const digits = Number.isInteger(d) ? d : 0; return num(v) === null ? "n/a" : new Intl.NumberFormat("en-US",{style:"currency",currency:"USD",minimumFractionDigits:digits,maximumFractionDigits:digits}).format(Number(v)); };
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
function renderCharts(){ for (const [id, config] of pending.splice(0)) { const el = $(id); if (el) new Chart(el, config); } }
function actionRows(data){
  return table("Ticker-level action queue", data.ticker_situations || [], [
    {key:"priority",label:"Priority",format:v=>`<span class="pill ${safe(v)}">${safe(v)}</span>`},
    {key:"ticker",label:"Ticker"},
    {key:"category",label:"Category"},
    {key:"objective",label:"Objective"},
    {key:"impact",label:"Impact",format:fmtMoney,num:true,className:cls},
    {key:"expiry",label:"Expiry",format:fmtDate},
    {key:"dte",label:"DTE",num:true},
    {key:"recommendation",label:"Recommendation"},
    {key:"supporting_signals",label:"Signals",format:v=>Array.isArray(v)?safe(v.join(" · ")):safe(v)}
  ], {wide:true, note:"One consolidated row per ticker-level situation; duplicate raw source rows are intentionally removed."});
}
function cycleBlock(data){
  const c = data.active_cycle || {};
  const parts = [
    ["Open option net", c.open_option_net],
    ["Projected cycle P&L", c.projected_pnl],
    ["Target P&L", c.target_pnl],
    ["Remaining", c.remaining_to_target],
  ];
  const max = Math.max(...parts.map(p => Math.abs(num(p[1]) || 0)), 1);
  return `<div class="grid two"><div class="panel"><h3>${safe(c.cycle_label || "Active cycle")}</h3><div class="sub">Expiries ${safe((c.expiry_dates || []).join(", ") || "n/a")} · DTE ${safe(c.min_dte)}-${safe(c.max_dte)} · ${fmtNum(c.open_contract_count)} contracts</div><div class="waterfall" style="margin-top:12px">${parts.map(([label,value])=>`<div class="water-row"><strong>${safe(label)}</strong><div class="bar-track"><div class="bar ${(num(value)||0)<0?"neg":label==="Remaining"?"warn":"pos"}" style="width:${Math.max(4,Math.abs(num(value)||0)/max*100)}%"></div></div><div class="mono ${cls(value)}">${fmtMoney(value)}</div></div>`).join("")}</div></div><div class="grid metrics" style="grid-template-columns:repeat(2,minmax(0,1fr))">${metric("Projected RoAC",fmtPct(c.projected_return_roac),`Target ${fmtPct(c.target_return)}`,cls((num(c.projected_return_roac)||0)-(num(c.target_return)||0)))}${metric("Cycle put exposure",fmtMoney(c.cycle_put_exposure),`${fmtMoney(c.cycle_itm_put_exposure)} ITM`,c.cycle_itm_put_exposure?"neg":"")}${metric("Portfolio put exposure",fmtMoney(c.portfolio_put_exposure),`${fmtMoney(c.portfolio_itm_put_exposure)} ITM`,c.portfolio_itm_put_exposure?"neg":"")}${metric("Near-strike exposure",fmtMoney(c.near_strike_put_exposure),"Active cycle puts near strike",c.near_strike_put_exposure?"warn":"")}</div></div>`;
}
function candidateCard(row){
  const r = row.recommended || {};
  const alts = row.alternatives || [];
  return `<div class="candidate-card"><div class="candidate-head"><div><div class="candidate-action">${safe(row.ticker)} · ${safe(r.action)}</div><div class="sub">${safe(row.category)} · ${safe(row.objective)}</div></div><span class="pill sim">simulated</span></div><div class="candidate-grid">${mini("Strike", r.strike ? fmtMoney(r.strike,2) : "n/a")}${mini("Expiry", fmtDate(r.expiry) || "n/a")}${mini("DTE", fmtNum(r.dte))}${mini("Premium", fmtMoney(r.premium))}${mini("Delta", num(r.delta)===null?"n/a":Number(r.delta).toFixed(2))}${mini("Score", fmtNum(r.score))}</div><div class="sub">${safe(r.explanation)}</div><div class="candidate-alt">${alts.map(a=>`<div class="alt"><b>${safe(a.action)}</b> ${a.strike ? fmtMoney(a.strike,2) : "n/a"} · ${fmtDate(a.expiry) || "n/a"} · score ${fmtNum(a.score)}</div>`).join("")}</div><div class="metric-note">${safe(row.disclaimer)}</div></div>`;
}
function mini(label, value){ return `<div class="candidate-mini"><b>${safe(label)}</b><span class="mono">${safe(value)}</span></div>`; }
function candidates(data){
  const rows = data.recommendation_candidates || [];
  if (!rows.length) return `<div class="panel empty">No recommendation candidates.</div>`;
  return `<div class="grid">${rows.map(candidateCard).join("")}</div>`;
}
function strikeQuality(data){
  const s = data.strike_quality || {};
  const putRows = (s.put_entry_quality || {}).bucket_summary || [];
  const callRows = (s.call_exit_quality || {}).bucket_summary || [];
  return `<div class="grid two-even">
    ${chartCard("Put Entry Quality", "estimated lifecycle P&L by entry-risk bucket", {type:"bar",data:{labels:putRows.map(r=>r.bucket),datasets:[{label:"Estimated lifecycle P&L",data:putRows.map(r=>r.lifecycle_pnl_estimated||0),backgroundColor:putRows.map(r=>(r.lifecycle_pnl_estimated||0)>=0?"#7ee092":"#ff6f78")}]},options:chartCommon(v=>fmtMoney(v))})}
    ${chartCard("Covered Call / Exit Quality", "estimated exit/cap result by role", {type:"bar",data:{labels:callRows.map(r=>r.bucket),datasets:[{label:"Estimated exit P&L",data:callRows.map(r=>r.exit_pnl_estimated||0),backgroundColor:callRows.map(r=>(r.exit_pnl_estimated||0)>=0?"#45d2c5":"#ff6f78")}]},options:chartCommon(v=>fmtMoney(v))})}
  </div><div style="height:12px"></div><div class="grid two-even">
    ${table("Put risk bucket lifecycle", putRows, [
      {key:"bucket",label:"Risk bucket"},
      {key:"count",label:"Trades",num:true},
      {key:"avg_assignment_risk_proxy",label:"Avg risk",format:fmtPct,num:true},
      {key:"opening_premium",label:"Premium",format:fmtMoney,num:true,className:cls},
      {key:"stock_pnl_estimated",label:"Stock P&L est.",format:fmtMoney,num:true,className:cls},
      {key:"unrealized_drag_estimated",label:"Unrlzd drag est.",format:fmtMoney,num:true,className:cls},
      {key:"lifecycle_pnl_estimated",label:"Lifecycle est.",format:fmtMoney,num:true,className:cls},
      {key:"pnl_per_capital_estimated",label:"P&L/capital est.",format:fmtPct,num:true}
    ], {wide:true, note:"Estimated; not production attribution."})}
    ${table("Call recovery / exit quality", callRows, [
      {key:"bucket",label:"Role"},
      {key:"count",label:"Trades",num:true},
      {key:"avg_assignment_risk_proxy",label:"Avg risk",format:fmtPct,num:true},
      {key:"opening_premium",label:"Premium",format:fmtMoney,num:true,className:cls},
      {key:"capped_upside_estimated",label:"Cap impact est.",format:fmtMoney,num:true,className:cls},
      {key:"exit_pnl_estimated",label:"Exit P&L est.",format:fmtMoney,num:true,className:cls},
      {key:"pnl_per_capital_estimated",label:"P&L/capital est.",format:fmtPct,num:true},
      {key:"roll_usefulness",label:"Roll usefulness"}
    ], {wide:true, note:"Estimated; calls are not mixed with put-entry buckets."})}
  </div><div class="note" style="margin-top:12px"><strong>warning</strong> ${safe(s.note || "Estimated lifecycle attribution requires verification.")}</div>`;
}
function coverage(data){
  return `<div class="grid">${(data.coverage_notes||[]).map(n=>`<div class="note"><strong>${safe(n.severity)}</strong> ${safe(n.message)}</div>`).join("")}</div>`;
}
function render(data){
  $("generated").textContent = `Generated ${new Date(data.generated_at).toLocaleString()} · ${data.source?.label || "IBKR Flex"}`;
  const s = data.summary || {};
  $("content").innerHTML = `
    <div class="grid metrics">
      ${metric("YTD total P&L",fmtMoney(s.ytd_total_pnl),"Existing dashboard source",cls(s.ytd_total_pnl))}
      ${metric("Current unrealized",fmtMoney(s.current_unrealized_pnl),"Open stock/options snapshot",cls(s.current_unrealized_pnl))}
      ${metric("Action situations",fmtNum(s.action_item_count),"Ticker-level decisions",s.action_item_count?"warn":"pos")}
      ${metric("Active cycle",safe(s.active_cycle || "n/a"),"Nearest open expiry month","blue")}
      ${metric("Probability coverage",fmtPct(s.probability_coverage_rate),`${fmtNum(s.probability_match_count)}/${fmtNum(s.probability_trade_count)} trades`)}
    </div>
    ${head("actions","1. Action Needed","Only ticker-level situations that may require a decision.")}
    ${actionRows(data)}
    ${head("cycle","2. Active Cycle Target & Exposure","Focuses on the option cycle currently being managed, not a closed calendar month.")}
    ${cycleBlock(data)}
    ${head("candidates","3. Recommendation Candidates / Recovery Planner","Simulated candidates validate decision design before live option-chain integration.")}
    ${candidates(data)}
    ${head("strikes","4. Strike Selection Quality","Puts and calls are split because they answer different strategy questions.")}
    ${strikeQuality(data)}
    ${head("coverage","5. Data Coverage Notes")}
    ${coverage(data)}
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
