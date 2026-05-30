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
.note{border-left:3px solid var(--accent);background:#10181b;border-radius:8px;padding:10px 12px;color:var(--muted)}.waterfall{display:grid;gap:8px}.water-row{display:grid;grid-template-columns:190px minmax(120px,1fr) 115px;gap:10px;align-items:center}.bar-track{height:18px;border-radius:999px;background:#1b282b;overflow:hidden}.bar{height:100%;border-radius:999px}.bar.pos{background:var(--green)}.bar.neg{background:var(--red)}.bar.warn{background:var(--amber)}.bar.blue{background:var(--blue)}
.option-controls{display:flex;gap:12px;align-items:center;justify-content:space-between;margin:12px 0 18px;padding:12px 14px;background:var(--panel3);border:1px solid var(--line2);border-radius:8px;box-shadow:0 10px 28px rgba(0,0,0,.18)}.option-status{color:var(--muted);font-size:13px}.action-btn{border:0;border-radius:8px;background:var(--accent);color:#06201b;font-weight:950;padding:9px 12px;cursor:pointer;white-space:nowrap}.action-btn:disabled{opacity:.55;cursor:wait}
.candidate-card{padding:14px;border:1px solid var(--line2);border-radius:8px;background:#11191c}.candidate-head{display:flex;justify-content:space-between;gap:12px;align-items:start}.candidate-action{font-size:18px;font-weight:950}.state-title{margin-top:12px;color:var(--muted);font-weight:850;text-transform:uppercase;font-size:11px;letter-spacing:.04em}.state-grid{display:flex;flex-wrap:wrap;gap:7px;margin:6px 0 12px}.candidate-mini{padding:7px 8px;border-radius:7px;background:#0d1417;min-width:118px;max-width:260px;flex:0 1 auto}.candidate-mini.wide{min-width:220px;max-width:520px}.candidate-mini b{display:block;font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}.candidate-mini span{display:block;overflow:hidden;text-overflow:ellipsis}.candidate-alt{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}.alt{border:1px solid var(--line2);background:#0d1417;border-radius:7px;padding:7px 9px;color:var(--muted)}
.empty{padding:18px;color:var(--muted)}
@media(max-width:1050px){.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.two,.two-even,.three{grid-template-columns:1fr}.top{display:block}.chart-wrap{height:280px}}
@media(max-width:720px){.shell{padding:18px 12px 64px}h1{font-size:30px}.metrics{grid-template-columns:1fr}.nav-inner{padding:8px 12px}.water-row{grid-template-columns:1fr}.chart-wrap{height:260px}.option-controls{align-items:stretch;flex-direction:column}.action-btn{width:100%}}
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
      <div class="sub">Decision analytics from live IBKR dashboard data.</div>
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
const fmtDateTime = (v) => !v ? "not fetched" : new Date(v).toLocaleString([], {year:"numeric",month:"short",day:"2-digit",hour:"2-digit",minute:"2-digit"});
const bandTone = (value, floor, target) => num(value) === null ? "" : Number(value) < 0 ? "neg" : Number(value) < Number(floor || 0) ? "warn" : Number(value) <= Number(target || 0) ? "pos" : "blue";
const metric = (label, value, note="", tone="") => `<div class="card"><div class="metric-label">${safe(label)}</div><div class="metric-value mono ${tone}">${value}</div>${note ? `<div class="metric-note">${safe(note)}</div>` : ""}</div>`;
const head = (id, title, note="") => `<div id="${safe(id)}" class="section-head"><div><h2>${safe(title)}</h2>${note ? `<div class="section-note">${safe(note)}</div>` : ""}</div></div>`;
function table(title, rows, cols, opts={}){
  const titleBar = opts.noTitle ? "" : `<div class="table-title"><div><strong>${safe(title)}</strong>${opts.note ? `<span>${safe(opts.note)}</span>` : ""}</div><span class="pill">${(rows || []).length} rows</span></div>`;
  if (!rows || !rows.length) return `<div class="table-card">${titleBar}<div class="empty">No rows.</div></div>`;
  return `<div class="table-card ${opts.wide ? "wide" : ""} ${opts.compact ? "compact" : ""}">${titleBar}<div class="table-scroll"><table><thead><tr>${cols.map(c=>`<th class="${c.num?"num":""}">${safe(c.label)}</th>`).join("")}</tr></thead><tbody>${rows.map(r=>`<tr>${cols.map(c=>{const raw = c.value ? c.value(r) : r[c.key]; const val = c.format ? c.format(raw,r) : safe(raw); const tone = c.className ? c.className(raw,r) : ""; return `<td class="${c.num?"num":""} ${tone}">${val}</td>`}).join("")}</tr>`).join("")}</tbody></table></div></div>`;
}
function chartCommon(formatter){
  return {responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:"#c0cbc7",font:{weight:"700"}},position:"bottom"},tooltip:{callbacks:{label:(ctx)=>`${ctx.dataset.label}: ${formatter(ctx.parsed.y)}`}}},scales:{x:{ticks:{color:"#c0cbc7",font:{weight:"700"},maxRotation:0},grid:{display:false}},y:{ticks:{color:"#c0cbc7",font:{weight:"700"},callback:(v)=>formatter(v)},grid:{color:"#26383c"}}}};
}
let chartSeq = 0; const pending = [];
function chartCard(title, subtitle, config){
  const id = `chart-${++chartSeq}`; pending.push([id, config]);
  const note = subtitle ? `<span class="muted">${safe(subtitle)}</span>` : "";
  return `<div class="chart-card"><div class="chart-title"><strong>${safe(title)}</strong>${note}</div><div class="chart-wrap"><canvas id="${id}" class="chart-canvas"></canvas></div></div>`;
}
function renderCharts(){ for (const [id, config] of pending.splice(0)) { const el = $(id); if (el) new Chart(el, config); } }
function actionRows(data){
  return table("Ticker-level action queue", data.ticker_situations || [], [
    {key:"priority",label:"Priority",format:v=>`<span class="pill ${safe(v)}">${safe(v)}</span>`},
    {key:"ticker",label:"Ticker"},
    {key:"category",label:"Category"},
    {key:"objective",label:"Objective"},
    {key:"realized_pnl",label:"Realized P&L",format:fmtMoney,num:true,className:cls},
    {key:"unrealized_pnl",label:"Current unrealized",format:fmtMoney,num:true,className:cls},
    {key:"total_pnl",label:"Ticker total",format:fmtMoney,num:true,className:cls},
    {key:"signal_label",label:"Decision signal"},
    {key:"signal_value",label:"Signal value",format:fmtMoney,num:true,className:cls},
    {key:"expiry",label:"Expiry",format:fmtDate},
    {key:"dte",label:"DTE",num:true},
    {key:"recommendation",label:"Recommendation"},
    {key:"supporting_signals",label:"Signals",format:v=>Array.isArray(v)?safe(v.join(" · ")):safe(v)}
  ], {wide:true});
}
function cycleBlock(data){
  const c = data.active_cycle || {};
  const parts = [
    ["Realized cycle P&L", c.realized_cycle_pnl],
    ["Open premium collected", c.premium_component],
    ["ITM put unrealized loss", c.itm_put_unrealized_loss],
    ["Projected cycle P&L", c.projected_pnl],
    ["Target P&L", c.target_pnl],
    ["Remaining", c.remaining_to_target],
  ];
  const max = Math.max(...parts.map(p => Math.abs(num(p[1]) || 0)), 1);
  const targetTone = bandTone(c.projected_return_roac, c.target_floor, c.target_return);
  return `<div class="grid two"><div class="panel"><h3>${safe(c.cycle_label || "Active cycle")}</h3><div class="sub">Expiries ${safe((c.expiry_dates || []).join(", ") || "n/a")} · DTE ${safe(c.min_dte)}-${safe(c.max_dte)} · ${fmtNum(c.open_ticker_count ?? c.open_contract_count)} tickers</div><div class="waterfall" style="margin-top:12px">${parts.map(([label,value])=>{const tone = (num(value)||0)<0?"neg":label==="Remaining"?(num(value)>0?"warn":"blue"):label==="Projected cycle P&L"?targetTone:"pos"; return `<div class="water-row"><strong>${safe(label)}</strong><div class="bar-track"><div class="bar ${tone}" style="width:${Math.max(4,Math.abs(num(value)||0)/max*100)}%"></div></div><div class="mono ${tone}">${fmtMoney(value)}</div></div>`}).join("")}</div></div><div class="grid metrics" style="grid-template-columns:repeat(2,minmax(0,1fr))">${metric("Projected RoAC",fmtPct(c.projected_return_roac),`Target ${fmtPct(c.target_return)}`,targetTone)}${metric("Cycle put exposure",fmtMoney(c.cycle_put_exposure),`${fmtMoney(c.cycle_itm_put_exposure)} ITM`,c.cycle_itm_put_exposure?"neg":"")}${metric("Portfolio put exposure",fmtMoney(c.portfolio_put_exposure),`${fmtMoney(c.portfolio_itm_put_exposure)} ITM`,c.portfolio_itm_put_exposure?"neg":"")}${metric("Covered-call upside foregone",fmtMoney(c.covered_call_upside_foregone),"Active cycle signal",c.covered_call_upside_foregone?"warn":"")}</div></div>`;
}
function candidateCard(row){
  const rows = row.candidates || [row.recommended, ...(row.alternatives || [])].filter(Boolean);
  return `<div class="candidate-card"><div class="candidate-head"><div><div class="candidate-action">${safe(row.ticker)} · ${safe(row.category)}</div><div class="sub">${safe(row.objective)}</div></div></div>${currentState(row.current_state || {})}${candidateCompare(rows, row.candidate_status || {})}</div>`;
}
function mini(label, value, tone="", wide=false){ return `<div class="candidate-mini ${wide ? "wide" : ""}"><b>${safe(label)}</b><span class="mono ${tone}">${safe(value)}</span></div>`; }
function currentState(s){
  const open = (s.open_options || []).map(o=>`${safe(o.type)} ${fmtMoney(o.strike,2)} ${fmtDate(o.expiry)}${num(o.dte)!==null?` / ${safe(o.dte)} DTE`:""}`).join(" · ");
  return `<div class="state-title">Current state</div><div class="state-grid">
    ${mini("Current", fmtMoney(s.current_price,2))}
    ${mini("Assigned", `${fmtNum(s.assigned_shares || 0)} shares${s.assignment_date ? ` · ${fmtDate(s.assignment_date)}` : ""}`)}
    ${mini("Cost basis", fmtMoney(s.cost_basis,2))}
    ${mini("Realized", fmtMoney(s.realized_pnl), cls(s.realized_pnl))}
    ${mini("Unrealized", fmtMoney(s.current_unrealized), cls(s.current_unrealized))}
    ${mini("Ticker total", fmtMoney(s.ticker_total), cls(s.ticker_total))}
    ${mini("Open options", open || `${fmtNum(s.open_contracts || 0)} contracts`, "", true)}
  </div>`;
}
function candidateCompare(rows, status={}){
  if (!rows || !rows.length) {
    const msg = status.message || "No actionable provider contract found for this ticker/action.";
    return `<div class="panel empty" style="margin:8px 0 0">${safe(msg)}</div>`;
  }
  return table("", rows || [], [
    {key:"action",label:"Action"},
    {key:"contract_count",label:"Contracts",format:v=>num(v)===null?"n/a":fmtNum(v),num:true},
    {key:"strike",label:"Strike",format:v=>num(v)===null?"n/a":fmtMoney(v,2),num:true},
    {key:"expiry",label:"Expiry",format:fmtDate},
    {key:"dte",label:"DTE",num:true},
    {key:"premium",label:"Premium or new credit",format:fmtMoney,num:true,className:cls},
    {key:"roll_close_cost",label:"Est. close cost",format:fmtMoney,num:true,className:cls},
    {key:"roll_new_credit",label:"Est. new credit",format:fmtMoney,num:true,className:cls},
    {key:"roll_net_credit",label:"Net credit after close",format:fmtMoney,num:true,className:cls},
    {key:"incremental_exit_pnl",label:"Net improvement vs current",format:fmtMoney,num:true,className:cls},
    {key:"expected_value",label:"Expected value",format:fmtMoney,num:true,className:cls},
    {key:"expected_value_vs_current",label:"EV vs current",format:fmtMoney,num:true,className:cls},
    {key:"exercise_result",label:"If exercised",format:fmtMoney,num:true,className:cls},
    {key:"no_exercise_result",label:"If not exercised",format:fmtMoney,num:true,className:cls},
    {key:"upside_left",label:"Added upside room",format:fmtMoney,num:true,className:cls},
    {key:"upside_foregone",label:"Current price above strike",format:fmtMoney,num:true,className:cls},
    {key:"exercise_probability",label:"Exercise probability",format:v=>num(v)===null?"n/a":`${(Math.abs(Number(v))*100).toFixed(1)}%`,num:true},
    {key:"delta",label:"Delta/risk",format:v=>num(v)===null?"n/a":Math.abs(Number(v)).toFixed(2),num:true},
    {key:"liquidity",label:"Liquidity"},
    {key:"tradeability",label:"Tradeability"},
    {key:"price_source",label:"Price source"},
    {key:"provider",label:"Provider",format:v=>safe(v || "n/a")},
    {key:"score",label:"Score",num:true},
    {key:"score_reason",label:"Score reason"}
  ], {wide:true, compact:true, noTitle:true});
}
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
    ${chartCard("Put Entry Quality", "", {type:"bar",data:{labels:putRows.map(r=>r.bucket),datasets:[{label:"Lifecycle P&L",data:putRows.map(r=>num(r.lifecycle_pnl)===null?null:r.lifecycle_pnl),backgroundColor:putRows.map(r=>(num(r.lifecycle_pnl)||0)>=0?"#7ee092":"#ff6f78")}]},options:chartCommon(v=>fmtMoney(v))})}
    ${chartCard("Call / Exit Quality", "", {type:"bar",data:{labels:callRows.map(r=>r.bucket),datasets:[{label:"Lifecycle P&L",data:callRows.map(r=>num(r.lifecycle_pnl)===null?null:r.lifecycle_pnl),backgroundColor:callRows.map(r=>(num(r.lifecycle_pnl)||0)>=0?"#45d2c5":"#ff6f78")}]},options:chartCommon(v=>fmtMoney(v))})}
  </div><div style="height:12px"></div><div class="grid two-even">
    ${table("Put risk bucket lifecycle", putRows, [
      {key:"bucket",label:"Risk bucket"},
      {key:"count",label:"Trades",num:true},
      {key:"avg_assignment_risk_proxy",label:"Avg risk",format:fmtPct,num:true},
      {key:"opening_premium",label:"Premium",format:fmtMoney,num:true,className:cls},
      {key:"option_pnl",label:"Option P&L",format:fmtMoney,num:true,className:cls},
      {key:"stock_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"unrealized_drag",label:"Unrealized",format:fmtMoney,num:true,className:cls},
      {key:"lifecycle_pnl",label:"Lifecycle P&L",format:fmtMoney,num:true,className:cls},
      {key:"pnl_per_capital",label:"P&L/capital",format:fmtPct,num:true},
      {key:"attribution_rate",label:"Attributed",format:fmtPct,num:true}
    ], {wide:true})}
    ${table("Call risk bucket lifecycle", callRows, [
      {key:"bucket",label:"Risk bucket"},
      {key:"count",label:"Trades",num:true},
      {key:"avg_assignment_risk_proxy",label:"Avg risk",format:fmtPct,num:true},
      {key:"opening_premium",label:"Premium",format:fmtMoney,num:true,className:cls},
      {key:"option_pnl",label:"Option P&L",format:fmtMoney,num:true,className:cls},
      {key:"stock_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"unrealized_drag",label:"Unrealized",format:fmtMoney,num:true,className:cls},
      {key:"lifecycle_pnl",label:"Lifecycle P&L",format:fmtMoney,num:true,className:cls},
      {key:"pnl_per_capital",label:"P&L/capital",format:fmtPct,num:true},
      {key:"attribution_rate",label:"Attributed",format:fmtPct,num:true},
    ], {wide:true})}
  </div>`;
}
function coverage(data){
  return `<div class="grid">${(data.coverage_notes||[]).map(n=>`<div class="note"><strong>${safe(n.severity)}</strong> ${safe(n.message)}</div>`).join("")}</div>`;
}
function optionDataControls(data){
  const st = (data.option_market_data || {}).status || {};
  const text = st.last_fetched_at ? `${safe(st.provider || "option data")} fetched ${safe(fmtDateTime(st.last_fetched_at))} · ${fmtNum(st.contract_count)} contracts · ${safe(st.source || "")}` : `${safe(st.provider || "option data")} ${safe(st.status || "not fetched")} · ${fmtNum(st.contract_count || 0)} contracts`;
  return `<div class="option-controls"><div class="option-status mono">${text}</div><button id="fetchOptionData" class="action-btn" type="button">Fetch option data</button></div>`;
}
function attachOptionFetch(){
  const btn = $("fetchOptionData");
  if (!btn) return;
  btn.addEventListener("click", async () => {
    btn.disabled = true;
    btn.textContent = "Fetching...";
    try {
      const response = await fetch("/api/decision-lab/options/refresh" + window.location.search, {method:"POST", credentials:"same-origin"});
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      chartSeq = 0;
      pending.length = 0;
      render(data);
    } catch (err) {
      btn.disabled = false;
      btn.textContent = "Fetch option data";
      alert(err.message || err);
    }
  });
}
function render(data){
  $("generated").textContent = `Generated ${new Date(data.generated_at).toLocaleString()} · ${data.source?.label || "IBKR Flex"}`;
  const s = data.summary || {};
  $("content").innerHTML = `
    <div class="grid metrics">
      ${metric("YTD total P&L",fmtMoney(s.ytd_total_pnl),"Realized + unrealized",cls(s.ytd_total_pnl))}
      ${metric("Current unrealized",fmtMoney(s.current_unrealized_pnl),"Open stock/options snapshot",cls(s.current_unrealized_pnl))}
      ${metric("Action situations",fmtNum(s.action_item_count),"Ticker-level decisions",s.action_item_count?"warn":"pos")}
      ${metric("Active cycle",safe(s.active_cycle || "n/a"),"Nearest open expiry month","blue")}
      ${metric("Open put exposure",fmtMoney(s.total_open_put_exposure),`${fmtMoney(s.total_itm_put_exposure)} ITM`,s.total_itm_put_exposure?"neg":"")}
    </div>
    ${optionDataControls(data)}
    ${head("actions","1. Action Needed")}
    ${actionRows(data)}
    ${head("cycle","2. Active Cycle Target & Exposure")}
    ${cycleBlock(data)}
    ${head("candidates","3. Recommendation Candidates / Recovery Planner")}
    ${candidates(data)}
    ${head("strikes","4. Strike Selection Quality")}
    ${strikeQuality(data)}
    ${head("coverage","5. Data Coverage Notes")}
    ${coverage(data)}
  `;
  renderCharts();
  attachOptionFetch();
}
fetch("/api/decision-lab" + window.location.search, {credentials:"same-origin"})
  .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
  .then(render)
  .catch(err => {$("content").innerHTML = `<div class="panel"><h2>Decision lab failed to load</h2><p class="error">${safe(err.message || err)}</p></div>`});
</script>
</body>
</html>""".replace("__BASE_CSS__", BASE_CSS)
