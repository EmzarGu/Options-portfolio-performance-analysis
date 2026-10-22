from __future__ import annotations


DECISION_RESPONSE_JS = """
async function readDecisionLabResponse(response){
  if (!response.ok) {
    const fallback = response.status === 504
      ? "Decision Lab took too long to respond. Please retry; stored portfolio data is preserved."
      : `Decision Lab request failed (HTTP ${response.status}). Please retry.`;
    let error = fallback;
    try {
      const payload = await response.json();
      if (payload.error) error = payload.error;
    } catch (_) { /* Gateways may return plain text instead of JSON. */ }
    throw new Error(error);
  }
  try { return await response.json(); }
  catch (_) { throw new Error("Decision Lab returned an unreadable response. Please reload the page and retry."); }
}
"""

BASE_CSS = """
:root{color-scheme:dark;--color-bg:#080c0f;--color-surface:#10161a;--color-surface-raised:#11191c;--color-surface-muted:#0d1417;--color-border:#26383b;--color-border-strong:#365257;--color-text:#eef7ef;--color-text-muted:#aab7ad;--color-accent:#45d2c5;--color-positive:#7ee092;--color-negative:#ff6f78;--color-attention:#f6c25b;--color-info:#7aa7ff;--color-neutral:#aab7ad;--color-benchmark:#b8c2cc;--band-negative:var(--color-negative);--band-below:var(--color-attention);--band-target:var(--color-positive);--band-above:var(--color-info);--risk-itm:var(--color-negative);--risk-near:var(--color-attention);--risk-otm:var(--color-info);--priority-high:var(--color-negative);--priority-medium:var(--color-attention);--priority-low:var(--color-info);--status-current:var(--color-accent);--radius-control:6px;--radius-panel:8px;--bg:var(--color-bg);--panel:var(--color-surface);--panel2:var(--color-surface-raised);--line:var(--color-border);--muted:var(--color-text-muted);--text:var(--color-text);--accent:var(--color-accent);--accent2:var(--color-positive);--warn:var(--color-attention);--bad:var(--color-negative);--good:var(--color-positive);--shadow:0 12px 40px rgba(0,0,0,.24)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
a{color:var(--accent)}button,input{font:inherit}.login{max-width:520px;margin:14vh auto;padding:32px;background:var(--panel);border:1px solid var(--line);border-radius:8px;box-shadow:var(--shadow)}
.login h1{margin:0 0 8px;font-size:32px}.login p{color:var(--muted)}.login input{width:100%;padding:13px 14px;background:#0c120f;color:var(--text);border:1px solid var(--line);border-radius:8px;margin:12px 0}
.login button,.primary{background:var(--accent);color:#06201b;border:0;border-radius:var(--radius-control);padding:12px 16px;font-weight:750;cursor:pointer}.secondary{background:var(--panel2);border:1px solid var(--line);color:var(--text);border-radius:var(--radius-control);padding:10px 13px;cursor:pointer}button:focus-visible,a:focus-visible,input:focus-visible,select:focus-visible,summary:focus-visible{outline:2px solid var(--color-info);outline-offset:2px}button:disabled{opacity:.55;cursor:not-allowed}
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
:root{--bg2:var(--color-bg);--panel3:var(--color-surface);--panel4:var(--color-surface-raised);--line2:var(--color-border);--blue:var(--color-info);--amber:var(--color-attention);--red:var(--color-negative);--green:var(--color-positive);--teal:var(--color-accent)}
body{background:var(--color-bg);color:var(--text)}
.topbar{position:sticky;top:0;z-index:40;background:rgba(8,12,15,.94);backdrop-filter:blur(14px);border-bottom:1px solid var(--line2)}
.topbar-inner{max-width:1420px;margin:0 auto;padding:9px 18px;display:flex;gap:16px;align-items:center;justify-content:space-between}
.brand{font-size:18px;font-weight:850;letter-spacing:.01em;white-space:nowrap}.brand small{display:block;color:var(--muted);font-size:11px;font-weight:650;margin-top:-2px}
.nav{display:flex;gap:2px;flex:1;justify-content:flex-start;min-width:0;overflow:auto;scrollbar-width:none;border-left:1px solid var(--line2);padding-left:12px}.nav::-webkit-scrollbar{display:none}.nav button{background:transparent;color:var(--muted);border:0;border-bottom:2px solid transparent;border-radius:0;padding:9px 9px 7px;cursor:pointer;font-weight:750;white-space:nowrap}.nav button.active{color:var(--text);border-bottom-color:var(--accent)}
.segmented button,.segmented a{background:transparent;color:var(--muted);border:1px solid transparent;border-radius:8px;padding:8px 10px;cursor:pointer;font-weight:750;white-space:nowrap}.segmented button.active,.segmented a.active{color:#061a18;background:var(--accent);border-color:transparent}
.actions{display:flex;gap:7px;align-items:center;flex:0 0 auto;border-left:1px solid var(--line2);padding-left:12px}.actions form{margin:0}.actions .primary,.actions .secondary{padding:9px 11px}.actions button[disabled]{opacity:.72;cursor:wait}.auth-user{display:none;color:var(--muted);font-size:12px;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.shell{max-width:1420px;margin:0 auto;padding:22px 18px 64px}.hero{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:18px;align-items:end;margin:4px 0 16px}
.hero h1{font-size:34px;line-height:1.05;margin:0 0 8px}.sub{color:var(--muted);font-size:14px}.status-strip{display:flex;gap:8px;flex-wrap:wrap;align-items:center;justify-content:flex-end}
.badge{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line2);background:var(--color-surface-raised);border-radius:var(--radius-control);padding:5px 9px;color:var(--muted);font-size:12px;font-weight:750;white-space:nowrap}.badge.good{color:#c6f6ce;border-color:#2f6941}.badge.warn{color:#ffe3a3;border-color:#73551d}.badge.bad{color:#ffc0c5;border-color:#70303b}.badge.blue{color:#cfe0ff;border-color:#355189}
.basis-control{display:flex;justify-content:flex-end;margin-top:8px}.basis-control .segmented{border:1px solid var(--line2);border-radius:8px;padding:4px;background:#0d1417}.basis-control a,.range-panel a{display:inline-flex;align-items:center;text-decoration:none}
.control-label{color:var(--muted);font-size:12px;text-transform:uppercase;font-weight:800;letter-spacing:.06em}.segmented{display:flex;gap:4px;flex-wrap:wrap}.range-panel{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:0 0 12px;padding:10px 12px;background:#0d1417;border:1px solid var(--line2);border-radius:8px}.range-panel .segmented button{padding:7px 10px}
.grid{display:grid;gap:12px}.grid>*{min-width:0}.metrics{grid-template-columns:repeat(4,minmax(180px,1fr))}.two{grid-template-columns:minmax(0,1.08fr) minmax(360px,.92fr)}.two-even{grid-template-columns:repeat(2,minmax(0,1fr))}.three{grid-template-columns:repeat(3,minmax(0,1fr))}
.card,.panel{background:var(--panel3);border:1px solid var(--line2);border-radius:var(--radius-panel);box-shadow:0 10px 28px rgba(0,0,0,.18)}.card{padding:14px}.panel{padding:16px}
.metric-label{color:var(--muted);font-size:12px;font-weight:800;text-transform:uppercase;letter-spacing:.04em}.metric-value{font-size:25px;font-weight:900;margin-top:5px;line-height:1.08}.metric-note{color:var(--muted);font-size:12px;margin-top:5px}.pos{color:var(--color-positive)}.neg{color:var(--color-negative)}.neutral{color:var(--color-neutral)}.muted{color:var(--muted)}.warn-text{color:var(--color-attention)}.blue,.risk-otm-text{color:var(--color-info)}.risk-near-text{color:var(--color-attention)}.risk-itm-text{color:var(--color-negative)}.mono{font-variant-numeric:tabular-nums}
h2{font-size:21px;margin:22px 0 10px}h3{font-size:16px;margin:0 0 10px}.section{display:none}.section.active{display:block}.section-head{display:flex;align-items:end;justify-content:space-between;gap:14px;margin:20px 0 10px}.section-head h2{margin:0}.section-note{color:var(--muted);font-size:13px;margin-top:4px}
.toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:0 0 10px}.toolbar input,.toolbar select,.section-head select{background:#0b1113;color:var(--text);border:1px solid var(--line2);border-radius:8px;padding:9px 11px;min-height:38px}.toolbar input{min-width:260px}.toolbar .segmented button{padding:7px 10px}
.table-card{background:var(--panel3);border:1px solid var(--line2);border-radius:var(--radius-panel);overflow:hidden}.table-title{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;padding:12px 14px;border-bottom:1px solid var(--line2);background:var(--color-surface-raised)}.table-title strong{font-size:15px}.table-title span{display:block;color:var(--muted);font-size:12px;margin-top:2px}
.table-scroll{overflow:auto;max-height:620px;scrollbar-color:var(--color-border-strong) var(--color-surface-muted)}table{width:100%;border-collapse:separate;border-spacing:0;font-size:13px;min-width:820px;font-variant-numeric:tabular-nums}th,td{text-align:left;padding:8px 10px;border-bottom:1px solid #213034;vertical-align:middle;white-space:nowrap}tbody tr:nth-child(even) td{background:rgba(255,255,255,.012)}tbody tr:hover td{background:rgba(122,167,255,.055)}th{position:sticky;top:0;z-index:3;background:#0f171a;color:#c4d0cc;font-size:11.5px;text-transform:uppercase;letter-spacing:.04em}th button{all:unset;cursor:pointer}th button:focus-visible{outline:2px solid var(--color-info);outline-offset:2px}td.num,th.num{text-align:right}th.sticky-col,td.sticky-col{position:sticky;left:0}th.sticky-col{z-index:5;background:#0f171a}td.sticky-col{z-index:1;background:var(--color-surface)}tbody tr:nth-child(even) td.sticky-col{background:#11181c}tbody tr:hover td.sticky-col{background:#172126}.small-table table{min-width:620px}.wide-table table{min-width:1120px}.compact-table table{min-width:100%;table-layout:auto}.col-compact{width:1%;max-width:96px}.empty{padding:18px;color:var(--muted)}
.risk-row-itm td{background:rgba(255,111,120,.10)}.risk-row-near td{background:rgba(246,194,91,.09)}.risk-row-clear td{background:rgba(122,167,255,.07)}.risk-row-itm td:first-child{box-shadow:inset 3px 0 0 var(--risk-itm)}.risk-row-near td:first-child{box-shadow:inset 3px 0 0 var(--risk-near)}.risk-row-clear td:first-child{box-shadow:inset 3px 0 0 var(--risk-otm)}.risk-dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px;vertical-align:middle}.dot-bad{background:var(--risk-itm)}.dot-warn{background:var(--risk-near)}.dot-good,.dot-blue{background:var(--risk-otm)}
.risk-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px}.risk-card{background:var(--color-surface-raised);border:1px solid var(--line2);border-radius:var(--radius-panel);padding:12px}.risk-head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}.risk-title{font-weight:900}.risk-meta{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:4px;margin-top:8px;color:var(--muted);font-size:12px}.pill{display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;max-width:150px;min-height:22px;line-height:1.05;font-size:11px;border-radius:var(--radius-control);padding:4px 8px;background:#1c2a2d;color:var(--muted);font-weight:850;white-space:nowrap}.pill.high,.pill.bad{background:#3a171d;color:#ffc4c9}.pill.medium,.pill.warn{background:#4a330f;color:#ffe1a0}.pill.low,.pill.blue{background:#17243c;color:#cadcff}.pill.good{background:#143420;color:#bff3c7}
.chart-card{background:var(--panel3);border:1px solid var(--line2);border-radius:8px;padding:14px;min-width:0;overflow:hidden}.chart-title{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:8px}.chart-title strong{font-size:16px}.chart-title .muted{font-size:13px}.chart-wrap{height:310px;position:relative;min-width:0}.chart-canvas{width:100%!important;height:100%!important}.chart{width:100%;height:285px;display:block}.chart text{fill:#c0cbc7;font-size:15px;font-weight:650}.axis{stroke:#42585d;stroke-width:1.2}.grid-line{stroke:#26383c;stroke-width:1}.line{fill:none;stroke-width:3}.bar-pos{fill:#66d37a}.bar-neg{fill:#ff7078}.bar-label{fill:#d7e3de;font-size:12px;font-weight:750}.legend{display:flex;gap:10px;flex-wrap:wrap;margin-top:8px}.legend-item{font-size:13px;color:var(--muted);display:inline-flex;gap:6px;align-items:center}.legend-swatch{width:11px;height:11px;border-radius:2px}.chart-legend{display:flex;gap:14px;flex-wrap:wrap;margin-top:10px;color:#c0cbc7!important;font-size:13px;font-weight:700}.chart-legend span{display:inline-flex;align-items:center;gap:6px;color:#c0cbc7!important}.chart-legend i{display:inline-block;width:12px;height:12px;border-radius:2px}
.note-list{display:grid;gap:8px}.note{border-left:3px solid var(--accent);background:var(--panel3);border-radius:8px;padding:10px 12px}.note strong{display:block}details{border:1px solid var(--line2);border-radius:8px;padding:12px;background:var(--panel3)}summary{cursor:pointer;font-weight:850}.footnote{font-size:12px;color:var(--muted);margin-top:8px}
.waterfall{display:grid;gap:8px}.water-row{display:grid;grid-template-columns:210px minmax(140px,1fr) 112px;gap:10px;align-items:center}.bar-track{height:18px;border-radius:999px;background:#1b282b;overflow:hidden}.bar{height:100%;border-radius:999px}.bar.pos{background:var(--green)}.bar.neg{background:var(--red)}.bar.warn-text{background:var(--amber)}.bar.blue{background:var(--blue)}
.option-controls{display:flex;gap:12px;align-items:center;justify-content:space-between;margin:0 0 14px;padding:12px 14px;background:var(--panel3);border:1px solid var(--line2);border-radius:8px;box-shadow:0 10px 28px rgba(0,0,0,.18)}.option-status{color:var(--muted);font-size:13px}.action-btn{border:0;border-radius:8px;background:var(--accent);color:#06201b;font-weight:900;padding:9px 12px;cursor:pointer;white-space:nowrap}.action-btn:disabled{opacity:.55;cursor:wait}
.candidate-card{padding:14px;border:1px solid var(--line2);border-radius:8px;background:#11191c}.candidate-action{font-size:18px;font-weight:900}.state-title{margin-top:12px;color:var(--muted);font-weight:850;text-transform:uppercase;font-size:11px;letter-spacing:.04em}.state-grid{display:flex;flex-wrap:wrap;gap:7px;margin:6px 0 12px}.candidate-mini{padding:7px 8px;border-radius:7px;background:#0d1417;min-width:118px;max-width:260px;flex:0 1 auto}.candidate-mini.wide{min-width:220px;max-width:520px}.candidate-mini b{display:block;font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}.candidate-mini span{display:block;overflow:hidden;text-overflow:ellipsis}
.loading-panel{min-height:220px;display:grid;place-items:center;text-align:center}.loading-panel h2{margin:0 0 8px}.loading-panel p{margin:0;color:var(--muted)}.retry-load{margin-top:14px}
.view-updating{position:fixed;right:18px;bottom:18px;z-index:90;background:#11201d;border:1px solid #2d4b46;color:#c8fff6;border-radius:999px;padding:8px 12px;font-weight:850;box-shadow:0 10px 28px rgba(0,0,0,.28);opacity:0;transform:translateY(8px);transition:opacity .12s ease,transform .12s ease;pointer-events:none}body.is-rendering .view-updating,body.is-loading .view-updating{opacity:1;transform:translateY(0)}
.new-tag{display:inline-block;margin-left:5px;font-size:10px;font-weight:850;letter-spacing:.06em;color:var(--accent);vertical-align:1px}
.td-fresh{display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin:0 0 14px;font-size:13px;color:var(--muted)}.td-fresh .td-chip{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--line2);border-radius:14px;padding:6px 12px;background:var(--color-surface-muted)}.td-fresh b{color:var(--text);font-weight:750}.td-dot{width:8px;height:8px;border-radius:50%;background:var(--color-positive)}.td-dot.warn{background:var(--color-attention)}.td-dot.bad{background:var(--color-negative)}
.td-hero{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(0,1fr) minmax(0,1fr);gap:12px;margin-bottom:12px}.td-kpi{padding:16px}.td-big{font-size:34px;font-weight:850;line-height:1;margin:8px 0 6px;font-variant-numeric:tabular-nums}.td-sub{color:var(--muted);font-size:13px}
.td-gauge{position:relative;height:12px;border-radius:6px;background:var(--color-surface-muted);margin:16px 0 4px}.td-gauge .seg{position:absolute;top:0;height:100%;opacity:.3}.td-gauge .fill{position:absolute;top:3px;height:6px;border-radius:3px}.td-gauge .mk{position:absolute;top:-5px;width:2px;height:22px;background:var(--text)}.td-scale{position:relative;height:16px;font-size:11px;color:var(--muted)}.td-scale span{position:absolute;transform:translateX(-50%);white-space:nowrap}
.td-build{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:6px;margin-top:10px}.td-build div{background:var(--color-surface-muted);border-radius:6px;padding:8px;min-width:0}.td-build b{display:block;font-variant-numeric:tabular-nums}.td-build span{display:block;font-size:11.5px;color:var(--muted);line-height:1.25}
.td-line{display:flex;justify-content:space-between;gap:10px;font-size:13px;padding:5px 0;border-bottom:1px solid var(--line2)}.td-line:last-child{border-bottom:0}.td-line b{font-variant-numeric:tabular-nums}
.td-pace{position:relative;height:10px;border-radius:5px;background:var(--color-surface-muted);margin:14px 0 4px}.td-pace .fill{position:absolute;left:0;top:0;bottom:0;border-radius:5px}.td-pace .mk{position:absolute;top:-4px;width:2px;height:18px;background:var(--text)}
.td-main{display:grid;grid-template-columns:minmax(0,1.75fr) minmax(0,1fr);gap:12px;align-items:start}.td-side{display:grid;gap:12px;min-width:0}
.td-ph{display:flex;justify-content:space-between;align-items:baseline;gap:10px;flex-wrap:wrap;padding:14px 14px 0}.td-ph h3{margin:0}.td-ph span{color:var(--muted);font-size:12px}
.td-tabs{display:flex;gap:6px;flex-wrap:wrap;padding:12px 14px}.td-tabs button{background:transparent;border:1px solid var(--line2);color:var(--muted);border-radius:6px;padding:6px 10px;font-weight:750;font-size:12.5px;cursor:pointer}.td-tabs button.active{color:var(--text);background:var(--color-surface-raised);border-color:var(--color-border-strong)}
.td-group{font-size:11px;font-weight:850;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);padding:12px 14px 6px;border-top:1px solid var(--line2)}
.td-row{display:grid;grid-template-columns:4px 70px minmax(0,1.4fr) minmax(0,1.3fr) 112px;gap:12px;align-items:center;width:100%;text-align:left;background:transparent;color:var(--text);border:0;border-top:1px solid var(--line2);padding:10px 14px 10px 0;cursor:pointer;font:inherit}.td-row:hover,.td-row.open{background:var(--color-surface-raised)}.td-row:focus-visible{outline:2px solid var(--accent);outline-offset:-2px}
.td-row .stripe{align-self:stretch;border-radius:0 3px 3px 0;background:var(--color-info)}.td-row .stripe.now{background:var(--color-negative)}.td-row .stripe.plan{background:var(--color-attention)}
.td-row .tk{font-weight:900;font-size:15px}.td-row .what b,.td-row .move b{display:block;font-size:13.5px}.td-row .what span,.td-row .move span{font-size:12.5px;color:var(--muted)}.td-row .val{text-align:right;font-variant-numeric:tabular-nums;font-weight:750}.td-row .val span{display:block;font-size:11px;color:var(--muted);font-weight:600}
.td-more{color:var(--accent);font-weight:750}
.dc{border:1px solid var(--color-border-strong);border-radius:8px;background:var(--color-bg);overflow:hidden}.td-inline{margin:0 14px 14px}
.dc-head{display:flex;flex-wrap:wrap;gap:4px 16px;align-items:baseline;padding:12px 14px;border-bottom:1px solid var(--line2)}.dc-head h4{margin:0;font-size:16px}.dc-head span{font-size:12.5px;color:var(--muted)}
.dc-grid{display:grid;grid-template-columns:repeat(var(--cols,4),minmax(0,1fr))}.dc-opt{padding:12px 14px;border-right:1px solid var(--line2);min-width:0;display:flex;flex-direction:column;gap:5px}.dc-opt:last-child{border-right:0}.dc-opt.top{background:rgba(69,210,197,.07);box-shadow:inset 0 2px 0 var(--accent)}
.dc-opt h5{margin:0;font-size:13.5px}.dc-opt h5 small{display:block;color:var(--muted);font-weight:650;font-size:12px;margin-top:2px}.dc-head-num{font-size:20px;font-weight:850;font-variant-numeric:tabular-nums;margin-top:4px}.dc-head-num small{display:block;font-size:11px;color:var(--muted);font-weight:600}
.dc-chips{display:flex;flex-wrap:wrap;gap:4px;margin-top:auto;padding-top:6px}.dc-chip{display:inline-flex;font-size:11px;font-weight:750;padding:3px 7px;border-radius:5px;border:1px solid var(--line2);color:var(--muted)}.dc-chip.ok{color:var(--color-positive);border-color:rgba(126,224,146,.45)}.dc-chip.fail{color:var(--color-negative);border-color:rgba(255,111,120,.45)}.dc-chip.near{color:var(--color-attention);border-color:rgba(246,194,91,.45)}
.dc-verdict{padding:11px 14px;border-top:1px solid var(--line2);font-size:13px}.dc-verdict b{color:var(--accent)}.dc-verdict.neutral b{color:var(--text)}
.dc-list{display:grid;gap:12px}.td-rules{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:0 0 12px;color:var(--muted);font-size:13px}
.td-svg{padding:6px 10px 10px}.td-svg svg{display:block;width:100%;height:auto}.td-svg text{font-size:10px;fill:var(--muted)}
abbr.def{text-decoration:underline dotted;text-underline-offset:3px;cursor:help}
@media(max-width:1160px){.td-hero{grid-template-columns:1fr 1fr}.td-hero>.panel:first-child{grid-column:1/-1}.td-main{grid-template-columns:1fr}.dc-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.dc-opt:nth-child(2n){border-right:0}.dc-opt{border-bottom:1px solid var(--line2)}}
@media(max-width:760px){.td-hero{grid-template-columns:1fr}.td-row{grid-template-columns:4px 56px minmax(0,1fr) 90px}.td-row .stripe{grid-row:1/3}.td-row .tk,.td-row .what{grid-row:1}.td-row .val{grid-row:1;grid-column:4}.td-row .move{grid-column:3/5;grid-row:2}.dc-grid{grid-template-columns:1fr}.dc-opt{border-right:0}.td-build{grid-template-columns:1fr 1fr}}
@media(max-width:1160px){.topbar-inner{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center}.nav{grid-column:1/-1;grid-row:2;width:100%;justify-content:flex-start}.hero{grid-template-columns:1fr}.hero h1{font-size:28px}.brand small{display:none}.nav button{padding:8px}.actions .secondary,.actions .primary{padding:8px 10px}}
@media(max-width:980px){.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.two,.two-even,.three{grid-template-columns:1fr}.chart,.chart-wrap{height:280px}}
@media(max-width:760px){.topbar-inner{display:flex;flex-direction:column;align-items:stretch;gap:10px;padding:10px 12px}.brand{width:100%;font-size:18px;line-height:1.1;overflow:visible}.brand small{display:none}.actions{order:2;width:100%;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}.actions form{margin:0;min-width:0}.actions .primary,.actions .secondary{width:100%;padding:8px 7px;font-size:14px}.nav{order:3;grid-column:auto;grid-row:auto;width:100%;justify-content:flex-start;gap:6px}.nav button{padding:8px 10px}.shell{padding:18px 12px 64px}.hero{margin-top:6px}.status-strip{justify-content:flex-start}.basis-control{justify-content:flex-start}.metrics{grid-template-columns:1fr}.toolbar input{min-width:100%;width:100%}.range-panel{align-items:flex-start;flex-direction:column}.range-panel .segmented{width:100%}.metric-value{font-size:22px}.chart,.chart-wrap{height:260px}}
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
  <section id="today" class="section"></section>
  <section id="decision_cards" class="section"></section>
  <section id="decision_lab" class="section"></section>
  <section id="assignment_quality" class="section"></section>
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
let dashboardLoaded = data.loading !== true;
const appState = {
  active: "dashboard",
  range: "YTD",
  includeUnrealized: data.web?.include_unrealized !== false,
  targetReturn: data.web?.target_return ?? data.monthly?.target_return ?? null,
  targetFloor: data.web?.target_floor ?? data.monthly?.target_floor ?? null,
  openRisk: "all",
  openType: "all",
  openSearch: "",
  tickerSearch: "",
  tickerYear: "all",
  expectancyYear: "all",
  assignmentYear: "all",
  assignmentHorizon: "to_as_of",
  assignmentQuality: null,
  assignmentQualityLoading: false,
  assignmentQualityError: "",
  decisionLab: null,
  decisionLabLoading: false,
  decisionLabError: "",
  todayGroup: "all",
  todayOpen: null,
  cardsGroup: "all",
  decisionRulesSaved: null,
  decisionRulesLoading: false,
  sort: {},
  renderTimer: null
};
const sections = [
  ["dashboard","Dashboard"], ["today","Today"], ["decision_cards","Decision cards"], ["decision_lab","Decision Lab"], ["assignment_quality","Assignment Quality Lab"], ["performance","Performance"], ["monthly","Monthly"], ["tickers","Tickers"],
  ["settings","Settings"], ["diagnostics","Diagnostics"],
  ["methodology","Methodology"]
];
const pageTitles = {
  dashboard: "Dashboard",
  today: "Today",
  decision_cards: "Decision cards",
  decision_lab: "Decision Lab",
  assignment_quality: "Assignment Quality Lab",
  performance: "Performance",
  monthly: "Monthly",
  tickers: "Tickers",
  settings: "Settings",
  diagnostics: "Diagnostics",
  methodology: "Methodology"
};
const rangeOptions = ["3M","6M","YTD","1Y","Since inception"];
const cssColor = (name, fallback) => getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
const theme = {
  accent: cssColor("--color-accent", "#45d2c5"),
  info: cssColor("--color-info", "#7aa7ff"),
  attention: cssColor("--color-attention", "#f6c25b"),
  negative: cssColor("--color-negative", "#ff6f78"),
  positive: cssColor("--color-positive", "#7ee092"),
  neutral: cssColor("--color-neutral", "#aab7ad"),
  benchmark: cssColor("--color-benchmark", "#b8c2cc"),
  surfaceRaised: cssColor("--color-surface-raised", "#11191c"),
  borderStrong: cssColor("--color-border-strong", "#365257"),
  text: cssColor("--color-text", "#eef7ef"),
  textMuted: cssColor("--color-text-muted", "#aab7ad")
};
const colors = [theme.accent,theme.info,theme.attention,"#b99cff","#ff8e96",theme.positive];
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
const cls = (v) => numeric(v) === null ? "neutral" : Number(v) < 0 ? "neg" : Number(v) > 0 ? "pos" : "neutral";
const moneynessCls = (v) => numeric(v) === null ? "neutral" : Number(v) > 0 ? "risk-itm-text" : Number(v) < 0 ? "risk-otm-text" : "neutral";
const bandTone = (value, floor=targetFloor(), target=targetReturn()) => {
  if (numeric(value) === null) return "";
  if (numeric(floor) === null || numeric(target) === null) return cls(value);
  const v = Number(value);
  if (v < 0) return "neg";
  if (v < Number(floor || 0)) return "warn-text";
  if (v <= Number(target || 0)) return "pos";
  return "blue";
};
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
  const floor = targetFloor();
  let tone = "warn";
  if (projected !== null && target !== null && floor !== null) {
    tone = projected < 0 ? "bad" : projected < floor ? "warn" : projected <= target ? "good" : "blue";
  } else if (String(value || "").toLowerCase().includes("beat")) {
    tone = "blue";
  } else if (String(value || "").toLowerCase().includes("below")) {
    tone = "warn";
  }
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
  url.searchParams.delete("target_return");
  url.searchParams.delete("target_return_pct");
  url.searchParams.delete("target_floor");
  url.searchParams.delete("target_floor_pct");
  url.searchParams.delete("target_good");
  url.searchParams.delete("refreshed");
  window.history.replaceState(null, "", `${url.pathname}${url.search}`);
}
function apiDashboardUrl(){
  const url = new URL("/api/dashboard", window.location.origin);
  url.searchParams.set("include_unrealized", appState.includeUnrealized ? "1" : "0");
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
    appState.targetReturn = data.web?.target_return ?? appState.targetReturn ?? null;
    appState.targetFloor = data.web?.target_floor ?? appState.targetFloor ?? null;
    appState.decisionLab = null;
    appState.decisionLabError = "";
    appState.decisionLabLoading = false;
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
    const thClass = [c.num ? "num" : "", c.compact ? "col-compact" : "", c.key === "ticker" ? "sticky-col" : "", c.thClass || ""].filter(Boolean).join(" ");
    return `<th class="${thClass}"><button type="button" onclick="sortTable('${tableId}','${c.key}')">${safe(c.label)}</button></th>`;
  }).join("")}</tr>`;
  const body = tableRows.map(row => `<tr class="${opts.rowClass ? opts.rowClass(row) : ""}">${columns.map(c => {
    const raw = c.value ? c.value(row) : get(row, c.key);
    const value = c.format ? c.format(raw,row) : safe(raw);
    const cellClass = [
      c.num ? "num" : "",
      c.compact ? "col-compact" : "",
      c.key === "ticker" ? "sticky-col" : "",
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
    const activeMonth = decisionActiveCycle()?.cycle ? monthIndex(parseDate(`${decisionActiveCycle().cycle}-01`)) : null;
    const endMonth = activeMonth === null ? asOfMonth : Math.max(asOfMonth, activeMonth);
    let startMonth = null;
    if (range === "3M") startMonth = endMonth - 2;
    if (range === "6M") startMonth = endMonth - 5;
    if (range === "1Y") startMonth = endMonth - 11;
    if (range === "YTD") startMonth = asOf.getUTCFullYear() * 12;
    return (rows || []).filter(row => {
      const d = parseDate(get(row,dateKey));
      if (!d) return false;
      const m = monthIndex(d);
      return (startMonth === null || m >= startMonth) && m <= endMonth;
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
        labels: {color: theme.textMuted, boxWidth: 12, boxHeight: 12, padding: 14, font: {size: 13, weight: 650}}
      },
      tooltip: {
        backgroundColor: theme.surfaceRaised,
        borderColor: theme.borderStrong,
        borderWidth: 1,
        titleColor: theme.text,
        bodyColor: theme.text,
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
        offset: true,
        grid: {display: false},
        ticks: {
          color: theme.textMuted,
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
        grid: {color: cssColor("--color-border", "#26383b")},
        border: {color: theme.borderStrong},
        ticks: {
          color: theme.textMuted,
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
  window.Chart.defaults.color = theme.textMuted;
  window.Chart.defaults.font.family = "-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif";
  pendingCharts.forEach(({id, config}) => {
    const canvas = $(id);
    if (!canvas) return;
    chartInstances.push(new window.Chart(canvas, config));
  });
  pendingCharts = [];
}
function targetReturn(){ return numeric(appState.targetReturn) ?? numeric(data.monthly?.target_return); }
function targetFloor(){
  const target = targetReturn();
  const floor = numeric(appState.targetFloor) ?? numeric(data.monthly?.target_floor);
  if (floor === null) return null;
  return target === null ? floor : Math.min(target, floor);
}
function displayMonthPnl(row){
  return numeric(row.projected_month_pnl);
}
function displayMonthReturn(row){
  return numeric(row.projected_return_roac);
}
function displayTargetPnl(row){
  return numeric(row.target_pnl);
}
function displayRemainingToTarget(row){
  return numeric(row.projected_remaining_pnl);
}
function displayTargetStatus(row){
  return row.monthly_target_status || "n/a";
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
        backgroundColor: values.map(v => v >= 0 ? theme.positive : theme.negative),
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
        backgroundColor: values.map(v => v >= 0 ? theme.positive : theme.negative),
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
  const bandColor = (value) => value < 0 ? theme.negative : value < floor ? theme.attention : value <= target ? theme.positive : theme.info;
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
      ctx.strokeStyle = theme.info;
      ctx.lineWidth = 2;
      ctx.setLineDash([7, 5]);
      ctx.beginPath();
      ctx.moveTo(chartArea.left, yTarget);
      ctx.lineTo(chartArea.right, yTarget);
      ctx.stroke();
      ctx.strokeStyle = theme.attention;
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
  const footer = `<div class="chart-legend"><span><i style="background:var(--band-negative)"></i>Negative</span><span><i style="background:var(--band-below)"></i>0-${safe(fmtPct(floor))}</span><span><i style="background:var(--band-target)"></i>Target band ${safe(fmtPct(floor))}-${safe(fmtPct(target))}</span><span><i style="background:var(--band-above)"></i>Above target</span></div>`;
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
    {key:"accounting_open_premium",label:"Open strategy balance",value:r=>numeric(r.accounting_open_premium) || 0,format:v=>fmtMoney(v,2),num:true,className:cls},
    {key:"strategy_premium_collected",label:"Net strategy balance",value:r=>numeric(r.strategy_premium_collected) || 0,format:v=>fmtMoney(v,2),num:true,className:cls},
    {key:"projected_pnl",label:"Projected P&L",format:v=>fmtMoney(v,2),num:true,className:cls},
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
    const premium = numeric(r.strategy_premium_collected) || 0;
    return `<div class="risk-card"><div class="risk-head"><div><div class="risk-title">${safe(r.ticker)} ${safe(r.option_type)} ${safe(fmtDec(r.strike,2))}</div><div class="muted">${safe(fmtDate(r.expiration))} - ${safe(r.days_to_expiration)} DTE</div></div><span class="pill ${tone}">${safe(riskPill(r))}</span></div><div class="risk-meta"><span>Current ${safe(fmtMoney(r.current_price,2))}</span><span>Moneyness ${safe(fmtPct(r.moneyness))}</span><span>Qty ${safe(r.quantity)}</span><span>${labelize(r.covered_status)}</span><span>Net strategy balance ${safe(fmtMoney(premium,2))}</span><span>Opened ${safe(fmtDate(r.opened))}</span></div></div>`;
  }).join("")}</div>`;
}
function monthlyRows(){
  const cycles = new Map((data.tables.monthly_cycles || []).map(row => [fmtDate(row.month), row]));
  return (data.monthly.months || []).map(row => ({...(cycles.get(fmtDate(row.month)) || {}), ...row}));
}
function monthlyChartRows(){
  return monthlyRows();
}
function futureCycleRows(){
  return (data.monthly.future_months || []).map(row => {
    const cycle = row.cycle_projection || {};
    return {
      month: row.month,
      open_ticker_count: row.open_ticker_count,
      open_option_count: row.open_option_count,
      realized_cycle_pnl: cycle.realized_cycle_pnl,
      open_premium_collected: cycle.open_premium_collected,
      stock_unrealized_pnl: cycle.stock_unrealized_pnl,
      itm_call_stock_pnl: cycle.itm_call_stock_pnl,
      itm_put_unrealized_loss: cycle.itm_put_unrealized_loss,
      projected_cycle_pnl: cycle.projected_cycle_pnl,
      target_pnl: cycle.target_pnl,
      remaining_to_target: cycle.remaining_to_target,
      projected_return_roac: cycle.projected_return_roac,
    };
  });
}
function inventoryColumns(){
  return [
    {key:"ticker",label:"Ticker",format:v=>`<strong>${safe(v)}</strong>`,compact:true},
    {key:"lot_count",label:"Lots",value:r=>numeric(r.lot_count)||1,num:true,compact:true},
    {key:"buy_date",label:"Last assigned",format:fmtDate},
    {key:"shares",label:"Shares",num:true},
    {key:"cost_per_share",label:"Avg cost",format:v=>fmtMoney(v,2),num:true},
    {key:"current_price",label:"Current",format:v=>fmtMoney(v,2),num:true},
    {key:"covered_shares",label:"Covered shares",num:true},
    {key:"covered_strike",label:"Covered strike",format:(v,r)=>r.covered_strike_mixed ? "Mixed" : fmtMoney(v,2),num:true},
    {key:"unrealized_pnl",label:"Unrealized",format:fmtMoney,num:true,className:cls}
  ];
}
function decisionData(){
  return appState.decisionLab || data.decision_lab || {};
}
function decisionActiveCycle(){
  return (data.monthly || {}).active_cycle || ((data.dashboard || {}).monthly_target || {}).cycle_projection || {};
}
function activeCycleWaterfall(c){
  const parts = [
    ["Realized cycle P&L", c.realized_cycle_pnl],
    ["Open strategy balance", c.open_premium_collected],
    ["ITM call stock P&L", c.itm_call_stock_pnl],
    ["ITM put assignment P&L", c.itm_put_unrealized_loss],
    ["Projected cycle P&L", c.projected_cycle_pnl],
    ["Target P&L", c.target_pnl],
    ["Remaining", c.remaining_to_target],
  ];
  const max = Math.max(...parts.map(p => Math.abs(numeric(p[1]) || 0)), 1);
  const returnTone = bandTone(c.projected_return_roac, c.target_floor ?? targetFloor(), c.target_return ?? targetReturn());
  return `<div class="waterfall" style="margin-top:12px">${parts.map(([label,value]) => {
    let tone = (numeric(value) || 0) < 0 ? "neg" : "pos";
    if (label === "Projected cycle P&L") tone = returnTone;
    if (label === "Remaining") tone = (numeric(value) || 0) > 0 ? "warn-text" : "blue";
    return `<div class="water-row"><strong>${safe(label)}</strong><div class="bar-track"><div class="bar ${tone}" style="width:${Math.max(4, Math.abs(numeric(value) || 0) / max * 100)}%"></div></div><div class="mono ${tone}">${fmtMoney(value)}</div></div>`;
  }).join("")}</div>`;
}
function activeCycleBlock(){
  const c = decisionActiveCycle();
  if (!c || !Object.keys(c).length) return "";
  const snap = currentSnapshot();
  const returnTone = bandTone(c.projected_return_roac, c.target_floor ?? targetFloor(), c.target_return ?? targetReturn());
  return `
    ${sectionHead("Active Cycle Target & Exposure")}
    <div class="grid two">
      <div class="panel">
        <h3>${safe(c.cycle_label || "Active cycle")}</h3>
        <div class="sub">Expiries ${safe((c.expiry_dates || []).join(", ") || "n/a")} · DTE ${safe(c.min_dte)}-${safe(c.max_dte)} · ${fmtNum(c.open_ticker_count ?? c.open_contract_count)} tickers</div>
        ${activeCycleWaterfall(c)}
      </div>
      <div class="grid metrics" style="grid-template-columns:repeat(2,minmax(0,1fr))">
        ${card("Projected RoAC", fmtPct(c.projected_return_roac), `Target ${safe(fmtPct(c.target_return ?? targetReturn()))}`, returnTone)}
        ${card("Cycle put exposure", fmtMoney(c.cycle_put_exposure), `${safe(fmtMoney(c.cycle_itm_put_exposure))} ITM`, c.cycle_itm_put_exposure ? "neg" : "")}
        ${card("Portfolio put exposure", fmtMoney(c.portfolio_put_exposure), `${safe(fmtMoney(c.portfolio_itm_put_exposure))} ITM`, c.portfolio_itm_put_exposure ? "neg" : "")}
        ${card("ITM put cash required", fmtMoney(snap.itm_put_cash_required ?? c.portfolio_itm_put_exposure), `${safe(snap.itm_put_contracts || 0)} ITM puts`, (numeric(snap.itm_put_cash_required ?? c.portfolio_itm_put_exposure) || 0) > 0 ? "neg" : "")}
        ${card("ITM put unrealized loss", fmtMoney(c.itm_put_unrealized_loss), "Active cycle signal", c.itm_put_unrealized_loss ? "neg" : "")}
        ${card("Covered-call upside foregone", fmtMoney(c.covered_call_upside_foregone), "Active cycle signal", c.covered_call_upside_foregone ? "warn-text" : "")}
      </div>
    </div>`;
}
function optionDataStatusText(){
  const status = ((decisionData().option_market_data || {}).status) || {};
  if (status.last_fetched_at) {
    return `${safe(status.provider || "option data")} fetched ${safe(fmtLocalDateTime(status.last_fetched_at))} · ${safe(fmtNum(status.contract_count || 0))} contracts`;
  }
  return `${safe(status.provider || "option data")} ${safe(status.status || "not fetched")} · ${safe(fmtNum(status.contract_count || 0))} contracts`;
}
function optionDataControls(){
  const status = ((decisionData().option_market_data || {}).status) || {};
  const notice = status.message ? `<div class="option-status">${safe(status.message)}</div>` : "";
  const dated = status.provider === "marketdata" ? `<div class="option-status mono"><strong>Quotes: ${safe(fmtDate(status.quote_date))}</strong> · ${fmtNum(status.credits_used)} / ${fmtNum(status.account_daily_limit)} daily credits used</div><div>Download for ${safe(fmtDate(status.latest_quote_date || status.quote_date))}: ${fmtNum(status.prepared_request_count ?? ((status.request_count || 0) - (status.missing_request_count || 0)))} / ${fmtNum(status.request_count)} selections ready${status.missing_request_count ? ` · ${fmtNum(status.missing_request_count)} pending` : ""}</div>` : "";
  return `<div class="option-controls"><div><div class="metric-label">Option data</div>${dated}<div class="option-status mono">${optionDataStatusText()}</div>${notice}</div><button id="fetchOptionData" class="action-btn" type="button">Fetch option data</button></div>`;
}
function decisionActionRows(){
  return dataTable("decision-actions", decisionData().ticker_situations || [], [
    {key:"priority",label:"Priority",format:v=>`<span class="pill ${safe(v)}">${safe(v)}</span>`},
    {key:"ticker",label:"Ticker",format:v=>`<strong>${safe(v)}</strong>`},
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
  ], {title:"Ticker-level action queue", wide:true});
}
function mini(label, value, tone="", wide=false){
  return `<div class="candidate-mini ${wide ? "wide" : ""}"><b>${safe(label)}</b><span class="mono ${tone}">${safe(value)}</span></div>`;
}
function currentCandidateState(s){
  const open = (s.open_options || []).map(o=>`${safe(o.type)} ${fmtMoney(o.strike,2)} ${fmtDate(o.expiry)}${numeric(o.dte)!==null?` / ${safe(o.dte)} DTE`:""}`).join(" · ");
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
function candidateTable(rows, status={}){
  if (!rows || !rows.length) {
    const msg = status.message || "No actionable provider contract found for this ticker/action.";
    return `<div class="panel empty" style="margin:8px 0 0">${safe(msg)}${status.near_miss_message ? `<br>${safe(status.near_miss_message)}` : ""}</div>`;
  }
  const nearMiss = status.near_miss_message ? `<div class="note" style="margin:8px 0 0">${safe(status.near_miss_message)}</div>` : "";
  return nearMiss + dataTable("candidate-" + Math.random().toString(36).slice(2), rows || [], [
    {key:"action",label:"Action"},
    {key:"contract_count",label:"Contracts",format:v=>numeric(v)===null?"n/a":fmtNum(v),num:true},
    {key:"strike",label:"Strike",format:v=>numeric(v)===null?"n/a":fmtMoney(v,2),num:true},
    {key:"expiry",label:"Expiry",format:fmtDate},
    {key:"dte",label:"DTE",num:true},
    {key:"premium",label:"Premium or new credit",format:fmtMoney,num:true,className:cls},
    {key:"roll_close_cost",label:"Est. close cost",format:fmtMoney,num:true,className:cls},
    {key:"roll_new_credit",label:"Est. new credit",format:fmtMoney,num:true,className:cls},
    {key:"roll_net_credit",label:"Net credit after close",format:fmtMoney,num:true,className:cls},
    {key:"incremental_exit_pnl",label:"Exit proceeds change",format:fmtMoney,num:true,className:cls},
    {key:"expected_value",label:"Scenario estimate",format:fmtMoney,num:true,className:cls},
    {key:"expected_value_vs_current",label:"Scenario vs current",format:fmtMoney,num:true,className:cls},
    {key:"exercise_result",label:"If exercised",format:fmtMoney,num:true,className:cls},
    {key:"no_exercise_result",label:"If not exercised",format:fmtMoney,num:true,className:cls},
    {key:"upside_left",label:"Added upside room",format:fmtMoney,num:true,className:cls},
    {key:"upside_foregone",label:"Current price above strike",format:fmtMoney,num:true,className:cls},
    {key:"exercise_probability",label:"Delta exercise proxy",format:v=>numeric(v)===null?"n/a":`${(Math.abs(Number(v))*100).toFixed(1)}%`,num:true},
    {key:"delta",label:"Delta/risk",format:v=>numeric(v)===null?"n/a":Math.abs(Number(v)).toFixed(2),num:true},
    {key:"liquidity",label:"Liquidity"},
    {key:"tradeability",label:"Tradeability"},
    {key:"quote_date",label:"Quote date",format:fmtDate},
    {key:"price_source",label:"Price source"},
    {key:"score",label:"Score",num:true},
    {key:"score_reason",label:"Score reason"}
  ], {wide:true, compact:true, count:false});
}
function candidateCard(row){
  const rows = row.candidates || [row.recommended, ...(row.alternatives || [])].filter(Boolean);
  return `<div class="candidate-card"><div><div class="candidate-action">${safe(row.ticker)} · ${safe(row.category)}</div><div class="sub">${safe(row.objective)}</div></div>${currentCandidateState(row.current_state || {})}${candidateTable(rows, row.candidate_status || {})}</div>`;
}
function decisionCandidates(){
  const rows = decisionData().recommendation_candidates || [];
  if (!rows.length) return `<div class="panel empty">No recommendation candidates.</div>`;
  return `<div class="grid">${rows.map(candidateCard).join("")}</div>`;
}
function decisionStrikeQuality(){
  const s = decisionData().strike_quality || {};
  const putRows = (s.put_entry_quality || {}).bucket_summary || [];
  const callRows = (s.call_exit_quality || {}).bucket_summary || [];
  return `<div class="grid two-even">
    ${strikeQualityChart("Put Entry Quality", putRows, theme.positive)}
    ${strikeQualityChart("Call / Exit Quality", callRows, theme.accent)}
  </div><div style="height:12px"></div><div class="grid two-even">
    ${strikeQualityTable("Put risk bucket lifecycle", putRows)}
    ${strikeQualityTable("Call risk bucket lifecycle", callRows)}
  </div>`;
}
function strikeQualityChart(title, rows, color){
  return chartRegister(title, "", {
    type:"bar",
    data:{labels:(rows || []).map(r=>r.bucket),datasets:[{label:"Lifecycle P&L",data:(rows || []).map(r=>numeric(r.lifecycle_pnl)===null?null:r.lifecycle_pnl),backgroundColor:(rows || []).map(r=>(numeric(r.lifecycle_pnl)||0)>=0?color:theme.negative),borderWidth:0,borderRadius:2}]},
    options: chartCommonOptions(fmtCompactMoney)
  });
}
function strikeQualityTable(title, rows){
  return dataTable(title.toLowerCase().replaceAll(" ","-"), rows || [], [
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
  ], {title, wide:true});
}
function decisionCoverage(){
  const notes = decisionData().coverage_notes || [];
  return `<div class="note-list">${notes.map(n=>`<div class="note"><strong>${safe(n.severity)}</strong><span class="muted">${safe(n.message)}</span></div>`).join("")}</div>`;
}
__DECISION_RESPONSE_JS__
async function loadDecisionLab(){
  if (appState.decisionLabLoading) return;
  appState.decisionLabLoading = true;
  appState.decisionLabError = "";
  setUpdating(true, "Loading Decision Lab...");
  try {
    const response = await fetch("/api/decision-lab" + window.location.search, {
      credentials: "same-origin",
      headers: {"Accept": "application/json"}
    });
    const payload = await readDecisionLabResponse(response);
    appState.decisionLab = payload;
    data.decision_lab = payload;
  } catch (err) {
    appState.decisionLabError = err && err.message || String(err);
  } finally {
    appState.decisionLabLoading = false;
    setUpdating(false);
    if (DECISION_SECTIONS.includes(appState.active)) render();
  }
}
function renderDecisionLab(){
  const decision = decisionData();
  if (decision.deferred || (!appState.decisionLab && !decision.error)) {
    if (!appState.decisionLabLoading && !appState.decisionLabError) {
      setTimeout(loadDecisionLab, 0);
    }
    $("decision_lab").innerHTML = loadingPanel(
      "Loading Decision Lab...",
      "",
      appState.decisionLabError
    );
    const retry = $("retryDashboardLoad");
    if (retry) retry.addEventListener("click", loadDecisionLab);
    return;
  }
  if (!decision || decision.error) {
    $("decision_lab").innerHTML = `<div class="panel"><h2>Decision Lab unavailable</h2><p class="error">${safe(decision?.error || "Decision data has not loaded.")}</p></div>`;
    return;
  }
  $("decision_lab").innerHTML = `
    ${optionDataControls()}
    ${sectionHead("Action Needed")}
    ${decisionActionRows()}
    ${sectionHead("Recommendation Candidates / Recovery Planner")}
    <div class="sub">Current position appears first as the comparison baseline, not a ranking. Scenario estimates use delta as a rough exercise weight and current stock-price inputs, not a future price forecast. Expiries have different horizons. Estimates exclude future trading fees.</div>
    ${decisionCandidates()}
    ${sectionHead("Strike Selection Quality")}
    ${decisionStrikeQuality()}
    ${sectionHead("Data Coverage Notes")}
    ${decisionCoverage()}
  `;
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
  const snap=currentSnapshot(), shorts=openShortRows();
  $("dashboard").innerHTML = `
    <div class="grid metrics">
      ${card("YTD total P&L", fmtMoney(snap.ytd_total_pnl), snap.unrealized_adjusted ? "Realized YTD + current unrealized" : "Realized P&L only", cls(snap.ytd_total_pnl))}
      ${card("YTD realized P&L", fmtMoney(snap.ytd_realized_pnl), "Options, stock P&L, and dividends", cls(snap.ytd_realized_pnl))}
      ${card("Current unrealized", fmtMoney(snap.current_unrealized_pnl), `Options ${safe(fmtMoney(snap.current_option_unrealized_pnl))}${numeric(snap.current_put_assignment_unrealized_pnl) ? ` (net option balance ${safe(fmtMoney(snap.current_option_premium_unrealized_pnl))}, ITM put unrealized loss ${safe(fmtMoney(snap.current_put_assignment_unrealized_pnl))})` : ""} / Stock ${safe(fmtMoney(snap.current_stock_unrealized_pnl))}`, cls(snap.current_unrealized_pnl))}
      ${card("YTD annualized TWR", fmtPct(snap.ytd_annualized_twr), snap.unrealized_adjusted ? "Unrealized-adjusted" : "Realized only", cls(snap.ytd_annualized_twr))}
    </div>
    ${activeCycleBlock()}
    ${sectionHead("Assigned Holdings and Exposure")}
    ${dataTable("dashboard-inventory", data.positions.inventory || [], inventoryColumns(), {title:"Assigned holdings", compact:true})}
    ${sectionHead("Open Option Shorts", `${shorts.length} rows after filters.`)}
    ${openShortToolbar()}
    ${dataTable("dashboard-open-shorts", shorts, openShortColumns(), {title:"Open option shorts", rowClass:rowRiskClass, wide:true})}
  `;
}
function renderMonthly(){
  const rows = monthlyRows();
  const chartRows = monthlyChartRows();
  const filtered = rangeFiltered(chartRows, "month");
  const future = futureCycleRows();
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
      {key:"open_premium_collected",label:"Open strategy balance",format:fmtMoney,num:true,className:cls},
      {key:"projected_month_pnl",label:"Projected P&L",value:displayMonthPnl,format:fmtMoney,num:true,className:cls},
      {key:"projected_return_roac",label:"Projected RoAC",value:displayMonthReturn,format:fmtPct,num:true,className:v=>bandTone(v, targetFloor(), targetReturn())}
    ], {title:"Monthly table", subtitle:"Realized option P&L includes completed strategies. Linked rolls carry their net balance until final close, expiration or assignment.", wide:true})}
    ${sectionHead("Future Open Expiry Months")}
    ${dataTable("future-months", future, [
      {key:"month",label:"Month",format:monthName},
      {key:"open_ticker_count",label:"Open tickers",num:true},
      {key:"realized_cycle_pnl",label:"Realized cycle P&L",format:fmtMoney,num:true,className:cls},
      {key:"open_premium_collected",label:"Open strategy balance",format:fmtMoney,num:true,className:cls},
      {key:"itm_call_stock_pnl",label:"ITM call stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"itm_put_unrealized_loss",label:"ITM put assignment P&L",format:fmtMoney,num:true,className:cls},
      {key:"projected_cycle_pnl",label:"Projected cycle P&L",format:fmtMoney,num:true,className:cls},
      {key:"target_pnl",label:"Target P&L",format:fmtMoney,num:true},
      {key:"remaining_to_target",label:"Remaining",format:fmtMoney,num:true,className:(v)=>numeric(v)>0?"warn-text":"blue"},
      {key:"projected_return_roac",label:"Projected RoAC",format:fmtPct,num:true}
    ], {title:"Future expiry months", small:true})}
  `;
}
function assignmentQualityData(){
  return appState.assignmentQuality || data.assignment_quality || {};
}
async function loadAssignmentQuality(){
  if (appState.assignmentQualityLoading) return;
  appState.assignmentQualityLoading = true;
  appState.assignmentQualityError = "";
  try {
    const response = await fetch("/api/assignment-quality", {
      credentials: "same-origin",
      headers: {"Accept": "application/json"}
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    appState.assignmentQuality = payload;
  } catch (err) {
    appState.assignmentQualityError = err && err.message || String(err);
  } finally {
    appState.assignmentQualityLoading = false;
    if (appState.active === "assignment_quality") render();
  }
}
function assignmentRowsForHorizon(horizon=appState.assignmentHorizon){
  const aq = assignmentQualityData();
  const rows = horizon === "to_as_of"
    ? (aq.cohort_detail || aq.lot_detail || [])
    : (aq.horizon_detail || []).filter(r => String(r.horizon) === String(horizon));
  return rows.filter(r => appState.assignmentYear === "all" || String(r.assignment_year) === String(appState.assignmentYear));
}
function assignmentRows(){
  return assignmentRowsForHorizon(appState.assignmentHorizon);
}
function assignmentLotRows(){
  const aq = assignmentQualityData();
  const rows = appState.assignmentHorizon === "to_as_of"
    ? (aq.lot_detail || [])
    : (aq.horizon_detail || []).filter(r => String(r.horizon) === String(appState.assignmentHorizon));
  return rows.filter(r => appState.assignmentYear === "all" || String(r.assignment_year) === String(appState.assignmentYear));
}
function assignmentTickerRows(){
  const rows = assignmentRows();
  const groups = {};
  rows.forEach(r => {
    const ticker = String(r.ticker || "n/a");
    const g = groups[ticker] ||= {ticker, lots:0, assigned_shares:0, assigned_capital:0, assigned_put_pnl:0, post_assignment_put_pnl:0, put_pnl:0, call_cashflow_pnl:0, actual_stock_pnl:0, hold_stock_pnl:0, opportunity_cost:0, actual_total_pnl:0, counterfactual_total_pnl:0, delta_actual_minus_hold:0};
    g.lots += numeric(r.lots) || 1;
    g.assigned_shares += numeric(r.assigned_shares) || numeric(r.original_shares) || 0;
    ["assigned_capital","assigned_put_pnl","post_assignment_put_pnl","put_pnl","call_cashflow_pnl","actual_stock_pnl","hold_stock_pnl","opportunity_cost","actual_total_pnl","counterfactual_total_pnl","delta_actual_minus_hold"].forEach(k => { g[k] += numeric(r[k]) || 0; });
  });
  return Object.values(groups).map(r => ({
    ...r,
    actual_roi: r.assigned_capital ? r.actual_total_pnl / r.assigned_capital : null,
    hold_roi: r.assigned_capital ? r.counterfactual_total_pnl / r.assigned_capital : null,
    delta_roi: r.assigned_capital ? r.delta_actual_minus_hold / r.assigned_capital : null,
    decision_read: assignmentDecision(r.delta_actual_minus_hold, r.assigned_capital)
  })).sort((a,b)=>(numeric(a.delta_actual_minus_hold)||0)-(numeric(b.delta_actual_minus_hold)||0));
}
function assignmentSummary(rows=assignmentRows()){
  const lots = rows.length;
  const sum = (key) => rows.reduce((acc, r) => acc + (numeric(r[key]) || 0), 0);
  const deltaRows = rows.filter(r => numeric(r.delta_actual_minus_hold) !== null);
  const assignedCapital = sum("assigned_capital");
  const actualTotal = sum("actual_total_pnl");
  const holdTotal = sum("counterfactual_total_pnl");
  const delta = sum("delta_actual_minus_hold");
  return {
    lots,
    assigned_capital: assignedCapital,
    assigned_shares: sum("assigned_shares") || sum("original_shares"),
    put_pnl: sum("put_pnl"),
    call_cashflow_pnl: sum("call_cashflow_pnl"),
    actual_stock_pnl: sum("actual_stock_pnl"),
    hold_stock_pnl: sum("hold_stock_pnl"),
    opportunity_cost: sum("opportunity_cost"),
    actual_total_pnl: actualTotal,
    counterfactual_total_pnl: holdTotal,
    delta_actual_minus_hold: delta,
    actual_roi: assignedCapital ? actualTotal / assignedCapital : null,
    hold_roi: assignedCapital ? holdTotal / assignedCapital : null,
    delta_roi: assignedCapital ? delta / assignedCapital : null,
    actual_beat_hold_rate: deltaRows.length ? deltaRows.filter(r => (numeric(r.delta_actual_minus_hold) || 0) > 0).length / deltaRows.length : null
  };
}
function assignmentDecision(delta, capital){
  const d = numeric(delta) || 0, c = numeric(capital) || 0;
  if (d <= -1500 || (c > 0 && d / c <= -0.10)) return "Graduate candidate";
  if (d > 0) return "Wheel worked better";
  return "Small difference";
}
function assignmentDecisionPill(value){
  const text = String(value || "Small difference");
  const tone = text === "Graduate candidate" ? "warn" : text === "Wheel worked better" ? "good" : "blue";
  return `<span class="pill ${tone}">${safe(text)}</span>`;
}
function assignmentDeltaColor(delta, capital=0){
  const d = numeric(delta) || 0;
  const close = Math.max(500, Math.abs(numeric(capital) || 0) * 0.02);
  if (d > close) return theme.positive;
  if (d < -close) return theme.negative;
  return theme.attention;
}
function assignmentDeltaCls(delta, row={}){
  const d = numeric(delta) || 0;
  const close = Math.max(500, Math.abs(numeric(row.assigned_capital) || 0) * 0.02);
  if (d > close) return "pos";
  if (d < -close) return "neg";
  return "warn-text";
}
function assignmentHorizonLabel(h){
  if (String(h) === "to_as_of") return "To current";
  return String(h || "n/a");
}
function assignmentCohortRows(){
  const rows = assignmentRows();
  const groups = {};
  rows.forEach(r => {
    const year = String(r.assignment_year || "n/a");
    const g = groups[year] ||= {assignment_year: year, lots:0, assigned_capital:0, actual_total_pnl:0, counterfactual_total_pnl:0, delta_actual_minus_hold:0, opportunity_cost:0};
    g.lots += numeric(r.lots) || 1;
    ["assigned_capital","actual_total_pnl","counterfactual_total_pnl","delta_actual_minus_hold","opportunity_cost"].forEach(k => { g[k] += numeric(r[k]) || 0; });
  });
  return Object.values(groups).sort((a,b)=>String(a.assignment_year).localeCompare(String(b.assignment_year)));
}
function assignmentHorizonRows(){
  const controls = assignmentQualityData().controls || {};
  const horizons = controls.horizons || ["to_as_of"];
  return horizons.map(h => {
    const rows = assignmentRowsForHorizon(h);
    const summary = assignmentSummary(rows);
    return {
      ...summary,
      horizon: h,
      label: assignmentHorizonLabel(h),
      complete_lots: rows.length,
      total_lots: h === "to_as_of" ? rows.length : ((assignmentQualityData().lot_detail || []).filter(r => appState.assignmentYear === "all" || String(r.assignment_year) === String(appState.assignmentYear)).length)
    };
  });
}
function assignmentHorizontalOptions(valueFormatter){
  const options = chartCommonOptions(valueFormatter);
  options.indexAxis = "y";
  options.plugins.tooltip.callbacks.label = function(item){
    const label = item.dataset.label ? `${item.dataset.label}: ` : "";
    return `${label}${valueFormatter(item.parsed.x)}`;
  };
  options.scales.x = {
    grid: {color: cssColor("--color-border", "#26383b")},
    border: {color: theme.borderStrong},
    ticks: {color: theme.textMuted, font: {size: 12, weight: 650}, callback: (value) => valueFormatter(value)}
  };
  options.scales.y = {
    grid: {display: false},
    ticks: {color: theme.text, font: {size: 12, weight: 750}, autoSkip: false}
  };
  return options;
}
function assignmentOutcomeChart(summary){
  const options = chartCommonOptions(fmtCompactMoney);
  options.plugins.legend.display = false;
  const rows = [
    {label:"Actual path", value:summary.actual_total_pnl, color:theme.info},
    {label:"Hold counterfactual", value:summary.counterfactual_total_pnl, color:theme.benchmark},
    {label:"Actual less hold", value:summary.delta_actual_minus_hold, color:assignmentDeltaColor(summary.delta_actual_minus_hold, summary.assigned_capital)}
  ];
  return chartRegister("Actual vs Hold Outcome", "same selected cohort and horizon", {
    type: "bar",
    data: {labels: rows.map(r => r.label), datasets: [{label:"P&L", data: rows.map(r => numeric(r.value)||0), backgroundColor: rows.map(r => r.color), borderWidth:0, borderRadius:2}]},
    options
  }, `<div class="footnote">Actual path is assigned-put premium plus post-assignment option cashflows and assigned-stock P&L. Hold counterfactual is assigned shares held to the selected horizon, less estimated redeployment income lost.</div>`);
}
function assignmentTickerDeltaChart(rows){
  const sorted = [...(rows || [])].sort((a,b)=>(numeric(a.delta_actual_minus_hold)||0)-(numeric(b.delta_actual_minus_hold)||0));
  const clean = sorted.length > 18 ? [...sorted.slice(0,9), ...sorted.slice(-9)] : sorted;
  if (!clean.length) return `<div class="chart-card"><div class="chart-title"><strong>Ticker Delta Ranking</strong></div><div class="empty">Chart unavailable.</div></div>`;
  const labels = clean.map(r => r.ticker);
  const values = clean.map(r => numeric(r.delta_actual_minus_hold) || 0);
  const options = assignmentHorizontalOptions(fmtCompactMoney);
  options.plugins.legend.display = false;
  return chartRegister("Ticker Delta Ranking", "actual less hold; green wheel wins, red hold wins", {
    type: "bar",
    data: {labels, datasets: [{label:"Actual less hold", data: values, backgroundColor: clean.map(r => assignmentDeltaColor(r.delta_actual_minus_hold, r.assigned_capital)), borderWidth:0, borderRadius:2}]},
    options
  });
}
function assignmentCohortChart(rows){
  if (!rows.length) return `<div class="chart-card"><div class="chart-title"><strong>Assignment Cohorts</strong></div><div class="empty">Chart unavailable.</div></div>`;
  const labels = rows.map(r => String(r.assignment_year));
  const options = chartCommonOptions(fmtCompactMoney);
  return chartRegister("Assignment Cohorts", "selected horizon, grouped by assignment year", {
    type: "bar",
    data: {labels, datasets: [
      {label:"Actual path", data: rows.map(r => numeric(r.actual_total_pnl)||0), backgroundColor:theme.info, borderWidth:0, borderRadius:2},
      {label:"Hold counterfactual", data: rows.map(r => numeric(r.counterfactual_total_pnl)||0), backgroundColor:theme.benchmark, borderWidth:0, borderRadius:2},
      {label:"Actual less hold", data: rows.map(r => numeric(r.delta_actual_minus_hold)||0), backgroundColor:rows.map(r => assignmentDeltaColor(r.delta_actual_minus_hold, r.assigned_capital)), borderWidth:0, borderRadius:2}
    ]},
    options
  });
}
function assignmentHorizonChart(rows){
  if (!rows.length) return `<div class="chart-card"><div class="chart-title"><strong>Hold Exit Horizon</strong></div><div class="empty">Chart unavailable.</div></div>`;
  const labels = rows.map(r => r.label);
  const options = chartCommonOptions(fmtCompactMoney);
  return chartRegister("Hold Exit Horizon", "same cohort filter; fixed windows require price history", {
    type: "bar",
    data: {labels, datasets: [
      {label:"Actual path", data: rows.map(r => numeric(r.actual_total_pnl)||0), backgroundColor:theme.info, borderWidth:0, borderRadius:2},
      {label:"Hold counterfactual", data: rows.map(r => numeric(r.counterfactual_total_pnl)||0), backgroundColor:theme.benchmark, borderWidth:0, borderRadius:2},
      {label:"Actual less hold", data: rows.map(r => numeric(r.delta_actual_minus_hold)||0), backgroundColor:rows.map(r => assignmentDeltaColor(r.delta_actual_minus_hold, r.assigned_capital)), borderWidth:0, borderRadius:2}
    ]},
    options
  }, `<div class="footnote">6M/12M/18M assume the assigned shares were sold at that post-assignment date. Lots without enough elapsed time or price history are excluded from that horizon.</div>`);
}
function assignmentComponentTable(summary){
  const rows = [
    {component:"Put premiums", actual:summary.put_pnl, hold:"included in hold", note:"Assigned-put premium plus later post-assignment put premiums."},
    {component:"Call cashflows", actual:summary.call_cashflow_pnl, hold:0, note:"Covered-call premium and assignment cashflows in the actual wheel path."},
    {component:"Assigned-stock P&L", actual:summary.actual_stock_pnl, hold:summary.hold_stock_pnl, note:"Actual stock sale/current P&L versus the theoretical hold stock P&L."},
    {component:"Estimated redeployment income lost", actual:0, hold:-summary.opportunity_cost, note:"Assigned strike notional compounded at the monthly redeployment rate while capital is tied in stock."}
  ];
  return dataTable("assignment-components", rows, [
    {key:"component",label:"Component"},
    {key:"actual",label:"Actual path",format:v=>typeof v === "number" ? fmtMoney(v) : safe(v),num:true,className:cls},
    {key:"hold",label:"Hold counterfactual",format:v=>typeof v === "number" ? fmtMoney(v) : safe(v),num:true,className:cls},
    {key:"note",label:"Interpretation"}
  ], {title:"P&L construction", subtitle:"How the selected figures are built.", compact:true, count:false});
}
function assignmentEventRows(){
  const rows = assignmentLotRows();
  const groups = {};
  rows.forEach(r => {
    const key = [r.ticker, r.assignment_date, r.assignment_year, r.expiration, r.strike].join("|");
    const g = groups[key] ||= {ticker:r.ticker, assignment_date:r.assignment_date, assignment_year:r.assignment_year, expiration:r.expiration, strike:r.strike, contracts:0, original_shares:0, months_held:0, opportunity_cost:0, assigned_put_pnl:0, post_assignment_put_pnl:0, put_pnl:0, call_cashflow_pnl:0, actual_stock_pnl:0, actual_total_pnl:0, counterfactual_before_opportunity:0, counterfactual_total_pnl:0, delta_actual_minus_hold:0, assigned_capital:0};
    g.contracts += Math.max((numeric(r.original_shares) || 0) / 100, 1);
    ["original_shares","opportunity_cost","assigned_put_pnl","post_assignment_put_pnl","put_pnl","call_cashflow_pnl","actual_stock_pnl","actual_total_pnl","counterfactual_before_opportunity","counterfactual_total_pnl","delta_actual_minus_hold","assigned_capital"].forEach(k => { g[k] += numeric(r[k]) || 0; });
    g.months_held = Math.max(g.months_held, numeric(r.months_held) || 0);
  });
  return Object.values(groups).map(r => ({...r, decision_read: assignmentDecision(r.delta_actual_minus_hold, r.assigned_capital)})).sort((a,b)=>String(a.assignment_date).localeCompare(String(b.assignment_date)) || String(a.ticker).localeCompare(String(b.ticker)));
}
function renderAssignmentQuality(){
  const aq = assignmentQualityData();
  if (aq.deferred || (!appState.assignmentQuality && !aq.error)) {
    if (!appState.assignmentQualityLoading && !appState.assignmentQualityError) {
      setTimeout(loadAssignmentQuality, 0);
    }
    $("assignment_quality").innerHTML = loadingPanel(
      "Loading assignment quality...",
      "This analysis uses historical price data and loads only when the tab is opened.",
      appState.assignmentQualityError
    );
    const retry = $("retryDashboardLoad");
    if (retry) retry.addEventListener("click", loadAssignmentQuality);
    return;
  }
  if (aq.error) {
    $("assignment_quality").innerHTML = `<div class="panel"><h2>Assignment Quality Lab unavailable</h2><p class="error">${safe(aq.error)}</p></div>`;
    return;
  }
  const controls = aq.controls || {};
  const years = ["all", ...(controls.assignment_years || []).map(String)];
  const horizonRows = assignmentHorizonRows();
  const tickerRows = assignmentTickerRows();
  const rows = assignmentRows();
  const eventRows = assignmentEventRows();
  const summary = assignmentSummary(rows);
  const modeNote = appState.assignmentHorizon === "to_as_of"
    ? "Comparison uses current prices/open-call caps as of the dashboard date."
    : `Hold scenario assumes sale ${assignmentHorizonLabel(appState.assignmentHorizon)} after each assignment; incomplete lots are excluded.`;
  $("assignment_quality").innerHTML = `
    ${sectionHead("Assignment Quality Lab", "Actual wheel management versus a counterfactual hold of the assigned shares.")}
    <div class="toolbar">
      <label><span class="control-label">Assignment cohort</span><select id="assignmentYear">${years.map(y=>`<option value="${safe(y)}" ${String(y)===String(appState.assignmentYear)?"selected":""}>${safe(y==="all"?"Full period":y)}</option>`).join("")}</select></label>
      <label><span class="control-label">Hold exit horizon</span><select id="assignmentHorizon">${(controls.horizons || ["to_as_of"]).map(h=>`<option value="${safe(h)}" ${String(h)===String(appState.assignmentHorizon)?"selected":""}>${safe(assignmentHorizonLabel(h))}</option>`).join("")}</select></label>
      ${badge(`Redeployment assumption ${fmtPct(controls.opportunity_monthly_rate)} monthly, compounded`, "blue")}
    </div>
    <div class="grid metrics">
      ${card("Actual path P&L", fmtMoney(summary.actual_total_pnl), "Assigned put + post-assignment options + stock P&L", cls(summary.actual_total_pnl))}
      ${card("Hold counterfactual", fmtMoney(summary.counterfactual_total_pnl), `${assignmentHorizonLabel(appState.assignmentHorizon)} net of redeployment cost`, cls(summary.counterfactual_total_pnl))}
      ${card("Wheel advantage", fmtMoney(summary.delta_actual_minus_hold), "Actual path minus hold counterfactual", assignmentDeltaCls(summary.delta_actual_minus_hold, summary))}
      ${card("Ticker win rate", fmtPct(summary.actual_beat_hold_rate), "Share of selected rows where wheel beat hold")}
    </div>
    <div class="panel"><p>${safe(modeNote)} The hold counterfactual subtracts estimated redeployment income lost: assigned strike notional compounded at ${fmtPct(controls.opportunity_monthly_rate)} per month. Strike notional is cumulative context, not peak dollars held at one time.</p></div>
    <div style="height:12px"></div>
    <div class="grid two-even">${assignmentOutcomeChart(summary)}${assignmentTickerDeltaChart(tickerRows)}</div>
    <div style="height:12px"></div>
    ${assignmentComponentTable(summary)}
    ${sectionHead("Cohorts and Horizons")}
    <div class="grid two-even">${assignmentCohortChart(assignmentCohortRows())}${assignmentHorizonChart(horizonRows)}</div>
    ${sectionHead("Ticker Detail")}
    ${dataTable("assignment-tickers", tickerRows, [
      {key:"ticker",label:"Ticker",format:v=>`<strong>${safe(v)}</strong>`},
      {key:"lots",label:"Events",num:true},
      {key:"assigned_shares",label:"Shares assigned",num:true},
      {key:"put_pnl",label:"Put premiums",format:fmtMoney,num:true,className:cls},
      {key:"call_cashflow_pnl",label:"Call cashflows",format:fmtMoney,num:true,className:cls},
      {key:"actual_stock_pnl",label:"Stock P&L",format:fmtMoney,num:true,className:cls},
      {key:"actual_total_pnl",label:"Actual total",format:fmtMoney,num:true,className:cls},
      {key:"counterfactual_total_pnl",label:"Hold total",format:fmtMoney,num:true,className:cls},
      {key:"delta_actual_minus_hold",label:"Wheel advantage",format:fmtMoney,num:true,className:assignmentDeltaCls},
      {key:"delta_roi",label:"Advantage / notional",format:fmtPct,num:true,className:(v,row)=>assignmentDeltaCls(row.delta_actual_minus_hold,row)},
      {key:"decision_read",label:"Read",format:assignmentDecisionPill}
    ], {title:"Ticker assignment quality", subtitle:"Actual total = assigned-put premium + post-assignment puts + calls + stock P&L. Hold total is the selected hold counterfactual.", wide:true})}
    ${sectionHead("Open Call Cap Exposure")}
    ${dataTable("assignment-caps", aq.open_call_caps || [], [
      {key:"ticker",label:"Ticker"},
      {key:"assignment_date",label:"Assigned",format:fmtDate},
      {key:"remaining_shares",label:"Shares",num:true},
      {key:"covered_shares",label:"Covered",num:true},
      {key:"assignment_strike",label:"Basis",format:v=>fmtMoney(v,2),num:true},
      {key:"current_price",label:"Price",format:v=>fmtMoney(v,2),num:true},
      {key:"open_call_min_strike",label:"Call strike",format:v=>fmtMoney(v,2),num:true},
      {key:"cap_drag",label:"Cap drag",format:fmtMoney,num:true,className:cls}
    ], {title:"Covered-call cap drag", small:true})}
    ${sectionHead("Assignment Event Detail")}
    ${dataTable("assignment-lots", eventRows, [
      {key:"ticker",label:"Ticker"},
      {key:"assignment_date",label:"Assigned",format:fmtDate},
      {key:"assignment_year",label:"Year",num:true},
      {key:"expiration",label:"Put expiry",format:fmtDate},
      {key:"strike",label:"Strike",format:v=>fmtMoney(v,2),num:true},
      {key:"contracts",label:"Contracts",format:v=>fmtDec(v,0),num:true},
      {key:"original_shares",label:"Shares",num:true},
      {key:"months_held",label:"Months",format:v=>fmtDec(v,1),num:true},
      {key:"opportunity_cost",label:"Redeploy cost",format:fmtMoney,num:true},
      {key:"actual_total_pnl",label:"Actual total",format:fmtMoney,num:true,className:cls},
      {key:"counterfactual_before_opportunity",label:"Gross hold",format:fmtMoney,num:true,className:cls},
      {key:"counterfactual_total_pnl",label:"Hold total",format:fmtMoney,num:true,className:cls},
      {key:"delta_actual_minus_hold",label:"Wheel advantage",format:fmtMoney,num:true,className:assignmentDeltaCls},
      {key:"decision_read",label:"Read",format:assignmentDecisionPill}
    ], {title:"Assignment events", subtitle:"Identical contracts are grouped so repeated rows become one event.", wide:true, maxHeight:620})}
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
      {key:"dividends",label:"Dividends",format:fmtMoney,num:true,className:cls},
      {key:"combined_realized",label:"Total realized",format:fmtMoney,num:true,className:cls}
    ], {title:"Per-year realized P&L", compact:true})}
  `;
}
function expectancyByYearRows(){
  return (data.tables.expectancy_by_year || []).filter(r => appState.expectancyYear === "all" || String(r.Year) === appState.expectancyYear);
}
function benchmarkMetricRows(){
  return data.tables.benchmark_metrics || [];
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
    ${sectionHead("Benchmark Metrics", "WTPI was formerly PUTW. Its investment policy changed on 4 April 2025; earlier returns reflect the former policy.")}
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
    <div class="panel">
      <h3>IBKR import</h3>
      <p class="muted">Use this when Diagnostics shows a missing or deferred IBKR statement. It starts the import job; refresh data after it finishes.</p>
      <form id="importForm" method="post" action="/import?section=diagnostics">
        <button class="primary" type="submit">Retry IBKR import</button>
      </form>
    </div>
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
  if (!appState.decisionRulesSaved && !appState.decisionRulesLoading) {
    appState.decisionRulesLoading = true;
    fetch("/api/settings/decision-rules", {credentials: "same-origin", headers: {"Accept": "application/json"}})
      .then(response => response.ok ? response.json() : null)
      .then(saved => { if (saved) { appState.decisionRulesSaved = saved; if (appState.active === "settings") render(); } })
      .catch(() => {});
  }
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
    <div style="height:12px"></div>
    ${decisionRulesPanel()}
  `;
}
function renderMethodology(){
  $("methodology").innerHTML = `
    ${sectionHead("Methodology", "Same backend accounting as iOS, with web-only diagnostic breadth.")}
    <div class="grid two-even">
      <div class="panel"><h3>Source</h3><p>Production web and iOS read imported IBKR Flex data from Firestore. Streamlit remains the Google Sheets backup/control dashboard.</p><h3>Wheel scope</h3><p>Wheel P&L starts with assigned puts. Covered calls are included when backed by assignment-derived shares or valid covered-call roll replacements. Expected non-wheel exclusions are audit notes, not actionable issues.</p><h3>Realized option P&L</h3><p>Open options and continuing rolls remain outside realized P&L. Each linked roll carries premiums minus buybacks and fees forward; the full net result is recognized once at final close, expiration or assignment. Partial closures recognize only the quantity that ends.</p><h3>Active cycle</h3><p>The Dashboard target panel follows the nearest open option expiry cycle, while Monthly keeps closed calendar-month reporting and the future expiry table for reconciliation.</p></div>
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
  const importForm = $("importForm");
  if (importForm && !importForm.dataset.bound) {
    importForm.dataset.bound = "1";
    importForm.addEventListener("submit", () => {
      const button = importForm.querySelector("button");
      if (button) {
        button.disabled = true;
        button.textContent = "Starting import...";
      }
      setUpdating(true, "Starting IBKR import...");
    });
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
  const assignmentYear = $("assignmentYear"); if (assignmentYear) assignmentYear.addEventListener("change", (e) => { appState.assignmentYear = e.target.value; render(); });
  const assignmentHorizon = $("assignmentHorizon"); if (assignmentHorizon) assignmentHorizon.addEventListener("change", (e) => { appState.assignmentHorizon = e.target.value; render(); });
  const targetSettingsForm = $("targetSettingsForm");
  if (targetSettingsForm) targetSettingsForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const form = new FormData(targetSettingsForm);
    const nextTarget = Number(form.get("target_return_pct")) / 100;
    const nextFloor = Number(form.get("target_floor_pct")) / 100;
    if (!Number.isFinite(nextTarget) || !Number.isFinite(nextFloor)) return;
    const button = targetSettingsForm.querySelector("button[type='submit']");
    if (button) {
      button.disabled = true;
      button.textContent = "Saving...";
    }
    try {
      const response = await fetch("/api/settings/monthly-target-band", {
        method: "POST",
        credentials: "same-origin",
        headers: {"Content-Type": "application/json", "Accept": "application/json"},
        body: JSON.stringify({
          target_return: Math.max(nextTarget, 0),
          target_floor: Math.max(nextFloor, 0)
        })
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text.slice(0, 300) || `HTTP ${response.status}`);
      }
      const saved = await response.json();
      appState.targetReturn = Number(saved.target_return ?? nextTarget);
      appState.targetFloor = Number(saved.target_floor ?? nextFloor);
      updateUrlState();
      await loadDashboardData();
    } catch (err) {
      alert(`Could not save target band: ${(err && err.message) || err}`);
    } finally {
      if (button) {
        button.disabled = false;
        button.textContent = "Apply";
      }
    }
  });
  bindTodayControls();
  const optionFetch = $("fetchOptionData");
  if (optionFetch && !optionFetch.dataset.bound) {
    optionFetch.dataset.bound = "1";
    optionFetch.addEventListener("click", async () => {
      optionFetch.disabled = true;
      optionFetch.textContent = "Fetching...";
      setUpdating(true, "Fetching option data...");
      try {
        const response = await fetch("/api/decision-lab/options/refresh" + window.location.search, {
          method: "POST",
          credentials: "same-origin",
          headers: {"Accept": "application/json"}
        });
        const refreshed = await readDecisionLabResponse(response);
        data.decision_lab = refreshed;
        appState.decisionLab = refreshed;
        render();
      } catch (err) {
        optionFetch.disabled = false;
        optionFetch.textContent = "Fetch option data";
        alert(err && err.message || err);
      } finally {
        setUpdating(false);
      }
    });
  }
}
const DECISION_SECTIONS = ["decision_lab", "today", "decision_cards"];
const DEFAULT_DECISION_RULES = {annual_target_return: 0.20, put_delta_min: 0.15, put_delta_max: 0.25, dte_min: 20, dte_max: 45};
const GLOSSARY = {
  "RoAC": "Return on average capital: P&L divided by the average of put reserve plus share value.",
  "Realized this cycle": "Completed-strategy P&L already recognized for options expiring in this cycle (realized cycle P&L).",
  "Premium still open": "Net credit carried on open option chains expiring this cycle, after linked rolls and commissions (open strategy balance). It can be negative.",
  "Stock P&L if ITM calls assign": "Assigned-stock P&L, capped at the call strike, for in-the-money covered calls expiring this cycle (ITM call stock P&L).",
  "Loss if ITM puts assign": "Strike minus current price on in-the-money puts expiring this cycle (ITM put assignment P&L).",
  "Put notional": "Strike x 100 x contracts for every open short put, whether or not it is in the money.",
  "Cash if ITM puts assign": "Cash needed to take assignment of every put that is currently in the money.",
  "Chain net credit": "The carried net credit of the open option chain, after every linked roll and commission.",
  "Better or worse than keeping": "Scenario estimate of this option minus the scenario estimate of keeping the current position. Delta is used as a rough exercise weight; it is not a forecast."
};
function def(label){ const text = GLOSSARY[label]; return text ? `<abbr class="def" title="${safe(text)}">${safe(label)}</abbr>` : safe(label); }
function decisionRules(){ return {...DEFAULT_DECISION_RULES, ...(decisionData().decision_rules || {})}; }
function decisionReady(){
  const decision = decisionData();
  if (decision.deferred || (!appState.decisionLab && !decision.error)) {
    if (!appState.decisionLabLoading && !appState.decisionLabError) setTimeout(loadDecisionLab, 0);
    return false;
  }
  return true;
}
function decisionPending(id){
  const target = $(id);
  if (appState.decisionLabError) {
    return `<div class="panel"><h3>Action queue unavailable</h3><p class="error">${safe(appState.decisionLabError)}</p><button class="secondary" type="button" data-retry-decision>Retry</button></div>`;
  }
  return `<div class="panel loading-panel" style="min-height:160px"><div><h3>Loading the action queue...</h3><p>Reading Decision Lab classifications and stored option quotes.</p></div></div>`;
}
function daysOld(value){
  const d = parseDate(value);
  if (!d) return null;
  const asOf = parseDate(data.dashboard?.request?.as_of || data.generated_at) || new Date();
  return Math.round((asOf - d) / 86400000);
}
function freshnessChip(){
  const meta = data.source_metadata || {};
  const fresh = data.dashboard?.data_freshness || {};
  const coverage = fresh.price_coverage || {};
  const issues = data.dashboard?.issue_summary?.total_count || 0;
  const toDate = meta.ibkr_import_to_date;
  const status = (decisionData().option_market_data || {}).status || {};
  const quoteDate = status.quote_date || status.latest_quote_date || (status.last_fetched_at ? String(status.last_fetched_at).slice(0,10) : null);
  const missing = coverage.missing_count ?? Math.max((coverage.required_count || 0) - (coverage.priced_count || 0), 0);
  const age = daysOld(toDate);
  let tone = "";
  if (issues > 0 || missing > 0) tone = "bad";
  else if (age === null || age > 3) tone = "warn";
  const parts = [
    `IBKR statement to <b>${safe(toDate ? fmtDate(toDate) : "unknown")}</b>`,
    `prices <b>${safe(fmtLocalTime(fresh.prices_updated_at))}</b>${missing ? ` (${safe(missing)} missing)` : ""}`,
    `quotes <b>${safe(quoteDate ? fmtDate(quoteDate) : "not fetched")}</b>`
  ];
  const extra = issues ? `<span class="td-chip"><span class="td-dot bad"></span>${safe(issues)} data issue${issues === 1 ? "" : "s"}, see Diagnostics</span>` : "";
  return `<div class="td-fresh"><span class="td-chip"><span class="td-dot ${tone}"></span><span>${parts.join(" · ")}</span></span>${extra}</div>`;
}
function bandColorVar(v, floor, target){
  if (numeric(v) === null) return "var(--muted)";
  if (v < 0) return "var(--color-negative)";
  if (floor !== null && v < floor) return "var(--color-attention)";
  if (target !== null && v <= target) return "var(--color-positive)";
  return "var(--color-info)";
}
function cycleHero(){
  const c = decisionActiveCycle();
  if (!c || !Object.keys(c).length) return `<div class="panel td-kpi"><div class="metric-label">Active cycle</div><p class="muted">No open options in an active expiry cycle.</p></div>`;
  const target = numeric(c.target_return ?? targetReturn());
  const floor = numeric(c.target_floor ?? targetFloor());
  const proj = numeric(c.projected_return_roac);
  const lo = Math.min(-0.005, proj ?? 0);
  const hi = Math.max((target ?? 0.02) * 1.3, (proj ?? 0) + 0.002);
  const x = v => `${((v - lo) / (hi - lo) * 100).toFixed(2)}%`;
  const seg = (a, b, color) => b > a ? `<div class="seg" style="left:${x(a)};width:calc(${x(b)} - ${x(a)});background:${color}"></div>` : "";
  const color = bandColorVar(proj, floor, target);
  const gauge = `<div class="td-gauge" role="img" aria-label="Projected RoAC ${safe(fmtPct(proj))} against the ${safe(fmtPct(floor))} to ${safe(fmtPct(target))} band">
    ${seg(lo, 0, "var(--color-negative)")}${floor !== null ? seg(0, floor, "var(--color-attention)") : ""}${floor !== null && target !== null ? seg(floor, target, "var(--color-positive)") : ""}${target !== null ? seg(target, hi, "var(--color-info)") : ""}
    ${proj !== null ? `<div class="fill" style="left:${x(Math.min(0, proj))};width:calc(${x(Math.max(0, proj))} - ${x(Math.min(0, proj))});background:${color}"></div><div class="mk" style="left:${x(proj)}"></div>` : ""}
  </div>
  <div class="td-scale"><span style="left:${x(0)}">0</span>${floor !== null ? `<span style="left:${x(floor)}">${safe(fmtPct(floor))}</span>` : ""}${target !== null ? `<span style="left:${x(target)}">${safe(fmtPct(target))}</span>` : ""}</div>`;
  const remaining = numeric(c.remaining_to_target);
  const status = proj === null ? "" : proj < 0 ? "below zero" : floor !== null && proj < floor ? "below your band" : target !== null && proj <= target ? "inside your band" : "above your target";
  const tile = (label, value) => `<div><b class="mono ${numeric(value) < 0 ? "neg" : ""}">${fmtMoney(value)}</b><span>${def(label)}</span></div>`;
  return `<div class="panel td-kpi">
    <div class="metric-label">${safe(c.cycle_label || "Active cycle")} · expiries ${safe((c.expiry_dates || []).map(fmtDate).join(", ") || "n/a")} · ${safe(c.min_dte ?? "n/a")}-${safe(c.max_dte ?? "n/a")} days left</div>
    <div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap"><div class="td-big" style="color:${color}">${safe(fmtPct(proj))}</div>
    <div class="td-sub">projected ${def("RoAC")} · ${safe(status)}${remaining !== null && remaining > 0 ? ` · <span class="warn-text mono">${fmtMoney(remaining)}</span> short of target` : ""}</div></div>
    ${gauge}
    <div class="td-build">${tile("Realized this cycle", c.realized_cycle_pnl)}${tile("Premium still open", c.open_premium_collected)}${tile("Stock P&L if ITM calls assign", c.itm_call_stock_pnl)}${tile("Loss if ITM puts assign", c.itm_put_unrealized_loss)}</div>
  </div>`;
}
function ytdPanel(){
  const rules = decisionRules();
  const asOf = parseDate(data.dashboard?.request?.as_of || data.generated_at) || new Date();
  const year = asOf.getUTCFullYear();
  const row = (yearlyRows() || []).find(r => Number(r.year) === year) || {};
  const roac = numeric(row.roac_year);
  const start = Date.UTC(year, 0, 1), end = Date.UTC(year + 1, 0, 1);
  const pace = numeric(rules.annual_target_return) === null ? null : rules.annual_target_return * ((asOf - start) / (end - start));
  const gap = roac !== null && pace !== null ? roac - pace : null;
  const scaleMax = Math.max(rules.annual_target_return || 0.2, roac || 0, 0.01);
  const w = v => `${Math.max(0, Math.min(100, v / scaleMax * 100)).toFixed(2)}%`;
  const snap = currentSnapshot();
  return `<div class="panel td-kpi">
    <div class="metric-label">${safe(year)} to date · ${def("RoAC")}</div>
    <div class="td-big">${safe(fmtPct(roac))}</div>
    <div class="td-sub">${safe(fmtPct(rules.annual_target_return))} a year means ${safe(fmtPct(pace))} by today${gap !== null ? ` · <span class="${gap < 0 ? "warn-text" : "pos"}">${safe((Math.abs(gap) * 100).toFixed(1))} pts ${gap < 0 ? "behind" : "ahead"}</span>` : ""}</div>
    ${roac !== null ? `<div class="td-pace" role="img" aria-label="Year-to-date RoAC against pace"><div class="fill" style="width:${w(Math.max(roac, 0))};background:${gap !== null && gap < 0 ? "var(--color-attention)" : "var(--color-positive)"}"></div>${pace !== null ? `<div class="mk" style="left:${w(pace)}"></div>` : ""}</div>` : `<p class="muted">${safe(row.suppression_reason ? "RoAC is suppressed while capital history is incomplete." : "No RoAC for this year yet.")}</p>`}
    <div class="td-sub" style="margin-top:8px">YTD P&L <span class="mono">${fmtMoney(snap.ytd_total_pnl)}</span>${snap.unrealized_adjusted ? `, of which unrealized <span class="mono">${fmtMoney(snap.current_unrealized_pnl)}</span>` : " (realized only)"}</div>
  </div>`;
}
function capitalPanel(){
  const c = decisionActiveCycle() || {};
  const snap = currentSnapshot();
  const inventory = data.positions?.inventory || [];
  const atCost = inventory.reduce((sum, r) => sum + (numeric(r.shares) || 0) * (numeric(r.cost_per_share) || 0), 0);
  const cash = numeric(snap.available_cash);
  return `<div class="panel td-kpi">
    <div class="metric-label">Capital at work</div>
    <div class="td-big mono" style="font-size:28px">${fmtCompactMoney(c.portfolio_put_exposure)}</div>
    <div class="td-sub" style="margin-bottom:8px">${def("Put notional")}</div>
    <div class="td-line"><span>In the money</span><b class="${(numeric(c.portfolio_itm_put_exposure) || 0) > 0 ? "neg" : ""}">${fmtCompactMoney(c.portfolio_itm_put_exposure)}</b></div>
    <div class="td-line"><span>${def("Cash if ITM puts assign")}</span><b>${fmtCompactMoney(snap.itm_put_cash_required ?? c.portfolio_itm_put_exposure)}</b></div>
    <div class="td-line"><span>Assigned shares at cost</span><b>${fmtCompactMoney(atCost)}</b></div>
    <div class="td-line"><span>Available cash</span><b class="${cash === null ? "muted" : ""}">${cash === null ? "not imported" : fmtCompactMoney(cash)}</b></div>
  </div>`;
}
function decisionCardFor(ticker){
  return (decisionData().decision_cards || []).find(card => String(card.ticker) === String(ticker));
}
function fmtCardValue(value, format){
  if (format === "text") return safe(value);
  if (numeric(value) === null) return "n/a";
  if (format === "money") return fmtMoney(value);
  if (format === "price") return fmtMoney(value, 2);
  if (format === "delta") return Math.abs(Number(value)).toFixed(2);
  if (format === "days") return `${fmtNum(value)}`;
  if (format === "shares") return `${fmtNum(value)} sh`;
  return safe(value);
}
function decisionCardHtml(card, inline=false){
  if (!card) return "";
  const options = card.options || [];
  const cols = Math.max(2, Math.min(4, options.length));
  const quote = card.quote_date ? ` · quotes ${safe(fmtDate(card.quote_date))}` : "";
  const grid = options.length ? `<div class="dc-grid" style="--cols:${cols}">${options.map(opt => {
    const head = opt.headline || {};
    const tone = opt.kind === "close" || opt.kind === "proposal" ? cls(head.value) : "";
    const metrics = (opt.metrics || []).map(m => `<div class="td-line"><span>${def(m.label)}</span><b class="mono">${fmtCardValue(m.value, m.format)}</b></div>`).join("");
    const chips = (opt.checks || []).map(ch => `<span class="dc-chip ${ch.status === "ok" ? "ok" : ch.status === "fail" ? "fail" : ch.status === "near" ? "near" : ""}">${ch.status === "ok" ? "✓ " : ch.status === "fail" ? "✗ " : ch.status === "near" ? "≈ " : ""}${safe(ch.text)}</span>`).join("");
    return `<div class="dc-opt ${opt.top ? "top" : ""}" title="${safe(opt.reason || "")}">
      <h5>${safe(opt.title)}${opt.top ? ` <span class="dc-chip ok">top ranked</span>` : ""}<small>${safe(opt.contract || "")}</small></h5>
      <div class="dc-head-num ${tone}">${fmtCardValue(head.value, head.format)}<small>${def(head.label === "chain net credit so far" ? "Chain net credit" : head.label)}</small></div>
      ${metrics}
      ${chips ? `<div class="dc-chips">${chips}</div>` : ""}
    </div>`;
  }).join("")}</div>` : "";
  const verdict = card.verdict || {};
  return `<div class="dc ${inline ? "td-inline" : ""}">
    <div class="dc-head"><h4>${safe(card.ticker)} · ${safe(card.category)}</h4><span>${safe(card.objective || "")}${numeric(card.current_price) !== null ? ` · stock ${fmtMoney(card.current_price, 2)}` : ""}${quote}</span></div>
    ${grid}
    <div class="dc-verdict ${verdict.tone === "positive" ? "" : "neutral"}"><b>${verdict.tone === "positive" ? "Suggestion" : "Note"}:</b> ${safe(verdict.text || "No comparison available.")}</div>
  </div>`;
}
function queueValue(row){
  const value = numeric(row.signal_value);
  return `<div class="val"><span class="mono ${value !== null && value < 0 ? "neg" : ""}">${fmtMoney(value)}</span><span>${safe(row.signal_label || "")}</span></div>`;
}
function queueRowHtml(row){
  const open = appState.todayOpen === row.ticker;
  const when = row.expiry ? `${fmtDate(row.expiry)}${numeric(row.dte) !== null ? ` · ${fmtNum(row.dte)} days` : ""}` : "no expiry";
  const button = `<button type="button" class="td-row ${open ? "open" : ""}" data-today-row="${safe(row.ticker)}" aria-expanded="${open ? "true" : "false"}">
    <span class="stripe ${safe(row.group)}"></span>
    <span class="tk">${safe(row.ticker)}</span>
    <span class="what"><b>${safe(row.category)}</b><span>${safe(row.reason || "")} · ${safe(when)}</span></span>
    <span class="move"><b>${safe(row.recommendation || "")}</b><span>${row.has_card ? `<span class="td-more">${open ? "hide options ▴" : "compare options ▾"}</span>` : "no priced options yet"}</span></span>
    ${queueValue(row)}
  </button>`;
  return open ? button + decisionCardHtml(decisionCardFor(row.ticker), true) : button;
}
function noActionCount(queue){
  const active = new Set(queue.map(r => String(r.ticker)));
  return (data.tickers?.items || []).filter(r => ((numeric(r.open_option_count) || 0) > 0 || (numeric(r.inventory_share_count) || 0) > 0) && !active.has(String(r.ticker))).length;
}
function queuePanel(){
  const queue = decisionData().action_queue || [];
  const filter = appState.todayGroup || "all";
  const counts = {now: 0, plan: 0, monitor: 0};
  queue.forEach(r => { counts[r.group] = (counts[r.group] || 0) + 1; });
  const tabs = [["all", "All", queue.length], ["now", "Act now", counts.now], ["plan", "Plan this week", counts.plan], ["monitor", "Monitor", counts.monitor]];
  const labels = {now: "Act now", plan: "Plan this week", monitor: "Monitor"};
  let body = "";
  ["now", "plan", "monitor"].forEach(group => {
    if (filter !== "all" && filter !== group) return;
    const rows = queue.filter(r => r.group === group);
    if (!rows.length) return;
    body += `<div class="td-group">${safe(labels[group])}</div>${rows.map(queueRowHtml).join("")}`;
  });
  if (!body) body = `<div class="empty">No tickers in this group.</div>`;
  const idle = noActionCount(queue);
  return `<div class="panel" style="padding:0">
    <div class="td-ph"><h3>Action queue</h3><span>${safe(queue.length)} need a look${idle ? ` · ${safe(idle)} with no action` : ""} · click a row to compare options</span></div>
    <div class="td-tabs">${tabs.map(([key, label, n]) => `<button type="button" data-today-group="${key}" class="${filter === key ? "active" : ""}">${safe(label)} ${safe(n)}</button>`).join("")}</div>
    ${body}
  </div>`;
}
function riskMapSvg(){
  const rows = (data.positions?.open_option_shorts || data.open_shorts?.items || []).filter(r => numeric(r.moneyness) !== null && numeric(r.days_to_expiration) !== null);
  if (!rows.length) return `<div class="empty">No open shorts with prices.</div>`;
  const W = 360, H = 240, p = {l: 36, r: 12, t: 10, b: 40};
  const maxD = Math.max(50, ...rows.map(r => Number(r.days_to_expiration)));
  const ys = rows.map(r => Number(r.moneyness) * 100);
  const yTop = Math.max(6, Math.ceil(Math.max(...ys) + 1)), yBot = Math.min(-15, Math.floor(Math.min(...ys) - 1));
  const x = d => p.l + (Math.min(d, maxD) / maxD) * (W - p.l - p.r);
  const y = v => p.t + ((yTop - v) / (yTop - yBot)) * (H - p.t - p.b);
  let s = `<rect x="${p.l}" y="${y(yTop)}" width="${W - p.l - p.r}" height="${y(0) - y(yTop)}" fill="var(--color-negative)" opacity=".1"/>`;
  s += `<rect x="${p.l}" y="${y(0)}" width="${W - p.l - p.r}" height="${y(-5) - y(0)}" fill="var(--color-attention)" opacity=".08"/>`;
  const step = (yTop - yBot) > 30 ? 10 : 5;
  for (let v = Math.ceil(yBot / step) * step; v <= yTop; v += step) {
    s += `<line x1="${p.l}" x2="${W - p.r}" y1="${y(v)}" y2="${y(v)}" stroke="var(--line2)" stroke-width="${v === 0 ? 1.2 : .6}"/><text x="${p.l - 5}" y="${y(v) + 3}" text-anchor="end">${v > 0 ? "+" : ""}${v}%</text>`;
  }
  const dStep = maxD > 60 ? 20 : 10;
  for (let d = 0; d <= maxD; d += dStep) s += `<text x="${x(d)}" y="${H - p.b + 14}" text-anchor="middle">${d}d</text>`;
  s += `<text x="${W - p.r}" y="${y(yTop) + 11}" text-anchor="end" style="fill:var(--color-negative)">in the money</text>`;
  const placed = [];
  rows.forEach(r => {
    const v = Number(r.moneyness) * 100, d = Number(r.days_to_expiration);
    const color = v >= 0 ? "var(--color-negative)" : v >= -5 ? "var(--color-attention)" : "var(--muted)";
    const isCall = String(r.option_type || "").toLowerCase().startsWith("call");
    const title = `<title>${safe(r.ticker)} ${safe(r.option_type)} ${safe(r.strike)} · ${safe(fmtDate(r.expiration))} · ${(v >= 0 ? v.toFixed(1) + "% ITM" : Math.abs(v).toFixed(1) + "% OTM")}</title>`;
    s += isCall
      ? `<rect x="${x(d) - 4.5}" y="${y(v) - 4.5}" width="9" height="9" fill="none" stroke="${color}" stroke-width="2">${title}</rect>`
      : `<circle cx="${x(d)}" cy="${y(v)}" r="5" fill="${color}" stroke="var(--color-bg)" stroke-width="1.5">${title}</circle>`;
    if (v > -5) {
      let ly = y(v) + 3;
      while (placed.some(([px, py]) => Math.abs(px - x(d)) < 40 && Math.abs(py - ly) < 11)) ly += 11;
      placed.push([x(d), ly]);
      s += `<text x="${x(d) + 8}" y="${ly}" style="fill:var(--text)">${safe(r.ticker)}</text>`;
    }
  });
  s += `<circle cx="${p.l + 4}" cy="${H - 9}" r="4" fill="var(--muted)"/><text x="${p.l + 12}" y="${H - 6}">put</text><rect x="${p.l + 40}" y="${H - 13}" width="8" height="8" fill="none" stroke="var(--muted)" stroke-width="1.6"/><text x="${p.l + 52}" y="${H - 6}">call</text><text x="${W - p.r}" y="${H - 6}" text-anchor="end">days to expiry vs % to strike</text>`;
  return `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Open shorts by days to expiry and distance to strike">${s}</svg>`;
}
function ladderSvg(){
  const puts = (data.positions?.open_option_shorts || data.open_shorts?.items || []).filter(r => String(r.option_type || "").toLowerCase().startsWith("put"));
  const byExpiry = new Map();
  puts.forEach(r => {
    const key = fmtDate(r.expiration);
    const g = byExpiry.get(key) || {total: 0, itm: 0};
    const notional = numeric(r.notional_at_strike) ?? (Math.abs(numeric(r.quantity) || 0) * (numeric(r.strike) || 0) * 100);
    g.total += notional;
    if ((numeric(r.moneyness) || -1) >= 0) g.itm += notional;
    byExpiry.set(key, g);
  });
  const rows = [...byExpiry.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  if (!rows.length) return `<div class="empty">No open puts.</div>`;
  const W = 360, rowH = 34, p = {l: 70, r: 64};
  const max = Math.max(...rows.map(([, g]) => g.total), 1);
  const x = v => p.l + (v / max) * (W - p.l - p.r);
  let s = "";
  rows.forEach(([label, g], i) => {
    const yy = 6 + i * rowH;
    s += `<text x="0" y="${yy + 13}">${safe(dateLabel(label, "day-month"))}</text>`;
    s += `<rect x="${p.l}" y="${yy + 2}" width="${Math.max(2, x(g.total) - p.l)}" height="15" rx="3" fill="var(--color-surface-muted)" stroke="var(--line2)"/>`;
    if (g.itm > 0) s += `<rect x="${p.l}" y="${yy + 2}" width="${Math.max(2, x(g.itm) - p.l)}" height="15" rx="3" fill="var(--color-negative)" opacity=".85"/><text x="${p.l}" y="${yy + 30}" style="fill:var(--color-negative)">${safe(fmtCompactMoney(g.itm))} ITM</text>`;
    s += `<text x="${x(g.total) + 6}" y="${yy + 14}" style="fill:var(--text)">${safe(fmtCompactMoney(g.total))}</text>`;
  });
  return `<svg viewBox="0 0 ${W} ${rows.length * rowH + 8}" role="img" aria-label="Put notional by expiry">${s}</svg>`;
}
function renderToday(){
  const ready = decisionReady();
  $("today").innerHTML = `
    ${freshnessChip()}
    <div class="td-hero">${cycleHero()}${ytdPanel()}${capitalPanel()}</div>
    <div class="td-main">
      <div>${ready ? queuePanel() : decisionPending("today")}</div>
      <div class="td-side">
        <div class="panel" style="padding:0"><div class="td-ph"><h3>Risk map</h3><span>open shorts</span></div><div class="td-svg">${riskMapSvg()}</div></div>
        <div class="panel" style="padding:0"><div class="td-ph"><h3>Put notional by expiry</h3><span>in the money in red</span></div><div class="td-svg">${ladderSvg()}</div></div>
      </div>
    </div>
    <p class="footnote">New layout preview. It reads the same data as Dashboard and Decision Lab and changes no calculations. Hover a dotted label for its definition.</p>`;
}
function renderDecisionCards(){
  const ready = decisionReady();
  const rules = decisionRules();
  const status = (decisionData().option_market_data || {}).status || {};
  const head = `${freshnessChip()}
    <div class="td-rules"><span>Rule checks: put delta ${safe(fmtDec(rules.put_delta_min, 2))}-${safe(fmtDec(rules.put_delta_max, 2))} · ${safe(fmtNum(rules.dte_min))}-${safe(fmtNum(rules.dte_max))} DTE · spread within 35% of mid.</span><a href="?section=settings" data-goto-section="settings">Edit in Settings</a>
    <button class="action-btn" type="button" data-fetch-options>Fetch option data</button><span class="mono">${safe(status.provider ? optionDataStatusText() : "")}</span></div>`;
  if (!ready) { $("decision_cards").innerHTML = head + decisionPending("decision_cards"); return; }
  const cards = decisionData().decision_cards || [];
  const filter = appState.cardsGroup || "all";
  const shown = cards.filter(c => filter === "all" || c.group === filter);
  const counts = {now: 0, plan: 0, monitor: 0};
  cards.forEach(c => { counts[c.group] = (counts[c.group] || 0) + 1; });
  const tabs = [["all", "All", cards.length], ["now", "Act now", counts.now], ["plan", "Plan this week", counts.plan], ["monitor", "Monitor", counts.monitor]];
  const without = (decisionData().action_queue || []).filter(r => !r.has_card);
  $("decision_cards").innerHTML = `${head}
    <div class="td-tabs" style="padding:0 0 12px">${tabs.map(([key, label, n]) => `<button type="button" data-cards-group="${key}" class="${filter === key ? "active" : ""}">${safe(label)} ${safe(n)}</button>`).join("")}</div>
    <div class="dc-list">${shown.length ? shown.map(card => decisionCardHtml(card)).join("") : `<div class="panel empty">No decision cards in this group.</div>`}</div>
    ${without.length ? `<p class="footnote">No option comparison for ${without.map(r => `${safe(r.ticker)} (${safe(r.category)})`).join(", ")}: the Decision Lab does not price options for these situations.</p>` : ""}
    <p class="footnote">Keep and Close come from the current position; alternatives are the Decision Lab's top two scored candidates. Scenario values use delta as a rough exercise weight and are not forecasts.</p>`;
}
async function fetchOptionDataFrom(button){
  button.disabled = true;
  button.textContent = "Fetching...";
  setUpdating(true, "Fetching option data...");
  try {
    const response = await fetch("/api/decision-lab/options/refresh" + window.location.search, {method: "POST", credentials: "same-origin", headers: {"Accept": "application/json"}});
    const refreshed = await readDecisionLabResponse(response);
    data.decision_lab = refreshed;
    appState.decisionLab = refreshed;
    render();
  } catch (err) {
    button.disabled = false;
    button.textContent = "Fetch option data";
    appState.decisionLabError = "";
    alert(err && err.message || err);
  } finally {
    setUpdating(false);
  }
}
function bindTodayControls(){
  const scope = document.querySelector(".section.active") || document;
  scope.querySelectorAll("[data-today-row]").forEach(btn => btn.addEventListener("click", () => {
    appState.todayOpen = appState.todayOpen === btn.dataset.todayRow ? null : btn.dataset.todayRow;
    render();
  }));
  scope.querySelectorAll("[data-today-group]").forEach(btn => btn.addEventListener("click", () => { appState.todayGroup = btn.dataset.todayGroup; render(); }));
  scope.querySelectorAll("[data-cards-group]").forEach(btn => btn.addEventListener("click", () => { appState.cardsGroup = btn.dataset.cardsGroup; render(); }));
  scope.querySelectorAll("[data-retry-decision]").forEach(btn => btn.addEventListener("click", () => { appState.decisionLabError = ""; loadDecisionLab(); }));
  scope.querySelectorAll("[data-fetch-options]").forEach(btn => btn.addEventListener("click", () => fetchOptionDataFrom(btn)));
  scope.querySelectorAll("[data-goto-section]").forEach(link => link.addEventListener("click", (e) => {
    e.preventDefault();
    appState.active = link.dataset.gotoSection;
    updateUrlState();
    render();
    window.scrollTo({top: 0, behavior: "instant"});
  }));
  const rulesForm = $("decisionRulesForm");
  if (rulesForm && !rulesForm.dataset.bound) {
    rulesForm.dataset.bound = "1";
    rulesForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const form = new FormData(rulesForm);
      const body = {
        annual_target_return: Number(form.get("annual_target_pct")) / 100,
        put_delta_min: Number(form.get("put_delta_min")),
        put_delta_max: Number(form.get("put_delta_max")),
        dte_min: Number(form.get("dte_min")),
        dte_max: Number(form.get("dte_max"))
      };
      const button = rulesForm.querySelector("button[type='submit']");
      if (button) { button.disabled = true; button.textContent = "Saving..."; }
      try {
        const response = await fetch("/api/settings/decision-rules", {method: "POST", credentials: "same-origin", headers: {"Content-Type": "application/json", "Accept": "application/json"}, body: JSON.stringify(body)});
        const saved = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(saved.error || `HTTP ${response.status}`);
        appState.decisionRulesSaved = saved;
        appState.decisionLab = null;
        data.decision_lab = {deferred: true};
        render();
      } catch (err) {
        alert(`Could not save decision rules: ${(err && err.message) || err}`);
      } finally {
        if (button) { button.disabled = false; button.textContent = "Apply"; }
      }
    });
  }
}
function decisionRulesPanel(){
  const rules = {...decisionRules(), ...(appState.decisionRulesSaved || {})};
  const input = (name, label, value, step, min, max) => `<label><span class="control-label">${safe(label)}</span><input name="${name}" id="rule-${name}" type="number" min="${min}" max="${max}" step="${step}" value="${safe(value)}" style="min-width:110px"></label>`;
  return `<div class="panel">
    <h3>Decision rules</h3>
    <p class="muted">Used by the Today and Decision cards tabs to check each option against your entry rules, and for the yearly pace. They do not filter candidates or change any calculation.</p>
    <form id="decisionRulesForm" class="toolbar" style="align-items:end">
      ${input("annual_target_pct", "Annual target %", fmtPctNumber(rules.annual_target_return), "0.5", 0, 100)}
      ${input("put_delta_min", "Put delta min", rules.put_delta_min, "0.01", 0, 1)}
      ${input("put_delta_max", "Put delta max", rules.put_delta_max, "0.01", 0, 1)}
      ${input("dte_min", "DTE min", rules.dte_min, "1", 0, 365)}
      ${input("dte_max", "DTE max", rules.dte_max, "1", 0, 365)}
      <button class="primary" type="submit">Apply</button>
    </form>
  </div>`;
}

const sectionRenderers = {
  dashboard: renderDashboard,
  today: renderToday,
  decision_cards: renderDecisionCards,
  decision_lab: renderDecisionLab,
  assignment_quality: renderAssignmentQuality,
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
  const newSections = new Set(["today","decision_cards"]);
  $("nav").innerHTML = sections.map(([id,label]) => `<button type="button" data-section="${id}" class="${id === appState.active ? "active" : ""}">${safe(label)}${newSections.has(id) ? `<span class="new-tag">NEW</span>` : ""}</button>`).join("");
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
).replace("__DECISION_RESPONSE_JS__", DECISION_RESPONSE_JS)
