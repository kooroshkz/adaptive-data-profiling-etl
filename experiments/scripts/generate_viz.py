#!/usr/bin/env python3
"""
Generate a self-contained interactive HTML visualization from scored anomaly
prediction data.

Output: /tmp/anomaly_viz/index.html  (open in any browser, no server needed)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from utils.paths import AUTOML_DIR, latest_scored_dir

MAX_TN = 4000  # downsample normal points per series (browser performance)


# ── Data loading ──────────────────────────────────────────────────────────────

def build_dataset(scored_dir: Path) -> dict:
    cities_seen, cols_seen = set(), set()
    datasets: dict = {}

    for fpath in sorted(scored_dir.glob("scored_*_univariate_*.csv")):
        df = pd.read_csv(fpath)
        if df.empty:
            continue

        city  = df["city_id"].iloc[0]
        col   = df["target_column"].iloc[0]
        model = df["model_name"].iloc[0]
        cont  = float(df["contamination"].iloc[0])
        cities_seen.add(city)
        cols_seen.add(col)

        df["ts"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%dT%H:%M")

        lv = df["label"].value_counts()
        tp, fp, fn, tn = (int(lv.get(l, 0)) for l in ("TP", "FP", "FN", "TN"))
        p  = tp / (tp + fp) if tp + fp else 0.0
        r  = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * p * r / (p + r) if (p + r) else 0.0
        f2 = 5 * p * r / (4*p + r) if (4*p + r) else 0.0

        flagged = df[df["y_pred"] == 1]["anomaly_prob"]
        thresh_prob = float(flagged.min()) if len(flagged) else 0.0

        def pack(mask):
            s = df[mask].copy()
            cd = np.column_stack([
                s["anomaly_prob"].values,
                s["anomaly_score"].values,
                s["score_percentile"].values,
                s["shift_pct"].fillna(0).values,
                s["original_value"].fillna(-9999).values,
            ]).round(4)
            return {"t": s["ts"].tolist(), "v": s["y_value"].round(4).tolist(), "cd": cd.tolist()}

        tn_df  = df[df["label"] == "TN"]
        step   = max(1, len(tn_df) // MAX_TN)
        tn_df  = tn_df.iloc[::step].head(MAX_TN)
        tn_cd  = np.column_stack([
            tn_df["anomaly_prob"].values,
            tn_df["anomaly_score"].values,
            tn_df["score_percentile"].values,
            np.zeros(len(tn_df)),
            np.full(len(tn_df), -9999.0),
        ]).round(4)

        key = f"{city}:{col}"
        datasets[key] = {
            "city": city, "col": col, "model": model,
            "cont": round(cont, 6),
            "best_f2": round(float(df["best_obj_f2"].iloc[0]), 4),
            "thresh_prob": round(thresh_prob, 4),
            "m": {"p": round(p,4), "r": round(r,4), "f1": round(f1,4), "f2": round(f2,4),
                  "tp": tp, "fp": fp, "fn": fn, "tn": tn},
            "TN": {"t": tn_df["ts"].tolist(), "v": tn_df["y_value"].round(4).tolist(), "cd": tn_cd.tolist()},
            "TP": pack(df["label"] == "TP"),
            "FP": pack(df["label"] == "FP"),
            "FN": pack(df["label"] == "FN"),
        }

    cities = sorted(cities_seen)
    cols   = sorted(cols_seen)
    col_labels = {c: c.replace("_2m","").replace("_7_to_28cm","").replace("_"," ").title() for c in cols}
    return {"cities": cities, "cols": cols, "colLabels": col_labels, "ds": datasets}


# ── HTML template ─────────────────────────────────────────────────────────────
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Anomaly Detection Explorer</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js" charset="utf-8"></script>
<style>
:root{
  --blue:#2563EB;--red:#DC2626;--green:#16A34A;--orange:#EA580C;
  --leiden:#002E68;--bg:#F8FAFC;--card:#FFFFFF;--border:#E2E8F0;
  --text:#0F172A;--muted:#64748B;
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:var(--bg);color:var(--text)}
header{background:var(--leiden);color:#fff;padding:16px 24px;display:flex;flex-wrap:wrap;align-items:center;gap:16px;position:sticky;top:0;z-index:100;box-shadow:0 2px 8px rgba(0,0,0,.3)}
header h1{font-size:1.1rem;font-weight:700;white-space:nowrap}
.ctrl-row{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-left:auto}
.ctrl-row label{font-size:.78rem;color:#CBD5E1;display:flex;flex-direction:column;gap:3px}
.ctrl-row select{background:#1E3A5F;border:1px solid #4A6FA5;color:#EFF6FF;padding:5px 10px;border-radius:6px;font-size:.85rem;cursor:pointer;min-width:140px}
.badge{font-size:.72rem;background:rgba(255,255,255,.12);padding:3px 10px;border-radius:12px;color:#BFDBFE;white-space:nowrap}
.metric-strip{background:var(--card);border-bottom:1px solid var(--border);display:flex;flex-wrap:wrap}
.metric-card{flex:1 1 80px;text-align:center;padding:10px 6px;border-right:1px solid var(--border)}
.metric-card:last-child{border-right:none}
.metric-card .lbl{font-size:.65rem;text-transform:uppercase;letter-spacing:.6px;color:var(--muted);margin-bottom:3px}
.metric-card .val{font-size:1.25rem;font-weight:700}
.mc-p .val{color:var(--blue)} .mc-r .val{color:var(--green)} .mc-f1 .val{color:var(--orange)}
.mc-f2 .val{color:var(--leiden)} .mc-tp .val{color:var(--green)} .mc-fp .val{color:var(--red)}
.mc-fn .val{color:var(--orange)} .mc-tn .val{color:var(--muted)}
.leg{display:flex;flex-wrap:wrap;gap:14px;padding:8px 24px;background:var(--card);border-bottom:1px solid var(--border);font-size:.75rem;color:var(--muted)}
.leg-item{display:flex;align-items:center;gap:5px}
.dot{width:12px;height:12px;border-radius:50%;flex-shrink:0}
.dot-tn{background:rgba(59,130,246,.3);border:1.5px solid rgba(59,130,246,.5)}
.dot-tp{background:#DC2626;border:2.5px solid #16A34A;box-shadow:0 0 0 1px #16A34A}
.dot-fp{background:#3B82F6;border:2.5px solid #DC2626;box-shadow:0 0 0 1px #DC2626}
.dot-fn{background:#F97316;border:1.5px solid #DC2626}
.section{padding:18px 20px 8px}
.section-title{font-size:.82rem;font-weight:700;color:var(--leiden);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
.section-sub{font-size:.76rem;color:var(--muted);margin-bottom:10px}
.chart-card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:8px 6px;overflow:hidden}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:12px;padding:0 20px 16px}
@media(max-width:720px){.two-col{grid-template-columns:1fr}}
#scrolltop{position:fixed;bottom:20px;right:20px;background:var(--leiden);color:#fff;border:none;border-radius:50%;width:36px;height:36px;font-size:1rem;cursor:pointer;box-shadow:0 2px 8px rgba(0,0,0,.3);display:none;z-index:200}
</style>
<script>const DATA = __DATA_JSON__;</script>
</head>
<body>
<header>
  <h1>Anomaly Detection Explorer</h1>
  <div class="ctrl-row">
    <label>City<select id="city-sel"></select></label>
    <label>Column<select id="col-sel"></select></label>
    <span class="badge" id="badge-model">—</span>
    <span class="badge" id="badge-cont">—</span>
  </div>
</header>
<div class="metric-strip">
  <div class="metric-card mc-p"><div class="lbl">Precision</div><div class="val" id="mc-p">—</div></div>
  <div class="metric-card mc-r"><div class="lbl">Recall</div><div class="val" id="mc-r">—</div></div>
  <div class="metric-card mc-f1"><div class="lbl">F1</div><div class="val" id="mc-f1">—</div></div>
  <div class="metric-card mc-f2"><div class="lbl">F2</div><div class="val" id="mc-f2">—</div></div>
  <div class="metric-card mc-tp"><div class="lbl">True Pos</div><div class="val" id="mc-tp">—</div></div>
  <div class="metric-card mc-fp"><div class="lbl">False Pos</div><div class="val" id="mc-fp">—</div></div>
  <div class="metric-card mc-fn"><div class="lbl">False Neg</div><div class="val" id="mc-fn">—</div></div>
  <div class="metric-card mc-tn"><div class="lbl">True Neg</div><div class="val" id="mc-tn">—</div></div>
</div>
<div class="leg">
  <div class="leg-item"><div class="dot dot-tn"></div>Normal — not flagged (TN, sampled)</div>
  <div class="leg-item"><div class="dot dot-tp"></div>Synthetic anomaly — detected (TP · green glow)</div>
  <div class="leg-item"><div class="dot dot-fp"></div>Normal — falsely flagged (FP · red glow)</div>
  <div class="leg-item"><div class="dot dot-fn"></div>Synthetic anomaly — missed (FN)</div>
</div>
<div class="section">
  <div class="section-title">Sensor readings over time</div>
  <div class="section-sub">Hover any point for confidence level, score, and anomaly details.</div>
  <div class="chart-card"><div id="scatter-div" style="height:460px"></div></div>
</div>
<div class="two-col">
  <div><div class="section-title" style="padding:0 0 6px">Label breakdown</div>
       <div class="chart-card"><div id="counts-div" style="height:290px"></div></div></div>
  <div><div class="section-title" style="padding:0 0 6px">Confidence by label</div>
       <div class="chart-card"><div id="violin-div" style="height:290px"></div></div></div>
</div>
<div class="section">
  <div class="section-title">False positive confidence detail</div>
  <div class="section-sub">Normal records the model flagged — their confidence scores and timing. Dashed line = effective probability threshold.</div>
  <div class="chart-card"><div id="fp-scatter-div" style="height:320px"></div></div>
</div>
<div class="section" style="padding-bottom:30px">
  <div class="section-title">Confidence histogram — false positives vs normal</div>
  <div class="section-sub">How the model scored normal (TN) points vs. the ones it flagged (FP).</div>
  <div class="chart-card"><div id="fp-hist-div" style="height:300px"></div></div>
</div>
<button id="scrolltop" onclick="window.scrollTo({top:0,behavior:'smooth'})">↑</button>
<script>
window.addEventListener('scroll',()=>{document.getElementById('scrolltop').style.display=window.scrollY>300?'block':'none'});
const CITIES=DATA.cities,COLS=DATA.cols,LABELS=DATA.colLabels;
let curCity=CITIES[0],curCol=COLS[0];
function ds(){return DATA.ds[curCity+':'+curCol]||null}
function pct(v){return(v*100).toFixed(1)+'%'}
function num(v){return Number(v).toLocaleString()}
function buildSelectors(){
  const cs=document.getElementById('city-sel');
  CITIES.forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c.charAt(0).toUpperCase()+c.slice(1).replace('_',' ');cs.appendChild(o)});
  function rebuildCols(){
    const qs=document.getElementById('col-sel');qs.innerHTML='';
    COLS.forEach(col=>{if(!DATA.ds[curCity+':'+col])return;const o=document.createElement('option');o.value=col;o.textContent=LABELS[col]||col;if(col===curCol)o.selected=true;qs.appendChild(o)});
  }
  cs.addEventListener('change',()=>{curCity=cs.value;rebuildCols();updateAll()});
  document.getElementById('col-sel').addEventListener('change',e=>{curCol=e.target.value;updateAll()});
  rebuildCols();
}
function updateMetrics(d){
  const m=d.m;
  document.getElementById('mc-p').textContent=pct(m.p);document.getElementById('mc-r').textContent=pct(m.r);
  document.getElementById('mc-f1').textContent=pct(m.f1);document.getElementById('mc-f2').textContent=pct(m.f2);
  document.getElementById('mc-tp').textContent=num(m.tp);document.getElementById('mc-fp').textContent=num(m.fp);
  document.getElementById('mc-fn').textContent=num(m.fn);document.getElementById('mc-tn').textContent=num(m.tn);
  document.getElementById('badge-model').textContent='Model: '+d.model;
  document.getElementById('badge-cont').textContent='cont: '+d.cont.toFixed(5)+'  best F2: '+d.best_f2.toFixed(3);
}
const SL={margin:{t:10,b:50,l:52,r:20},xaxis:{type:'date',gridcolor:'#E2E8F0',tickfont:{size:11}},yaxis:{gridcolor:'#E2E8F0',tickfont:{size:11}},plot_bgcolor:'#FAFCFF',paper_bgcolor:'#FFFFFF',hovermode:'closest'};
function mkTrace(lbl,src,col,bcol,bw,sz,op,htExtra){
  if(!src||!src.t||!src.t.length)return null;
  const cd=src.cd||src.t.map(()=>[0,0,0,0,-9999]);
  return{type:lbl==='TN'?'scattergl':'scatter',mode:'markers',name:lbl,x:src.t,y:src.v,customdata:cd,
    marker:{color:col,size:sz,opacity:op,line:bcol?{color:bcol,width:bw}:{width:0}},showlegend:false,
    hovertemplate:'<b>%{x}</b><br>Value: <b>%{y}</b><br>Confidence: <b>%{customdata[0]:.4f}</b><br>Score: %{customdata[1]:.4f}<br>Percentile: %{customdata[2]:.1f}%'+(htExtra||'')+'<extra>'+lbl+'</extra>'};
}
function updateScatter(d){
  const synH='<br>⚠ Synthetic<br>Shift: %{customdata[3]:.1f}%';
  const traces=[
    mkTrace('TN',d.TN,'rgba(59,130,246,0.18)',null,0,4,1,null),
    mkTrace('FN',d.FN,'#F97316','#B45309',1.5,9,0.9,synH),
    mkTrace('TP',d.TP,'#DC2626','#16A34A',3,11,1,synH),
    mkTrace('FP',d.FP,'#3B82F6','#DC2626',3,11,1,null),
  ].filter(Boolean);
  const layout=Object.assign({},SL,{yaxis:Object.assign({},SL.yaxis,{title:{text:(LABELS[curCol]||curCol)+' value',font:{size:12}}})});
  Plotly.react('scatter-div',traces,layout,{responsive:true,displayModeBar:false});
}
function updateCounts(d){
  const m=d.m;
  Plotly.react('counts-div',[{type:'bar',x:['True Pos','False Pos','False Neg','True Neg'],y:[m.tp,m.fp,m.fn,m.tn],
    marker:{color:['#16A34A','#DC2626','#EA580C','#94A3B8']},text:[m.tp,m.fp,m.fn,m.tn].map(String),textposition:'outside',
    hovertemplate:'%{x}: <b>%{y}</b><extra></extra>'}],
    {margin:{t:10,b:40,l:40,r:20},yaxis:{gridcolor:'#E2E8F0',tickfont:{size:11}},xaxis:{tickfont:{size:11}},plot_bgcolor:'#FAFCFF',paper_bgcolor:'#FFFFFF',showlegend:false},{responsive:true,displayModeBar:false});
}
function updateViolin(d){
  const groups=[{lbl:'TN',src:d.TN,col:'rgba(59,130,246,0.4)'},{lbl:'FN',src:d.FN,col:'rgba(249,115,22,0.7)'},
                {lbl:'FP',src:d.FP,col:'rgba(220,38,38,0.7)'},{lbl:'TP',src:d.TP,col:'rgba(22,163,74,0.7)'}];
  const traces=groups.filter(g=>g.src&&g.src.cd&&g.src.cd.length).map(g=>({
    type:'violin',name:g.lbl,y:g.src.cd.map(r=>r[0]),line:{color:g.col},fillcolor:g.col,box:{visible:true},meanline:{visible:true},hoverinfo:'y',showlegend:true}));
  Plotly.react('violin-div',traces,{margin:{t:10,b:40,l:50,r:20},yaxis:{title:{text:'Anomaly probability',font:{size:11}},range:[-0.05,1.05],gridcolor:'#E2E8F0',tickfont:{size:11}},
    xaxis:{tickfont:{size:12}},plot_bgcolor:'#FAFCFF',paper_bgcolor:'#FFFFFF',violinmode:'overlay',legend:{orientation:'h',y:-0.15,font:{size:11}}},{responsive:true,displayModeBar:false});
}
function updateFpScatter(d){
  const fp=d.FP;
  if(!fp||!fp.t||!fp.t.length){
    Plotly.react('fp-scatter-div',[{type:'scatter',x:[],y:[]}],
      {annotations:[{text:'No false positives for this selection',xref:'paper',yref:'paper',x:.5,y:.5,showarrow:false,font:{size:14,color:'#94A3B8'}}],
       plot_bgcolor:'#FAFCFF',paper_bgcolor:'#FFFFFF',margin:{t:10,b:50,l:52,r:20}},{responsive:true,displayModeBar:false});return;
  }
  const probs=fp.cd.map(r=>r[0]),scores=fp.cd.map(r=>r[2]);
  const shapes=d.thresh_prob>0?[{type:'line',x0:0,x1:1,xref:'paper',y0:d.thresh_prob,y1:d.thresh_prob,line:{color:'#DC2626',width:1.5,dash:'dash'}}]:[];
  const annotations=d.thresh_prob>0?[{x:1,xref:'paper',y:d.thresh_prob,yref:'y',text:'  threshold ('+d.thresh_prob.toFixed(3)+')',showarrow:false,xanchor:'left',font:{size:11,color:'#DC2626'}}]:[];
  Plotly.react('fp-scatter-div',[{type:'scatter',mode:'markers',x:fp.t,y:probs,customdata:fp.cd,
    marker:{color:scores,colorscale:'YlOrRd',cmin:0,cmax:100,size:9,colorbar:{title:{text:'Score\nPctile',side:'right'},thickness:12,len:0.8,tickfont:{size:10}},line:{color:'#DC2626',width:1.5}},
    hovertemplate:'<b>%{x}</b><br>Value: <b>%{customdata[1]:.4f}</b><br>Confidence: <b>%{y:.4f}</b><br>Percentile: %{customdata[2]:.1f}%<extra>FP</extra>',showlegend:false}],
    {margin:{t:10,b:50,l:52,r:100},xaxis:{type:'date',gridcolor:'#E2E8F0',tickfont:{size:11}},yaxis:{title:{text:'Anomaly probability',font:{size:12}},range:[-0.02,1.05],gridcolor:'#E2E8F0',tickfont:{size:11}},
     shapes,annotations,plot_bgcolor:'#FAFCFF',paper_bgcolor:'#FFFFFF',hovermode:'closest'},{responsive:true,displayModeBar:false});
}
function updateFpHist(d){
  const tn_p=(d.TN.cd||[]).map(r=>r[0]),fp_p=(d.FP.cd||[]).map(r=>r[0]);
  const shapes=d.thresh_prob>0?[{type:'line',x0:d.thresh_prob,x1:d.thresh_prob,yref:'paper',y0:0,y1:1,line:{color:'#DC2626',width:1.5,dash:'dash'}}]:[];
  const annotations=d.thresh_prob>0?[{x:d.thresh_prob,xref:'x',y:1,yref:'paper',text:'threshold',showarrow:true,arrowhead:2,ay:-30,font:{size:10,color:'#DC2626'},arrowcolor:'#DC2626'}]:[];
  const traces=[
    {type:'histogram',name:'Normal (TN)',x:tn_p,nbinsx:40,marker:{color:'rgba(59,130,246,0.45)',line:{color:'rgba(59,130,246,0.8)',width:1}},opacity:0.8,hovertemplate:'Prob %{x:.2f}<br>Count: %{y}<extra>TN</extra>'},
    {type:'histogram',name:'False Positive (FP)',x:fp_p,nbinsx:40,marker:{color:'rgba(220,38,38,0.55)',line:{color:'rgba(220,38,38,0.9)',width:1}},opacity:0.8,hovertemplate:'Prob %{x:.2f}<br>Count: %{y}<extra>FP</extra>'},
  ].filter(t=>t.x.length);
  Plotly.react('fp-hist-div',traces,{margin:{t:10,b:50,l:52,r:20},barmode:'overlay',
    xaxis:{title:{text:'Anomaly probability',font:{size:12}},range:[-0.02,1.05],gridcolor:'#E2E8F0',tickfont:{size:11}},
    yaxis:{title:{text:'Count',font:{size:12}},gridcolor:'#E2E8F0',tickfont:{size:11}},
    shapes,annotations,plot_bgcolor:'#FAFCFF',paper_bgcolor:'#FFFFFF',legend:{orientation:'h',y:-0.18,font:{size:11}},hovermode:'x unified'},{responsive:true,displayModeBar:false});
}
function updateAll(){
  const d=ds();if(!d)return;
  updateMetrics(d);updateScatter(d);updateCounts(d);updateViolin(d);updateFpScatter(d);updateFpHist(d);
  document.getElementById('col-sel').value=curCol;
}
document.addEventListener('DOMContentLoaded',()=>{buildSelectors();updateAll()});
</script>
</body>
</html>"""


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--scored-dir", type=Path, default=None)
    ap.add_argument("--out-dir",    type=Path, default=Path("/tmp/anomaly_viz"))
    args = ap.parse_args()

    scored_dir = args.scored_dir or latest_scored_dir()
    print(f"Loading scored data from: {scored_dir}")

    data = build_dataset(scored_dir)
    print(f"  {len(data['cities'])} cities, {len(data['cols'])} columns, "
          f"{len(data['ds'])} datasets loaded")

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_html = out_dir / "index.html"

    html = HTML_TEMPLATE.replace("__DATA_JSON__", json.dumps(data, separators=(",", ":"), allow_nan=False))
    out_html.write_text(html, encoding="utf-8")

    size_mb = out_html.stat().st_size / 1e6
    print(f"\nGenerated: {out_html}  ({size_mb:.1f} MB)")
    print(f"Open in browser:  open {out_html}")


if __name__ == "__main__":
    main()
