const express = require("express");
const WebSocket = require("ws");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(express.json({limit:"2mb"}));
const PORT = process.env.PORT || 8080;
const UPSTREAM = process.env.BULETO_WS_URL || "wss://buleto.com/ws";
const DATA_DIR = process.env.DATA_DIR || "/data";
const STORE_FILE = path.join(DATA_DIR, "tracker-history.json");
const MAX_HISTORY = 2000;
let currentRound = null, history = [], upstreamStatus = "connecting", lastUpstreamMessageAt = null;
let seenFactorTimes = new Set(), reconnectTimer = null, upstream;

function normalizeNumber(raw) {
  const n=Number(raw);
  if(!Number.isFinite(n) || n<0 || n>37 || n===37) return 0;
  return Math.trunc(n);
}
function save() {
  try { fs.mkdirSync(DATA_DIR,{recursive:true}); fs.writeFileSync(STORE_FILE,JSON.stringify({history,currentRound},null,2)); }
  catch(e){ console.error("History persistence failed:",e.message); }
}
function load() {
  try { if(fs.existsSync(STORE_FILE)){const d=JSON.parse(fs.readFileSync(STORE_FILE,"utf8"));history=Array.isArray(d.history)?d.history:[];currentRound=d.currentRound||null;} }
  catch(e){console.error("History load failed:",e.message);}
}
function startRound(d) {
  const id=String(d.id ?? d.roundId ?? Date.now());
  currentRound={roundId:id, startedAt:d.sd||new Date().toISOString(), bettingCloseAt:d.bcd||null, endedAt:d.ed||null,
    sequence:[], totals:{}, anomalies:[], lastNumber:null,lastChangeAt:null,finalNumber:null,rawSamples:0,source:"buleto-ws"};
  seenFactorTimes=new Set(); console.log("Round started",id); save();
}
function finishRound(rawFinal,ts=Date.now()) {
  if(!currentRound)return;
  if(currentRound.lastNumber!==null && currentRound.lastChangeAt!==null){
    const elapsed=Math.max(0,ts-currentRound.lastChangeAt)/1000;
    const n=currentRound.lastNumber; currentRound.totals[n]=(currentRound.totals[n]||0)+elapsed;
    if(currentRound.totals[n]>15&&!currentRound.anomalies.includes(n))currentRound.anomalies.push(n);
  }
  if(rawFinal!==undefined&&rawFinal!==null)currentRound.finalNumber=normalizeNumber(rawFinal);
  currentRound.endedAt=new Date(ts).toISOString();
  history.unshift(currentRound); if(history.length>MAX_HISTORY)history.length=MAX_HISTORY;
  console.log("Round finished",currentRound.roundId,"final",currentRound.finalNumber);
  currentRound=null;seenFactorTimes=new Set();save();
}
function numberFromQuote(v,cls){
  const q=Number(v); if(!Number.isFinite(q)||!Array.isArray(cls))return 0;
  // Buleto class intervals: vf <= quote < vt (same as app's active-factor lookup).
  const item=cls.find(x=>q>=Number(x.vf)&&q<Number(x.vt));
  return item?normalizeNumber(item.c):0;
}
function recordSample(q,dt,cls){
  if(!currentRound||!Array.isArray(cls))return;
  const stamp=Date.parse(dt); if(!Number.isFinite(stamp))return;
  const key=String(dt); if(seenFactorTimes.has(key))return; seenFactorTimes.add(key);
  const n=numberFromQuote(q,cls); currentRound.rawSamples++;
  if(currentRound.lastNumber===null){
    currentRound.lastNumber=n;currentRound.lastChangeAt=stamp;
    currentRound.sequence.push({number:n,at:new Date(stamp).toISOString(),quote:Number(q)});
  } else {
    const prev=currentRound.lastNumber;
    const elapsed=Math.max(0,stamp-currentRound.lastChangeAt)/1000;
    currentRound.totals[prev]=(currentRound.totals[prev]||0)+elapsed;
    if(currentRound.totals[prev]>15&&!currentRound.anomalies.includes(prev)){
      currentRound.anomalies.push(prev); console.log("ANOMALY >15 sec",prev,currentRound.roundId);
    }
    if(n!==prev){
      currentRound.sequence.push({number:n,at:new Date(stamp).toISOString(),quote:Number(q)});
      currentRound.lastNumber=n;currentRound.lastChangeAt=stamp;
    }
  }
  save();
}
function handleMessage(msg){
  lastUpstreamMessageAt=new Date().toISOString();
  if(msg.type==="round"&&msg.data){
    const id=String(msg.data.id);
    if(currentRound&&currentRound.roundId!==id)finishRound();
    if(!currentRound||currentRound.roundId!==id)startRound(msg.data);
    else {currentRound.bettingCloseAt=msg.data.bcd||currentRound.bettingCloseAt;currentRound.endedAt=msg.data.ed||currentRound.endedAt;}
    save();
  } else if(msg.type==="factors"&&msg.data&&currentRound){
    // Factors can be an array or object with an array property; preserve timestamp deduplication.
    const arr=Array.isArray(msg.data)?msg.data:(Array.isArray(msg.data.factors)?msg.data.factors:Array.isArray(msg.data.items)?msg.data.items:null);
    if(arr)for(const f of arr)recordSample(f.v,f.dt,msg.cls||msg.data.cls||currentRound.cls);
    else if(msg.data.v!==undefined&&msg.data.dt)recordSample(msg.data.v,msg.data.dt,msg.cls||currentRound.cls);
  } else if(msg.type==="last-results"){
    // Keep raw result packet for diagnostics; determine final number only if recognizable.
    const d=msg.data;
    let finalRaw;
    if(typeof d==="number")finalRaw=d;
    else if(d&&typeof d==="object"){
      finalRaw=d.c??d.number??d.result??d.v;
      if(finalRaw===undefined&&Array.isArray(d)&&d.length)finalRaw=d[0]?.c??d[0]?.number??d[0]?.result;
    }
    if(currentRound&&finalRaw!==undefined)finishRound(finalRaw);
  }
}
function connect(){
  clearTimeout(reconnectTimer); upstreamStatus="connecting";
  console.log("Connecting upstream",UPSTREAM);
  upstream=new WebSocket(UPSTREAM,{headers:{Origin:"https://buleto.com"},handshakeTimeout:15000});
  upstream.on("open",()=>{upstreamStatus="connected";console.log("Buleto WS connected");});
  upstream.on("message",raw=>{
    try{const msg=JSON.parse(raw.toString());handleMessage(msg);}
    catch(e){console.error("Message parse/handle error",e.message);}
  });
  upstream.on("error",e=>{upstreamStatus="error: "+e.message;console.error("Upstream WS error",e.message);});
  upstream.on("close",(code,reason)=>{upstreamStatus=`closed ${code} ${reason}`;console.log("Upstream closed",upstreamStatus);reconnectTimer=setTimeout(connect,5000);});
}
load();connect();
app.get("/",(_req,res)=>res.json({service:"Buleto Number Tracker Worker",version:"3.0.0-direct",upstream:upstreamStatus,round:currentRound?.roundId||null}));
app.get("/health",(_req,res)=>res.json({ok:true,service:"buleto-number-tracker",upstream:upstreamStatus,lastMessage:lastUpstreamMessageAt}));
app.get("/api/current",(_req,res)=>res.json(currentRound));
app.get("/api/history",(_req,res)=>res.json(history));
app.get("/api/status",(_req,res)=>res.json({upstream:upstreamStatus,lastMessage:lastUpstreamMessageAt,currentRound:currentRound?.roundId||null,samples:currentRound?.rawSamples||0}));
app.listen(PORT,()=>console.log(`Buleto Number Tracker v3 listening on ${PORT}`));
