const express = require("express");
const WebSocket = require("ws");
const fs = require("fs");
const path = require("path");

const app=express();
const allowedOrigins = (process.env.CORS_ORIGIN || "*").split(",").map(x=>x.trim()).filter(Boolean);
app.use((req,res,next)=>{
  const origin=req.headers.origin;
  if(!origin || allowedOrigins.includes("*") || allowedOrigins.includes(origin)){
    res.setHeader("Access-Control-Allow-Origin", origin || "*");
    res.setHeader("Vary","Origin");
  }
  res.setHeader("Access-Control-Allow-Methods","GET,HEAD,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers","Content-Type,Authorization");
  if(req.method==="OPTIONS") return res.sendStatus(204);
  next();
});
app.use(express.json({limit:"2mb"}));
const PORT=process.env.PORT||8080, UPSTREAM=process.env.BULETO_WS_URL||"wss://buleto.com/ws";
const DATA_DIR=process.env.DATA_DIR||"/data", STORE=path.join(DATA_DIR,"tracker-history.json"), MAX_HISTORY=2000;
let currentRound=null,history=[],upstreamStatus="connecting",lastMessageAt=null,upstream,reconnectTimer,pendingFinal=null;
let seenFactorTimes=new Set(),finishTimer=null;

function norm(raw){const n=Number(raw);return !Number.isFinite(n)||n<0||n>=37?0:Math.trunc(n);}
function save(){try{fs.mkdirSync(DATA_DIR,{recursive:true});fs.writeFileSync(STORE,JSON.stringify({history,currentRound,pendingFinal},null,2));}catch(e){console.error("Save failed",e.message);}}
function load(){try{if(fs.existsSync(STORE)){const d=JSON.parse(fs.readFileSync(STORE,"utf8"));history=Array.isArray(d.history)?d.history:[];currentRound=d.currentRound||null;pendingFinal=d.pendingFinal??null;}}catch(e){console.error("Load failed",e.message);}}
function clearFinishTimer(){if(finishTimer){clearTimeout(finishTimer);finishTimer=null;}}
function scheduleFinish(){
 clearFinishTimer(); if(!currentRound?.endedAt)return;
 const end=Date.parse(currentRound.endedAt);if(!Number.isFinite(end))return;
 const delay=Math.max(0,end-Date.now());
 finishTimer=setTimeout(()=>{if(currentRound&&Date.now()>=Date.parse(currentRound.endedAt))finishRound(pendingFinal,Date.parse(currentRound.endedAt));},Math.min(delay+50,2147480000));
}
function startRound(d){
 const id=String(d.id??d.roundId??Date.now()), start=Date.parse(d.sd)||Date.now();
 currentRound={roundId:id,startedAt:d.sd||new Date(start).toISOString(),bettingCloseAt:d.bcd||null,endedAt:d.ed||null,cls:Array.isArray(d.cls)?d.cls:[],sequence:[],totals:{},anomalies:[],lastNumber:null,lastSampleAt:null,finalNumber:null,rawSamples:0,source:"buleto-ws"};
 seenFactorTimes=new Set();pendingFinal=null;console.log("Round started",id);scheduleFinish();save();
}
function finishRound(rawFinal,ts=Date.now()){
 if(!currentRound)return;
 if(currentRound.lastNumber!==null&&currentRound.lastSampleAt!==null){
  const end=Date.parse(currentRound.endedAt);
  const finishAt=Number.isFinite(end)?Math.min(ts,end):ts;
  const elapsed=Math.max(0,finishAt-currentRound.lastSampleAt)/1000, n=currentRound.lastNumber;
  currentRound.totals[n]=(currentRound.totals[n]||0)+elapsed;
  if(currentRound.totals[n]>15&&!currentRound.anomalies.includes(n))currentRound.anomalies.push(n);
 }
 if(rawFinal!==undefined&&rawFinal!==null)currentRound.finalNumber=norm(rawFinal);
 currentRound.endedAt=new Date(ts).toISOString();
 if(!history.some(r=>String(r.roundId)===String(currentRound.roundId))){history.unshift(currentRound);if(history.length>MAX_HISTORY)history.length=MAX_HISTORY;console.log("Round finished",currentRound.roundId,"final",currentRound.finalNumber);}
 else console.log("Duplicate finish ignored",currentRound.roundId);
 currentRound=null;seenFactorTimes=new Set();pendingFinal=null;clearFinishTimer();save();
}
function numberFromQuote(v,cls){const q=Number(v);if(!Number.isFinite(q)||!Array.isArray(cls))return 0;const x=cls.find(a=>q>=Number(a.vf)&&q<Number(a.vt));return x?norm(x.c):0;}
function sample(q,dt,cls){
 if(!currentRound||!Array.isArray(cls)||!dt)return;
 const stamp=Date.parse(dt),start=Date.parse(currentRound.startedAt),end=Date.parse(currentRound.endedAt);
 if(!Number.isFinite(stamp)||stamp<start||(Number.isFinite(end)&&stamp>end))return;
 const key=String(dt);if(seenFactorTimes.has(key))return;seenFactorTimes.add(key);
 const n=numberFromQuote(q,cls);currentRound.rawSamples++;
 if(currentRound.lastNumber===null){
  currentRound.lastNumber=n;currentRound.lastSampleAt=stamp;
  currentRound.sequence.push({number:n,at:new Date(stamp).toISOString(),quote:Number(q)});
 } else {
  const prev=currentRound.lastNumber;
  // Count only the interval since the immediately previous sample (no overlap).
  const elapsed=Math.max(0,stamp-(currentRound.lastSampleAt??stamp))/1000;
  currentRound.totals[prev]=(currentRound.totals[prev]||0)+elapsed;
  if(currentRound.totals[prev]>15&&!currentRound.anomalies.includes(prev)){
   currentRound.anomalies.push(prev);console.log("ANOMALY >15 sec",prev,currentRound.roundId);
  }
  if(n!==prev)currentRound.sequence.push({number:n,at:new Date(stamp).toISOString(),quote:Number(q)});
  currentRound.lastNumber=n;currentRound.lastSampleAt=stamp;
 }
 save();
}
function finalFrom(d){if(typeof d==="number")return d;if(Array.isArray(d)&&d.length)return d[0]?.c??d[0]?.number??d[0]?.result;if(d&&typeof d==="object")return d.c??d.number??d.result??d.v;return undefined;}
function handle(m){
 lastMessageAt=new Date().toISOString();
 if(m.type==="round"&&m.data){
  const id=String(m.data.id??m.data.roundId);
  if(currentRound&&currentRound.roundId!==id)finishRound(pendingFinal);
  if(!currentRound)startRound(m.data);
  else{currentRound.bettingCloseAt=m.data.bcd||currentRound.bettingCloseAt;currentRound.endedAt=m.data.ed||currentRound.endedAt;if(Array.isArray(m.data.cls)&&m.data.cls.length)currentRound.cls=m.data.cls;scheduleFinish();save();}
 }else if(m.type==="factors"&&m.data&&currentRound){
  const cls=m.cls||m.data.cls||currentRound.cls,arr=Array.isArray(m.data)?m.data:Array.isArray(m.data.factors)?m.data.factors:Array.isArray(m.data.items)?m.data.items:null;
  if(arr)for(const f of arr)sample(f.v,f.dt,cls);else if(m.data.v!==undefined&&m.data.dt)sample(m.data.v,m.data.dt,cls);
 }else if(m.type==="last-results"){const f=finalFrom(m.data);if(f!==undefined&&f!==null){pendingFinal=f;save();}}
}
function connect(){clearTimeout(reconnectTimer);upstreamStatus="connecting";console.log("Connecting",UPSTREAM);upstream=new WebSocket(UPSTREAM,{headers:{Origin:"https://buleto.com"},handshakeTimeout:15000});
 upstream.on("open",()=>{upstreamStatus="connected";console.log("Buleto WS connected");});
 upstream.on("message",raw=>{try{handle(JSON.parse(raw.toString()));}catch(e){console.error("Handle error",e.message);}});
 upstream.on("error",e=>{upstreamStatus="error: "+e.message;console.error("WS error",e.message);});
 upstream.on("close",(c,r)=>{upstreamStatus=`closed ${c} ${r}`;console.log(upstreamStatus);reconnectTimer=setTimeout(connect,5000);});
}
load();if(currentRound)scheduleFinish();connect();
app.get("/",(_,r)=>r.json({service:"Buleto Number Tracker Worker",version:"3.4.0",upstream:upstreamStatus,round:currentRound?.roundId||null}));
app.get("/health",(_,r)=>r.json({ok:true,service:"buleto-number-tracker",upstream:upstreamStatus,lastMessage:lastMessageAt}));
app.get("/api/status",(_,r)=>r.json({upstream:upstreamStatus,lastMessage:lastMessageAt,currentRound:currentRound?.roundId||null,samples:currentRound?.rawSamples||0}));
app.get("/api/current",(_,r)=>r.json(currentRound));app.get("/api/history",(_,r)=>r.json(history));
app.listen(PORT,()=>console.log("Buleto Number Tracker v3.4 listening",PORT));
