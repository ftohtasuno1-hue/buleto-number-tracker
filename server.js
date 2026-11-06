const express = require("express");
const WebSocket = require("ws");
const fs = require("fs");
const path = require("path");
const webpush = require("web-push");

const app = express();
const allowedOrigins = (process.env.CORS_ORIGIN || "*").split(",").map(x => x.trim()).filter(Boolean);
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (!origin || allowedOrigins.includes("*") || allowedOrigins.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin || "*");
    res.setHeader("Vary", "Origin");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET,HEAD,POST,OPTIONS,DELETE");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 8080;
const UPSTREAM = process.env.BULETO_WS_URL || "wss://buleto.com/ws";
const DATA_DIR = process.env.DATA_DIR || "/data";
const STORE = path.join(DATA_DIR, "tracker-history.json");
const PUSH_STORE = path.join(DATA_DIR, "push-subscriptions.json");
const MAX_HISTORY = 2000;

let currentRound = null, history = [], upstreamStatus = "connecting", lastMessageAt = null;
let upstream, reconnectTimer, pendingFinal = null;
let seenFactorTimes = new Set(), finishTimer = null;
let subscriptions = [];

function loadSubscriptions() {
  try {
    if (fs.existsSync(PUSH_STORE)) {
      const d = JSON.parse(fs.readFileSync(PUSH_STORE, "utf8"));
      subscriptions = Array.isArray(d) ? d : [];
    }
  } catch (e) { console.error("Push subscriptions load failed", e.message); }
}
function saveSubscriptions() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(PUSH_STORE, JSON.stringify(subscriptions, null, 2));
  } catch (e) { console.error("Push subscriptions save failed", e.message); }
}
const vapidPublic = process.env.VAPID_PUBLIC_KEY;
const vapidPrivate = process.env.VAPID_PRIVATE_KEY;
if (vapidPublic && vapidPrivate) {
  webpush.setVapidDetails(process.env.VAPID_SUBJECT || "mailto:admin@example.com", vapidPublic, vapidPrivate);
  console.log("Web Push VAPID configured");
} else {
  console.warn("Web Push disabled: set VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY");
}

async function sendPush(payload) {
  if (!vapidPublic || !vapidPrivate) return { sent: 0, skipped: true };
  const body = JSON.stringify(payload);
  let sent = 0;
  const keep = [];
  for (const sub of subscriptions) {
    try {
      await webpush.sendNotification(sub, body);
      sent++;
      keep.push(sub);
    } catch (err) {
      if (err.statusCode !== 404 && err.statusCode !== 410) keep.push(sub);
      console.error("Push send failed", err.statusCode || "", err.message);
    }
  }
  subscriptions = keep;
  saveSubscriptions();
  return { sent };
}
function norm(raw) { const n = Number(raw); return !Number.isFinite(n) || n < 0 || n >= 37 ? 0 : Math.trunc(n); }
function save() {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(STORE, JSON.stringify({ history, currentRound, pendingFinal }, null, 2)); }
  catch (e) { console.error("Save failed", e.message); }
}
function load() {
  try {
    if (fs.existsSync(STORE)) {
      const d = JSON.parse(fs.readFileSync(STORE, "utf8"));
      history = Array.isArray(d.history) ? d.history : [];
      currentRound = d.currentRound || null;
      pendingFinal = d.pendingFinal ?? null;
    }
  } catch (e) { console.error("Load failed", e.message); }
}
function clearFinishTimer() { if (finishTimer) { clearTimeout(finishTimer); finishTimer = null; } }
function scheduleFinish() {
  clearFinishTimer();
  if (!currentRound?.endedAt) return;
  const end = Date.parse(currentRound.endedAt);
  if (!Number.isFinite(end)) return;
  const delay = Math.max(0, end - Date.now());
  finishTimer = setTimeout(() => {
    if (currentRound && !currentRound.finished && Number.isFinite(Date.parse(currentRound.endedAt)) && Date.now() >= Date.parse(currentRound.endedAt)) finishRound(pendingFinal, Date.parse(currentRound.endedAt));
  }, Math.min(delay + 50, 2147480000));
}
function startRound(d) {
  const id = String(d.id ?? d.roundId ?? Date.now()), start = Date.parse(d.sd) || Date.now();
  currentRound = {
    roundId: id, startedAt: d.sd || new Date(start).toISOString(), bettingCloseAt: d.bcd || null,
    endedAt: d.ed || null, cls: Array.isArray(d.cls) ? d.cls : [], sequence: [], totals: {},
    anomalies: [], notifiedAnomalies: [], lastNumber: null, lastSampleAt: null, finalNumber: null, rawSamples: 0, source: "buleto-ws"
  };
  seenFactorTimes = new Set(); pendingFinal = null; console.log("Round started", id); scheduleFinish(); save();
}
function finishRound(rawFinal, ts = Date.now()) {
  if (!currentRound || currentRound.finished) return;
  currentRound.finished = true;
  if (currentRound.lastNumber !== null && currentRound.lastSampleAt !== null) {
    const end = Date.parse(currentRound.endedAt);
    const finishAt = Number.isFinite(end) ? Math.min(ts, end) : ts;
    const elapsed = Math.max(0, finishAt - currentRound.lastSampleAt) / 1000, n = currentRound.lastNumber;
    currentRound.totals[n] = (currentRound.totals[n] || 0) + elapsed;
    if (currentRound.totals[n] > 25 && !currentRound.anomalies.includes(n)) currentRound.anomalies.push(n);
  }
  if (rawFinal !== undefined && rawFinal !== null) currentRound.finalNumber = norm(rawFinal);
  const officialEnd = Date.parse(currentRound.endedAt);
  currentRound.endedAt = new Date(Number.isFinite(officialEnd) ? officialEnd : ts).toISOString();
  if (!history.some(r => String(r.roundId) === String(currentRound.roundId))) {
    history.unshift(currentRound); if (history.length > MAX_HISTORY) history.length = MAX_HISTORY;
    console.log("Round finished", currentRound.roundId, "final", currentRound.finalNumber);
  } else console.log("Duplicate finish ignored", currentRound.roundId);
  currentRound = null; seenFactorTimes = new Set(); pendingFinal = null; clearFinishTimer(); save();
}
function numberFromQuote(v, cls) {
  const q = Number(v); if (!Number.isFinite(q) || !Array.isArray(cls)) return 0;
  const x = cls.find(a => q >= Number(a.vf) && q < Number(a.vt)); return x ? norm(x.c) : 0;
}
function sample(q, dt, cls) {
  if (!currentRound || currentRound.finished || !Array.isArray(cls) || !dt) return;
  const stamp = Date.parse(dt), start = Date.parse(currentRound.startedAt), end = Date.parse(currentRound.endedAt);
  if (!Number.isFinite(stamp) || stamp < start || (Number.isFinite(end) && stamp >= end)) return;
  // Once the official round end has passed, ignore late factors from Buleto.
  if (Number.isFinite(end) && Date.now() >= end) return;
  const key = String(dt); if (seenFactorTimes.has(key)) return; seenFactorTimes.add(key);
  const n = numberFromQuote(q, cls); currentRound.rawSamples++;
  if (currentRound.lastNumber === null) {
    currentRound.lastNumber = n; currentRound.lastSampleAt = stamp;
    currentRound.sequence.push({ number: n, at: new Date(stamp).toISOString(), quote: Number(q) });
  } else {
    const prev = currentRound.lastNumber;
    const elapsed = Math.max(0, stamp - (currentRound.lastSampleAt ?? stamp)) / 1000;
    currentRound.totals[prev] = (currentRound.totals[prev] || 0) + elapsed;
    if (currentRound.totals[prev] > 25 && !currentRound.anomalies.includes(prev)) {
      currentRound.anomalies.push(prev); console.log("ANOMALY >15 sec", prev, currentRound.roundId);
      currentRound.notifiedAnomalies = currentRound.notifiedAnomalies || [];
      if (!currentRound.notifiedAnomalies.includes(prev)) {
        // Mark and persist before sending: repeated factors/reconnects cannot trigger duplicates.
        currentRound.notifiedAnomalies.push(prev);
        save();
        sendPush({
          title: "⚠️ Аномалия Buleto",
          body: `Раунд ${currentRound.roundId} · номер ${prev} · более 25 секунд`,
          url: "/",
          tag: `anomaly-${currentRound.roundId}-${prev}`,
          renotify: false
        }).then(r=>console.log("Anomaly push", r)).catch(e=>console.error("Anomaly push error",e.message));
      }
    }
    if (n !== prev) currentRound.sequence.push({ number: n, at: new Date(stamp).toISOString(), quote: Number(q) });
    currentRound.lastNumber = n; currentRound.lastSampleAt = stamp;
  }
  save();
}
function finalFrom(d) {
  if (typeof d === "number") return d;
  if (Array.isArray(d) && d.length) return d[0]?.c ?? d[0]?.number ?? d[0]?.result;
  if (d && typeof d === "object") return d.c ?? d.number ?? d.result ?? d.v;
  return undefined;
}
function handle(m) {
  lastMessageAt = new Date().toISOString();
  if (m.type === "round" && m.data) {
    const id = String(m.data.id ?? m.data.roundId);
    if (currentRound && currentRound.roundId !== id) finishRound(pendingFinal);
    if (!currentRound) startRound(m.data);
    else {
      currentRound.bettingCloseAt = m.data.bcd || currentRound.bettingCloseAt;
      currentRound.endedAt = m.data.ed || currentRound.endedAt;
      if (Array.isArray(m.data.cls) && m.data.cls.length) currentRound.cls = m.data.cls;
      scheduleFinish(); save();
    }
  } else if (m.type === "factors" && m.data && currentRound) {
    const cls = m.cls || m.data.cls || currentRound.cls;
    const arr = Array.isArray(m.data) ? m.data : Array.isArray(m.data.factors) ? m.data.factors : Array.isArray(m.data.items) ? m.data.items : null;
    if (arr) for (const f of arr) sample(f.v, f.dt, cls);
    else if (m.data.v !== undefined && m.data.dt) sample(m.data.v, m.data.dt, cls);
  } else if (m.type === "last-results") {
    const f = finalFrom(m.data); if (f !== undefined && f !== null) { pendingFinal = f; save(); }
  }
}
function connect() {
  clearTimeout(reconnectTimer); upstreamStatus = "connecting"; console.log("Connecting", UPSTREAM);
  upstream = new WebSocket(UPSTREAM, { headers: { Origin: "https://buleto.com" }, handshakeTimeout: 15000 });
  upstream.on("open", () => { upstreamStatus = "connected"; console.log("Buleto WS connected"); });
  upstream.on("message", raw => { try { handle(JSON.parse(raw.toString())); } catch (e) { console.error("Handle error", e.message); } });
  upstream.on("error", e => { upstreamStatus = "error: " + e.message; console.error("WS error", e.message); });
  upstream.on("close", (c, r) => { upstreamStatus = `closed ${c} ${r}`; console.log(upstreamStatus); reconnectTimer = setTimeout(connect, 5000); });
}

load(); loadSubscriptions(); if (currentRound) { currentRound.notifiedAnomalies = currentRound.notifiedAnomalies || []; scheduleFinish(); } connect();

app.get("/", (_, res) => res.json({ service: "Buleto Number Tracker Worker", version: "3.5.0", upstream: upstreamStatus, round: currentRound?.roundId || null }));
app.get("/health", (_, res) => res.json({ ok: true, service: "buleto-number-tracker", upstream: upstreamStatus, lastMessage: lastMessageAt }));
app.get("/api/status", (_, res) => res.json({ upstream: upstreamStatus, lastMessage: lastMessageAt, currentRound: currentRound?.roundId || null, samples: currentRound?.rawSamples || 0 }));
app.get("/api/current", (_, res) => res.json(currentRound));
app.get("/api/history", (_, res) => res.json(history));

// Web Push: frontend sends its browser subscription here.
app.get("/api/push/vapid-public-key", (_, res) => res.json({ publicKey: vapidPublic || null }));
app.post("/api/push/subscribe", (req, res) => {
  const sub = req.body;
  if (!sub || !sub.endpoint || !sub.keys?.p256dh || !sub.keys?.auth) return res.status(400).json({ error: "Invalid push subscription" });
  const exists = subscriptions.some(s => s.endpoint === sub.endpoint);
  if (!exists) { subscriptions.push(sub); saveSubscriptions(); }
  res.status(201).json({ ok: true, subscriptions: subscriptions.length });
});
app.post("/api/push/unsubscribe", (req, res) => {
  const endpoint = req.body?.endpoint;
  if (!endpoint) return res.status(400).json({ error: "endpoint required" });
  subscriptions = subscriptions.filter(s => s.endpoint !== endpoint); saveSubscriptions();
  res.json({ ok: true, subscriptions: subscriptions.length });
});
// Test endpoint: POST /api/push/test to send a test notification to all subscribed devices.
app.post("/api/push/test", async (_, res) => {
  try {
    const result = await sendPush({ title: "Tracker", body: "Тест Push-уведомления", url: "/" });
    res.json({ ok: true, ...result, subscriptions: subscriptions.length });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.listen(PORT, () => console.log("Buleto Number Tracker v3.5 listening", PORT));
