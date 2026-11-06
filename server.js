const express = require("express");

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = process.env.PORT || 3000;

// In-memory round state for the first deployment.
// Persistent database and Buleto upstream connection will be added next.
let currentRound = null;
const history = [];
const MAX_HISTORY = 1000;

function normalizeNumber(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return 0;
  // User rule: 37 and values outside 0..37 are treated as 0.
  if (n < 0 || n > 37 || n === 37) return 0;
  return Math.trunc(n);
}

function beginRound(roundId, startedAt = Date.now()) {
  currentRound = {
    roundId: String(roundId ?? Date.now()),
    startedAt,
    endedAt: null,
    sequence: [],
    totals: {},
    anomalies: [],
    lastNumber: null,
    lastChangeAt: startedAt,
    finalNumber: null
  };
  return currentRound;
}

// Accept quote-derived number samples from a future Buleto adapter or test endpoint.
function recordNumber(rawNumber, timestamp = Date.now()) {
  if (!currentRound) beginRound("manual-" + Date.now(), timestamp);

  const number = normalizeNumber(rawNumber);
  const ts = Number(timestamp) || Date.now();

  if (currentRound.lastNumber !== null) {
    const elapsed = Math.max(0, ts - currentRound.lastChangeAt) / 1000;
    const previous = currentRound.lastNumber;
    currentRound.totals[previous] = (currentRound.totals[previous] || 0) + elapsed;

    if (currentRound.totals[previous] > 15 && !currentRound.anomalies.includes(previous)) {
      currentRound.anomalies.push(previous);
    }
  }

  if (currentRound.lastNumber !== number) {
    currentRound.sequence.push({ number, at: new Date(ts).toISOString() });
    currentRound.lastNumber = number;
    currentRound.lastChangeAt = ts;
  }
  return currentRound;
}

function finishRound(finalRawNumber, timestamp = Date.now()) {
  if (!currentRound) return null;

  const ts = Number(timestamp) || Date.now();
  if (currentRound.lastNumber !== null) {
    const elapsed = Math.max(0, ts - currentRound.lastChangeAt) / 1000;
    const previous = currentRound.lastNumber;
    currentRound.totals[previous] = (currentRound.totals[previous] || 0) + elapsed;
    if (currentRound.totals[previous] > 15 && !currentRound.anomalies.includes(previous)) {
      currentRound.anomalies.push(previous);
    }
  }

  currentRound.finalNumber = normalizeNumber(finalRawNumber);
  currentRound.endedAt = new Date(ts).toISOString();
  const completed = currentRound;
  history.unshift(completed);
  if (history.length > MAX_HISTORY) history.length = MAX_HISTORY;
  currentRound = null;
  return completed;
}

app.get("/", (_req, res) => {
  res.json({
    service: "Buleto Number Tracker Worker",
    status: "online",
    version: "1.0.0",
    note: "Backend scaffold deployed. Buleto live feed adapter is not connected yet."
  });
});

app.get("/health", (_req, res) => res.json({ ok: true, service: "buleto-number-tracker" }));
app.get("/api/current", (_req, res) => res.json(currentRound));
app.get("/api/history", (_req, res) => res.json(history));

app.post("/api/round/start", (req, res) => {
  const round = beginRound(req.body.roundId, req.body.timestamp);
  res.json({ ok: true, round });
});

app.post("/api/number", (req, res) => {
  if (req.body.number === undefined) return res.status(400).json({ error: "number is required" });
  res.json({ ok: true, round: recordNumber(req.body.number, req.body.timestamp) });
});

app.post("/api/round/finish", (req, res) => {
  if (req.body.number === undefined) return res.status(400).json({ error: "final number is required" });
  const round = finishRound(req.body.number, req.body.timestamp);
  if (!round) return res.status(400).json({ error: "no active round" });
  res.json({ ok: true, round });
});

app.listen(PORT, () => console.log(`Buleto Number Tracker Worker listening on ${PORT}`));
