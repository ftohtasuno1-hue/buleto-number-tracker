# Buleto Number Tracker Worker v3.2

Direct upstream WebSocket: `wss://buleto.com/ws` (no browser bridge).
- Filters factor samples to current round time window `sd` through `ed`.
- Deduplicates `dt`.
- Same round updates do not reset state.
- `last-results` is pending; finalization occurs at round end (`ed`) or next round transition, not on every repeated result message.
- Prevents duplicate round IDs in history.
- 37 and values outside 0..36 normalize to 0.
- Totals by number and anomaly strictly >15 seconds.
- Persist `/data/tracker-history.json`; Railway Volume mount `/data` recommended.
Endpoints: `/health`, `/api/status`, `/api/current`, `/api/history`.
