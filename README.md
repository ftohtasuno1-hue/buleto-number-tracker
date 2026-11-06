# Buleto Number Tracker Worker v3.1 Direct

- Connects directly to `wss://buleto.com/ws` (no browser bridge or cookies).
- Reconnects after disconnect.
- Maintains one active round per round ID; duplicate `round` updates don't reset it.
- Stores `last-results` as pending and finalizes only on round ID transition, preventing repeated finish/start cycles.
- Carries `cls` from the `round` message to quote mapping.
- Maps 37 and out-of-range values to 0; aggregates per-number duration; flags totals strictly above 15 seconds.
- Saves data to `/data/tracker-history.json`; configure Railway Volume mounted at `/data`.
- `GET /api/status`, `/api/current`, `/api/history`, `/health`.

**Important:** The unauthenticated upstream WebSocket must actually permit Railway connections. Validate `/api/status` and deployment logs. The upstream payload shapes and round boundary should be confirmed against live data.
