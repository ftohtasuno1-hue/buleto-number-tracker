# Buleto Number Tracker Worker

Independent backend for the new tracker. It does not touch the existing Tracker.

## Deploy on Railway
Railway should detect `package.json` and run `npm start`.

## Endpoints
- `GET /` — service status
- `GET /health` — health check
- `GET /api/current` — active round
- `GET /api/history` — completed rounds
- `POST /api/round/start` — JSON: `{"roundId":"123"}`
- `POST /api/number` — JSON: `{"number":12}`
- `POST /api/round/finish` — JSON: `{"number":37}` (37 is normalized to 0)

## Rules implemented
- 37 -> 0
- values outside 0..37 -> 0
- total per-number display duration is summed during a round
- anomaly when a number's total duration is strictly greater than 15 seconds

## Current limitation
This is the first backend scaffold. It does not connect to Buleto's upstream WebSocket yet. The next step is to add the verified Buleto feed adapter and persistent storage.
