# Buleto Number Tracker Worker v3 Direct

Railway worker attempts direct connection to `wss://buleto.com/ws` without browser relay.
- auto reconnects every 5s
- parses `round`, `factors`, `last-results`
- quote-to-number via `cls` interval
- 37 and out-of-range -> 0
- deduplicates factor samples by `dt`
- aggregates time per number and anomaly if total >15 sec
- stores state in `/data/tracker-history.json` (set Railway Volume mount at `/data` for persistence)

## Deploy
1. Push files to new tracker repo (not old Tracker).
2. Railway service start command `npm start`, port 8080.
3. Add Railway Volume mounted at `/data` for persistence.
4. Check `/api/status`, `/api/current`, `/api/history`.

## Important validation
This version must be validated against the actual upstream stream in Railway logs. It intentionally does not use browser cookies or user session tokens. If Buleto rejects unauthenticated server connections, it will show upstream error/close and will need a supported public feed/auth method.

`factors` payload structure may vary; confirm it from actual live messages before treating output as complete.
