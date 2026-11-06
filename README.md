# Buleto Number Tracker Worker v3.4

- Direct Buleto WebSocket: `wss://buleto.com/ws`
- Adds CORS middleware for the Netlify frontend (default `CORS_ORIGIN=*`).
- OPTIONS preflight is answered with 204.
- Existing v3.3 sample duration logic retained.
- Endpoints: `/health`, `/api/status`, `/api/current`, `/api/history`

## Deploy on Railway
Upload/replace `server.js`, `package.json`, and this README in the project/repository, then redeploy.
Optional Railway variable `CORS_ORIGIN`: comma-separated allowed origins, e.g. `https://YOUR-SITE.netlify.app`.
Leave unset to allow all origins (API is public/read-only).
