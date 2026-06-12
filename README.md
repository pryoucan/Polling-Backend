# Live Timed Polling

A live, timed, multiple-choice polling app built for a one-time event at ~5000
concurrent participants. Everyone votes on the same question inside a countdown
window; when the timer expires the question is scored (you earn points for
picking the most-voted answers) and the leaderboard updates live.

## How it works (architecture)

```
Browser (SPA) ──REST──┐         ┌─────────────────────────────┐
                      ├────────►│ Node cluster                │
Browser ──SSE─────────┘         │  • PRIMARY = the poll clock  │──► Postgres (truth)
                                │  • WORKERS = HTTP + SSE       │──► Redis (live cache + pub/sub bus)
                                └─────────────────────────────┘
```

- **Server-authoritative clock.** One process (the cluster *primary*) owns the
  timeline: it opens each question for `VOTE_SECONDS`, closes it, scores it, then
  moves on. Clients only *display* the countdown; the server enforces it, so
  clock drift / lag can't be exploited.
- **Scoring at question close.** When a question's timer ends, the final tally is
  read from Postgres (the source of truth) and points are awarded for selecting
  the top-N options (default weights `3,2,1` for 1st/2nd/3rd). This is
  deterministic and fair — early voters aren't scored against an unstable
  ranking — yet the leaderboard visibly jumps every round.
- **Ties** are broken by *who reached that vote count first* (earlier last-vote
  timestamp wins), then by option order. Fully deterministic.
- **Workers share state via Redis pub/sub.** The primary publishes every state
  change to a Redis channel; each worker fans it out to its own SSE clients.
  This is what lets a clustered (multi-core) Node app stay in sync. Use sticky
  sessions at the load balancer so an SSE connection stays pinned to one worker.
- **Redis is a cache, not truth.** Live (cosmetic) tallies live in Redis;
  durable votes and final scoring live in Postgres and can rebuild Redis.

## Project layout

```
server/
  src/
    config.js          env-driven config
    db.js              Postgres pool + tx helper
    redis.js           Redis clients + key helpers
    schema.sql         database schema
    state-machine.js   the authoritative poll clock + scoring (runs in primary)
    sse.js             per-worker SSE hub (subscribes to the pub/sub bus)
    routes.js          REST API (join / state / vote / leaderboard) + /events
    auth.js            signed-cookie anonymous identity
    leaderboard.js     top-N + per-user standing queries
    server.js          cluster entrypoint (primary = clock, workers = HTTP)
    scripts/           migrate.js, seed.js, content.js (demo questions)
  test/simulate.js     concurrent-voter simulator / mini load test
client/                Vite + React frontend (deploys to Vercel)
docker-compose.yml     local Postgres + Redis
```

## Run locally

```bash
# 1. start Postgres + Redis
docker compose up -d

# 2. install + configure
cd server
npm install
cp .env.example .env        # local defaults already match docker-compose

# 3. create schema and seed questions (default 100; pass a number for fewer)
npm run migrate
npm run seed 100

# 4. start the server (cluster). The poll clock starts immediately.
npm start
```

Open http://localhost:8080 and join. The poll begins after `LOBBY_SECONDS`.

> The poll runs **once per server start**. To re-run, re-seed and restart the
> server. (A "host start/reset" admin control is an easy future addition.)

### Try the simulator

```bash
node test/simulate.js 50          # 50 concurrent voters through the whole poll
```

## Configuration (`server/.env`)

| Var | Meaning |
|-----|---------|
| `DATABASE_URL` | Postgres connection (point at RDS in prod) |
| `PGSSL` | `true` when RDS enforces SSL |
| `PG_POOL_MAX` | DB connections **per worker** (× workers must stay under RDS `max_connections`) |
| `REDIS_URL` | Redis / ElastiCache endpoint |
| `WORKERS` | cluster workers (default = cores − 1) |
| `VOTE_SECONDS` / `RESULTS_SECONDS` / `LOBBY_SECONDS` | timings |
| `SCORE_WEIGHTS` / `TOP_N_SCORING` | points per rank, how many ranks score |
| `SESSION_SECRET` | **set a long random value in prod** |

## Deploying to AWS (one-time event)

1. **EC2** `t3.xlarge` (4 vCPU). Run the server with `WORKERS=4` behind an
   **ALB** with **sticky sessions** enabled (needed for SSE).
2. **Redis**: ElastiCache, or co-located on the box for a single event.
3. **Postgres**: your existing RDS. Set `PGSSL=true`. Keep
   `PG_POOL_MAX × WORKERS` comfortably below `max_connections`.
4. **Load-test first** with `test/simulate.js` (or k6/Artillery) at your real
   concurrency before the event.

## Notes / future work

- Admin endpoint to start/pause/reset the poll without a restart.
- Per-question "responses so far" is best-effort (Redis); final results are
  always recomputed from Postgres.
- Identity is anonymous (signed cookie). Add email-OTP if you need it airtight.
