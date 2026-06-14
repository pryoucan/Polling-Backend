import dotenv from 'dotenv';
import os from 'node:os';

dotenv.config();

function int(name, def) {
  const v = process.env[name];
  return v === undefined || v === '' ? def : parseInt(v, 10);
}

function weights() {
  return (process.env.SCORE_WEIGHTS ?? '3,2,1')
    .split(',')
    .map((s) => parseInt(s.trim(), 10))
    .filter((n) => Number.isFinite(n));
}

export const config = {
  // Prefer an explicit DATABASE_URL; otherwise assemble one from the discrete
  // DB_* vars (the form used for RDS). Falls back to the local docker default.
  databaseUrl:
    process.env.DATABASE_URL ??
    (process.env.DB_HOST
      ? `postgres://${process.env.DB_USER}:${encodeURIComponent(process.env.DB_PASSWORD ?? '')}` +
        `@${process.env.DB_HOST}:${process.env.DB_PORT ?? 5432}/${process.env.DB_NAME}`
      : 'postgres://polling:polling@localhost:5433/polling'),
  // RDS uses PGSSL=require; local dev leaves it false. Accept both spellings.
  pgSsl: process.env.PGSSL === 'true' || process.env.PGSSL === 'require',
  pgPoolMax: int('PG_POOL_MAX', 10),

  redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6380',

  port: int('PORT', 8080),
  workers: int('WORKERS', Math.max(1, os.cpus().length - 1)),

  // Allowed CORS origins for the React frontend (comma-separated). When the
  // frontend (Vercel) and backend (AWS) live on different domains, the backend
  // must explicitly allow the frontend's origin. Empty/unset = allow all (dev).
  // Example prod value: "https://poll.vercel.app,https://yourdomain.com"
  corsOrigins: (process.env.CORS_ORIGIN ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean),

  voteSeconds: int('VOTE_SECONDS', 30),
  resultsSeconds: int('RESULTS_SECONDS', 8),
  lobbySeconds: int('LOBBY_SECONDS', 15),

  scoreWeights: weights(),
  topNScoring: int('TOP_N_SCORING', 3),

  // Segment leaderboard: a rolling sub-leaderboard that covers this many
  // consecutive questions (default 10), so you can award prizes per block.
  // The OVERALL leaderboard still accumulates across the whole poll.
  segmentSize: int('SEGMENT_SIZE', 10),

  // Secret used to sign the session token cookie. MUST be set to a long random
  // value in production (same value across all workers/instances).
  sessionSecret: process.env.SESSION_SECRET ?? 'dev-insecure-secret-change-me',

  // Token the host must present to call /api/admin/* endpoints.
  adminToken: process.env.ADMIN_TOKEN ?? 'admin-dev-token',

  // If true, the poll auto-starts when the server boots. Default false: the
  // poll waits idle until the host clicks Start in the admin panel.
  autoStart: process.env.AUTO_START === 'true',

  // Redis pub/sub channel the primary uses to push events to all workers.
  eventsChannel: 'poll:events',
  // Redis channel workers use to send control commands (start/stop/reset) to the primary.
  controlChannel: 'poll:control',
  // Redis key holding the current authoritative poll state (read by workers to validate votes).
  stateKey: 'poll:current',
};
