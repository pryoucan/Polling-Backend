// Entry point.
//   PRIMARY  process: runs the authoritative poll clock (state machine) and
//                     forks the workers. It does NOT serve HTTP.
//   WORKER   processes: serve HTTP/SSE, accept votes. They never own the clock.
//
// This is the Node answer to "use all CPU cores": one worker per core, all
// sharing the same listening port, coordinated through Redis pub/sub.

import cluster from 'node:cluster';
import express from 'express';
import cors from 'cors';

import { config } from './config.js';
import { initClock } from './state-machine.js';
import { makePublisher, makeSubscriber } from './redis.js';
import { buildRouter } from './routes.js';
import { buildAdminRouter } from './admin-routes.js';
import { initSseBus, clientCount } from './sse.js';

if (cluster.isPrimary) {
  console.log(`[primary] pid ${process.pid} starting ${config.workers} worker(s)`);

  for (let i = 0; i < config.workers; i++) cluster.fork();

  cluster.on('exit', (worker, code) => {
    console.error(`[primary] worker ${worker.process.pid} died (${code}); restarting in 1s`);
    // Back off so a worker that can't start (e.g. bad config) can't busy-loop.
    setTimeout(() => cluster.fork(), 1000);
  });

  // The primary owns the clock + listens for host control commands. Start once.
  initClock(makePublisher, makeSubscriber).catch((err) => {
    console.error('[primary] clock controller crashed:', err);
    process.exit(1);
  });
} else {
  const app = express();
  app.disable('x-powered-by');

  // CORS: allow the configured frontend origin(s). If none configured (dev),
  // reflect any origin. Credentials are enabled so a same-origin cookie still
  // works; the cross-origin client uses a Bearer token instead.
  app.use(
    cors({
      origin: config.corsOrigins.length ? config.corsOrigins : true,
      credentials: true,
    }),
  ); 

  initSseBus();

  app.get('/healthz', (_req, res) => res.json({ ok: true, pid: process.pid, clients: clientCount() }));
  app.use('/api/admin', buildAdminRouter());
  app.use('/api', buildRouter());

  // API only — the frontend (client/) is built and hosted separately (Vercel).

  app.listen(config.port, () => {
    console.log(`[worker ${process.pid}] listening on :${config.port}`);
  });
}
