// Per-worker SSE hub. Each worker holds its own set of open client connections.
// It subscribes to the Redis events channel; when the primary publishes a state
// change, every worker receives it and fans it out to ITS clients. This is the
// "shared bulletin board" that lets clustered workers stay in sync.

import { makeSubscriber, redis } from './redis.js';
import { config } from './config.js';

const clients = new Set(); // Set<res>
let lastState = null; // most recent 'state' snapshot, sent to new joiners immediately

function writeEvent(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

export function initSseBus() {
  // Seed lastState from Redis so a worker that missed an early publish still
  // has the current snapshot to hand to joiners.
  redis
    .get(config.stateKey)
    .then((raw) => {
      if (raw && !lastState) lastState = JSON.parse(raw);
    })
    .catch(() => {});

  const sub = makeSubscriber();
  sub.subscribe(config.eventsChannel);
  sub.on('message', (_channel, raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    if (msg.type === 'state') lastState = msg.state;
    for (const res of clients) {
      try {
        writeEvent(res, msg.type, msg);
      } catch {
        /* a dead socket will be cleaned up by its own 'close' handler */
      }
    }
  });
}

export function addClient(req, res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no', // disable proxy buffering (nginx)
  });
  res.write('retry: 3000\n\n'); // tell EventSource to reconnect after 3s if dropped

  // Immediately hand the newcomer the current state so they're not blank until
  // the next transition. Re-stamp serverNow to NOW so the client computes clock
  // skew correctly — the cached snapshot's own serverNow is stale (from when the
  // phase began), which would otherwise make the countdown restart from full.
  if (lastState) {
    writeEvent(res, 'state', { type: 'state', state: { ...lastState, serverNow: Date.now() } });
  }

  clients.add(res);

  // Heartbeat keeps the connection (and intermediary proxies) alive.
  const heartbeat = setInterval(() => {
    try {
      res.write(': ping\n\n');
    } catch {
      /* ignore */
    }
  }, 15_000);

  req.on('close', () => {
    clearInterval(heartbeat);
    clients.delete(res);
  });
}

export function clientCount() {
  return clients.size;
}
