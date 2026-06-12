import Redis from 'ioredis';
import { config } from './config.js';

// A general-purpose client for commands (ZINCRBY, GET, SET, pipelines).
// ioredis auto-reconnects; commands issued while down are queued by default.
export const redis = new Redis(config.redisUrl, {
  maxRetriesPerRequest: 3,
  lazyConnect: false,
});

// pub/sub requires DEDICATED connections (a subscriber can't run normal commands).
export function makeSubscriber() {
  return new Redis(config.redisUrl, { lazyConnect: false });
}

export function makePublisher() {
  return new Redis(config.redisUrl, { lazyConnect: false });
}

// --- key helpers ---
export const keys = {
  tally: (questionId) => `poll:q:${questionId}:tally`, // ZSET option_id -> live votes
};
