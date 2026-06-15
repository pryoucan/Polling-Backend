/**
 * k6 load test — run against an ALREADY-RUNNING poll.
 *
 * Install k6:
 *   Windows : winget install k6         (or download from https://k6.io)
 *   macOS   : brew install k6
 *   Linux   : sudo apt install k6
 *
 * Quick start (defaults: 500 voters + 200 SSE connections against localhost):
 *   k6 run test/k6/load-test.js
 *
 * Common flags:
 *   k6 run --env BASE_URL=http://3.110.x.x:8080 --env VOTERS=2000 --env SSE_CONNS=500 test/k6/load-test.js
 *   K6_WEB_DASHBOARD=true k6 run test/k6/load-test.js        # live browser dashboard
 *   k6 run --out json=results.json test/k6/load-test.js       # save full results
 *
 * Pre-requisite: start the server + start the poll in the admin panel BEFORE running this.
 * For a fully self-driving run (no admin panel needed) use auto-drive.js instead.
 *
 * Two simultaneous scenarios:
 *   voters      — join → poll /state in a tight loop → burst-vote each question.
 *                 Measures: join latency, vote latency (p50/p95/p99), rejected votes.
 *   sse_holders — hold one open SSE connection per VU the entire duration.
 *                 k6 uses Go goroutines so 10k+ connections are cheap vs Node.js.
 *
 * CI pass/fail thresholds:
 *   p(95) join  < 500 ms
 *   p(95) vote  < 1 000 ms
 *   p(99) vote  < 2 000 ms
 *   vote errors < 50
 *   HTTP error rate < 5 %
 */

import http from 'k6/http';
import { sleep, check } from 'k6';
import { Counter, Gauge, Trend } from 'k6/metrics';
import exec from 'k6/execution';

// ---------------------------------------------------------------------------
// Config (override via --env KEY=value)
// ---------------------------------------------------------------------------
const BASE_URL  = __ENV.BASE_URL   || 'http://localhost:8080';
const VOTERS    = parseInt(__ENV.VOTERS    || '500');
const SSE_CONNS = parseInt(__ENV.SSE_CONNS || '200');

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const joinMs    = new Trend('quiz_join_ms',      true); // true → reports ms
const voteMs    = new Trend('quiz_vote_ms',      true);
const pollMs    = new Trend('quiz_state_poll_ms', true);
const sseActive = new Gauge('quiz_sse_active');          // live connection count
const votesOk   = new Counter('quiz_votes_ok');
const votesKo   = new Counter('quiz_votes_fail');
const joinsKo   = new Counter('quiz_joins_fail');
const sseKo     = new Counter('quiz_sse_fail');

// ---------------------------------------------------------------------------
// Scenarios + thresholds
// ---------------------------------------------------------------------------
export const options = {
  scenarios: {
    voters: {
      executor: 'ramping-vus',
      exec: 'voterVu',
      stages: [
        { duration: '30s', target: VOTERS }, // ramp up
        { duration: '3m',  target: VOTERS }, // sustain
        { duration: '30s', target: 0 },      // ramp down
      ],
    },
    sse_holders: {
      executor: 'ramping-vus',
      exec: 'sseHolderVu',
      stages: [
        { duration: '30s', target: SSE_CONNS },
        { duration: '3m',  target: SSE_CONNS },
        { duration: '30s', target: 0 },
      ],
    },
  },
  thresholds: {
    quiz_join_ms:    ['p(95)<500',  'p(99)<1000'],
    quiz_vote_ms:    ['p(95)<1000', 'p(99)<2000'],
    quiz_votes_fail: ['count<50'],
    quiz_joins_fail: ['count<50'],
    http_req_failed: ['rate<0.05'],
  },
};

// ---------------------------------------------------------------------------
// Helpers (mirror simulate.js logic exactly)
// ---------------------------------------------------------------------------

// Deterministic 10-digit phone derived from VU id — keeps each voter distinct.
function phoneFor(vuId) {
  return String(9000000000 + (vuId % 1000000000)).slice(0, 10);
}

// Weighted-random option picker: bias toward earlier options so a clear winner
// emerges, matching simulate.js pickOptions().
function pickOptions(question) {
  const ids = question.options.map((o) => o.id);
  const n   = Math.min(question.maxSelect, 3);
  const picks = new Set();
  while (picks.size < n) {
    const r = Math.floor(Math.pow(Math.random(), 2) * ids.length);
    picks.add(ids[r]);
  }
  return [...picks];
}

// ---------------------------------------------------------------------------
// voterVu: join → poll /state → burst-vote on each open question → exit on complete
// ---------------------------------------------------------------------------
export function voterVu() {
  // Guard: each VU runs this function exactly once; subsequent k6 re-invocations
  // just sleep so the VU slot stays occupied (matches real browser behaviour).
  if (exec.vu.iterationInScenario > 0) { sleep(600); return; }

  const vuId = exec.vu.idInTest;

  // -- Join --
  const t0 = Date.now();
  const joinRes = http.post(
    `${BASE_URL}/api/join`,
    JSON.stringify({ name: `K6_${vuId}`, phone: phoneFor(vuId) }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  joinMs.add(Date.now() - t0);

  if (!check(joinRes, { 'join 200': (r) => r.status === 200 })) {
    joinsKo.add(1);
    return;
  }

  // Use Bearer token — reliable even when running cross-origin (Vercel → AWS).
  const token  = joinRes.json().token;
  const auth   = { Authorization: `Bearer ${token}` };
  const jsonHd = { ...auth, 'Content-Type': 'application/json' };

  const voted = new Set(); // guard against double-voting the same question

  // -- Poll loop --
  for (;;) {
    const t1 = Date.now();
    const stateRes = http.get(`${BASE_URL}/api/state`, { headers: auth });
    pollMs.add(Date.now() - t1);

    if (stateRes.status !== 200) { sleep(0.5); continue; }

    const st = stateRes.json();
    if (st.phase === 'complete') break;

    if (st.phase === 'voting' && st.question && !voted.has(st.question.id)) {
      voted.add(st.question.id);

      // Human-ish think time: 200–1500 ms jitter → realistic vote burst.
      sleep(0.2 + Math.random() * 1.3);

      const t2 = Date.now();
      const voteRes = http.post(
        `${BASE_URL}/api/vote`,
        JSON.stringify({ questionId: st.question.id, optionIds: pickOptions(st.question) }),
        { headers: jsonHd },
      );
      voteMs.add(Date.now() - t2);

      if (voteRes.status === 200) votesOk.add(1);
      else                        votesKo.add(1);
    }

    sleep(0.4 + Math.random() * 0.6); // 400–1000 ms between polls
  }
}

// ---------------------------------------------------------------------------
// sseHolderVu: open one persistent SSE connection per VU and hold it.
//
// k6's http.get() blocks until the server closes the connection.
// Since /api/events stays open until phase=complete, this VU holds the TCP
// socket for the entire poll duration — exactly like a real browser EventSource.
// k6 goroutines handle 10k+ concurrent blocked connections far cheaper than
// Node.js can with open TCP sockets (no event-loop saturation).
// ---------------------------------------------------------------------------
export function sseHolderVu() {
  if (exec.vu.iterationInScenario > 0) { sleep(600); return; }

  sseActive.add(1);

  const res = http.get(`${BASE_URL}/api/events`, {
    headers: { Accept: 'text/event-stream' },
    timeout: '600s', // poll rarely exceeds 10 minutes
  });

  check(res, { 'sse 200': (r) => r.status === 200 });
  if (res.status !== 200) sseKo.add(1);

  sseActive.add(-1);
}
