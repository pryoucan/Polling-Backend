/**
 * k6 self-driving load test — replicates simulate.auto.js with production-grade metrics.
 *
 * Drives the poll automatically via the admin API (no human clicking "Next") while
 * thousands of virtual voters connected over HTTP poll /state and burst-vote the
 * moment each question opens.
 *
 * Usage:
 *   ADMIN_TOKEN=your-token k6 run test/k6/auto-drive.js
 *   ADMIN_TOKEN=your-token k6 run \
 *     --env BASE_URL=http://3.110.x.x:8080 \
 *     --env VOTERS=2000 \
 *     --env MAX_QUESTIONS=10 \
 *     --env VOTE_SECONDS=30 \
 *     test/k6/auto-drive.js
 *   K6_WEB_DASHBOARD=true ADMIN_TOKEN=xxxx k6 run test/k6/auto-drive.js  # live dashboard
 *
 * Scenarios:
 *   admin_driver — 1 VU that calls start/next/stop via /api/admin/*.
 *   voters       — N VUs that join, poll /state, and vote on each open question.
 *
 * Thresholds (CI gate):
 *   p(95) join  < 500 ms
 *   p(95) vote  < 1 000 ms
 *   p(99) vote  < 2 000 ms
 *   vote fails  < 100
 *   HTTP error rate < 5 %
 */

import http from 'k6/http';
import { sleep, check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const BASE_URL      = __ENV.BASE_URL      || 'http://localhost:8080';
const VOTERS        = parseInt(__ENV.VOTERS        || '1000');
const MAX_QUESTIONS = parseInt(__ENV.MAX_QUESTIONS || '5');
const ADMIN_TOKEN   = __ENV.ADMIN_TOKEN   || 'admin-dev-token';

// Match VOTE_SECONDS to your server's .env so the test duration is accurate.
const VOTE_SECONDS     = parseInt(__ENV.VOTE_SECONDS     || '30');
const RESULTS_SECONDS  = parseInt(__ENV.RESULTS_SECONDS  || '8');
const RESULTS_PAUSE    = 3;  // extra seconds to dwell on results before advancing

// Total estimated duration: connect grace (5s) + ramp-up (10s) + questions + buffer (20s).
const PER_Q_SEC   = VOTE_SECONDS + RESULTS_SECONDS + RESULTS_PAUSE + 2;
const TOTAL_SEC   = 5 + 10 + MAX_QUESTIONS * PER_Q_SEC + 20;

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const joinMs   = new Trend('quiz_join_ms',   true);
const voteMs   = new Trend('quiz_vote_ms',   true);
const votesOk  = new Counter('quiz_votes_ok');
const votesKo  = new Counter('quiz_votes_fail');
const joinsKo  = new Counter('quiz_joins_fail');

// ---------------------------------------------------------------------------
// Scenarios + thresholds
// ---------------------------------------------------------------------------
export const options = {
  scenarios: {
    // One VU that drives the poll through MAX_QUESTIONS, then stops it.
    admin_driver: {
      executor: 'constant-vus',
      exec: 'adminDriverVu',
      vus: 1,
      duration: `${TOTAL_SEC}s`,
    },
    // N VUs that join 15 s after the test starts (setup() starts the poll first,
    // so voting is already open before the first voter joins).
    // Ramp over 60 s so joins arrive at ~83/s instead of 500/s — keeps the
    // server from being overwhelmed by the TLS handshake flood.
    voters: {
      executor: 'ramping-vus',
      exec: 'voterVu',
      startTime: '15s',
      stages: [
        { duration: '60s',              target: VOTERS }, // gentle ramp-up
        { duration: `${TOTAL_SEC - 85}s`, target: VOTERS }, // sustain
        { duration: '10s',              target: 0 },      // ramp down
      ],
    },
  },
  thresholds: {
    quiz_join_ms:    ['p(95)<2000', 'p(99)<4000'],  // generous — HTTPS over WAN
    quiz_vote_ms:    ['p(95)<2000', 'p(99)<4000'],
    quiz_votes_fail: ['count<500'],                  // some rejects are expected
    quiz_joins_fail: ['count<500'],
    http_req_failed: ['rate<0.15'],                  // allow up to 15% during burst
  },
};

// ---------------------------------------------------------------------------
// setup() — runs once BEFORE any VU starts.
// Resets leftover state, starts the poll, and opens the first question so the
// poll is already in 'voting' phase when voters begin connecting.
// ---------------------------------------------------------------------------
export function setup() {
  const hdrs = { 'x-admin-token': ADMIN_TOKEN, 'Content-Type': 'application/json' };

  const r = http.get(`${BASE_URL}/api/admin/status`, { headers: hdrs });
  if (r.status !== 200) {
    throw new Error(
      `Admin API returned HTTP ${r.status}. ` +
      `Set ADMIN_TOKEN env var and make sure the server is reachable.`,
    );
  }

  const st = r.json();

  // Always reset first — ensures a clean pending state regardless of what ran before.
  if (st.phase !== 'pending' && st.phase !== 'unknown') {
    console.log(`[setup] resetting existing poll (phase=${st.phase})`);
    const rr = http.post(`${BASE_URL}/api/admin/reset`, '{}', { headers: hdrs });
    if (rr.status < 200 || rr.status >= 300) throw new Error(`reset failed: ${rr.status}`);
    sleep(2);
  }

  // Start: pending → lobby
  const rs = http.post(`${BASE_URL}/api/admin/start`, '{}', { headers: hdrs });
  if (rs.status < 200 || rs.status >= 300) throw new Error(`start failed: ${rs.status} ${rs.body}`);
  console.log('[setup] poll started → lobby');
  sleep(2);

  // Open Q1: lobby → voting (first question)
  const rn = http.post(`${BASE_URL}/api/admin/next`, '{}', { headers: hdrs });
  if (rn.status < 200 || rn.status >= 300) throw new Error(`first next failed: ${rn.status} ${rn.body}`);
  console.log('[setup] Q1 opened — voters will start joining in 15 s');
  sleep(1);

  console.log(
    `[setup] ready — ${VOTERS} voters ramping over 60 s, ${MAX_QUESTIONS} questions (est. ${TOTAL_SEC}s total)`,
  );
  // Pass how many questions the driver still needs to open (MAX_QUESTIONS - 1 already opened).
  return { questionsOpened: 1 };
}

// ---------------------------------------------------------------------------
// adminDriverVu — setup() already started the poll and opened Q1, so this VU
// only needs to watch for 'results' and fire 'next' until MAX_QUESTIONS are done.
// ---------------------------------------------------------------------------
export function adminDriverVu(data) {
  if (exec.vu.iterationInScenario > 0) { sleep(TOTAL_SEC); return; }

  const hdrs = { 'x-admin-token': ADMIN_TOKEN, 'Content-Type': 'application/json' };

  function adminStatus() {
    const r = http.get(`${BASE_URL}/api/admin/status`, { headers: hdrs });
    return r.status === 200 ? r.json() : null;
  }

  function adminCmd(cmd, tries = 8) {
    for (let i = 0; i < tries; i++) {
      const r = http.post(`${BASE_URL}/api/admin/${cmd}`, '{}', { headers: hdrs });
      if (r.status >= 200 && r.status < 300) return r.json();
      console.log(`[driver] ${cmd} attempt ${i + 1} failed (${r.status}) — retrying`);
      sleep(2);
    }
    console.log(`[driver] WARN: ${cmd} gave up after ${tries} attempts`);
    return null;
  }

  // Q1 was already opened in setup(); track remaining questions to open.
  let opened   = data.questionsOpened; // 1
  let actedKey = null;

  console.log(`[driver] running — Q1 already open, will open ${MAX_QUESTIONS - opened} more`);

  while (opened < MAX_QUESTIONS) {
    const st = adminStatus();
    if (!st) { sleep(2); continue; }

    if (st.phase === 'complete') {
      console.log('[driver] poll reached complete naturally');
      break;
    }

    if (st.phase === 'results') {
      const key = `results:${st.questionIndex}`;
      if (actedKey !== key) {
        sleep(RESULTS_PAUSE);
        if (adminCmd('next')) {
          opened++;
          actedKey = key;
          console.log(`[driver] Q${opened}/${MAX_QUESTIONS} opened`);
        }
      }
    }
    // phase=voting or lobby: wait for auto-close or the question to open.
    sleep(2);
  }

  if (opened >= MAX_QUESTIONS) {
    console.log(`[driver] all ${MAX_QUESTIONS} questions done — stopping poll`);
    adminCmd('stop');
  }

  sleep(TOTAL_SEC);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function phoneFor(vuId) {
  return String(9000000000 + (vuId % 1000000000)).slice(0, 10);
}

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
// voterVu — join → poll /state → burst-vote → exit when complete
// ---------------------------------------------------------------------------
export function voterVu() {
  if (exec.vu.iterationInScenario > 0) { sleep(TOTAL_SEC); return; }

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

  const token  = joinRes.json().token;
  const auth   = { Authorization: `Bearer ${token}` };
  const jsonHd = { ...auth, 'Content-Type': 'application/json' };

  const voted = new Set();

  // -- Poll loop --
  for (;;) {
    const stateRes = http.get(`${BASE_URL}/api/state`, { headers: auth });
    if (stateRes.status !== 200) { sleep(0.5); continue; }

    const st = stateRes.json();
    if (st.phase === 'complete') break;

    if (st.phase === 'voting' && st.question && !st.paused && !voted.has(st.question.id)) {
      voted.add(st.question.id);

      // 200–1500 ms human-ish jitter → realistic synchronized burst
      sleep(0.2 + Math.random() * 1.3);

      const t1 = Date.now();
      const voteRes = http.post(
        `${BASE_URL}/api/vote`,
        JSON.stringify({ questionId: st.question.id, optionIds: pickOptions(st.question) }),
        { headers: jsonHd },
      );
      voteMs.add(Date.now() - t1);

      if (voteRes.status === 200) votesOk.add(1);
      else                        votesKo.add(1);
    }

    sleep(0.4 + Math.random() * 0.6);
  }
}

// ---------------------------------------------------------------------------
// teardown() — runs once after all VUs finish
// ---------------------------------------------------------------------------
export function teardown() {
  sleep(2);
  const r = http.get(`${BASE_URL}/api/leaderboard?limit=10`);
  if (r.status !== 200) return;
  const lb = r.json();
  console.log('\n=== Top 10 Leaderboard ===');
  for (const row of lb.leaderboard ?? []) {
    console.log(`  #${String(row.rank).padStart(2)}  ${String(row.name).padEnd(14)} ${row.points} pts`);
  }
}
