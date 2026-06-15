// Self-driving load simulator. Unlike simulate.js, this does NOT need a human
// clicking "Next" in the admin panel — it drives the poll itself via the admin
// API (start → open each question → let it close → advance), while thousands of
// virtual voters connected over SSE vote the moment each question opens.
//
// This exercises the FULL path under load: SSE fan-out + the vote/DB write burst
// when each question opens. (simulate.js only ever measured idle connections,
// so votes were always 0 unless someone clicked Next by hand.)
//
// Usage:  node test/simulate.auto.js [voters] [baseUrl] [questions]
//   ADMIN_TOKEN=xxxx node test/simulate.auto.js 2000 http://3.110.46.46:8080 5
//
//   voters     how many virtual voters to spawn          (default 1000)
//   baseUrl    server base URL                            (default http://localhost:8080)
//   questions  how many questions to auto-run, then stop  (default 5)
//
// The admin token MUST match the server's ADMIN_TOKEN (.env on the box). Pass it
// via the ADMIN_TOKEN env var; falls back to the dev default 'admin-dev-token'.
//
// NOTE: one machine can only open so many connections (ephemeral ports / CPU).
// For true 10k+, run several of these across multiple machines.

const VOTERS = parseInt(process.argv[2] ?? '1000', 10);
const BASE = process.argv[3] ?? 'http://localhost:8080';
const MAX_QUESTIONS = parseInt(process.argv[4] ?? '5', 10);
const ADMIN_TOKEN = process.env.ADMIN_TOKEN ?? 'admin-dev-token';

const RAMP_MS = 3; // stagger voter start-up so they don't all connect in the same ms
const CONNECT_GRACE_MS = 4000; // let voters connect before we start the poll
const DRIVE_POLL_MS = 1000; // how often the driver checks the poll phase
const RESULTS_PAUSE_MS = 3000; // dwell on each results screen before advancing

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// voter side (SSE) — copied from simulate.js, hardened against transient resets
// ---------------------------------------------------------------------------

function cookieFrom(res) {
  const sc = res.headers.get('set-cookie');
  if (!sc) return null;
  return sc.split(';')[0]; // "pid=...."
}

// Server requires a unique-ish 10-digit phone at join. Derive a deterministic
// 10-digit number from the voter index so each virtual voter is distinct.
function phoneFor(i) {
  return String(9000000000 + (i % 1000000000)).slice(0, 10);
}

// Tally a reason string into a histogram so "joinFail=358 / rejected=4137" becomes
// a breakdown that tells you whether the limit is THIS machine (network throws) or
// the SERVER (HTTP 5xx/429) or just benign timing (HTTP 409 = question closed).
function bump(obj, key) {
  obj[key] = (obj[key] ?? 0) + 1;
}

async function join(name, i) {
  // status 0 = fetch threw (client-side network/port exhaustion); a non-2xx status
  // = the server actively refused. Surfacing the difference is the whole point.
  try {
    const res = await fetch(`${BASE}/api/join`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, phone: phoneFor(i) }),
    });
    if (!res.ok) return { cookie: null, status: res.status };
    return { cookie: cookieFrom(res), status: res.status };
  } catch (e) {
    return { cookie: null, status: 0, err: e.cause?.code || e.name || 'network' };
  }
}

async function vote(cookie, questionId, optionIds) {
  try {
    const res = await fetch(`${BASE}/api/vote`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', cookie },
      body: JSON.stringify({ questionId, optionIds }),
    });
    return { status: res.status, body: await res.json().catch(() => ({})) };
  } catch {
    return { status: 0, body: {} };
  }
}

function pickOptions(question) {
  const ids = question.options.map((o) => o.id);
  const n = Math.min(question.maxSelect, 3);
  const picks = new Set();
  while (picks.size < n) {
    const r = Math.floor(Math.pow(Math.random(), 2) * ids.length); // bias to front
    picks.add(ids[r]);
  }
  return [...picks];
}

async function consumeSse(res, onEvent, isDone) {
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  outer: for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf('\n\n')) >= 0) {
      const frame = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      let event = 'message';
      let data = '';
      for (const line of frame.split('\n')) {
        if (line.startsWith('event:')) event = line.slice(6).trim();
        else if (line.startsWith('data:')) data += line.slice(5).trim();
      }
      if (data) onEvent(event, data);
      if (isDone()) break outer;
    }
  }
  try {
    await reader.cancel();
  } catch {
    /* already closed */
  }
}

async function runVoter(i, stats, isStopped) {
  const { cookie, status, err } = await join(`Voter ${i}`, i);
  if (!cookie) {
    stats.joinFail++;
    bump(stats.joinReasons, status === 0 ? `network(${err})` : `HTTP ${status}`);
    return;
  }
  stats.joined++;

  let res;
  try {
    res = await fetch(`${BASE}/api/events`, { headers: { Accept: 'text/event-stream' } });
  } catch {
    stats.sseFail++;
    return;
  }
  if (!res.ok || !res.body) {
    stats.sseFail++;
    return;
  }
  stats.connected++;

  const voted = new Set();

  const onEvent = (event, data) => {
    if (event !== 'state') return;
    let msg;
    try {
      msg = JSON.parse(data);
    } catch {
      return;
    }
    const s = msg.state;
    if (!s) return;
    // A new voting question just opened — vote once, with human-ish jitter.
    if (s.phase === 'voting' && s.question && !s.paused && !voted.has(s.question.id)) {
      voted.add(s.question.id);
      (async () => {
        await sleep(200 + Math.random() * 1300);
        const r = await vote(cookie, s.question.id, pickOptions(s.question));
        if (r.status === 200) stats.votes++;
        else {
          stats.voteRejected++;
          bump(stats.rejReasons, r.status === 0 ? 'network' : `HTTP ${r.status}: ${r.body?.error ?? ''}`);
        }
      })();
    }
  };

  try {
    await consumeSse(res, onEvent, isStopped);
  } catch {
    /* connection dropped */
  }
  stats.connected--;
  stats.finished++;
}

// ---------------------------------------------------------------------------
// admin driver — runs the poll automatically through MAX_QUESTIONS questions
// ---------------------------------------------------------------------------

async function adminStatus() {
  const res = await fetch(`${BASE}/api/admin/status`, {
    headers: { 'x-admin-token': ADMIN_TOKEN },
  });
  if (!res.ok) throw new Error(`admin status ${res.status} (check ADMIN_TOKEN)`);
  return res.json();
}

// Retry transient failures (e.g. the server is briefly slow under a vote burst
// and the request times out) instead of letting one blip crash the whole run.
async function adminCmd(cmd, tries = 5) {
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(`${BASE}/api/admin/${cmd}`, {
        method: 'POST',
        headers: { 'x-admin-token': ADMIN_TOKEN, 'Content-Type': 'application/json' },
        body: '{}',
      });
      if (res.ok) return res.json();
    } catch {
      /* network error / timeout — retry below */
    }
    await sleep(1000);
  }
  return null; // gave up; caller logs and keeps going
}

// Drives: pending -> start -> (lobby -> next) -> [voting -> auto-close -> results
// -> next] x MAX_QUESTIONS. Resolves when MAX_QUESTIONS have run (or the poll
// completes). Event-driven off the phase, so it works for any VOTE_SECONDS.
async function drivePoll(log) {
  let opened = 0;
  let actedKey = null; // dedupe: act once per held-phase entry
  let started = false;

  for (;;) {
    let st;
    try {
      st = await adminStatus();
    } catch (e) {
      log(`driver: ${e.message}`);
      await sleep(DRIVE_POLL_MS);
      continue;
    }

    if (st.phase === 'complete') {
      log('poll complete');
      return;
    }

    if (st.phase === 'pending' || st.phase === 'unknown') {
      if (!started) {
        if (await adminCmd('start')) {
          started = true;
          log('started — entering lobby');
        } else {
          log('start failed — retrying');
        }
      }
    } else if (st.phase === 'lobby') {
      if (actedKey !== 'lobby') {
        if (await adminCmd('next')) {
          opened++;
          actedKey = 'lobby';
          log(`opened question ${opened}/${MAX_QUESTIONS}`);
        } else {
          log('next failed — retrying');
        }
      }
    } else if (st.phase === 'results') {
      if (opened >= MAX_QUESTIONS) {
        log(`ran ${MAX_QUESTIONS} questions — stopping poll`);
        await adminCmd('stop');
        return;
      }
      const key = `results:${st.questionIndex}`;
      if (actedKey !== key) {
        await sleep(RESULTS_PAUSE_MS); // let voters/leaderboard settle
        if (await adminCmd('next')) {
          opened++;
          actedKey = key;
          log(`opened question ${opened}/${MAX_QUESTIONS}`);
        } else {
          log('next failed — retrying');
        }
      }
    }
    // 'voting' -> do nothing; wait for it to auto-close.
    await sleep(DRIVE_POLL_MS);
  }
}

// ---------------------------------------------------------------------------

async function main() {
  console.log(
    `Auto-sim: ${VOTERS} voters vs ${BASE}, running ${MAX_QUESTIONS} questions (token=${ADMIN_TOKEN === 'admin-dev-token' ? 'dev-default' : 'custom'})`,
  );

  // Fail fast with a clear message if the admin token is wrong / server unreachable.
  try {
    await adminStatus();
  } catch (e) {
    console.error(`\nCannot reach admin API: ${e.message}`);
    console.error('Set the right token:  ADMIN_TOKEN=your-token node test/simulate.auto.js ...');
    process.exit(1);
  }

  const stats = { joined: 0, joinFail: 0, connected: 0, sseFail: 0, votes: 0, voteRejected: 0, finished: 0, joinReasons: {}, rejReasons: {} };
  let stopped = false;
  const isStopped = () => stopped;

  // Spawn voters (staggered).
  const voters = Array.from({ length: VOTERS }, (_, i) =>
    (async () => {
      await sleep(i * RAMP_MS);
      return runVoter(i + 1, stats, isStopped);
    })(),
  );

  const ticker = setInterval(() => {
    process.stdout.write(
      `\r joined=${stats.joined} joinFail=${stats.joinFail} connected=${stats.connected} votes=${stats.votes} rejected=${stats.voteRejected} sseFail=${stats.sseFail}   `,
    );
  }, 1000);

  const log = (m) => process.stdout.write(`\n[driver] ${m}\n`);

  // Wait for most voters to actually CONNECT before driving the poll — otherwise
  // (on a slow client) questions open while nobody's connected and votes are
  // missed. Proceed once 90% are connected, or after a hard cap, whichever first.
  const target = Math.floor(VOTERS * 0.9);
  const maxWaitMs = 90_000;
  let waited = 0;
  while (stats.connected < target && stats.connected + stats.joinFail + stats.sseFail < VOTERS && waited < maxWaitMs) {
    await sleep(500);
    waited += 500;
  }
  log(`${stats.connected}/${VOTERS} connected — starting poll`);
  await drivePoll(log);

  // Poll is stopped/complete: tell voters to close and finish up.
  stopped = true;
  await Promise.allSettled([sleep(1500), ...voters.map((p) => Promise.race([p, sleep(1500)]))]);
  clearInterval(ticker);

  const { joinReasons, rejReasons, ...counts } = stats;
  console.log(`\n\nFinal stats:`, counts);
  // network(...) -> this machine's limit (ports/CPU). HTTP 5xx/429 -> server limit.
  // HTTP 409 -> benign (question closed / not open when the late vote landed).
  if (Object.keys(joinReasons).length) console.log(`\njoinFail reasons:`, joinReasons);
  if (Object.keys(rejReasons).length) console.log(`vote-reject reasons:`, rejReasons);
  try {
    const lb = await (await fetch(`${BASE}/api/leaderboard?limit=10`)).json();
    console.log(`\nTop 10 leaderboard:`);
    for (const row of lb.leaderboard ?? []) {
      console.log(`  #${row.rank}  ${String(row.name).padEnd(14)} ${row.points} pts`);
    }
  } catch (e) {
    console.log(`\n(leaderboard fetch failed: ${e.cause?.code ?? e.message})`);
  }
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
