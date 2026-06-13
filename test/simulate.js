// End-to-end load simulator. Two modes:
//
//   sse  (default) — each virtual voter behaves like a REAL browser: it opens a
//                    persistent SSE connection to /api/events and votes the
//                    moment a question opens. This reproduces the two things a
//                    real live event does that plain polling does not:
//                      (1) thousands of concurrent long-lived connections, and
//                      (2) a synchronized vote burst when each question opens.
// //
//   poll           — legacy: each voter polls /api/state on a loop. Stresses the
//                    request/DB path hard, but opens no live connection and
//                    smooths away the vote burst. Useful for raw-throughput runs.
//
// Usage:  node test/simulate.js [voters] [baseUrl] [mode]
//   node test/simulate.js 2000 http://localhost:8080            # sse (real-ish)
//   node test/simulate.js 2000 http://localhost:8080 poll       # legacy polling
//
// NOTE: holding thousands of OPEN SSE sockets from one machine hits the OS
// file-descriptor/port limit (raise it with `ulimit -n 100000`). For true 10k+,
// run several of these across multiple machines.

const VOTERS = parseInt(process.argv[2] ?? '20', 10);
const BASE = process.argv[3] ?? 'http://localhost:8080';
const MODE = (process.argv[4] ?? 'sse').toLowerCase();

// Stagger voter start-up so they connect over a few seconds instead of all in
// the same millisecond — more realistic, and it stops one Node process from
// stampeding the /join endpoint (and itself).
const RAMP_MS = 3;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function cookieFrom(res) {
  const sc = res.headers.get('set-cookie');
  if (!sc) return null;
  return sc.split(';')[0]; // "pid=...."
}

async function getState(cookie) {
  const res = await fetch(`${BASE}/api/state`, { headers: cookie ? { cookie } : {} });
  return res.json();
}

async function join(name) {
  // Swallow transient network errors (ECONNRESET, etc.) so one blip under load
  // doesn't abort the whole simulation — the caller treats null as a join fail.
  try {
    const res = await fetch(`${BASE}/api/join`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    return cookieFrom(res);
  } catch {
    return null;
  }
}

async function vote(cookie, questionId, optionIds) {
  // status 0 = the request never completed (transient network reset); callers
  // treat anything that isn't 200 as a rejected vote and keep going.
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
  // Bias toward earlier options so a clear top-N emerges, plus some spread.
  const ids = question.options.map((o) => o.id);
  const n = Math.min(question.maxSelect, 3);
  const picks = new Set();
  while (picks.size < n) {
    // weighted toward the front of the list
    const r = Math.floor(Math.pow(Math.random(), 2) * ids.length);
    picks.add(ids[r]);
  }
  return [...picks];
}

// --- SSE mode: behave like a real browser EventSource. ---

// Minimal SSE parser over native fetch streaming. Splits the byte stream into
// `\n\n`-delimited frames and pulls out the `event:`/`data:` fields. Stops as
// soon as isDone() returns true (so a voter exits promptly on 'complete').
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
        // ignore comment (': ping') and 'retry:' lines
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

async function runVoterSse(i, stats) {
  const cookie = await join(`Voter ${i}`);
  if (!cookie) {
    stats.joinFail++;
    return;
  }
  stats.joined++;

  // Open the live stream — public, exactly like a real browser's EventSource.
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
  let finished = false;

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
    if (s.phase === 'complete') {
      finished = true;
      return;
    }
    // A new voting question just opened (pushed to everyone at once). Vote once.
    if (s.phase === 'voting' && s.question && !voted.has(s.question.id)) {
      voted.add(s.question.id); // guard synchronously so we never double-vote
      // Human-ish think time so 10k clients don't hit the exact same millisecond,
      // but still inside a tight window -> realistic burst.
      (async () => {
        await sleep(200 + Math.random() * 1300);
        try {
          const r = await vote(cookie, s.question.id, pickOptions(s.question));
          if (r.status === 200) stats.votes++;
          else stats.voteRejected++;
        } catch {
          stats.voteRejected++;
        }
      })();
    }
  };

  try {
    await consumeSse(res, onEvent, () => finished);
  } catch {
    /* connection dropped */
  }
  stats.connected--;
  stats.finished++;
}

// --- poll mode (legacy): hammer /state on a loop, no live connection. ---

async function runVoterPoll(i, stats) {
  const cookie = await join(`Voter ${i}`);
  if (!cookie) {
    stats.joinFail++;
    return;
  }
  stats.joined++;

  const votedQuestions = new Set();
  for (;;) {
    let st;
    try {
      st = await getState(cookie);
    } catch {
      await sleep(500);
      continue;
    }

    if (st.phase === 'complete') break;
    if (st.phase === 'voting' && st.question && !votedQuestions.has(st.question.id)) {
      votedQuestions.add(st.question.id);
      const r = await vote(cookie, st.question.id, pickOptions(st.question));
      if (r.status === 200) stats.votes++;
      else stats.voteRejected++;
    }
    await sleep(400 + Math.random() * 600);
  }
  stats.finished++;
}

async function main() {
  console.log(`Simulating ${VOTERS} voters against ${BASE}  [mode=${MODE}]`);
  const stats = { joined: 0, joinFail: 0, connected: 0, sseFail: 0, votes: 0, voteRejected: 0, finished: 0 };
  const runVoter = MODE === 'poll' ? runVoterPoll : runVoterSse;

  // Ramp: stagger each voter's start so they don't all fire at once.
  const voters = Array.from({ length: VOTERS }, (_, i) =>
    (async () => {
      await sleep(i * RAMP_MS);
      return runVoter(i + 1, stats);
    })(),
  );

  // Progress ticker. `connected` = live SSE connections (the metric that matters);
  // joinFail = joins the server/proxy refused or reset (the real capacity signal).
  const ticker = setInterval(() => {
    process.stdout.write(
      `\r joined=${stats.joined} joinFail=${stats.joinFail} connected=${stats.connected} votes=${stats.votes} rejected=${stats.voteRejected} sseFail=${stats.sseFail} finished=${stats.finished}   `,
    );
  }, 1000);

  // allSettled (not all): one voter throwing must never abort the whole run.
  await Promise.allSettled(voters);
  clearInterval(ticker);

  console.log(`\n\nFinal stats:`, stats);
  try {
    const lb = await (await fetch(`${BASE}/api/leaderboard?limit=10`)).json();
    console.log(`\nTop 10 leaderboard:`);
    for (const row of lb.leaderboard ?? []) {
      console.log(`  #${row.rank}  ${row.name.padEnd(12)} ${row.points} pts`);
    }
  } catch (e) {
    console.log(`\n(leaderboard fetch failed: ${e.cause?.code ?? e.message})`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
