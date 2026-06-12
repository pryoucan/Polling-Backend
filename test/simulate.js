// Lightweight end-to-end simulator / mini load test.
// Spins up N concurrent voters who each join, then vote once on every question
// as it opens. Useful both as a smoke test and a small concurrency check.
//
// Usage:  node test/simulate.js [voters] [baseUrl]
//   node test/simulate.js 50 http://localhost:8080

const VOTERS = parseInt(process.argv[2] ?? '20', 10);
const BASE = process.argv[3] ?? 'http://localhost:8080';

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
  const res = await fetch(`${BASE}/api/join`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name }),
  });
  return cookieFrom(res);
}

async function vote(cookie, questionId, optionIds) {
  const res = await fetch(`${BASE}/api/vote`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', cookie },
    body: JSON.stringify({ questionId, optionIds }),
  });
  return { status: res.status, body: await res.json().catch(() => ({})) };
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

async function runVoter(i, stats) {
  const cookie = await join(`Voter ${i}`);
  if (!cookie) { stats.joinFail++; return; }
  stats.joined++;

  const votedQuestions = new Set();
  // Poll state until the poll completes.
  for (;;) {
    let st;
    try { st = await getState(cookie); } catch { await sleep(500); continue; }

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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  console.log(`Simulating ${VOTERS} voters against ${BASE}`);
  const stats = { joined: 0, joinFail: 0, votes: 0, voteRejected: 0, finished: 0 };
  const voters = Array.from({ length: VOTERS }, (_, i) => runVoter(i + 1, stats));

  // Progress ticker.
  const ticker = setInterval(() => {
    process.stdout.write(`\r joined=${stats.joined} votes=${stats.votes} rejected=${stats.voteRejected} finished=${stats.finished}   `);
  }, 1000);

  await Promise.all(voters);
  clearInterval(ticker);

  const lb = await (await fetch(`${BASE}/api/leaderboard?limit=10`)).json();
  console.log(`\n\nFinal stats:`, stats);
  console.log(`\nTop 10 leaderboard:`);
  for (const row of lb.leaderboard) {
    console.log(`  #${row.rank}  ${row.name.padEnd(12)} ${row.points} pts`);
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
