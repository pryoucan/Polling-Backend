// The authoritative poll clock. Runs in EXACTLY ONE process (the cluster
// primary). It owns phase transitions, scores each question at close, and
// publishes every state change to Redis pub/sub so all workers can fan it out
// to their SSE clients. Workers never mutate poll state — they only read it.

import { query, withTx } from './db.js';
import { redis, keys } from './redis.js';
import { config } from './config.js';
import { getLeaderboard, getSegmentLeaderboard } from './leaderboard.js';
import { seedPoll } from './scripts/seed-core.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Which segment (0-based block of `segmentSize` questions) a question index
// falls in, and its inclusive position range. e.g. size 10, index 12 -> seg 1,
// positions 10..19. Used to scope the rolling segment leaderboard.
function segmentRange(questionIndex) {
  const size = config.segmentSize;
  const index = Math.floor(questionIndex / size);
  return { index, size, from: index * size, to: index * size + size - 1 };
}

let publisher = null;
let subscriber = null;
let state = null; // in-memory mirror of the current authoritative state
let runId = 0; // bumped to cancel the currently-running poll loop
let advanceRequested = false; // set by the 'next' command to advance a host-gated phase

// pause/resume bookkeeping
let running = false; // true while a poll loop is active
let paused = false;
let pausedRemainingMs = 0; // time left in the current phase when paused
let phaseSnapshot = null; // the current timed-phase state object (to re-broadcast on resume)
let phaseDurationMs = 0; // original duration of the current phase (for the bar fill)
let phaseDeadline = 0; // absolute ms when the current phase ends (mutated on resume)

export function getState() {
  return state;
}

// Persist current state to Redis (for workers to validate votes) and broadcast.
async function setState(next) {
  state = next;
  await redis.set(config.stateKey, JSON.stringify(state));
  await publish({ type: 'state', state });
}

async function publish(msg) {
  await publisher.publish(config.eventsChannel, JSON.stringify(msg));
}

// Load the poll and all its questions/options into memory.
async function loadPoll() {
  const { rows: polls } = await query(
    `SELECT id, title FROM poll ORDER BY id DESC LIMIT 1`,
  );
  if (polls.length === 0) throw new Error('No poll found. Run `npm run seed` first.');
  const poll = polls[0];

  const { rows: questions } = await query(
    `SELECT id, prompt, position, min_select, max_select
       FROM question WHERE poll_id = $1 ORDER BY position ASC`,
    [poll.id],
  );

  const { rows: options } = await query(
    `SELECT o.id, o.label, o.position, o.question_id
       FROM option o
       JOIN question q ON q.id = o.question_id
      WHERE q.poll_id = $1
      ORDER BY o.question_id, o.position ASC`,
    [poll.id],
  );
  const byQuestion = new Map();
  for (const o of options) {
    if (!byQuestion.has(o.question_id)) byQuestion.set(o.question_id, []);
    byQuestion.get(o.question_id).push({ id: o.id, label: o.label });
  }

  poll.questions = questions.map((q, idx) => ({
    id: q.id,
    index: idx,
    prompt: q.prompt,
    minSelect: q.min_select,
    maxSelect: q.max_select,
    options: byQuestion.get(q.id) ?? [],
  }));
  return poll;
}

// Authoritative tally for a question, read from Postgres (the source of truth).
// Ranking: most votes first; ties broken by who reached that count FIRST
// (earlier last-vote timestamp wins); final tiebreak is option position.
async function computeTally(questionId) {
  const { rows } = await query(
    `SELECT o.id, o.label, o.position,
            COUNT(v.participant_id)::int AS votes,
            MAX(v.created_at)            AS last_vote_at
       FROM option o
       LEFT JOIN vote v ON v.option_id = o.id
      WHERE o.question_id = $1
      GROUP BY o.id, o.label, o.position
      ORDER BY votes DESC, last_vote_at ASC NULLS LAST, o.position ASC`,
    [questionId],
  );
  return rows.map((r, i) => ({
    rank: i + 1,
    optionId: r.id,
    label: r.label,
    votes: r.votes,
  }));
}

// Freeze the question, award points for top-N options, persist + broadcast.
async function closeQuestion(poll, question) {
  const tally = await computeTally(question.id);
  const winners = tally.slice(0, config.topNScoring); // [{rank, optionId, votes}]

  await withTx(async (c) => {
    // Persist the frozen result for audit/replay.
    await c.query(
      `INSERT INTO question_result (question_id, tally)
       VALUES ($1, $2)
       ON CONFLICT (question_id) DO UPDATE SET tally = EXCLUDED.tally`,
      [question.id, JSON.stringify(tally)],
    );

    // Award points: everyone who selected a winning option gets that rank's weight.
    for (const w of winners) {
      const weight = config.scoreWeights[w.rank - 1] ?? 0;
      if (weight <= 0) continue;
      await c.query(
        `INSERT INTO score (poll_id, participant_id, points)
         SELECT $1, v.participant_id, $2
           FROM vote v
          WHERE v.option_id = $3
         ON CONFLICT (poll_id, participant_id)
         DO UPDATE SET points = score.points + EXCLUDED.points`,
        [poll.id, weight, w.optionId],
      );
      // Same award recorded per-question — this is what the segment leaderboard
      // sums over a 10-question range (overall total lives in `score` above).
      await c.query(
        `INSERT INTO question_score (question_id, participant_id, points)
         SELECT $1, v.participant_id, $2
           FROM vote v
          WHERE v.option_id = $3
         ON CONFLICT (question_id, participant_id)
         DO UPDATE SET points = question_score.points + EXCLUDED.points`,
        [question.id, weight, w.optionId],
      );
    }
  });

  const leaderboard = await getLeaderboard(poll.id, 20);
  const seg = segmentRange(question.index);
  const segmentLeaderboard = await getSegmentLeaderboard(poll.id, seg.from, seg.to, 20);
  const winnersOut = winners.map((w) => ({ ...w, weight: config.scoreWeights[w.rank - 1] ?? 0 }));
  await publish({
    type: 'result',
    questionId: question.id,
    tally,
    winners: winnersOut,
    leaderboard,
    segment: seg, // { index, size, from, to }
    segmentLeaderboard, // standings for THIS 10-question block
  });
  return { tally, winners: winnersOut, leaderboard, segment: seg, segmentLeaderboard };
}

// Periodically push the LIVE (cosmetic) tally from Redis during a voting window.
function startLiveTally(question, closesAtMs) {
  const optionIds = question.options.map((o) => o.id);
  const timer = setInterval(async () => {
    try {
      const scores = await redis.zrevrange(keys.tally(question.id), 0, -1, 'WITHSCORES');
      const counts = {};
      for (let i = 0; i < scores.length; i += 2) counts[scores[i]] = parseInt(scores[i + 1], 10);
      const live = optionIds.map((id) => ({ optionId: id, votes: counts[id] ?? 0 }));
      await publish({ type: 'tally', questionId: question.id, tally: live });
    } catch {
      /* live tally is best-effort; ignore transient redis errors */
    }
  }, 2000);
  // Stop shortly after the window closes.
  setTimeout(() => clearInterval(timer), (closesAtMs - Date.now()) + 1500);
}

// Lightweight poll metadata for the idle screen (no questions loaded).
async function pollMeta() {
  const { rows } = await query(`SELECT id, title FROM poll ORDER BY id DESC LIMIT 1`);
  if (rows.length === 0) return null;
  const { rows: qc } = await query(`SELECT COUNT(*)::int AS c FROM question WHERE poll_id = $1`, [rows[0].id]);
  return { id: rows[0].id, title: rows[0].title, total: qc[0].c };
}

// The "waiting for the host to start" state.
async function setIdle() {
  const meta = await pollMeta();
  await setState({
    pollId: meta?.id ?? null,
    title: meta?.title ?? 'Live Poll',
    phase: 'pending',
    total: meta?.total ?? 0,
    question: null,
    serverNow: Date.now(),
  });
}

// Wipe a run's votes/submissions/scores, but KEEP question_result so a finished
// run's top-options records survive a restart (each is overwritten when its
// question closes again). Reseed still clears everything via CASCADE.
// Participants stay registered so they remain logged in across a restart.
async function clearRuntimeData() {
  await query('TRUNCATE vote, question_submission, score, question_score RESTART IDENTITY');
}

// Enter a timed phase: remember it (so pause/resume can re-broadcast it) and
// broadcast it. `durationMs` is the original phase length, used for the bar.
async function enterTimedPhase(stateObj) {
  phaseSnapshot = stateObj;
  phaseDurationMs = stateObj.closesAt - stateObj.opensAt;
  phaseDeadline = stateObj.closesAt;
  await setState(stateObj);
}

// Enter a host-gated phase (lobby / results): no timer, nothing to pause. The
// loop blocks in waitForNext() until the host clicks Next.
async function enterHeldPhase(stateObj) {
  phaseSnapshot = null; // a held phase has no countdown to freeze
  phaseDeadline = 0;
  await setState(stateObj);
}

// Wait until the host issues 'next' (or the run is cancelled by start/stop/
// reset). Used for the host-gated phases, which have no auto timer. Returns
// true when advanced, false if the run was superseded.
async function waitForNext(isActive) {
  advanceRequested = false; // ignore any stray click from the previous phase
  for (;;) {
    if (!isActive()) return false;
    if (advanceRequested) {
      advanceRequested = false;
      return true;
    }
    await sleep(120);
  }
}

// Wait until the current phase's deadline, honoring pause. Returns true when the
// phase completes, false if the run was cancelled (start/stop/reset).
async function waitPhase(isActive) {
  for (;;) {
    if (!isActive()) return false;
    if (paused) {
      await sleep(120); // frozen — don't advance toward the deadline
      continue;
    }
    const remain = phaseDeadline - Date.now();
    if (remain <= 0) return true;
    await sleep(Math.min(remain, 120));
  }
}

// The poll loop for ONE run. `isActive()` returns false once this run has been
// superseded (start/stop/reset bumped runId), so the loop bails out cleanly.
async function runPoll(isActive) {
  running = true;
  try {
    const poll = await loadPoll();
    await query(`UPDATE poll SET status = 'running' WHERE id = $1`, [poll.id]);
    const total = poll.questions.length;

    // Lobby — held until the host clicks Begin (no auto timer).
    if (!isActive()) return;
    console.log(`[clock] lobby — waiting for host to begin (${total} questions queued)`);
    await enterHeldPhase({
      pollId: poll.id,
      title: poll.title,
      phase: 'lobby',
      total,
      question: null,
      awaitingNext: true,
      serverNow: Date.now(),
    });
    if (!(await waitForNext(isActive))) return;

    for (const question of poll.questions) {
      if (!isActive()) return;
      const opensAt = Date.now();
      const closesAt = opensAt + config.voteSeconds * 1000;

      await redis.del(keys.tally(question.id)); // fresh live count

      // Segment standings going INTO this question (empty at the start of a new
      // round, then reflects the earlier questions in the block).
      const votingSeg = segmentRange(question.index);
      const votingSegBoard = await getSegmentLeaderboard(poll.id, votingSeg.from, votingSeg.to, 20);
      await enterTimedPhase({
        pollId: poll.id,
        title: poll.title,
        phase: 'voting',
        total,
        question: {
          id: question.id,
          index: question.index,
          prompt: question.prompt,
          minSelect: question.minSelect,
          maxSelect: question.maxSelect,
          options: question.options,
        },
        opensAt,
        closesAt,
        segment: votingSeg, // which 10-question block / round this is
        segmentLeaderboard: votingSegBoard,
        serverNow: Date.now(),
      });
      console.log(`[clock] Q${question.index + 1}/${total} "${question.prompt}" open for ${config.voteSeconds}s`);
      startLiveTally(question, closesAt);

      if (!(await waitPhase(isActive))) return;
      // Wait past the vote grace window before tallying. routes.js accepts votes
      // up to closesAt + LATE_GRACE_MS (750ms); we wait 1000ms so there's a
      // comfortable ~250ms margin for an accepted vote's transaction to COMMIT
      // before the tally reads — otherwise, under DB load, a last-instant vote
      // could commit after the tally and silently miss the count/scoring.
      await sleep(1000);
      if (!isActive()) return;

      const { tally, winners, leaderboard, segment, segmentLeaderboard } = await closeQuestion(poll, question);
      // Results — held until the host clicks Next (no auto timer).
      await enterHeldPhase({
        pollId: poll.id,
        title: poll.title,
        phase: 'results',
        total,
        question: { id: question.id, index: question.index, prompt: question.prompt },
        tally,
        winners,
        leaderboard,
        segment, // { index, size, from, to } of the 10-question block this question belongs to
        segmentLeaderboard, // standings for that block
        segmentEnd: question.index === segment.to || question.index === total - 1, // last Q of a block → prize moment
        awaitingNext: true,
        serverNow: Date.now(),
      });
      if (!(await waitForNext(isActive))) return;
    }

    if (!isActive()) return;
    await query(`UPDATE poll SET status = 'complete' WHERE id = $1`, [poll.id]);
    const finalBoard = await getLeaderboard(poll.id, 50);
    const lastSeg = segmentRange(total - 1);
    const lastSegBoard = await getSegmentLeaderboard(poll.id, lastSeg.from, lastSeg.to, 50);
    phaseSnapshot = null; // nothing pausable once complete
    await setState({
      pollId: poll.id,
      title: poll.title,
      phase: 'complete',
      total,
      question: null,
      leaderboard: finalBoard,
      segment: lastSeg, // the final block's standings, for one last prize moment
      segmentLeaderboard: lastSegBoard,
      segmentEnd: true,
      serverNow: Date.now(),
    });
    console.log('[clock] poll complete');
  } finally {
    running = false;
  }
}

// --- control commands (invoked by the primary on Redis control messages) ---

function cancelCurrentRun() {
  runId += 1; // any in-flight runPoll sees its isActive() turn false
  paused = false;
  advanceRequested = false;
  phaseSnapshot = null;
}

// Advance a host-gated phase: lobby -> first question, or results -> next
// question (or -> complete after the last one). No-op if nothing is waiting.
function advancePoll() {
  if (!running) return;
  advanceRequested = true;
}

// Pause: freeze the current phase, remembering how much time was left.
async function pausePoll() {
  if (!running || paused || !phaseSnapshot) return;
  paused = true;
  pausedRemainingMs = Math.max(0, phaseDeadline - Date.now());
  await setState({ ...phaseSnapshot, paused: true, pausedRemainingMs, serverNow: Date.now() });
  console.log(`[clock] paused (${Math.round(pausedRemainingMs / 1000)}s left)`);
}

// Resume: shift the deadline forward by the frozen remaining time and continue.
async function resumePoll() {
  if (!running || !paused || !phaseSnapshot) return;
  paused = false;
  const now = Date.now();
  const newClose = now + pausedRemainingMs;
  phaseDeadline = newClose;
  phaseSnapshot = { ...phaseSnapshot, opensAt: newClose - phaseDurationMs, closesAt: newClose };
  await setState({ ...phaseSnapshot, paused: false, serverNow: now });
  console.log('[clock] resumed');
}

async function startPoll() {
  cancelCurrentRun();
  const meta = await pollMeta();
  if (!meta || meta.total === 0) {
    console.warn('[clock] start ignored — no questions seeded');
    return;
  }
  await clearRuntimeData(); // fresh scores for the new run
  const myRun = runId;
  runPoll(() => myRun === runId).catch((err) => console.error('[clock] run error:', err));
}

async function stopPoll() {
  cancelCurrentRun();
  await setIdle();
}

async function resetPoll() {
  cancelCurrentRun();
  await clearRuntimeData();
  await setIdle();
}

async function reseedPoll(count) {
  cancelCurrentRun();
  await seedPoll(count); // truncates + re-creates questions (and clears votes via cascade)
  await setIdle();
}

async function handleControl(raw) {
  let msg;
  try {
    msg = JSON.parse(raw);
  } catch {
    return;
  }
  console.log('[clock] control command:', msg.cmd);
  switch (msg.cmd) {
    case 'start':
      return startPoll();
    case 'stop':
      return stopPoll();
    case 'reset':
      return resetPoll();
    case 'reseed':
      return reseedPoll(parseInt(msg.count, 10) || 100);
    case 'pause':
      return pausePoll();
    case 'resume':
      return resumePoll();
    case 'next':
      return advancePoll();
    default:
      console.warn('[clock] unknown control command:', msg.cmd);
  }
}

// Entry point (runs in the cluster primary). Sets the idle state and listens for
// host control commands. Auto-starts only if AUTO_START=true.
export async function initClock(makePublisher, makeSubscriber) {
  publisher = makePublisher();
  subscriber = makeSubscriber();

  await setIdle();

  await subscriber.subscribe(config.controlChannel);
  subscriber.on('message', (_channel, raw) => {
    handleControl(raw).catch((err) => console.error('[clock] control error:', err));
  });

  console.log(`[clock] controller ready (autoStart=${config.autoStart})`);
  if (config.autoStart) startPoll();
}
