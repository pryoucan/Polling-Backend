import express from 'express';
import { query, withTx } from './db.js';
import { redis, keys } from './redis.js';
import { config } from './config.js';
import { getLeaderboard, getParticipantStanding } from './leaderboard.js';
import { makeToken, setSessionCookie, withParticipant } from './auth.js';
import { addClient } from './sse.js';

const LATE_GRACE_MS = 750; // allow votes that left the client just before close

async function readState() {
  const raw = await redis.get(config.stateKey);
  return raw ? JSON.parse(raw) : null;
}

export function buildRouter() {
  const router = express.Router();
  router.use(express.json({ limit: '8kb' }));
  router.use(withParticipant);

  // --- Join: create an anonymous participant, set the signed cookie. ---
  router.post('/join', async (req, res) => {
    const name = String(req.body?.name ?? '').trim().slice(0, 40);
    if (name.length < 1) return res.status(400).json({ error: 'name required' });

    // Keep the leaderboard readable: if this display name (case-insensitive) is
    // already taken, append a numeric suffix, e.g. "Rahul" -> "Rahul (2)".
    const { rows: dup } = await query(
      `SELECT COUNT(*)::int AS c FROM participant
        WHERE lower(display_name) = lower($1)
           OR lower(display_name) LIKE lower($1) || ' (%)'`,
      [name],
    );
    const finalName = dup[0].c === 0 ? name : `${name} (${dup[0].c + 1})`;

    const token = makeToken();
    // The id is the part before the final dot.
    const id = token.slice(0, token.lastIndexOf('.'));
    await query(
      `INSERT INTO participant (id, display_name) VALUES ($1, $2)
       ON CONFLICT (id) DO NOTHING`,
      [id, finalName],
    );
    // Set a cookie (same-origin clients) AND return the token in the body
    // (cross-origin React client stores it and sends it as a Bearer header).
    setSessionCookie(res, token);
    res.json({ id, name: finalName, token });
  });

  // --- Current state snapshot (used on load and on reconnect). ---
  router.get('/state', async (req, res) => {
    const state = await readState();
    if (!state) return res.json({ phase: 'pending' });

    // Fresh serverNow so a late fetch computes clock skew correctly.
    const out = { ...state, serverNow: Date.now() };
    if (req.participantId && state.pollId) {
      out.you = await getParticipantStanding(state.pollId, req.participantId);
      // Has this participant already voted on the current question?
      if (state.phase === 'voting' && state.question) {
        const { rowCount } = await query(
          `SELECT 1 FROM question_submission WHERE participant_id = $1 AND question_id = $2`,
          [req.participantId, state.question.id],
        );
        out.youVoted = rowCount > 0;
      }
    }
    res.json(out);
  });

  // --- Leaderboard (top N). ---
  router.get('/leaderboard', async (req, res) => {
    const state = await readState();
    if (!state?.pollId) return res.json({ leaderboard: [] });
    const limit = Math.min(parseInt(req.query.limit ?? '20', 10) || 20, 100);
    res.json({ leaderboard: await getLeaderboard(state.pollId, limit) });
  });

  // --- Vote. ---
  router.post('/vote', async (req, res) => {
    if (!req.participantId) return res.status(401).json({ error: 'join first' });

    const state = await readState();
    if (!state || state.phase !== 'voting' || !state.question) {
      return res.status(409).json({ error: 'voting is not open' });
    }
    if (state.paused) {
      return res.status(409).json({ error: 'voting is paused' });
    }

    const { questionId, optionIds } = req.body ?? {};
    if (questionId !== state.question.id) {
      return res.status(409).json({ error: 'that question is no longer open' });
    }
    if (Date.now() > state.closesAt + LATE_GRACE_MS) {
      return res.status(409).json({ error: 'time is up for this question' });
    }

    // Validate selection.
    if (!Array.isArray(optionIds)) return res.status(400).json({ error: 'optionIds must be an array' });
    const unique = [...new Set(optionIds)];
    const { minSelect, maxSelect } = state.question;
    if (unique.length < minSelect || unique.length > maxSelect) {
      return res.status(400).json({ error: `select between ${minSelect} and ${maxSelect} options` });
    }
    const validIds = new Set(state.question.options.map((o) => o.id));
    if (!unique.every((id) => validIds.has(id))) {
      return res.status(400).json({ error: 'invalid option for this question' });
    }

    // Atomic one-submission-per-question guard + vote insert.
    try {
      const inserted = await withTx(async (c) => {
        const guard = await c.query(
          `INSERT INTO question_submission (participant_id, question_id)
           VALUES ($1, $2) ON CONFLICT DO NOTHING`,
          [req.participantId, questionId],
        );
        if (guard.rowCount === 0) return false; // already voted this question

        const values = [];
        const params = [];
        unique.forEach((optId, i) => {
          const b = i * 3;
          params.push(`($${b + 1}, $${b + 2}, $${b + 3})`);
          values.push(req.participantId, questionId, optId);
        });
        await c.query(
          `INSERT INTO vote (participant_id, question_id, option_id)
           VALUES ${params.join(', ')} ON CONFLICT DO NOTHING`,
          values,
        );
        return true;
      });

      if (!inserted) return res.status(409).json({ error: 'you already voted on this question' });
    } catch (err) {
      console.error('vote insert failed:', err.message);
      return res.status(500).json({ error: 'could not record vote' });
    }

    // Best-effort live tally bump (cosmetic; the close-time tally is from Postgres).
    try {
      const pipe = redis.pipeline();
      for (const optId of unique) pipe.zincrby(keys.tally(questionId), 1, String(optId));
      pipe.expire(keys.tally(questionId), 3600);
      await pipe.exec();
    } catch {
      /* ignore */
    }

    res.json({ ok: true });
  });

  // --- SSE stream. ---
  router.get('/events', (req, res) => {
    addClient(req, res);
  });

  return router;
}
