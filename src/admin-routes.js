// Host-only endpoints to control the poll. These run in WORKER processes, so
// they can't touch the clock directly — they publish a command on the Redis
// control channel, which the PRIMARY (the clock) consumes and acts on.
import express from 'express';
import { redis } from './redis.js';
import { query } from './db.js';
import { config } from './config.js';
import { asyncHandler, errorHandler } from './http-helpers.js';

// Escape a field for CSV (wrap in quotes, double internal quotes).
function csvCell(v) {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function requireAdmin(req, res, next) {
  const header = req.headers['x-admin-token'];
  const bearer = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  const token = header || bearer;
  if (!config.adminToken || token !== config.adminToken) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

function sendCommand(cmd, extra = {}) {
  return redis.publish(config.controlChannel, JSON.stringify({ cmd, ...extra }));
}

export function buildAdminRouter() {
  const router = express.Router();
  router.use(express.json({ limit: '4kb' }));
  router.use(requireAdmin);

  // Start (or restart) the poll from the lobby. Clears previous run's scores.
  router.post('/start', asyncHandler(async (_req, res) => {
    await sendCommand('start');
    res.json({ ok: true, cmd: 'start' });
  }));

  // Halt the poll and return everyone to the idle/standby screen.
  router.post('/stop', asyncHandler(async (_req, res) => {
    await sendCommand('stop');
    res.json({ ok: true, cmd: 'stop' });
  }));

  // Pause: freeze the current question with its remaining time intact.
  router.post('/pause', asyncHandler(async (_req, res) => {
    await sendCommand('pause');
    res.json({ ok: true, cmd: 'pause' });
  }));

  // Resume: continue from exactly where it was paused.
  router.post('/resume', asyncHandler(async (_req, res) => {
    await sendCommand('resume');
    res.json({ ok: true, cmd: 'resume' });
  }));

  // Advance a host-gated phase: lobby -> first question, or results -> next.
  router.post('/next', asyncHandler(async (_req, res) => {
    await sendCommand('next');
    res.json({ ok: true, cmd: 'next' });
  }));

  // Halt + wipe votes/scores, back to idle.
  router.post('/reset', asyncHandler(async (_req, res) => {
    await sendCommand('reset');
    res.json({ ok: true, cmd: 'reset' });
  }));

  // Replace the questions with a freshly seeded set of `count` questions.
  router.post('/reseed', asyncHandler(async (req, res) => {
    const count = Math.min(Math.max(parseInt(req.body?.count, 10) || 100, 1), 500);
    await sendCommand('reseed', { count });
    res.json({ ok: true, cmd: 'reseed', count });
  }));

  // Export the top-N options for every question that has closed.
  // ?top=10 (default), ?format=csv|json (default json).
  router.get('/results', asyncHandler(async (req, res) => {
    const top = Math.min(Math.max(parseInt(req.query.top, 10) || 10, 1), 50);
    const { rows } = await query(
      `SELECT q.position, q.prompt, qr.tally
         FROM question q
         JOIN question_result qr ON qr.question_id = q.id
        ORDER BY q.position ASC`,
    );
    const results = rows.map((r) => ({
      questionNo: r.position + 1,
      prompt: r.prompt,
      top: (r.tally || []).slice(0, top).map((t) => ({ rank: t.rank, option: t.label, votes: t.votes })),
    }));

    if (req.query.format === 'csv') {
      const lines = ['question_no,prompt,rank,option,votes'];
      for (const q of results) {
        for (const t of q.top) {
          lines.push([q.questionNo, csvCell(q.prompt), t.rank, csvCell(t.option), t.votes].join(','));
        }
      }
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', 'attachment; filename="poll-results.csv"');
      // BOM so Excel reads UTF-8 (Hindi) correctly.
      return res.send('﻿' + lines.join('\n'));
    }

    res.json({ questionsClosed: results.length, top, results });
  }));

  // Current poll status (read from the authoritative Redis state).
  router.get('/status', asyncHandler(async (_req, res) => {
    const raw = await redis.get(config.stateKey);
    const state = raw ? JSON.parse(raw) : null;
    res.json({
      phase: state?.phase ?? 'unknown',
      paused: state?.paused ?? false,
      title: state?.title ?? null,
      total: state?.total ?? 0,
      questionIndex: state?.question?.index ?? null,
      questionPrompt: state?.question?.prompt ?? null,
    });
  }));

  // Tail error handler: a rejected async handler lands here as a 503 instead of
  // crashing the worker. Must be registered after all routes.
  router.use(errorHandler);

  return router;
}
