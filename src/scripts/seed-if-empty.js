// Seeds the default question set ONLY if no poll exists yet. Used by the Docker
// entrypoint so `docker compose up` gives a working stack out of the box,
// without clobbering existing data on restarts.
//   SEED_COUNT controls how many questions (default 100).
import { pool } from '../db.js';
import { seedPoll, pollExists } from './seed-core.js';

const count = parseInt(process.env.SEED_COUNT ?? '100', 10);

try {
  if (await pollExists()) {
    console.log('[seed] a poll already exists — skipping');
  } else {
    await seedPoll(count);
    console.log(`[seed] seeded ${count} questions`);
  }
} catch (err) {
  console.error('[seed] failed:', err);
  process.exitCode = 1;
} finally {
  await pool.end();
}
