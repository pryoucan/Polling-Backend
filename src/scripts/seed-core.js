// Shared seeding logic, used both by the `seed` CLI and the Docker entrypoint's
// "seed if empty" step. Cycles through the curated categories in content.js to
// reach the requested question count.
import { query, withTx } from '../db.js';
import { CATEGORIES } from './content.js';

// Every question requires exactly 3 selections — pick your 3, the top-3
// most-voted options win points (weights 3/2/1).
const MIN_SELECT = 3;
const MAX_SELECT = 3;

export async function pollExists() {
  const { rows } = await query('SELECT 1 FROM poll LIMIT 1');
  return rows.length > 0;
}

export async function seedPoll(count = 100, title = 'Live Poll Event') {
  await withTx(async (c) => {
    // Clean slate (CASCADE clears questions/options/votes/etc).
    await c.query('TRUNCATE poll RESTART IDENTITY CASCADE');

    const { rows } = await c.query(
      `INSERT INTO poll (title, status) VALUES ($1, 'pending') RETURNING id`,
      [title],
    );
    const pollId = rows[0].id;

    for (let i = 0; i < count; i++) {
      const cat = CATEGORIES[i % CATEGORIES.length];
      const round = Math.floor(i / CATEGORIES.length);
      const prompt = round === 0 ? cat.prompt : `${cat.prompt} (Set ${round + 1})`;

      const { rows: qr } = await c.query(
        `INSERT INTO question (poll_id, prompt, position, min_select, max_select)
         VALUES ($1, $2, $3, $4, $5) RETURNING id`,
        [pollId, prompt, i, MIN_SELECT, MAX_SELECT],
      );
      const questionId = qr[0].id;

      const params = [];
      const values = [];
      cat.options.forEach((label, idx) => {
        const b = idx * 3;
        params.push(`($${b + 1}, $${b + 2}, $${b + 3})`);
        values.push(questionId, label, idx);
      });
      await c.query(
        `INSERT INTO option (question_id, label, position) VALUES ${params.join(', ')}`,
        values,
      );
    }
    return pollId;
  });
}
