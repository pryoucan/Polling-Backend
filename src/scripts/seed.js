// CLI: seed one poll with N questions (default 100). Wipes prior poll data.
//   node src/scripts/seed.js [questionCount]
import { pool } from '../db.js';
import { seedPoll } from './seed-core.js';

const count = parseInt(process.argv[2] ?? '100', 10);

seedPoll(count)
  .then(() => {
    console.log(`✓ seeded poll with ${count} questions`);
    return pool.end();
  })
  .catch((err) => {
    console.error('seed failed:', err);
    process.exit(1);
  });
