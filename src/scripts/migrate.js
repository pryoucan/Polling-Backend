// Applies schema.sql. Idempotent (all CREATE ... IF NOT EXISTS).
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { pool } from '../db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

async function main() {
  const sql = fs.readFileSync(path.join(__dirname, '..', 'schema.sql'), 'utf8');
  await pool.query(sql);
  console.log('✓ schema applied');
  await pool.end();
}

main().catch((err) => {
  console.error('migrate failed:', err);
  process.exit(1);
});
