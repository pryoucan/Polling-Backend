import pg from 'pg';
import { config } from './config.js';

// One pool PER PROCESS (so per worker). pgPoolMax * workers must stay under
// your RDS max_connections.
export const pool = new pg.Pool({
  connectionString: config.databaseUrl,
  max: config.pgPoolMax,
  ssl: config.pgSsl ? { rejectUnauthorized: false } : false,
  idleTimeoutMillis: 30_000,
});

export function query(text, params) {
  return pool.query(text, params);
}

// Run fn inside a transaction with a dedicated client.
export async function withTx(fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}
