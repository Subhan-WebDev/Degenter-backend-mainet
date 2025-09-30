// lib/db.js
import 'dotenv/config';
import { Pool } from 'pg';
import { info, warn, err } from './log.js';

const DEFAULTS = {
  max: 20,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 10_000
};

export const DB = new Pool({
  connectionString: process.env.DATABASE_URL,
  ...DEFAULTS
});

// optional: enforce sane statement timeouts per session
DB.on('connect', (client) => {
  client.query(`
    SET application_name = 'zigdex-indexer';
    SET statement_timeout = '120s';
    SET idle_in_transaction_session_timeout = '60s';
  `).catch(() => {});
});

export async function init() {
  try {
    const r = await DB.query('SELECT NOW() as now');
    info('db connected @', r.rows[0].now);
  } catch (e) {
    err('db connect failed:', e.message);
    throw e;
  }
}

export async function tx(fn) {
  const client = await DB.connect();
  try {
    await client.query('BEGIN');
    const res = await fn(client);
    await client.query('COMMIT');
    return res;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

/** simple helper that retries transient failures a few times */
export async function queryRetry(sql, args = [], attempts = 3) {
  for (let i = 0; i < attempts; i++) {
    try {
      return await DB.query(sql, args);
    } catch (e) {
      const last = i === attempts - 1;
      if (last) throw e;
      warn('db retry', i + 1, e.message);
      await new Promise(r => setTimeout(r, 150 * (i + 1)));
    }
  }
}

/** graceful shutdown */
export async function close() {
  try {
    await DB.end();
    info('db pool closed');
  } catch (e) {
    warn('db close error:', e.message);
  }
}

export default { DB, init, tx, queryRetry, close };
