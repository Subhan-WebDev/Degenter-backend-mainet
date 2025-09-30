// jobs/holders-refresher.js
import { DB } from '../lib/db.js';
import { lcdDenomOwners } from '../lib/lcd.js';
import { info, warn } from '../lib/log.js';

const HOLDERS_REFRESH_SEC = parseInt(process.env.HOLDERS_REFRESH_SEC || '180', 10);
const MAX_HOLDER_PAGES_PER_CYCLE = parseInt(process.env.MAX_HOLDER_PAGES_PER_CYCLE || '30', 10);

function digitsOrNull(x) {
  const s = String(x ?? '');
  return /^\d+$/.test(s) ? s : null;
}

/**
 * ➕ One-shot for fast-track: fully sweep holders for a single token right now.
 * FIX: accumulate a full set of seen addresses across pages and only zero-out
 *      non-seen holders once at the very end (prevents page-to-page clobbering).
 */
export async function refreshHoldersOnce(token_id, denom, maxPages = MAX_HOLDER_PAGES_PER_CYCLE) {
  if (!token_id || !denom) return;

  const seen = new Set();
  let nextKey = null;

  for (let i = 0; i < maxPages; i++) {
    const page = await lcdDenomOwners(denom, nextKey).catch((e) => {
      warn('[holders/owners]', denom, e.message);
      return null;
    });

    const items = page?.denom_owners || [];
    if (items.length === 0 && !page?.pagination?.next_key) {
      // no holders at all; continue to final normalization
    }

    const client = await DB.connect();
    try {
      await client.query('BEGIN');

      for (const it of items) {
        const addr = it.address;
        const amt = it.balance?.amount || '0';
        seen.add(addr);

        await client.query(`
          INSERT INTO holders(token_id, address, balance_base, updated_at)
          VALUES ($1,$2,$3, now())
          ON CONFLICT (token_id, address) DO UPDATE SET
            balance_base = EXCLUDED.balance_base,
            updated_at = now()
        `, [token_id, addr, digitsOrNull(amt)]);

        await client.query(`
          INSERT INTO wallets(address, last_seen)
          VALUES ($1, now())
          ON CONFLICT (address) DO NOTHING
        `, [addr]);
      }

      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    nextKey = page?.pagination?.next_key || null;
    if (!nextKey) break; // finished all pages
  }

  // Final normalization & stats (single pass after the sweep)
  const all = Array.from(seen);
  const client = await DB.connect();
  try {
    await client.query('BEGIN');

    if (all.length > 0) {
      const params = [token_id, ...all];
      const placeholders = all.map((_, i) => `$${i + 2}`).join(',');
      await client.query(`
        UPDATE holders
        SET balance_base = '0', updated_at = now()
        WHERE token_id = $1 AND address NOT IN (${placeholders})
      `, params);
    } else {
      // No holders returned by LCD -> zero out any existing balances for this token
      await client.query(`
        UPDATE holders
        SET balance_base = '0', updated_at = now()
        WHERE token_id = $1
      `, [token_id]);
    }

    const { rows: hc } = await client.query(
      `SELECT COUNT(*)::BIGINT AS c
       FROM holders
       WHERE token_id = $1 AND balance_base::NUMERIC > 0`,
      [token_id]
    );

    await client.query(`
      INSERT INTO token_holders_stats(token_id, holders_count, updated_at)
      VALUES ($1, $2, now())
      ON CONFLICT (token_id) DO UPDATE
        SET holders_count = EXCLUDED.holders_count,
            updated_at = now()
    `, [token_id, hc[0].c]);

    await client.query('COMMIT');
    info('[holders/once] updated', denom, 'count=', hc[0].c);
  } catch (e) {
    await client.query('ROLLBACK');
    warn('[holders/once]', denom, e.message);
  } finally {
    client.release();
  }
}

export function startHoldersRefresher() {
  (async function loop() {
    while (true) {
      try {
        const { rows } = await DB.query(`
          SELECT token_id, denom FROM tokens
          WHERE denom <> 'uzig'
          ORDER BY random() LIMIT 1
        `);
        if (rows[0]) {
          await refreshHoldersOnce(rows[0].token_id, rows[0].denom);
        }
      } catch (e) {
        warn('[holders]', e.message);
      }
      await new Promise(r => setTimeout(r, HOLDERS_REFRESH_SEC * 1000));
    }
  })().catch(() => {});
}
