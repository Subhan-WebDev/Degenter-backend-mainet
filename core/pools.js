// core/pools.js
import { DB } from '../lib/db.js';
import { upsertTokenMinimal } from './tokens.js';
import { info } from '../lib/log.js';

export async function upsertPool({ pairContract, baseDenom, quoteDenom, pairType, createdAt, height, txHash, signer }) {
  const baseId  = await upsertTokenMinimal(baseDenom);
  const quoteId = await upsertTokenMinimal(quoteDenom);
  const isUzig  = (quoteDenom === 'uzig');
  const { rows } = await DB.query(
    `INSERT INTO pools(pair_contract, base_token_id, quote_token_id, pair_type, is_uzig_quote, created_at, created_height, created_tx_hash, signer)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (pair_contract) DO UPDATE SET
       base_token_id = EXCLUDED.base_token_id,
       quote_token_id = EXCLUDED.quote_token_id,
       pair_type = EXCLUDED.pair_type
     RETURNING pool_id`,
     [pairContract, baseId, quoteId, String(pairType), isUzig, createdAt, height, txHash, signer]
  );
  info('POOL UPSERT:', pairContract, `${baseDenom}/${quoteDenom}`, pairType, 'pool_id=', rows[0].pool_id);
  return rows[0].pool_id;
}

export async function poolWithTokens(pairContract) {
  const { rows } = await DB.query(`
    SELECT p.pool_id, p.is_uzig_quote,
           b.token_id AS base_id, b.denom AS base_denom, COALESCE(b.exponent,6) AS base_exp,
           q.token_id AS quote_id, q.denom AS quote_denom, COALESCE(q.exponent,6) AS quote_exp
    FROM pools p
    JOIN tokens b ON b.token_id=p.base_token_id
    JOIN tokens q ON q.token_id=p.quote_token_id
    WHERE p.pair_contract=$1`, [pairContract]);
  return rows[0] || null;
}

