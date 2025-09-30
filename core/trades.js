// core/trades.js
import { DB } from '../lib/db.js';
import BatchQueue from '../lib/batch.js';

const INSERT_SQL = `
  INSERT INTO trades
   (pool_id, pair_contract, action, direction,
    offer_asset_denom, offer_amount_base,
    ask_asset_denom, ask_amount_base,
    return_amount_base, is_router,
    reserve_asset1_denom, reserve_asset1_amount_base,
    reserve_asset2_denom, reserve_asset2_amount_base,
    height, tx_hash, signer, msg_index, created_at)
  VALUES %VALUES%
  ON CONFLICT (created_at, tx_hash, pool_id, msg_index) DO NOTHING
`;

function sqlValues(rows) {
  const vals = [];
  const args = [];
  let i = 1;
  for (const t of rows) {
    vals.push(`($${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++})`);
    args.push(
      t.pool_id, t.pair_contract, t.action, t.direction,
      t.offer_asset_denom, t.offer_amount_base,
      t.ask_asset_denom, t.ask_amount_base,
      t.return_amount_base, t.is_router,
      t.reserve_asset1_denom, t.reserve_asset1_amount_base,
      t.reserve_asset2_denom, t.reserve_asset2_amount_base,
      t.height, t.tx_hash, t.signer, t.msg_index, t.created_at
    );
  }
  return { text: INSERT_SQL.replace('%VALUES%', vals.join(',')), args };
}

const tradesQueue = new BatchQueue({
  maxItems: Number(process.env.TRADES_BATCH_MAX || 800),
  maxWaitMs: Number(process.env.TRADES_BATCH_WAIT_MS || 120),
  flushFn: async (items) => {
    const { text, args } = sqlValues(items);
    await DB.query(text, args);
  }
});

export async function insertTrade(t) {
  tradesQueue.push(t);
}

export async function drainTrades() {
  await tradesQueue.drain();
}
