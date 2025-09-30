// core/ohlcv.js
import { DB } from '../lib/db.js';
import BatchQueue from '../lib/batch.js';

/**
 * We preserve the original semantics:
 *   - open = previous minute's close
 *   - first upsert per (pool_id, bucket) sets open (from prev close or price),
 *     subsequent upserts only affect high/low/close/volume/trade_count via ON CONFLICT.
 *
 * To keep DB round-trips low, we batch:
 *   1) aggregate by (pool_id, bucket_start): high=max(price), low=min(price),
 *      close=last price seen in the batch order, volume=sum, trades=sum
 *   2) fetch prev closes for all keys in one query
 *   3) INSERT ... ON CONFLICT with per-row open determined from prev close
 */

function keyOf(pool_id, bucket_start) {
  // bucket_start is a Date; we store as ISO to avoid floating equality issues in a Map key
  return `${pool_id}__${new Date(bucket_start).toISOString()}`;
}

function aggregateBatch(items) {
  // Collapse duplicates for same (pool_id,bucket) so we do a single row per key in the INSERT
  const map = new Map();
  for (const it of items) {
    const k = keyOf(it.pool_id, it.bucket_start);
    const prev = map.get(k);
    if (!prev) {
      map.set(k, {
        pool_id: it.pool_id,
        bucket_start: it.bucket_start,
        // initialize with this trade's price
        high: it.price,
        low:  it.price,
        close: it.price,              // last price in this batch (order preserved below)
        volume_zig: it.vol_zig || 0,
        trade_count: it.trade_inc || 0,
        liquidity_zig: it.liquidity_zig ?? null,
      });
    } else {
      // update high/low/close, accumulate volume/trades
      if (it.price > prev.high) prev.high = it.price;
      if (it.price < prev.low)  prev.low  = it.price;
      prev.close = it.price; // last seen in arrival order
      prev.volume_zig += (it.vol_zig || 0);
      prev.trade_count += (it.trade_inc || 0);
      // keep latest non-null liquidity if provided
      if (it.liquidity_zig != null) prev.liquidity_zig = it.liquidity_zig;
    }
  }
  return Array.from(map.values());
}

async function fetchPrevCloses(rows) {
  if (!rows.length) return new Map();

  // Build VALUES table for keys and join to ohlcv_1m at (bucket_start - 1 minute)
  // We’ll parameterize everything to avoid SQL injection and keep plans reusable.
  const params = [];
  const valuesSQL = rows.map((r, idx) => {
    const i = idx * 2;
    params.push(r.pool_id, r.bucket_start);
    return `($${i + 1}::BIGINT, $${i + 2}::timestamptz)`;
  }).join(',');

  const sql = `
    WITH keys(pool_id, bucket_start) AS (
      VALUES ${valuesSQL}
    )
    SELECT k.pool_id, k.bucket_start, o.close
    FROM keys k
    LEFT JOIN ohlcv_1m o
      ON o.pool_id = k.pool_id
     AND o.bucket_start = (k.bucket_start - INTERVAL '1 minute')
  `;

  const { rows: prevs } = await DB.query(sql, params);
  const out = new Map();
  for (const r of prevs) {
    const k = keyOf(r.pool_id, r.bucket_start);
    out.set(k, r.close == null ? null : Number(r.close));
  }
  return out;
}

function buildInsertSQL(rowsWithOpens) {
  // Single INSERT ... ON CONFLICT for all rows
  const cols = [
    'pool_id', 'bucket_start', 'open', 'high', 'low', 'close',
    'volume_zig', 'trade_count', 'liquidity_zig'
  ];
  const placeholders = [];
  const args = [];
  let p = 1;
  for (const r of rowsWithOpens) {
    placeholders.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    args.push(
      r.pool_id,
      r.bucket_start,
      r.open, r.high, r.low, r.close,
      r.volume_zig || 0,
      r.trade_count || 0,
      r.liquidity_zig ?? null
    );
  }

  const sql = `
    INSERT INTO ohlcv_1m
      (${cols.join(',')})
    VALUES
      ${placeholders.join(',')}
    ON CONFLICT (pool_id, bucket_start) DO UPDATE
      SET high          = GREATEST(ohlcv_1m.high, EXCLUDED.high),
          low           = LEAST(ohlcv_1m.low,  EXCLUDED.low),
          close         = EXCLUDED.close,
          volume_zig    = ohlcv_1m.volume_zig + EXCLUDED.volume_zig,
          trade_count   = ohlcv_1m.trade_count + EXCLUDED.trade_count,
          liquidity_zig = COALESCE(EXCLUDED.liquidity_zig, ohlcv_1m.liquidity_zig)
  `;
  return { sql, args };
}

const ohlcvQueue = new BatchQueue({
  maxItems: Number(process.env.OHLCV_BATCH_MAX || 600),
  maxWaitMs: Number(process.env.OHLCV_BATCH_WAIT_MS || 120),
  flushFn: async (items) => {
    // 1) aggregate within the batch to one row per (pool_id, bucket_start)
    const agg = aggregateBatch(items);

    // 2) fetch previous closes in ONE query
    const prevMap = await fetchPrevCloses(agg);

    // 3) build rows with correct OPEN (= prev close if present, else this minute's first price)
    const rowsWithOpens = agg.map(r => {
      const k = keyOf(r.pool_id, r.bucket_start);
      const prevClose = prevMap.get(k);
      const openVal = (prevClose ?? r.close); // if no prev candle, fall back to close (same as your original)
      return { ...r, open: openVal };
    });

    // 4) single INSERT ... ON CONFLICT
    const { sql, args } = buildInsertSQL(rowsWithOpens);
    await DB.query(sql, args);
  }
});

/**
 * Public API — same signature you had before
 * price: candle price to merge (display units; already computed upstream)
 */
export async function upsertOHLCV1m({ pool_id, bucket_start, price, vol_zig, trade_inc, liquidity_zig = null }) {
  ohlcvQueue.push({
    pool_id,
    bucket_start,
    price,
    vol_zig: vol_zig || 0,
    trade_inc: trade_inc || 0,
    liquidity_zig
  });
}

export async function drainOHLCV() {
  await ohlcvQueue.drain();
}
