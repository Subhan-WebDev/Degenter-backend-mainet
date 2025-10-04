// api/util/ohlcv-agg.js
import { DB } from '../../lib/db.js';

export const TF_MAP = {
  '1m': '1 minute',
  '5m': '5 minutes',
  '15m': '15 minutes',
  '30m': '30 minutes',
  '1h': '1 hour',
  '2h': '2 hours',
  '4h': '4 hours',
  '8h': '8 hours',
  '12h': '12 hours',
  '1d': '1 day',
  '3d': '3 days',
  '5d': '5 days',
  '1w': '7 days',
  '1mth': '1 month',
  '3mth': '3 months',
};

export function ensureTf(tf) {
  const k = (tf || '1m').toLowerCase();
  if (!TF_MAP[k]) return '1m';
  return k;
}

/**
 * getCandles({ mode, unit, tf, from, to, priceSource, poolId, tokenId, useAll })
 *  - mode: 'price' | 'mcap'
 *  - unit: 'native' | 'usd'
 *  - useAll: true to aggregate across all UZIG-quote pools for the token
 *  - fill: 'prev' | 'zero' | 'none'
 */
export async function getCandles(args) {
  const {
    mode = 'price',
    unit = 'native',
    tf = '1m',
    from,
    to,
    priceSource = 'best',
    poolId = null,
    tokenId = null,
    useAll = false,
    zigUsd = 1,
    circ = null,        // for mcap
    fill = 'prev',
  } = args;

  const tfsql = TF_MAP[ensureTf(tf)];
  const params = [];
  let whereSql = '';
  let idx = 1;

  if (useAll) {
    // all UZIG quote pools of token
    params.push(tokenId);
    params.push(from);
    params.push(to);
    whereSql = `
      FROM ohlcv_1m o
      JOIN pools p ON p.pool_id=o.pool_id
      WHERE p.base_token_id=$${idx++} AND p.is_uzig_quote=TRUE
        AND o.bucket_start >= $${idx++}::timestamptz
        AND o.bucket_start <  $${idx++}::timestamptz
    `;
  } else {
    // single pool
    params.push(poolId);
    params.push(from);
    params.push(to);
    whereSql = `
      FROM ohlcv_1m o
      WHERE o.pool_id=$${idx++}
        AND o.bucket_start >= $${idx++}::timestamptz
        AND o.bucket_start <  $${idx++}::timestamptz
    `;
  }

  // Aggregate to requested TF; keep OHLC semantics via first/last on minute buckets
  const sql = `
    WITH raw AS (
      SELECT o.bucket_start, o.open, o.high, o.low, o.close, o.volume_zig, o.trade_count
      ${whereSql}
    ),
    bucketed AS (
      SELECT
        date_trunc('${tfsql}', bucket_start) AS ts,
        FIRST_VALUE(open)  OVER (PARTITION BY date_trunc('${tfsql}', bucket_start) ORDER BY bucket_start ASC)  AS open,
        MAX(high) AS high,
        MIN(low)  AS low,
        FIRST_VALUE(close) OVER (PARTITION BY date_trunc('${tfsql}', bucket_start) ORDER BY bucket_start DESC) AS close,
        SUM(volume_zig) AS vol,
        SUM(trade_count) AS trades
      FROM raw
      GROUP BY ts
    ),
    range AS (
      SELECT generate_series($2::timestamptz, $3::timestamptz - INTERVAL '1 second', '${tfsql}'::interval) AS ts
    ),
    series AS (
      SELECT r.ts, b.open, b.high, b.low, b.close, b.vol, b.trades
      FROM range r
      LEFT JOIN bucketed b ON b.ts = r.ts
      ORDER BY r.ts
    )
    SELECT * FROM series;
  `;

  const { rows } = await DB.query(sql, params);

  // fill behaviour + unit/mode transform
  const out = [];
  let lastClose = null;

  for (const r of rows) {
    let open = r.open, high = r.high, low = r.low, close = r.close;
    let vol = r.vol, trades = r.trades;

    if (open == null || high == null || low == null || close == null) {
      if (fill === 'prev' && lastClose != null) {
        open = high = low = close = lastClose;
        vol = 0; trades = 0;
      } else if (fill === 'zero') {
        open = high = low = close = 0;
        vol = 0; trades = 0;
      } else {
        // none: skip empty bars
        continue;
      }
    }

    // transform to mcap if requested
    if (mode === 'mcap' && circ != null) {
      open = open * circ; high = high * circ; low = low * circ; close = close * circ;
    }

    if (unit === 'usd') {
      open *= zigUsd; high *= zigUsd; low *= zigUsd; close *= zigUsd; vol = (vol ?? 0) * zigUsd;
    }

    lastClose = close;
    out.push({
      ts: r.ts,
      open: Number(open ?? 0),
      high: Number(high ?? 0),
      low: Number(low ?? 0),
      close: Number(close ?? 0),
      volume: Number(vol ?? 0),
      trades: Number(trades ?? 0),
    });
  }

  return out;
}
