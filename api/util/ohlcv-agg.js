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
  return TF_MAP[k] ? k : '1m';
}

/**
 * Robust aggregator that:
 *  - Builds a continuous time series (from..to) at the chosen TF
 *  - Aggregates OHLC with proper first/last on minute buckets (no GROUP BY errors)
 *  - Supports fill = prev | zero | none
 */
export async function getCandles({
  mode = 'price',
  unit = 'native',
  tf = '1m',
  from,
  to,
  useAll = false,
  tokenId = null,
  poolId = null,
  zigUsd = 1,
  circ = null,
  fill = 'prev',
}) {
  const tfi = TF_MAP[ensureTf(tf)];
  const params = [from, to];
  let whereSql;
  if (useAll) {
    // aggregate all UZIG-quote pools for this token
    params.push(tokenId);
    whereSql = `
      FROM ohlcv_1m o
      JOIN pools p ON p.pool_id=o.pool_id
      WHERE p.base_token_id=$3 AND p.is_uzig_quote=TRUE
        AND o.bucket_start >= $1::timestamptz
        AND o.bucket_start <  $2::timestamptz
    `;
  } else {
    params.push(poolId);
    whereSql = `
      FROM ohlcv_1m o
      WHERE o.pool_id=$3
        AND o.bucket_start >= $1::timestamptz
        AND o.bucket_start <  $2::timestamptz
    `;
  }

  // Use a generate_series to create a continuous bucket timeline, then left join per-bucket aggregates.
  // We compute open/close by selecting the minute rows at min/max bucket_start within each bucket.
  const sql = `
    WITH raw AS (
      SELECT o.bucket_start, o.open, o.high, o.low, o.close, o.volume_zig, o.trade_count
      ${whereSql}
    ),
    tagged AS (
      SELECT
        bucket_start,
        open, high, low, close, volume_zig, trade_count,
        date_trunc('${tfi}', bucket_start) AS bucket_ts
      FROM raw
    ),
    agg AS (
      SELECT
        bucket_ts,
        MIN(low) AS low,
        MAX(high) AS high,
        SUM(volume_zig) AS volume_native,
        SUM(trade_count) AS trades,
        MIN(bucket_start) AS ts_open,
        MAX(bucket_start) AS ts_close
      FROM tagged
      GROUP BY bucket_ts
    ),
    o AS (
      SELECT
        a.bucket_ts AS ts,
        (SELECT t.open  FROM tagged t WHERE t.bucket_start=a.ts_open  LIMIT 1) AS open,
        (SELECT t.close FROM tagged t WHERE t.bucket_start=a.ts_close LIMIT 1) AS close,
        a.low, a.high, a.volume_native, a.trades
      FROM agg a
    ),
    series AS (
      SELECT generate_series($1::timestamptz, $2::timestamptz - interval '1 second', '${tfi}'::interval) AS ts
    )
    SELECT s.ts,
           o.open, o.high, o.low, o.close, o.volume_native, o.trades
    FROM series s
    LEFT JOIN o ON o.ts = s.ts
    ORDER BY s.ts;
  `;

  const { rows } = await DB.query(sql, params);

  const out = [];
  let lastClose = null;

  for (const r of rows) {
    let open = r.open, high = r.high, low = r.low, close = r.close;
    let vol = r.volume_native, trades = r.trades;

    const empty = (open == null || high == null || low == null || close == null);
    if (empty) {
      if (fill === 'prev' && lastClose != null) {
        open = high = low = close = lastClose;
        vol = 0; trades = 0;
      } else if (fill === 'zero') {
        open = high = low = close = 0;
        vol = 0; trades = 0;
      } else { // none
        continue;
      }
    }

    if (mode === 'mcap' && circ != null) {
      open *= circ; high *= circ; low *= circ; close *= circ;
    }
    if (unit === 'usd') {
      open *= zigUsd; high *= zigUsd; low *= zigUsd; close *= zigUsd;
      vol = (vol ?? 0) * zigUsd;
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
