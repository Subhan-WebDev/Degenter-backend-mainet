// api/util/ohlcv-agg.js
import { DB } from '../../lib/db.js';

// human-facing keys we accept
export const TF_MAP = {
  '1m': 60,
  '5m': 300,
  '15m': 900,
  '30m': 1800,
  '1h': 3600,
  '2h': 7200,
  '4h': 14400,
  '8h': 28800,
  '12h': 43200,
  '1d': 86400,
  '3d': 259200,
  '5d': 432000,
  '1w': 604800,
  '1mth': 2592000,  // ~30d
  '3mth': 7776000   // ~90d
};

export function ensureTf(tf) {
  const k = String(tf || '1m').toLowerCase();
  return TF_MAP[k] ? k : '1m';
}

/**
 * Build OHLCV with a continuous series and optional fill:
 *  - Buckets by floor(epoch/step)*step (no date_trunc)
 *  - open/close from first/last minute inside each bucket
 *  - fill = prev | zero | none
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
  const tfKey = ensureTf(tf);
  const stepSec = TF_MAP[tfKey];

  const params = [from, to, stepSec];
  let whereSql;

  if (useAll) {
    params.push(tokenId);
    whereSql = `
      FROM ohlcv_1m o
      JOIN pools p ON p.pool_id=o.pool_id
      WHERE p.base_token_id=$4 AND p.is_uzig_quote=TRUE
        AND o.bucket_start >= $1::timestamptz
        AND o.bucket_start <  $2::timestamptz
    `;
  } else {
    params.push(poolId);
    whereSql = `
      FROM ohlcv_1m o
      WHERE o.pool_id=$4
        AND o.bucket_start >= $1::timestamptz
        AND o.bucket_start <  $2::timestamptz
    `;
  }

  // Bucket with floor(epoch/step)*step to avoid date_trunc unit errors
  const sql = `
    WITH raw AS (
      SELECT o.bucket_start, o.open, o.high, o.low, o.close, o.volume_zig, o.trade_count
      ${whereSql}
    ),
    tagged AS (
      SELECT
        bucket_start,
        open, high, low, close, volume_zig, trade_count,
        to_timestamp(floor(extract(epoch from bucket_start)/$3)*$3) AT TIME ZONE 'UTC' AS bucket_ts
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
      SELECT generate_series($1::timestamptz, $2::timestamptz - interval '1 second', make_interval(secs => $3)) AS ts
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
