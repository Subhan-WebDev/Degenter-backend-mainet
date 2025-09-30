
// jobs/leaderboards.js
import { DB } from '../lib/db.js';
import { warn, debug } from '../lib/log.js';

const LEADERBOARD_SEC = parseInt(process.env.LEADERBOARD_SEC || '60', 10);
const LARGE_TRADE_MIN_ZIG = Number(process.env.LARGE_TRADE_MIN_ZIG || '1000');

export function startLeaderboards() {
  const BUCKETS = [['30m',30], ['1h',60], ['4h',240], ['24h',1440]];
  (async function loop () {
    while (true) {
      try {
        for (const [label, mins] of BUCKETS) {
          await DB.query(`
            WITH base AS (
              SELECT
                t.signer,
                t.pool_id,
                p.is_uzig_quote,
                qtk.exponent AS qexp,
                t.direction,
                t.offer_amount_base::NUMERIC AS offer_base,
                t.return_amount_base::NUMERIC AS return_base,
                t.created_at
              FROM trades t
              JOIN pools p ON p.pool_id=t.pool_id
              JOIN tokens qtk ON qtk.token_id=p.quote_token_id
              WHERE t.action='swap' AND t.created_at >= now() - INTERVAL '${mins} minutes'
                AND t.signer IS NOT NULL
            ),
            priced AS (
              SELECT
                b.signer,
                b.direction,
                CASE
                  WHEN b.is_uzig_quote THEN b.offer_base / power(10, COALESCE(b.qexp,6))
                  ELSE (b.offer_base / power(10, COALESCE(b.qexp,6))) * COALESCE(pr.price_in_zig,0)
                END AS offer_zig,
                CASE
                  WHEN b.is_uzig_quote THEN b.return_base / power(10, COALESCE(b.qexp,6))
                  ELSE (b.return_base / power(10, COALESCE(b.qexp,6))) * COALESCE(pr.price_in_zig,0)
                END AS return_zig
              FROM base b
              LEFT JOIN LATERAL (
                SELECT price_in_zig FROM prices
                WHERE pool_id=b.pool_id
                ORDER BY updated_at DESC LIMIT 1
              ) pr ON TRUE
            ),
            agg AS (
              SELECT
                signer,
                COUNT(*) AS trades_count,
                SUM(offer_zig + return_zig) AS volume_zig,
                SUM((return_zig - offer_zig)) AS gross_pnl_zig
              FROM priced
              GROUP BY signer
            )
            INSERT INTO leaderboard_traders(bucket, address, trades_count, volume_zig, gross_pnl_zig, updated_at)
            SELECT '${label}', signer, trades_count, volume_zig, gross_pnl_zig, now()
            FROM agg
            ON CONFLICT (bucket, address)
            DO UPDATE SET
              trades_count  = EXCLUDED.trades_count,
              volume_zig    = EXCLUDED.volume_zig,
              gross_pnl_zig = EXCLUDED.gross_pnl_zig,
              updated_at    = now();
          `);

          await DB.query(`
            WITH recent AS (
              SELECT
                t.pool_id, t.tx_hash, t.signer, t.direction, t.created_at,
                p.is_uzig_quote, qtk.exponent AS qexp,
                CASE WHEN t.direction='buy'  THEN t.offer_amount_base::NUMERIC
                     WHEN t.direction='sell' THEN t.return_amount_base::NUMERIC
                     ELSE 0 END AS quote_leg_base
              FROM trades t
              JOIN pools p ON p.pool_id=t.pool_id
              JOIN tokens qtk ON qtk.token_id=p.quote_token_id
              WHERE t.action='swap' AND t.created_at >= now() - INTERVAL '${mins} minutes'
            ),
            valued AS (
              SELECT
                r.pool_id, r.tx_hash, r.signer, r.direction, r.created_at,
                CASE
                  WHEN r.is_uzig_quote THEN (r.quote_leg_base / power(10, COALESCE(r.qexp,6)))
                  ELSE (r.quote_leg_base / power(10, COALESCE(r.qexp,6))) * COALESCE(pr.price_in_zig,0)
                END AS value_zig
              FROM recent r
              LEFT JOIN LATERAL (
                SELECT price_in_zig FROM prices
                WHERE pool_id=r.pool_id
                ORDER BY updated_at DESC LIMIT 1
              ) pr ON TRUE
            )
            INSERT INTO large_trades(bucket, pool_id, tx_hash, signer, direction, value_zig, created_at)
            SELECT '${label}', pool_id, tx_hash, signer, direction, value_zig, created_at
            FROM valued
            WHERE value_zig >= ${LARGE_TRADE_MIN_ZIG}
            ON CONFLICT DO NOTHING;
          `);
        }
        debug('[leaderboard] updated');
      } catch (e) { warn('[leaderboard]', e.message); }
      await new Promise(r => setTimeout(r, LEADERBOARD_SEC * 1000));
    }
  })().catch(()=>{});
}
