// jobs/matrix-rollups.js
import { DB } from '../lib/db.js';
import log from '../lib/log.js';

const LOOP_SEC = parseInt(process.env.MATRIX_ROLLUP_SEC || '60', 10);
const BUCKETS = [['30m',30], ['1h',60], ['4h',240], ['24h',1440]];
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/**
 * UNIT RULES
 * - prices.price_in_zig: ZIG per 1 DISPLAY unit of that token (post-fix).
 * - ohlcv_1m.close: price in DISPLAY units.
 * - *_base columns in trades/pool_state are RAW; convert RAW→DISPLAY by /10^exp.
 * - UZIG RAW exponent is always 6.
 */

async function rollPoolVolumes(label, mins, onlyPoolId = null) {
  await DB.query(`
    WITH q AS (
      SELECT
        t.pool_id,
        SUM(CASE WHEN t.direction='buy'  THEN t.offer_amount_base::NUMERIC ELSE 0 END)  AS buy_quote_base,
        SUM(CASE WHEN t.direction='sell' THEN t.return_amount_base::NUMERIC ELSE 0 END) AS sell_quote_base,
        SUM(CASE WHEN t.direction='buy'  THEN 1 ELSE 0 END) AS tx_buy,
        SUM(CASE WHEN t.direction='sell' THEN 1 ELSE 0 END) AS tx_sell,
        COUNT(DISTINCT t.signer) AS uniq
      FROM trades t
      WHERE t.action='swap'
        AND t.created_at >= now() - ($1 || ' minutes')::interval
        ${onlyPoolId ? 'AND t.pool_id = $4' : ''}
      GROUP BY t.pool_id
    ),
    enriched AS (
      SELECT
        q.pool_id, q.tx_buy, q.tx_sell, q.uniq,
        p.is_uzig_quote,
        qtk.exponent AS qexp,

        /* Quote volume in DISPLAY units */
        CASE WHEN p.is_uzig_quote
             THEN q.buy_quote_base  / 1e6
             ELSE q.buy_quote_base  / power(10::numeric, COALESCE(qtk.exponent::int,6))
        END AS vol_buy_quote,
        CASE WHEN p.is_uzig_quote
             THEN q.sell_quote_base / 1e6
             ELSE q.sell_quote_base / power(10::numeric, COALESCE(qtk.exponent::int,6))
        END AS vol_sell_quote,

        /* Value in ZIG (DISPLAY quote * DISPLAY price) */
        CASE WHEN p.is_uzig_quote
             THEN (q.buy_quote_base  / 1e6)
             ELSE ((q.buy_quote_base  / power(10::numeric, COALESCE(qtk.exponent::int,6))) * COALESCE(pr.price_in_zig,0))
        END AS vol_buy_zig,
        CASE WHEN p.is_uzig_quote
             THEN (q.sell_quote_base / 1e6)
             ELSE ((q.sell_quote_base / power(10::numeric, COALESCE(qtk.exponent::int,6))) * COALESCE(pr.price_in_zig,0))
        END AS vol_sell_zig

      FROM q
      JOIN pools  p   ON p.pool_id=q.pool_id
      JOIN tokens qtk ON qtk.token_id=p.quote_token_id
      LEFT JOIN LATERAL (
        SELECT CASE WHEN p.is_uzig_quote THEN 1::numeric ELSE price_in_zig END AS price_in_zig
        FROM prices
        WHERE token_id = p.quote_token_id
        ORDER BY updated_at DESC
        LIMIT 1
      ) pr ON TRUE
    )
    INSERT INTO pool_matrix(
      pool_id, bucket,
      vol_buy_quote, vol_sell_quote,
      vol_buy_zig,  vol_sell_zig,
      tx_buy, tx_sell, unique_traders, updated_at
    )
    SELECT
      pool_id, $2,
      vol_buy_quote, vol_sell_quote,
      vol_buy_zig,  vol_sell_zig,
      tx_buy, tx_sell, uniq, now()
    FROM enriched
    ${onlyPoolId ? 'WHERE pool_id=$4' : ''}
    ON CONFLICT (pool_id, bucket)
    DO UPDATE SET
      vol_buy_quote = EXCLUDED.vol_buy_quote,
      vol_sell_quote = EXCLUDED.vol_sell_quote,
      vol_buy_zig = EXCLUDED.vol_buy_zig,
      vol_sell_zig = EXCLUDED.vol_sell_zig,
      tx_buy = EXCLUDED.tx_buy,
      tx_sell = EXCLUDED.tx_sell,
      unique_traders = EXCLUDED.unique_traders,
      updated_at = now();
  `, onlyPoolId ? [mins, label, null, onlyPoolId] : [mins, label]);
}

async function rollPoolTVL(label, onlyPoolId = null) {
  await DB.query(`
    /* DISPLAY prices for base/quote */
    WITH latest_price_disp AS (
      SELECT
        p.pool_id,
        COALESCE(
          (SELECT pr.price_in_zig FROM prices pr
           WHERE pr.token_id=p.base_token_id AND pr.pool_id=p.pool_id
           ORDER BY pr.updated_at DESC LIMIT 1),
          (SELECT pr2.price_in_zig FROM prices pr2
           JOIN pools px ON px.pool_id=pr2.pool_id
           WHERE pr2.token_id=p.base_token_id AND px.is_uzig_quote=TRUE
           ORDER BY pr2.updated_at DESC LIMIT 1),
          (SELECT o.close FROM ohlcv_1m o WHERE o.pool_id=p.pool_id ORDER BY o.bucket_start DESC LIMIT 1)
        ) AS base_px_disp_zig,
        CASE
          WHEN p.is_uzig_quote THEN 1::numeric
          ELSE COALESCE(
            (SELECT pr3.price_in_zig FROM prices pr3
             JOIN pools py ON py.pool_id=pr3.pool_id
             WHERE pr3.token_id=p.quote_token_id AND py.is_uzig_quote=TRUE
             ORDER BY pr3.updated_at DESC LIMIT 1),
            NULL
          )
        END AS quote_px_disp_zig
      FROM pools p
      ${onlyPoolId ? 'WHERE p.pool_id = $2' : ''}
    ),
    reserves AS (
      SELECT
        s.pool_id,
        (s.reserve_base_base  / power(10::numeric, COALESCE(b.exponent::int,6))) AS reserve_base_disp,
        CASE
          WHEN p.is_uzig_quote THEN (s.reserve_quote_base / 1e6)
          ELSE (s.reserve_quote_base / power(10::numeric, COALESCE(q.exponent::int,6)))
        END AS reserve_quote_disp
      FROM pool_state s
      JOIN pools p   ON p.pool_id=s.pool_id
      JOIN tokens b  ON b.token_id=p.base_token_id
      JOIN tokens q  ON q.token_id=p.quote_token_id
      ${onlyPoolId ? 'WHERE s.pool_id = $2' : ''}
    ),
    pool_tvl AS (
      SELECT
        p.pool_id,
        r.reserve_base_disp,
        r.reserve_quote_disp,
        (COALESCE(r.reserve_quote_disp,0) * COALESCE(lpd.quote_px_disp_zig,0)) +
        (COALESCE(r.reserve_base_disp,0)  * COALESCE(lpd.base_px_disp_zig,0))   AS tvl_zig
      FROM pools p
      LEFT JOIN reserves          r   ON r.pool_id=p.pool_id
      LEFT JOIN latest_price_disp lpd ON lpd.pool_id=p.pool_id
      ${onlyPoolId ? 'WHERE p.pool_id = $2' : ''}
    )
    UPDATE pool_matrix pm
    SET tvl_zig = pt.tvl_zig,
        reserve_base_disp = pt.reserve_base_disp,
        reserve_quote_disp= pt.reserve_quote_disp,
        updated_at = now()
    FROM pool_tvl pt
    WHERE pm.pool_id = pt.pool_id
      AND pm.bucket = $1
      ${onlyPoolId ? 'AND pm.pool_id = $2' : ''};
  `, onlyPoolId ? [label, onlyPoolId] : [label]);
}

async function rollTokenMatrix(label, onlyTokenId = null) {
  await DB.query(`
    /* 1) Candidate price from PRICES (DISPLAY) */
    WITH px_from_prices AS (
      SELECT
        t.token_id,
        (SELECT pr.price_in_zig
         FROM prices pr
         JOIN pools p2 ON p2.pool_id=pr.pool_id
         WHERE pr.token_id=t.token_id AND p2.is_uzig_quote=TRUE
         ORDER BY pr.updated_at DESC LIMIT 1) AS price_disp_zig_prices
      FROM tokens t
      ${onlyTokenId ? 'WHERE t.token_id = $2' : ''}
    ),

    /* 2) Candidate price from OHLCV (DISPLAY) */
    px_from_ohlcv AS (
      SELECT
        t.token_id,
        (SELECT AVG(o.close)
         FROM ohlcv_1m o
         JOIN pools p3 ON p3.pool_id=o.pool_id
         WHERE p3.base_token_id=t.token_id AND p3.is_uzig_quote=TRUE
           AND o.bucket_start >= now() - INTERVAL '60 minutes') AS price_disp_zig_ohlcv
      FROM tokens t
      ${onlyTokenId ? 'WHERE t.token_id = $2' : ''}
    ),

    token_price_disp AS (
      SELECT
        t.token_id,
        t.exponent AS texp,
        CASE
          WHEN pp.price_disp_zig_prices IS NOT NULL AND po.price_disp_zig_ohlcv IS NOT NULL
               AND po.price_disp_zig_ohlcv > 0
               AND (pp.price_disp_zig_prices / po.price_disp_zig_ohlcv) BETWEEN 1e5 AND 1e7
               AND COALESCE(t.exponent,6) = 6
            THEN (pp.price_disp_zig_prices / 1e6)
          WHEN pp.price_disp_zig_prices IS NOT NULL
            THEN pp.price_disp_zig_prices
          WHEN po.price_disp_zig_ohlcv IS NOT NULL
            THEN po.price_disp_zig_ohlcv
          ELSE 0
        END AS price_disp_zig
      FROM tokens t
      LEFT JOIN px_from_prices pp ON pp.token_id=t.token_id
      LEFT JOIN px_from_ohlcv po  ON po.token_id=t.token_id
      ${onlyTokenId ? 'WHERE t.token_id = $2' : ''}
    ),

    holders AS (
      SELECT h.token_id, COUNT(*)::BIGINT AS holders
      FROM holders h
      WHERE h.balance_base::NUMERIC > 0
      ${onlyTokenId ? 'AND h.token_id = $2' : ''}
      GROUP BY h.token_id
    ),

    scaled AS (
      SELECT
        t.token_id,
        tpd.price_disp_zig AS price_in_zig,
        (t.total_supply_base / power(10::numeric, COALESCE(t.exponent::int,6))) AS circ_disp,
        (t.max_supply_base   / power(10::numeric, COALESCE(t.exponent::int,6))) AS max_disp,
        COALESCE(h.holders, 0) AS holders
      FROM tokens t
      LEFT JOIN token_price_disp tpd ON tpd.token_id=t.token_id
      LEFT JOIN holders         h    ON h.token_id=t.token_id
      ${onlyTokenId ? 'WHERE t.token_id = $2' : ''}
    )

    INSERT INTO token_matrix(token_id, bucket, price_in_zig, mcap_zig, fdv_zig, holders, updated_at)
    SELECT
      token_id, $1,
      price_in_zig,
      (circ_disp * price_in_zig) AS mcap_zig,
      (max_disp  * price_in_zig) AS fdv_zig,
      holders,
      now()
    FROM scaled
    ON CONFLICT (token_id, bucket) DO UPDATE
    SET price_in_zig = EXCLUDED.price_in_zig,
        mcap_zig     = EXCLUDED.mcap_zig,
        fdv_zig      = EXCLUDED.fdv_zig,
        holders      = EXCLUDED.holders,
        updated_at   = now();
  `, onlyTokenId ? [label, onlyTokenId] : [label]);
}

async function once() {
  for (const [label, mins] of BUCKETS) {
    await rollPoolVolumes(label, mins);
    await rollPoolTVL(label);
    await rollTokenMatrix(label);
  }
  log.debug('[matrix] pools & tokens rollups done');
}

async function start() {
  log.info(`[matrix] starting loop (every ${LOOP_SEC}s)`);
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try { await once(); }
    catch (e) { log.warn('[matrix]', e.message || e); }
    await sleep(LOOP_SEC * 1000);
  }
}

/** ➕ One-shots for fast-track */
export async function refreshPoolMatrixOnce(poolId) {
  if (!poolId) return;
  for (const [label, mins] of BUCKETS) {
    await rollPoolVolumes(label, mins, poolId);
    await rollPoolTVL(label, poolId);
  }
  log.info('[matrix/once] pool', poolId, 'updated for all buckets');
}

export async function refreshTokenMatrixOnce(tokenId) {
  if (!tokenId) return;
  for (const [label] of BUCKETS) {
    await rollTokenMatrix(label, tokenId);
  }
  log.info('[matrix/once] token', tokenId, 'updated for all buckets');
}

export default { start, once, refreshPoolMatrixOnce, refreshTokenMatrixOnce };
