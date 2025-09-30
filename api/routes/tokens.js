// api/routes/tokens.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { resolveTokenId, getZigUsd } from '../util/resolve-token.js';
import { listTokenPoolsWithMetrics } from '../util/prices.js';
import { resolvePoolSelection, changePctForMinutes } from '../util/pool-select.js';

const router = express.Router();

/* ---------------- helpers ---------------- */

function dispSupply(n, exp) {
  if (n == null) return 0;
  const useBase = process.env.SUPPLY_IN_BASE_UNITS === '1';
  const x = Number(n);
  return useBase ? (x / Math.pow(10, exp || 0)) : x;
}

function zeroBuckets(keys) {
  const o = {};
  for (const k of keys) o[k] = 0;
  return o;
}

/* ---------------- routes ---------------- */

/**
 * GET /tokens
 * Query:
 *  - bucket=30m|1h|4h|24h
 *  - priceSource=best|first
 *  - sort=price|mcap|volume|tx|traders|created|change24h
 *  - dir=asc|desc
 *  - includeChange=1
 */
router.get('/', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const sort = (req.query.sort || 'mcap').toLowerCase();
    const dir  = (req.query.dir || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    const includeChange = req.query.includeChange === '1';
    const priceSource = (req.query.priceSource || 'best').toLowerCase(); // best|first

    const { rows } = await DB.query(`
      WITH base AS (
        SELECT
          t.token_id, t.denom, t.symbol, t.name, t.exponent, t.created_at,
          tm.price_in_zig, tm.mcap_zig, tm.fdv_zig, tm.holders,
          COALESCE((
            SELECT SUM(pm.vol_buy_zig + pm.vol_sell_zig)
            FROM pools p JOIN pool_matrix pm
              ON pm.pool_id=p.pool_id AND pm.bucket=$1
            WHERE p.base_token_id=t.token_id
          ),0) AS vol_zig_agg,
          COALESCE((
            SELECT SUM(pm.tx_buy + pm.tx_sell)
            FROM pools p JOIN pool_matrix pm
              ON pm.pool_id=p.pool_id AND pm.bucket=$1
            WHERE p.base_token_id=t.token_id
          ),0) AS tx_agg
        FROM tokens t
        LEFT JOIN token_matrix tm ON tm.token_id=t.token_id AND tm.bucket=$1
      )
      SELECT * FROM base
      ORDER BY token_id DESC
      LIMIT 400
    `, [bucket]);

    const zigUsd = await getZigUsd();

    // compute BEST price per token (UZIG-quoted pools)
    let bestPriceMap = new Map();
    if (rows.length) {
      const ids = rows.map(r => r.token_id);
      const q = await DB.query(`
        WITH tvl AS (
          SELECT pool_id, tvl_zig
          FROM pool_matrix
          WHERE bucket='24h'
        ),
        ranked AS (
          SELECT
            pr.token_id,
            pr.pool_id,
            pr.price_in_zig,
            ROW_NUMBER() OVER (
              PARTITION BY pr.token_id
              ORDER BY pr.price_in_zig ASC,
                       COALESCE((SELECT t.tvl_zig FROM tvl t WHERE t.pool_id=pr.pool_id),0) DESC,
                       pr.updated_at DESC
            ) AS rn
          FROM prices pr
          JOIN pools p ON p.pool_id=pr.pool_id
          WHERE pr.token_id = ANY($1) AND p.is_uzig_quote=TRUE
        )
        SELECT token_id, pool_id, price_in_zig
        FROM ranked
        WHERE rn=1
      `, [ids]);
      bestPriceMap = new Map(q.rows.map(r => [String(r.token_id), Number(r.price_in_zig)]));
    }

    // Optional: compute change24h per token using selected price source pool.
    let changeMap = new Map();
    if (includeChange && rows.length) {
      const resolved = await Promise.all(rows.map(async r => {
        const sel = await resolvePoolSelection(r.token_id, { priceSource });
        return { token_id: r.token_id, pool_id: sel.pool?.pool_id || null };
      }));
      await Promise.all(resolved.map(async x => {
        if (!x.pool_id) return;
        const pct = await changePctForMinutes(x.pool_id, 1440);
        changeMap.set(String(x.token_id), pct);
      }));
    }

    let data = rows.map(r => {
      const liveBest = bestPriceMap.get(String(r.token_id));
      const priceNative = (liveBest != null)
        ? liveBest
        : (r.price_in_zig != null ? Number(r.price_in_zig) : null);
      const mcapNative  = r.mcap_zig != null ? Number(r.mcap_zig) : null;
      const fdvNative   = r.fdv_zig  != null ? Number(r.fdv_zig)  : null;
      const change24hPct = includeChange ? (changeMap.get(String(r.token_id)) ?? null) : undefined;
      return {
        tokenId: r.token_id,
        denom: r.denom,
        symbol: r.symbol,
        name: r.name,
        priceNative,
        priceUsd: priceNative != null ? priceNative * zigUsd : null,
        mcapNative,
        mcapUsd: mcapNative != null ? mcapNative * zigUsd : null,
        fdvNative,
        fdvUsd: fdvNative != null ? fdvNative * zigUsd : null,
        holders: Number(r.holders || 0),
        volNative: Number(r.vol_zig_agg || 0),
        volUsd: Number(r.vol_zig_agg || 0) * zigUsd,
        tx: Number(r.tx_agg || 0),
        createdAt: r.created_at,
        change24hPct
      };
    });

    // Sorting
    if (sort === 'change24h' && includeChange) {
      data.sort((a, b) => {
        const av = a.change24hPct ?? -Infinity;
        const bv = b.change24hPct ?? -Infinity;
        return dir === 'ASC' ? (av - bv) : (bv - av);
      });
    } else {
      const keyMap = {
        price:   'priceNative',
        mcap:    'mcapNative',
        volume:  'volNative',
        tx:      'tx',
        traders: 'holders',
        created: 'createdAt',
      };
      const key = keyMap[sort] || 'mcapNative';
      data.sort((a, b) => {
        const av = a[key] ?? -Infinity;
        const bv = b[key] ?? -Infinity;
        return dir === 'ASC' ? (av - bv) : (bv - av);
      });
    }

    res.json({ success: true, data, meta: { priceSource, bucket, sort, dir } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/**
 * GET /tokens/:id
 * Query:
 *  - priceSource=best|first|pool
 *  - poolId=<id>
 *  - includePools=1
 */
router.get('/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const includePools = req.query.includePools === '1';
    const priceSource = (req.query.priceSource || 'best').toLowerCase();
    const poolIdParam = req.query.poolId;
    const zigUsd = await getZigUsd();

    // supplies
    const srow = await DB.query(
      `SELECT total_supply_base, max_supply_base, exponent
       FROM tokens WHERE token_id=$1`, [tok.token_id]
    );
    const sx  = srow.rows[0] || {};
    const exp = Number(sx.exponent || 6);
    const circ = dispSupply(sx.total_supply_base, exp);
    const max  = dispSupply(sx.max_supply_base,   exp);

    // pool selection for pricing + change
    const { pool } = await resolvePoolSelection(tok.token_id, { priceSource, poolId: poolIdParam });

    // latest price from the selected pool (if any)
    let priceNative = null;
    if (pool?.pool_id) {
      const pr = await DB.query(
        `SELECT price_in_zig FROM prices WHERE token_id=$1 AND pool_id=$2 ORDER BY updated_at DESC LIMIT 1`,
        [tok.token_id, pool.pool_id]
      );
      priceNative = pr.rows[0]?.price_in_zig != null ? Number(pr.rows[0].price_in_zig) : null;
    }
    const priceUsd = priceNative != null ? priceNative * zigUsd : null;

    // aggregates across ALL pools for these four buckets
    const buckets = ['30m','1h','4h','24h'];
    const agg = await DB.query(`
      SELECT pm.bucket,
             COALESCE(SUM(pm.vol_buy_zig),0)    AS vbuy,
             COALESCE(SUM(pm.vol_sell_zig),0)   AS vsell,
             COALESCE(SUM(pm.tx_buy),0)         AS tbuy,
             COALESCE(SUM(pm.tx_sell),0)        AS tsell,
             COALESCE(SUM(pm.unique_traders),0) AS uniq,
             COALESCE(SUM(pm.tvl_zig),0)        AS tvl
      FROM pools p
      JOIN pool_matrix pm ON pm.pool_id=p.pool_id
      WHERE p.base_token_id=$1
        AND pm.bucket = ANY($2)
      GROUP BY pm.bucket
    `, [tok.token_id, buckets]);

    const aggMap = new Map(agg.rows.map(r => [r.bucket, {
      vbuy: Number(r.vbuy || 0),
      vsell: Number(r.vsell || 0),
      tbuy: Number(r.tbuy || 0),
      tsell: Number(r.tsell || 0),
      uniq: Number(r.uniq || 0),
      tvl:  Number(r.tvl  || 0),
    }]));

    const volBuckets    = {};
    const volUSDBuckets = {};
    const txBuckets     = {};
    for (const b of buckets) {
      const r = aggMap.get(b);
      const v = r ? (r.vbuy + r.vsell) : 0;
      volBuckets[b]    = v;
      volUSDBuckets[b] = v * zigUsd;
      txBuckets[b]     = r ? (r.tbuy + r.tsell) : 0;
    }

    // pick 24h for summary stats
    const r24 = aggMap.get('24h') || { vbuy:0, vsell:0, tbuy:0, tsell:0, uniq:0, tvl:0 };
    const vbuy = r24.vbuy, vsell = r24.vsell, tvl = r24.tvl;
    const traders = r24.uniq, tradesBuy = r24.tbuy, tradesSell = r24.tsell;

    // price change (%) on selected pool
    const priceChange = {
      '30m': pool?.pool_id ? await changePctForMinutes(pool.pool_id, 30)   : null,
      '1h' : pool?.pool_id ? await changePctForMinutes(pool.pool_id, 60)   : null,
      '4h' : pool?.pool_id ? await changePctForMinutes(pool.pool_id, 240)  : null,
      '24h': pool?.pool_id ? await changePctForMinutes(pool.pool_id, 1440) : null,
    };

    // mcap / fdv
    const mcNative  = priceNative != null ? circ * priceNative : null;
    const mcUsd     = mcNative  != null ? mcNative  * zigUsd : null;
    const fdvNative = priceNative != null ? max * priceNative : null;
    const fdvUsd    = fdvNative != null ? fdvNative * zigUsd : null;

    // pools count & earliest creation
    const poolsMeta = await DB.query(`
      SELECT COUNT(*)::int AS c, MIN(created_at) AS first_ts
      FROM pools WHERE base_token_id=$1
    `, [tok.token_id]);
    const poolsCount = Number(poolsMeta.rows[0]?.c || 0);
    const creationTime = poolsMeta.rows[0]?.first_ts || null;

    // holders count
    const hs = await DB.query(`SELECT holders_count FROM token_holders_stats WHERE token_id=$1`, [tok.token_id]);
    const holders = Number(hs.rows[0]?.holders_count || 0);

    // Optional: embed ALL pools with rich details (24h metrics)
    let poolsEmbed;
    if (includePools) {
      const rows = await DB.query(`
        SELECT
          p.pool_id,
          p.pair_contract,
          p.base_token_id, p.quote_token_id,
          b.denom AS base_denom,  b.symbol AS base_symbol,  b.exponent AS base_exp,
          q.denom AS quote_denom, q.symbol AS quote_symbol, q.exponent AS quote_exp,
          p.is_uzig_quote,
          p.created_at,
          COALESCE(pm.tvl_zig,0)                                   AS tvl_zig,
          COALESCE(pm.vol_buy_zig,0) + COALESCE(pm.vol_sell_zig,0) AS vol_24h_zig,
          COALESCE(pm.tx_buy,0) + COALESCE(pm.tx_sell,0)           AS tx_24h,
          COALESCE(pm.unique_traders,0)                            AS unique_traders_24h,
          pr.price_in_zig
        FROM pools p
        JOIN tokens b ON b.token_id=p.base_token_id
        JOIN tokens q ON q.token_id=p.quote_token_id
        LEFT JOIN pool_matrix pm ON pm.pool_id=p.pool_id AND pm.bucket='24h'
        LEFT JOIN prices pr ON pr.pool_id=p.pool_id AND pr.token_id=p.base_token_id
        WHERE p.base_token_id=$1
        ORDER BY p.created_at ASC
      `, [tok.token_id]);

      poolsEmbed = rows.rows.map(r => ({
        poolId: r.pool_id,
        pairContract: r.pair_contract,
        base: { tokenId: r.base_token_id, denom: r.base_denom, symbol: r.base_symbol, exponent: Number(r.base_exp || 0) },
        quote:{ tokenId: r.quote_token_id, denom: r.quote_denom, symbol: r.quote_symbol, exponent: Number(r.quote_exp || 0) },
        isUzigQuote: r.is_uzig_quote === true,
        createdAt: r.created_at,
        feeBps: null, // schema has no fee_bps
        tvlNative: Number(r.tvl_zig || 0),
        priceNative: r.is_uzig_quote ? (r.price_in_zig != null ? Number(r.price_in_zig) : null) : null,
        volume24hNative: Number(r.vol_24h_zig || 0),
        tx24h: Number(r.tx_24h || 0),
        uniqueTraders24h: Number(r.unique_traders_24h || 0),
      }));
    }

    res.json({
      success: true,
      data: {
        priceInNative: priceNative,
        priceInUsd: priceUsd,
        priceSource,
        poolId: pool?.pool_id ?? null,

        // counts / meta
        pools: poolsCount,
        holder: holders,
        creationTime,

        // supply / caps
        supply: circ,
        circulatingSupply: circ,
        fdvNative,
        fdv: fdvUsd,
        mcNative: mcNative,
        mc: mcUsd,

        // changes & windows (no nulls)
        priceChange,
        volume: volBuckets,
        volumeUSD: volUSDBuckets,
        txBuckets,  // NEW

        // 24h rollups
        uniqueTraders: traders,
        trade: tradesBuy + tradesSell,
        sell: tradesSell,
        buy: tradesBuy,
        v: vbuy + vsell,
        vBuy: vbuy,
        vSell: vsell,
        vUSD: (vbuy + vsell) * zigUsd,
        vBuyUSD: vbuy * zigUsd,
        vSellUSD: vsell * zigUsd,
        tradeCount: { buy: tradesBuy, sell: tradesSell, total: tradesBuy + tradesSell },

        // liquidity (24h TVL)
        liquidity: tvl,

        ...(includePools ? { poolsDetailed: poolsEmbed } : {})
      }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/**
 * GET /tokens/:id/ohlcv
 * Query:
 *  - tf=1m|5m|15m|1h|4h|1d
 *  - from, to (ISO)
 *  - mode=price|mcap
 *  - unit=native|usd
 *  - priceSource=best|first|pool|all
 *  - poolId=<id>
 */
router.get('/:id/ohlcv', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const tf = (req.query.tf || '1m').toLowerCase();
    const toIso = req.query.to || new Date().toISOString();
    const fromIso = req.query.from || new Date(Date.now() - 24*3600*1000).toISOString();
    const mode = (req.query.mode || 'price').toLowerCase();
    const unit = (req.query.unit || 'native').toLowerCase();
    const priceSource = (req.query.priceSource || req.query.source || 'best').toLowerCase();
    const poolIdParam = req.query.poolId;
    const stepSec = { '1m':60, '5m':300, '15m':900, '1h':3600, '4h':14400, '1d':86400 }[tf] || 60;

    const zigUsd = await getZigUsd();

    // supply for mcap
    const ss = await DB.query(
      `SELECT total_supply_base, exponent FROM tokens WHERE token_id=$1`, [tok.token_id]
    );
    const exp = Number(ss.rows[0]?.exponent || 6);
    const circ = dispSupply(ss.rows[0]?.total_supply_base, exp);

    let params;
    let sqlHdr;
    if (priceSource === 'all') {
      params = [tok.token_id, fromIso, toIso, stepSec];
      sqlHdr = `
        WITH native AS (
          SELECT o.bucket_start, o.open, o.high, o.low, o.close, o.volume_zig, o.trade_count
          FROM ohlcv_1m o
          JOIN pools p ON p.pool_id=o.pool_id
          WHERE p.base_token_id=$1 AND p.is_uzig_quote=TRUE
            AND o.bucket_start >= $2::timestamptz
            AND o.bucket_start <  $3::timestamptz
        ),
      `;
    } else {
      const { pool } = await resolvePoolSelection(tok.token_id, { priceSource, poolId: poolIdParam });
      if (!pool?.pool_id) {
        return res.json({ success:true, data: [], meta: { mode, unit, priceSource, poolId: null } });
      }
      params = [pool.pool_id, fromIso, toIso, stepSec];
      sqlHdr = `
        WITH native AS (
          SELECT o.bucket_start, o.open, o.high, o.low, o.close, o.volume_zig, o.trade_count
          FROM ohlcv_1m o
          WHERE o.pool_id = $1
            AND o.bucket_start >= $2::timestamptz
            AND o.bucket_start <  $3::timestamptz
        ),
      `;
    }

    const { rows } = await DB.query(`
      ${sqlHdr}
      tagged AS (
        SELECT
          bucket_start, open, high, low, close, volume_zig, trade_count,
          to_timestamp(floor(extract(epoch from bucket_start)/$4)*$4) AT TIME ZONE 'UTC' AS bucket_ts
        FROM native
      ),
      agg AS (
        SELECT
          bucket_ts,
          MIN(low)  AS low,
          MAX(high) AS high,
          SUM(volume_zig)  AS volume_native,
          SUM(trade_count) AS trades,
          MIN(bucket_start) AS ts_open,
          MAX(bucket_start) AS ts_close
        FROM tagged
        GROUP BY bucket_ts
      ),
      o AS (
        SELECT a.bucket_ts AS ts,
               (SELECT t.open  FROM tagged t WHERE t.bucket_start=a.ts_open  LIMIT 1) AS open,
               (SELECT t.close FROM tagged t WHERE t.bucket_start=a.ts_close LIMIT 1) AS close,
               a.low, a.high, a.volume_native, a.trades
        FROM agg a
      )
      SELECT * FROM o ORDER BY ts
    `, params);

    const out = rows.map(b => {
      const openN = Number(b.open), highN = Number(b.high), lowN = Number(b.low), closeN = Number(b.close);
      const volN  = Number(b.volume_native);
      if (mode === 'mcap') {
        const openCapN  = openN  * circ;
        const highCapN  = highN  * circ;
        const lowCapN   = lowN   * circ;
        const closeCapN = closeN * circ;
        return (unit === 'usd')
          ? { ts: b.ts, open: openCapN*zigUsd, high: highCapN*zigUsd, low: lowCapN*zigUsd, close: closeCapN*zigUsd,
              volume: volN*zigUsd, trades: Number(b.trades) }
          : { ts: b.ts, open: openCapN, high: highCapN, low: lowCapN, close: closeCapN,
              volume: volN, trades: Number(b.trades) };
      }
      // price mode
      return (unit === 'usd')
        ? { ts: b.ts, open: openN*zigUsd, high: highN*zigUsd, low: lowN*zigUsd, close: closeN*zigUsd,
            volume: volN*zigUsd, trades: Number(b.trades) }
        : { ts: b.ts, open: openN, high: highN, low: lowN, close: closeN,
            volume: volN, trades: Number(b.trades) };
    });

    res.json({
      success:true,
      data: out,
      meta: { mode, unit, priceSource, poolId: priceSource==='all' ? null : params[0] }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /tokens/:id/holders */
router.get('/:id/holders', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const s = await DB.query(`SELECT max_supply_base, exponent FROM tokens WHERE token_id=$1`, [tok.token_id]);
    const exp = Number(s.rows[0]?.exponent || 6);
    const maxDisp = dispSupply(s.rows[0]?.max_supply_base, exp);
    const maxBase = process.env.SUPPLY_IN_BASE_UNITS === '1'
      ? Number(s.rows[0]?.max_supply_base || 0)
      : maxDisp * Math.pow(10, exp);

    const { rows } = await DB.query(`
      SELECT address, balance_base
      FROM holders
      WHERE token_id=$1 AND balance_base::numeric > 0
      ORDER BY balance_base::numeric DESC
      LIMIT 200
    `, [tok.token_id]);

    const top10 = rows.slice(0, 10);
    const sumTop10Base = top10.reduce((acc, r) => acc + Number(r.balance_base), 0);
    const pctTop10 = maxBase > 0 ? (sumTop10Base / maxBase) * 100 : 0;

    const data = rows.map(r => {
      const balDisp = process.env.SUPPLY_IN_BASE_UNITS === '1'
        ? Number(r.balance_base) / Math.pow(10, exp)
        : Number(r.balance_base);
      const pct = maxBase > 0 ? (Number(r.balance_base) / maxBase) * 100 : 0;
      return { address: r.address, balance: balDisp, pctOfMax: pct };
    });

    res.json({ success:true, data: { holders: data, top10Pct: pctTop10 } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /tokens/:id/pools
 *  Query:
 *   - bucket=30m|1h|4h|24h (default 24h)
 *   - includeCaps=1        (include mcap/fdv per pool using this pool's price)
 */
router.get('/:id/pools', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const bucket = (req.query.bucket || '24h').toLowerCase();
    const includeCaps = req.query.includeCaps === '1';
    const zigUsd = await getZigUsd();

    // Pull token supplies once for mcap/fdv (display units)
    let circDisp = null, maxDisp = null, exp = 6;
    if (includeCaps) {
      const s = await DB.query(`
        SELECT total_supply_base, max_supply_base, exponent
        FROM tokens WHERE token_id=$1
      `, [tok.token_id]);
      const row = s.rows[0] || {};
      exp = Number(row.exponent || 6);
      circDisp = row.total_supply_base != null ? (Number(row.total_supply_base) / Math.pow(10, exp)) : null;
      maxDisp  = row.max_supply_base   != null ? (Number(row.max_supply_base)   / Math.pow(10, exp)) : null;
    }

    // Pull pool metrics + latest price per pool (if any)
    const rows = await DB.query(`
      SELECT
        p.pool_id,
        p.pair_contract,
        p.base_token_id, p.quote_token_id,
        b.denom AS base_denom,  b.symbol AS base_symbol,  b.exponent AS base_exp,
        q.denom AS quote_denom, q.symbol AS quote_symbol, q.exponent AS quote_exp,
        p.is_uzig_quote,
        p.created_at,
        COALESCE(pm.tvl_zig,0)                                   AS tvl_zig,
        COALESCE(pm.vol_buy_zig,0) + COALESCE(pm.vol_sell_zig,0) AS vol_zig,
        COALESCE(pm.tx_buy,0) + COALESCE(pm.tx_sell,0)           AS tx_count,
        COALESCE(pm.unique_traders,0)                            AS unique_traders,
        pr.price_in_zig
      FROM pools p
      JOIN tokens b ON b.token_id=p.base_token_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      LEFT JOIN pool_matrix pm ON pm.pool_id=p.pool_id AND pm.bucket=$2
      LEFT JOIN LATERAL (
        SELECT price_in_zig FROM prices
        WHERE pool_id=p.pool_id AND token_id=p.base_token_id
        ORDER BY updated_at DESC LIMIT 1
      ) pr ON TRUE
      WHERE p.base_token_id=$1
      ORDER BY p.created_at ASC
    `, [tok.token_id, bucket]);

    const data = rows.rows.map(r => {
      const priceNative = r.is_uzig_quote ? (r.price_in_zig != null ? Number(r.price_in_zig) : null) : null;
      const priceUsd    = priceNative != null ? priceNative * zigUsd : null;
      const tvlNative   = r.tvl_zig != null ? Number(r.tvl_zig) : null;
      const tvlUsd      = tvlNative != null ? tvlNative * zigUsd : null;

      let mcapNative = null, mcapUsd = null, fdvNative = null, fdvUsd = null;
      if (includeCaps && priceNative != null) {
        if (circDisp != null) {
          mcapNative = circDisp * priceNative;
          mcapUsd    = mcapNative * zigUsd;
        }
        if (maxDisp != null) {
          fdvNative  = maxDisp * priceNative;
          fdvUsd     = fdvNative * zigUsd;
        }
      }

      return {
        poolId: r.pool_id,
        pairContract: r.pair_contract,
        base:  { tokenId: r.base_token_id,  denom: r.base_denom,  symbol: r.base_symbol, exponent: Number(r.base_exp || 0) },
        quote: { tokenId: r.quote_token_id, denom: r.quote_denom, symbol: r.quote_symbol, exponent: Number(r.quote_exp || 0) },
        isUzigQuote: r.is_uzig_quote === true,
        createdAt: r.created_at,
        feeBps: null, // schema has no fee_bps

        // price
        priceNative,
        priceUsd,

        // liquidity & activity
        tvlNative,
        tvlUsd,
        volumeNative: Number(r.vol_zig || 0),
        volumeUsd: Number(r.vol_zig || 0) * zigUsd,
        tx: Number(r.tx_count || 0),
        uniqueTraders: Number(r.unique_traders || 0),

        // optional caps (pool-priced)
        ...(includeCaps ? {
          mcapNative, mcapUsd, fdvNative, fdvUsd
        } : {})
      };
    });

    res.json({ success:true, data, meta: { bucket, includeCaps: includeCaps ? 1 : 0 } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /tokens/:id/security */
router.get('/:id/security', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const s = await DB.query(`
      SELECT max_supply_base, total_supply_base, exponent
      FROM tokens WHERE token_id=$1
    `, [tok.token_id]);
    const exp = Number(s.rows[0]?.exponent || 6);
    const maxBase   = Number(s.rows[0]?.max_supply_base || 0);
    const totalBase = Number(s.rows[0]?.total_supply_base || 0);

    const holders = await DB.query(`
      SELECT address, balance_base::numeric AS bal
      FROM holders
      WHERE token_id=$1
      ORDER BY balance_base::numeric DESC
      LIMIT 10
    `, [tok.token_id]);

    const top10Base = holders.rows.reduce((a, r) => a + Number(r.bal), 0);
    const top10Pct  = maxBase > 0 ? (top10Base / maxBase) * 100 : 0;
    const creatorAddr = holders.rows[0]?.address || null;
    const creatorBase = holders.rows[0]?.bal || 0;
    const creatorPct  = maxBase > 0 ? (Number(creatorBase) / maxBase) * 100 : 0;

    const mintable = !(totalBase === maxBase);

    res.json({
      success:true,
      data:{
        mintable,
        creator: creatorAddr,
        creatorPct: Number(creatorPct.toFixed(4)),
        top10Pct: Number(top10Pct.toFixed(4)),
        maxSupply: dispSupply(maxBase, exp),
        totalSupply: dispSupply(totalBase, exp)
      }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
