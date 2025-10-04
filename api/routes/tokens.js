// api/routes/tokens.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { resolveTokenId, getZigUsd } from '../util/resolve-token.js';
import { resolvePoolSelection, changePctForMinutes } from '../util/pool-select.js';
import { getCandles, ensureTf } from '../util/ohlcv-agg.js';

const router = express.Router();
const toNum = x => (x == null ? null : Number(x));
const disp = (base, exp) => (base == null ? null : Number(base) / (10 ** (exp || 0)));

/* =======================================================================
   GET /tokens  (pagination + images + cumulative metrics)
   ======================================================================= */
router.get('/', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const priceSource = (req.query.priceSource || 'best').toLowerCase();
    const sort = (req.query.sort || 'mcap').toLowerCase();
    const dir  = (req.query.dir || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    const includeChange = req.query.includeChange === '1';
    const limit  = Math.max(1, Math.min(parseInt(req.query.limit || '50', 10), 200));
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));
    const zigUsd = await getZigUsd();

    const rows = await DB.query(`
      WITH agg AS (
        SELECT p.base_token_id AS token_id,
               SUM(pm.vol_buy_zig + pm.vol_sell_zig) AS vol_zig,
               SUM(pm.tx_buy + pm.tx_sell) AS tx
        FROM pool_matrix pm
        JOIN pools p ON p.pool_id=pm.pool_id
        WHERE pm.bucket=$1
        GROUP BY p.base_token_id
      ),
      base AS (
        SELECT t.token_id, t.denom, t.symbol, t.name, t.image_uri, t.created_at,
               tm.price_in_zig, tm.mcap_zig, tm.fdv_zig, tm.holders,
               a.vol_zig, a.tx
        FROM tokens t
        LEFT JOIN token_matrix tm ON tm.token_id=t.token_id AND tm.bucket=$1
        LEFT JOIN agg a ON a.token_id=t.token_id
      ),
      ranked AS (
        SELECT b.*, COUNT(*) OVER() AS total
        FROM base b
      )
      SELECT * FROM ranked
      ORDER BY
        ${sort === 'created' ? `created_at ${dir}` :
          sort === 'volume'  ? `COALESCE(vol_zig,0) ${dir}` :
          sort === 'tx'      ? `COALESCE(tx,0) ${dir}` :
          sort === 'price'   ? `COALESCE(price_in_zig,0) ${dir}` :
          sort === 'traders' ? `COALESCE(holders,0) ${dir}` :
                               `COALESCE(mcap_zig,0) ${dir}`}
      LIMIT $2 OFFSET $3
    `, [bucket, limit, offset]);

    // optional 24h change using selected priceSource
    const changeMap = new Map();
    if (includeChange && rows.rows.length) {
      const pairs = await Promise.all(rows.rows.map(async r => {
        const sel = await resolvePoolSelection(r.token_id, { priceSource });
        return { id: r.token_id, poolId: sel.pool?.pool_id || null };
      }));
      for (const x of pairs) {
        if (!x.poolId) continue;
        const pct = await changePctForMinutes(x.poolId, 1440);
        changeMap.set(String(x.id), pct);
      }
    }

    const data = rows.rows.map(r => {
      const priceN = toNum(r.price_in_zig);
      const mcapN  = toNum(r.mcap_zig);
      const fdvN   = toNum(r.fdv_zig);
      const volN   = toNum(r.vol_zig) || 0;
      return {
        tokenId: r.token_id,
        denom: r.denom,
        symbol: r.symbol,
        name: r.name,
        imageUri: r.image_uri,
        createdAt: r.created_at,
        priceNative: priceN,
        priceUsd: priceN != null ? priceN * zigUsd : null,
        mcapNative: mcapN,
        mcapUsd: mcapN != null ? mcapN * zigUsd : null,
        fdvNative: fdvN,
        fdvUsd: fdvN != null ? fdvN * zigUsd : null,
        holders: toNum(r.holders) || 0,
        volNative: volN,
        volUsd: volN * zigUsd,
        tx: toNum(r.tx) || 0,
        ...(includeChange ? { change24hPct: changeMap.get(String(r.token_id)) ?? null } : {})
      };
    });

    const total = rows.rows[0]?.total ?? 0;
    res.json({ success: true, data, meta: { bucket, priceSource, sort, dir, limit, offset, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =======================================================================
   GET /tokens/swap-list  (image + hooky stats for dropdown)
   ======================================================================= */
router.get('/swap-list', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const limit  = Math.max(1, Math.min(parseInt(req.query.limit || '200', 10), 500));
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));
    const zigUsd = await getZigUsd();

    const rows = await DB.query(`
      WITH agg AS (
        SELECT p.base_token_id AS token_id,
               SUM(pm.vol_buy_zig + pm.vol_sell_zig) AS vol_zig,
               SUM(pm.tx_buy + pm.tx_sell) AS tx,
               SUM(pm.tvl_zig) AS tvl_zig
        FROM pool_matrix pm
        JOIN pools p ON p.pool_id=pm.pool_id
        WHERE pm.bucket=$1
        GROUP BY p.base_token_id
      )
      SELECT t.token_id, t.symbol, t.name, t.denom, t.image_uri,
             tm.price_in_zig, tm.mcap_zig, tm.fdv_zig,
             a.vol_zig, a.tx, a.tvl_zig
      FROM tokens t
      LEFT JOIN token_matrix tm ON tm.token_id=t.token_id AND tm.bucket=$1
      LEFT JOIN agg a ON a.token_id=t.token_id
      ORDER BY COALESCE(a.vol_zig,0) DESC NULLS LAST
      LIMIT $2 OFFSET $3
    `, [bucket, limit, offset]);

    const data = rows.rows.map(r => {
      const priceN = toNum(r.price_in_zig);
      const mcapN  = toNum(r.mcap_zig);
      const fdvN   = toNum(r.fdv_zig);
      const volN   = toNum(r.vol_zig) || 0;
      const tvlN   = toNum(r.tvl_zig) || 0;
      return {
        tokenId: r.token_id,
        symbol: r.symbol,
        name: r.name,
        denom: r.denom,
        imageUri: r.image_uri,
        priceNative: priceN,
        priceUsd: priceN != null ? priceN * zigUsd : null,
        mcapNative: mcapN,
        mcapUsd: mcapN != null ? mcapN * zigUsd : null,
        fdvNative: fdvN,
        fdvUsd: fdvN != null ? fdvN * zigUsd : null,
        volNative: volN,
        volUsd: volN * zigUsd,
        tvlNative: tvlN,
        tvlUsd: tvlN * zigUsd,
        tx: toNum(r.tx) || 0,
      };
    });

    res.json({ success: true, data, meta: { bucket, limit, offset }});
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =======================================================================
   GET /tokens/:id  (returns BOTH: your original stats block **as-is** + rich meta)
   ======================================================================= */
router.get('/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const zigUsd = await getZigUsd();
    const priceSource = (req.query.priceSource || 'best').toLowerCase();
    const sel = await resolvePoolSelection(tok.token_id, { priceSource, poolId: req.query.poolId });

    const pr = sel.pool?.pool_id
      ? await DB.query(
          `SELECT price_in_zig FROM prices WHERE token_id=$1 AND pool_id=$2 ORDER BY updated_at DESC LIMIT 1`,
          [tok.token_id, sel.pool.pool_id]
        )
      : { rows: [] };
    const priceNative = pr.rows[0]?.price_in_zig != null ? Number(pr.rows[0].price_in_zig) : null;

    const srow = await DB.query(`
      SELECT total_supply_base, max_supply_base, exponent, image_uri, website, twitter, telegram, description
      FROM tokens WHERE token_id=$1
    `, [tok.token_id]);
    const s = srow.rows[0] || {};
    const exp = Number(s.exponent || 6);
    const circ = disp(s.total_supply_base, exp);
    const max  = disp(s.max_supply_base, exp);

    // stats aggregates across all pools (4 buckets)
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

    const map = new Map(agg.rows.map(r => [r.bucket, {
      vbuy: Number(r.vbuy || 0),
      vsell: Number(r.vsell || 0),
      tbuy: Number(r.tbuy || 0),
      tsell: Number(r.tsell || 0),
      uniq: Number(r.uniq || 0),
      tvl:  Number(r.tvl  || 0),
    }]));

    const vol = {}, volUSD = {}, txBuckets = {};
    for (const b of buckets) {
      const r = map.get(b);
      const v = r ? (r.vbuy + r.vsell) : 0;
      vol[b] = v;
      volUSD[b] = v * zigUsd;
      txBuckets[b] = r ? (r.tbuy + r.tsell) : 0;
    }
    const r24 = map.get('24h') || { vbuy:0, vsell:0, tbuy:0, tsell:0, uniq:0, tvl:0 };

    const priceChange = {
      '30m': sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 30)   : 0,
      '1h' : sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 60)   : 0,
      '4h' : sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 240)  : 0,
      '24h': sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 1440) : 0,
    };

    const mcNative  = (priceNative != null && circ != null) ? circ * priceNative : null;
    const fdvNative = (priceNative != null && max  != null) ? max  * priceNative : null;

    // header meta counts
    const poolsCount = (await DB.query(`SELECT COUNT(*)::int AS c FROM pools WHERE base_token_id=$1`, [tok.token_id])).rows[0]?.c || 0;
    const holders    = (await DB.query(`SELECT holders_count FROM token_holders_stats WHERE token_id=$1`, [tok.token_id])).rows[0]?.holders_count || 0;
    const creation   = (await DB.query(`SELECT MIN(created_at) AS first_ts FROM pools WHERE base_token_id=$1`, [tok.token_id])).rows[0]?.first_ts || null;

    // socials (twitter block)
    const tw = await DB.query(`
      SELECT handle, user_id, name, is_blue_verified, verified_type, profile_picture, cover_picture,
             followers, following, created_at_twitter, last_refreshed
      FROM token_twitter WHERE token_id=$1
    `, [tok.token_id]);

    // === RESPONSE: meta block + your original stats block fields (unchanged names) ===
    res.json({
      success: true,
      data: {
        // Meta / identity
        tokenId: String(tok.token_id),
        denom: tok.denom,
        symbol: tok.symbol,
        name: tok.name,
        exponent: exp,
        imageUri: s.image_uri,
        website: s.website, twitter: s.twitter, telegram: s.telegram,
        description: s.description,
        socials: tw.rows[0] ? {
          twitter: {
            handle: tw.rows[0].handle,
            userId: tw.rows[0].user_id,
            name: tw.rows[0].name,
            isBlueVerified: !!tw.rows[0].is_blue_verified,
            verifiedType: tw.rows[0].verified_type,
            profilePicture: tw.rows[0].profile_picture,
            coverPicture: tw.rows[0].cover_picture,
            followers: toNum(tw.rows[0].followers),
            following: toNum(tw.rows[0].following),
            createdAtTwitter: tw.rows[0].created_at_twitter,
            lastRefreshed: tw.rows[0].last_refreshed
          }
        } : {},

        // Also expose a compact price sub-object for convenience
        price: {
          source: priceSource,
          poolId: sel.pool?.pool_id ? String(sel.pool.pool_id) : null,
          native: priceNative,
          usd: priceNative != null ? priceNative * zigUsd : null,
          changePct: priceChange
        },

        // Supply + caps in blocks
        supply: { circulating: circ, max },
        mcap:   { native: mcNative, usd: mcNative != null ? mcNative * zigUsd : null },
        fdv:    { native: fdvNative, usd: fdvNative != null ? fdvNative * zigUsd : null },

        // ===== Your ORIGINAL block (names/shape preserved) =====
        priceInNative: priceNative,
        priceInUsd: priceNative != null ? priceNative * zigUsd : null,
        priceSource: priceSource,
        poolId: sel.pool?.pool_id ? String(sel.pool.pool_id) : null,
        pools: poolsCount,
        holder: holders,
        creationTime: creation,
        supply: max ?? circ,                   // original field
        circulatingSupply: circ,
        fdvNative,
        fdv: fdvNative != null ? fdvNative * zigUsd : null,
        mcNative,
        mc: mcNative != null ? mcNative * zigUsd : null,
        priceChange,
        volume: vol,
        volumeUSD: volUSD,
        txBuckets,
        uniqueTraders: r24.uniq,
        trade: r24.tbuy + r24.tsell,
        sell: r24.tsell,
        buy:  r24.tbuy,
        v: r24.vbuy + r24.vsell,
        vBuy: r24.vbuy,
        vSell: r24.vsell,
        vUSD: (r24.vbuy + r24.vsell) * zigUsd,
        vBuyUSD: r24.vbuy * zigUsd,
        vSellUSD: r24.vsell * zigUsd,
        tradeCount: { buy: r24.tbuy, sell: r24.tsell, total: r24.tbuy + r24.tsell },
        liquidity: r24.tvl
      }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =======================================================================
   GET /tokens/:id/pools  (token header + image; pool metrics)
   ======================================================================= */
router.get('/:id/pools', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const includeCaps = req.query.includeCaps === '1';
    const zigUsd = await getZigUsd();

    const header = await DB.query(`SELECT token_id, symbol, denom, image_uri, total_supply_base, max_supply_base, exponent FROM tokens WHERE token_id=$1`, [tok.token_id]);
    const h = header.rows[0];
    const exp = Number(h.exponent || 6);
    const circ = disp(h.total_supply_base, exp);
    const max  = disp(h.max_supply_base, exp);

    const rows = await DB.query(`
      SELECT
        p.pool_id, p.pair_contract, p.base_token_id, p.quote_token_id, p.is_uzig_quote, p.created_at,
        b.symbol AS base_symbol, b.denom AS base_denom, b.exponent AS base_exp,
        q.symbol AS quote_symbol, q.denom AS quote_denom, q.exponent AS quote_exp,
        COALESCE(pm.tvl_zig,0) AS tvl_zig,
        COALESCE(pm.vol_buy_zig,0) + COALESCE(pm.vol_sell_zig,0) AS vol_zig,
        COALESCE(pm.tx_buy,0) + COALESCE(pm.tx_sell,0) AS tx,
        COALESCE(pm.unique_traders,0) AS unique_traders,
        pr.price_in_zig
      FROM pools p
      JOIN tokens b ON b.token_id=p.base_token_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      LEFT JOIN pool_matrix pm ON pm.pool_id=p.pool_id AND pm.bucket=$2
      LEFT JOIN LATERAL (
        SELECT price_in_zig FROM prices WHERE pool_id=p.pool_id AND token_id=p.base_token_id
        ORDER BY updated_at DESC LIMIT 1
      ) pr ON TRUE
      WHERE p.base_token_id=$1
      ORDER BY p.created_at ASC
    `, [tok.token_id, bucket]);

    const data = rows.rows.map(r => {
      const priceN = r.is_uzig_quote ? toNum(r.price_in_zig) : null;
      const tvlN   = toNum(r.tvl_zig) || 0;
      const volN   = toNum(r.vol_zig) || 0;
      const mcapN  = includeCaps && priceN != null && circ != null ? priceN * circ : null;
      const fdvN   = includeCaps && priceN != null && max  != null ? priceN * max  : null;
      return {
        pairContract: r.pair_contract,
        base: { tokenId: r.base_token_id, symbol: r.base_symbol, denom: r.base_denom, exponent: toNum(r.base_exp) },
        quote:{ tokenId: r.quote_token_id, symbol: r.quote_symbol, denom: r.quote_denom, exponent: toNum(r.quote_exp) },
        isUzigQuote: r.is_uzig_quote === true,
        createdAt: r.created_at,
        priceNative: priceN,
        priceUsd: priceN != null ? priceN * zigUsd : null,
        tvlNative: tvlN, tvlUsd: tvlN * zigUsd,
        volumeNative: volN, volumeUsd: volN * zigUsd,
        tx: toNum(r.tx) || 0,
        uniqueTraders: toNum(r.unique_traders) || 0,
        ...(includeCaps ? {
          mcapNative: mcapN, mcapUsd: mcapN != null ? mcapN * zigUsd : null,
          fdvNative: fdvN,   fdvUsd:  fdvN  != null ? fdvN  * zigUsd : null
        } : {})
      };
    });

    res.json({
      success: true,
      token: { tokenId: h.token_id, symbol: h.symbol, denom: h.denom, imageUri: h.image_uri },
      data,
      meta: { bucket, includeCaps: includeCaps ? 1 : 0 }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =======================================================================
   GET /tokens/:id/holders (with total count)
   ======================================================================= */
router.get('/:id/holders', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });
    const limit  = Math.max(1, Math.min(parseInt(req.query.limit || '200', 10), 500));
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));

    const sup = await DB.query(`SELECT max_supply_base, total_supply_base, exponent FROM tokens WHERE token_id=$1`, [tok.token_id]);
    const exp = Number(sup.rows[0]?.exponent || 6);
    const maxBase = Number(sup.rows[0]?.max_supply_base || 0);
    const totBase = Number(sup.rows[0]?.total_supply_base || 0);

    const totalRow = await DB.query(`SELECT COUNT(*)::bigint AS total FROM holders WHERE token_id=$1 AND balance_base::numeric > 0`, [tok.token_id]);
    const total = Number(totalRow.rows[0]?.total || 0);

    const { rows } = await DB.query(`
      SELECT address, balance_base::numeric AS bal
      FROM holders
      WHERE token_id=$1 AND balance_base::numeric > 0
      ORDER BY bal DESC
      LIMIT $2 OFFSET $3
    `, [tok.token_id, limit, offset]);

    const top10 = rows.slice(0, 10).reduce((a, r) => a + Number(r.bal), 0);
    const pctTop10Max = maxBase > 0 ? (top10 / maxBase) * 100 : null;

    const holders = rows.map(r => {
      const balDisp = Number(r.bal) / (10 ** exp);
      const pctMax  = maxBase > 0 ? (Number(r.bal) / maxBase) * 100 : null;
      const pctTot  = totBase > 0 ? (Number(r.bal) / totBase) * 100 : null;
      return { address: r.address, balance: balDisp, pctOfMax: pctMax, pctOfTotal: pctTot };
    });

    res.json({ success: true, data: holders, meta: { limit, offset, totalHolders: total, top10PctOfMax: pctTop10Max } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =======================================================================
   GET /tokens/:id/security  (use public.token_security)
   ======================================================================= */
router.get('/:id/security', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const r = await DB.query(`SELECT * FROM public.token_security WHERE token_id=$1 LIMIT 1`, [tok.token_id]);
    const s = r.rows[0] || null;

    const sup = await DB.query(`SELECT max_supply_base, total_supply_base, exponent FROM tokens WHERE token_id=$1`, [tok.token_id]);
    const exp = Number(sup.rows[0]?.exponent || 6);
    const maxBase = Number(sup.rows[0]?.max_supply_base || 0);
    const totBase = Number(sup.rows[0]?.total_supply_base || 0);

    // score (simple aggregation over checks; tweak as you wish)
    let score = 20;
    if (s?.contract_verified) score += 12;
    if (s?.owner_renounced)   score += 10;
    if (s?.liquidity_lock_pct >= 75) score += 10;
    if ((s?.buy_tax_bps ?? 0) <= 500 && (s?.sell_tax_bps ?? 0) <= 500) score += 12;
    score = Math.max(1, Math.min(99, Math.round(score)));

    res.json({
      success: true,
      data: {
        score,
        checks: {
          contractVerified: !!(s?.contract_verified),
          buyTaxBps: Number(s?.buy_tax_bps ?? 0),
          sellTaxBps: Number(s?.sell_tax_bps ?? 0),
          proxy: !!(s?.proxy_contract),
          mintable: s?.mintable ?? (totBase !== maxBase),
          owner: s?.owner ?? null,
          ownerRenounced: !!(s?.owner_renounced),
          liquidityLockPct: Number(s?.liquidity_lock_pct ?? 0),
          top10Pct: Number(s?.top10_pct ?? 0),
          creatorPct: Number(s?.creator_pct ?? 0),
          maxSupply: maxBase / (10 ** exp),
          totalSupply: totBase / (10 ** exp)
        },
        lastUpdated: s?.updated_at || s?.inserted_at || null,
        source: 'token_security'
      }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* =======================================================================
   GET /tokens/:id/ohlcv  (delegates to util; supports fill; no GROUP BY errors)
   ======================================================================= */
router.get('/:id/ohlcv', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const tf   = ensureTf(req.query.tf || '1m');
    const from = req.query.from || new Date(Date.now() - 24*3600*1000).toISOString();
    const to   = req.query.to   || new Date().toISOString();
    const mode = (req.query.mode || 'price').toLowerCase();
    const unit = (req.query.unit || 'native').toLowerCase();
    const fill = (req.query.fill || 'prev').toLowerCase();
    const priceSource = (req.query.priceSource || 'best').toLowerCase();
    const poolRef = req.query.poolId || req.query.pair || null;

    const zigUsd = await getZigUsd();

    let poolId = null, useAll = false;
    if (priceSource === 'all') useAll = true;
    else {
      const sel = await resolvePoolSelection(tok.token_id, { priceSource, poolId: poolRef });
      poolId = sel.pool?.pool_id ?? null;
    }

    const sup = await DB.query(`SELECT total_supply_base, exponent FROM tokens WHERE token_id=$1`, [tok.token_id]);
    const exp = Number(sup.rows[0]?.exponent || 6);
    const circ = disp(sup.rows[0]?.total_supply_base, exp);

    const bars = await getCandles({
      tokenId: tok.token_id,
      poolId,
      useAll,
      tf, from, to,
      mode, unit, fill,
      zigUsd, circ
    });

    res.json({ success: true, data: bars, meta: { tf, mode, unit, fill, priceSource, poolId: poolId ?? null } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
