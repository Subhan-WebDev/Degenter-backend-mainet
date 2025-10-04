// api/routes/tokens.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { resolveTokenId, getZigUsd } from '../util/resolve-token.js';
import { resolvePoolSelection, changePctForMinutes } from '../util/pool-select.js';
import { getCandles, ensureTf } from '../util/ohlcv-agg.js';

const router = express.Router();

/* ========== helpers ========== */
function toNum(x) { return x == null ? null : Number(x); }
function disp(base, exp) {
  if (base == null) return null;
  const useBase = process.env.SUPPLY_IN_BASE_UNITS === '1';
  const n = Number(base);
  return useBase ? n / (10 ** (exp || 0)) : n;
}

/* ========== GET /tokens (pagination + images + cumulative metrics) ========== */
/**
 * Query:
 *  bucket=30m|1h|4h|24h (default 24h)
 *  priceSource=best|first
 *  sort=price|mcap|volume|tx|traders|created|change24h
 *  dir=asc|desc
 *  includeChange=1
 *  limit (<=200), offset
 */
router.get('/', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const priceSource = (req.query.priceSource || 'best').toLowerCase();
    const sort = (req.query.sort || 'mcap').toLowerCase();
    const dir  = (req.query.dir || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    const includeChange = req.query.includeChange === '1';
    const limit  = Math.max(1, Math.min(parseInt(req.query.limit || '50', 10), 200));
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));

    // base rows + cumulative activity across ALL pools
    const rows = await DB.query(`
      WITH base AS (
        SELECT
          t.token_id, t.denom, t.symbol, t.name, t.image_uri, t.created_at,
          tm.price_in_zig, tm.mcap_zig, tm.fdv_zig, tm.holders
        FROM tokens t
        LEFT JOIN token_matrix tm ON tm.token_id=t.token_id AND tm.bucket=$1
      ),
      agg AS (
        SELECT p.base_token_id AS token_id,
               SUM(pm.vol_buy_zig + pm.vol_sell_zig) AS vol_zig,
               SUM(pm.tx_buy + pm.tx_sell) AS tx
        FROM pool_matrix pm
        JOIN pools p ON p.pool_id=pm.pool_id
        WHERE pm.bucket=$1
        GROUP BY p.base_token_id
      ),
      joined AS (
        SELECT b.*, a.vol_zig, a.tx
        FROM base b
        LEFT JOIN agg a ON a.token_id=b.token_id
      ),
      ranked AS (
        SELECT j.*, 
               ROW_NUMBER() OVER () as rn,
               COUNT(*) OVER() AS total
        FROM joined j
      )
      SELECT * FROM ranked
      ORDER BY
        ${sort === 'created' ? `created_at ${dir}` :
          sort === 'volume'  ? `COALESCE(vol_zig,0) ${dir}` :
          sort === 'tx'      ? `COALESCE(tx,0) ${dir}` :
          sort === 'price'   ? `COALESCE(price_in_zig,0) ${dir}` :
          sort === 'mcap'    ? `COALESCE(mcap_zig,0) ${dir}` :
          sort === 'traders' ? `COALESCE(holders,0) ${dir}` :
                               `COALESCE(mcap_zig,0) ${dir}`}
      LIMIT $2 OFFSET $3
    `, [bucket, limit, offset]);

    const zigUsd = await getZigUsd();

    // optional change% per token
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

/* ========== GET /tokens/swap-list (dropdown-friendly, image-heavy) ========== */
/**
 * Query: bucket=30m|1h|4h|24h (default 24h), limit, offset
 * Returns minimal token card + hooky stats (cumulative across pools)
 */
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

/* ========== GET /tokens/:id (enriched meta + socials) ========== */
router.get('/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const zigUsd = await getZigUsd();
    // price via selected pool (best by default)
    const priceSource = (req.query.priceSource || 'best').toLowerCase();
    const sel = await resolvePoolSelection(tok.token_id, { priceSource, poolId: req.query.poolId });

    const pr = sel.pool?.pool_id
      ? await DB.query(
          `SELECT price_in_zig FROM prices WHERE token_id=$1 AND pool_id=$2 ORDER BY updated_at DESC LIMIT 1`,
          [tok.token_id, sel.pool.pool_id]
        )
      : { rows: [] };
    const priceNative = pr.rows[0]?.price_in_zig != null ? Number(pr.rows[0].price_in_zig) : null;

    // supplies & caps
    const srow = await DB.query(`SELECT total_supply_base, max_supply_base, exponent, image_uri, website, twitter, telegram, description FROM tokens WHERE token_id=$1`, [tok.token_id]);
    const s = srow.rows[0] || {};
    const exp = Number(s.exponent || 6);
    const circ = disp(s.total_supply_base, exp);
    const max  = disp(s.max_supply_base, exp);
    const mcapNative = (priceNative != null && circ != null) ? priceNative * circ : null;
    const fdvNative  = (priceNative != null && max != null)  ? priceNative * max  : null;

    // socials from token_twitter
    const tw = await DB.query(`SELECT handle, user_id, name, is_blue_verified, verified_type, profile_picture, cover_picture, followers, following, created_at_twitter, last_refreshed FROM token_twitter WHERE token_id=$1`, [tok.token_id]);

    res.json({
      success: true,
      data: {
        tokenId: tok.token_id,
        denom: tok.denom, symbol: tok.symbol, name: tok.name, type: tok.type,
        exponent: tok.exponent,
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
            lastRefreshed: tw.rows[0].last_refreshed,
          }
        } : {},
        price: {
          source: priceSource,
          poolId: sel.pool?.pool_id ?? null,
          native: priceNative,
          usd: priceNative != null ? priceNative * zigUsd : null,
          changePct: {
            '30m': sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 30)   : null,
            '1h' : sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 60)   : null,
            '4h' : sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 240)  : null,
            '24h': sel.pool?.pool_id ? await changePctForMinutes(sel.pool.pool_id, 1440) : null,
          }
        },
        supply: { circulating: circ, max },
        mcap: { native: mcapNative, usd: mcapNative != null ? mcapNative * zigUsd : null },
        fdv:  { native: fdvNative,  usd: fdvNative  != null ? fdvNative  * zigUsd : null },
      }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* ========== GET /tokens/:id/pools (include token image + cumulative metrics per pool) ========== */
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
      const mcapN  = includeCaps && priceN != null && circ != null ? priceN * circ : null;
      const fdvN   = includeCaps && priceN != null && max  != null ? priceN * max  : null;
      const tvlN   = toNum(r.tvl_zig) || 0;
      const volN   = toNum(r.vol_zig) || 0;
      return {
        poolId: r.pool_id,
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

/* ========== GET /tokens/:id/holders (with pct of max & total) ========== */
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

    res.json({ success: true, data: holders, meta: { limit, offset, top10PctOfMax: pctTop10Max } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* ========== GET /tokens/:id/security (use tokens_security to compute DegenScore) ========== */
router.get('/:id/security', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    // Pull row from your tokens_security (adapt column names if different)
    const r = await DB.query(`
      SELECT *
      FROM tokens_security
      WHERE token_id=$1
      LIMIT 1
    `, [tok.token_id]);

    const s = r.rows[0] || {};
    // Fallbacks from holders/supply if needed
    const sup = await DB.query(`SELECT max_supply_base, total_supply_base, exponent FROM tokens WHERE token_id=$1`, [tok.token_id]);
    const exp = Number(sup.rows[0]?.exponent || 6);
    const maxBase = Number(sup.rows[0]?.max_supply_base || 0);
    const totBase = Number(sup.rows[0]?.total_supply_base || 0);

    // naive score (tune weights)
    let score = 0;
    if (s.contract_verified ?? false) score += 12;
    if (!(s.proxy_contract ?? false)) score += 10;
    if ((s.buy_tax_bps ?? 0) <= 500) score += 8;
    if ((s.sell_tax_bps ?? 0) <= 500) score += 8;
    if (s.owner_renounced ?? false) score += 10;
    if (!(s.mintable ?? true)) score += 12;
    const lock = Number(s.liquidity_lock_pct ?? 0);
    if (lock >= 90) score += 15; else if (lock >= 75) score += 10; else if (lock >= 50) score += 6;
    const top10 = Number(s.top10_pct ?? 0);
    if (top10 <= 20) score += 10; else if (top10 <= 30) score += 6;
    const creatorPct = Number(s.creator_pct ?? 0);
    if (creatorPct <= 5) score += 8; else if (creatorPct <= 10) score += 4;

    score = Math.max(1, Math.min(99, Math.round(score)));

    res.json({
      success: true,
      data: {
        score,
        checks: {
          contractVerified: !!s.contract_verified,
          buyTaxBps: Number(s.buy_tax_bps ?? 0),
          sellTaxBps: Number(s.sell_tax_bps ?? 0),
          proxy: !!s.proxy_contract,
          mintable: !!(s.mintable ?? (totBase !== maxBase)),
          owner: s.owner ?? null,
          ownerRenounced: !!s.owner_renounced,
          liquidityLockPct: lock,
          top10Pct: top10,
          creatorPct: creatorPct,
          maxSupply: disp(maxBase, exp),
          totalSupply: disp(totBase, exp)
        },
        lastUpdated: s.updated_at || s.inserted_at || null
      }
    });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/* ========== GET /tokens/:id/ohlcv (rich TFs + fill + 'all' price source) ========== */
router.get('/:id/ohlcv', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const tf   = ensureTf(req.query.tf || '1m');
    const from = req.query.from || new Date(Date.now() - 24*3600*1000).toISOString();
    const to   = req.query.to   || new Date().toISOString();
    const mode = (req.query.mode || 'price').toLowerCase();   // price | mcap
    const unit = (req.query.unit || 'native').toLowerCase();  // native | usd
    const fill = (req.query.fill || 'prev').toLowerCase();    // prev | zero | none

    const priceSource = (req.query.priceSource || 'best').toLowerCase(); // best|first|pool|all
    const poolRef = req.query.poolId || req.query.pair || null;

    const zigUsd = await getZigUsd();

    // resolve selection
    let poolId = null, useAll = false;
    if (priceSource === 'all') {
      useAll = true;
    } else {
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
      zigUsd,
      priceSource,
      circ
    });

    res.json({ success: true, data: bars, meta: { tf, mode, unit, fill, priceSource, poolId: poolId ?? null } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
