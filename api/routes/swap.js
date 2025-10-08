// api/routes/swap.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { resolveTokenId, getZigUsd } from '../util/resolve-token.js';

const router = express.Router();

/* ----------------------------- helpers ---------------------------------- */

const UZIG_KEYWORDS = new Set(['uzig', 'zig', 'uZIG', 'UZIG']);
const isUzigRef = (s) => !!s && UZIG_KEYWORDS.has(String(s).trim().toLowerCase());

async function resolveRef(ref) {
  if (isUzigRef(ref)) return { type: 'uzig' };
  const tok = await resolveTokenId(ref);
  if (!tok) return null;
  return { type: 'token', token: tok };
}

/** Parse Oroswap pair type → taker fee fraction. Supports both "custom(xyk_25)" and "custom-xyk_25". */
function pairFee(pairType) {
  if (!pairType) return 0.003; // ultra safe default

  const t = String(pairType).toLowerCase();

  if (t === 'xyk') return 0.0001;                 // 1 bps = 0.01%
  if (t === 'concentrated') return 0.01;          // 100 bps = 1%

  // custom formats:
  // "custom(xyk_25)" or "custom-xyk_25"
  const m = t.match(/xyk[_-](\d+)/);
  if (m) {
    const bps = Number(m[1]);
    if (Number.isFinite(bps)) return bps / 10_000; // bps → fraction
  }

  return 0.003;
}

/** XYK simulation with fee-on-input. */
function simulateXYK({ fromIsZig, amountIn, Rz, Rt, fee }) {
  if (!(Rz > 0 && Rt > 0) || !(amountIn > 0)) {
    return { out: 0, price: 0, impact: 0 };
  }
  const mid = Rz / Rt; // zig per token (quote/base)
  const xInAfterFee = amountIn * (1 - fee);

  if (fromIsZig) {
    // ZIG -> Token
    const outToken = (xInAfterFee * Rt) / (Rz + xInAfterFee);
    const effPriceZigPerToken = amountIn / Math.max(outToken, 1e-18);
    const impact = mid > 0 ? (effPriceZigPerToken / mid) - 1 : 0;
    return { out: outToken, price: effPriceZigPerToken, impact };
  } else {
    // Token -> ZIG
    const outZig = (xInAfterFee * Rz) / (Rt + xInAfterFee);
    const effPriceZigPerToken = outZig / amountIn; // zig per token actually received
    const impact = mid > 0 ? (mid / Math.max(effPriceZigPerToken, 1e-18)) - 1 : 0;
    return { out: outZig, price: effPriceZigPerToken, impact };
  }
}

/**
 * Load UZIG-quoted pools for a token with:
 *  - price_in_zig (mid)
 *  - reserves from pool_state converted to display units using tokens.exponent
 *  - tvl_zig from pool_matrix (24h)
 *
 * Assumption: is_uzig_quote=TRUE ⇒ quote is ZIG, base is the token.
 */
async function loadUzigPoolsForToken(tokenId, { minTvlZig = 0 } = {}) {
  const { rows } = await DB.query(
    `
    SELECT
      p.pool_id,
      p.pair_contract,
      p.pair_type,
      pr.price_in_zig,
      ps.reserve_base_base   AS res_base_base,
      ps.reserve_quote_base  AS res_quote_base,
      tb.exponent            AS base_exp,
      tq.exponent            AS quote_exp,
      COALESCE(pm.tvl_zig, 0) AS tvl_zig
    FROM pools p
    JOIN prices pr        ON pr.pool_id = p.pool_id AND pr.token_id = $1
    LEFT JOIN pool_state ps ON ps.pool_id = p.pool_id
    JOIN tokens tb        ON tb.token_id = p.base_token_id    -- the token itself
    JOIN tokens tq        ON tq.token_id = p.quote_token_id   -- ZIG
    LEFT JOIN pool_matrix pm ON pm.pool_id = p.pool_id AND pm.bucket = '24h'
    WHERE p.is_uzig_quote = TRUE
    `,
    [tokenId]
  );

  return rows
    .map(r => {
      const baseExp  = Number(r.base_exp || 0);
      const quoteExp = Number(r.quote_exp || 0);

      // Convert reserves from base units → display units
      const Rt = Number(r.res_base_base  || 0) / Math.pow(10, baseExp);   // token reserve
      const Rz = Number(r.res_quote_base || 0) / Math.pow(10, quoteExp);  // zig reserve

      return {
        poolId:        String(r.pool_id),
        pairContract:  r.pair_contract,
        pairType:      r.pair_type,
        priceInZig:    Number(r.price_in_zig), // mid zig per token
        tokenReserve:  Rt,
        zigReserve:    Rz,
        tvlZig:        Number(r.tvl_zig || 0),
      };
    })
    .filter(p => p.tvlZig >= minTvlZig);
}

/** Mid + TVL tie-break fallback when simulation not possible. */
function rankByMidWithTvl(pools, side /* 'buy' | 'sell' */, { tiePct = 0.004 } = {}) {
  const cmpMid = (a, b) => (side === 'buy' ? a.priceInZig - b.priceInZig : b.priceInZig - a.priceInZig);

  const sorted = [...pools].sort((a, b) => {
    const o = cmpMid(a, b);
    if (o !== 0) {
      const best = (side === 'buy' ? a : b);
      const worse = (side === 'buy' ? b : a);
      const diff = Math.abs(best.priceInZig - worse.priceInZig);
      const rel  = diff / Math.max(best.priceInZig, worse.priceInZig, 1e-18);
      if (rel <= tiePct) {
        return (b.tvlZig || 0) - (a.tvlZig || 0);
      }
      return o;
    }
    return (b.tvlZig || 0) - (a.tvlZig || 0);
  });

  return sorted[0] || null;
}

/** Pick the pool that maximizes executable output for a given notional. */
function pickBySimulation(pools, side /* 'buy' | 'sell' */, { fromIsZig, amountIn }) {
  let best = null;

  for (const p of pools) {
    const fee = pairFee(p.pairType);
    const hasRes = p.zigReserve > 0 && p.tokenReserve > 0;

    const sim = hasRes
      ? simulateXYK({
          fromIsZig,
          amountIn,
          Rz: p.zigReserve,
          Rt: p.tokenReserve,
          fee
        })
      : null;

    // Score equals output tokens (maximize out)
    const score = sim
      ? sim.out
      : (side === 'buy'
          ? (1 / Math.max(p.priceInZig, 1e-18)) * Math.log10(1 + (p.tvlZig || 1))
          : p.priceInZig * Math.log10(1 + (p.tvlZig || 1))
        );

    const cand = { ...p, fee, sim, score };
    if (!best || cand.score > best.score) best = cand;
  }
  return best;
}

/** choose default amounts (~$100 notional) if no 'amt' was provided */
function defaultAmounts({ side, zigUsd, pools }) {
  const targetUsd = 100;
  const zigAmt = targetUsd / Math.max(zigUsd, 1e-9);

  if (side === 'buy') {
    // From = ZIG
    return { amount: zigAmt, note: 'defaulted_to_~$100_in_ZIG' };
  }
  // side === 'sell': From = TOKEN. Estimate using average mid.
  const avgMid = pools.length
    ? pools.reduce((s, p) => s + (p.priceInZig || 0), 0) / pools.length
    : 1;
  const tokenAmt = zigAmt / Math.max(avgMid, 1e-12);
  return { amount: tokenAmt, note: 'defaulted_to_~$100_in_token' };
}

/* --------------------------- pool picking -------------------------------- */

async function bestBuyPool(tokenId, { amountIn, minTvlZig, tiePct, zigUsd }) {
  const pools = await loadUzigPoolsForToken(tokenId, { minTvlZig });
  if (!pools.length) return null;

  const amt = Number.isFinite(amountIn)
    ? Number(amountIn)
    : defaultAmounts({ side: 'buy', zigUsd, pools }).amount;

  const bySim = pickBySimulation(pools, 'buy', { fromIsZig: true, amountIn: amt });
  if (!bySim) return null;

  if (!bySim.sim) {
    const fallback = rankByMidWithTvl(pools, 'buy', { tiePct });
    if (!fallback) return null;
    return {
      ...fallback,
      priceInZig: fallback.priceInZig,
      diagnostics: { mode: 'fallback_mid_tvl' }
    };
  }

  return {
    ...bySim,
    priceInZig: bySim.sim.price, // executable zig per token for amt
    diagnostics: {
      mode: 'simulated',
      amountIn: amt,
      fromIsZig: true,
      reserves: { zig: bySim.zigReserve, token: bySim.tokenReserve },
      out: bySim.sim.out,
      impact: bySim.sim.impact,
      fee: bySim.fee
    }
  };
}

async function bestSellPool(tokenId, { amountIn, minTvlZig, tiePct, zigUsd }) {
  const pools = await loadUzigPoolsForToken(tokenId, { minTvlZig });
  if (!pools.length) return null;

  const amt = Number.isFinite(amountIn)
    ? Number(amountIn)
    : defaultAmounts({ side: 'sell', zigUsd, pools }).amount;

  const bySim = pickBySimulation(pools, 'sell', { fromIsZig: false, amountIn: amt });
  if (!bySim) return null;

  if (!bySim.sim) {
    const fallback = rankByMidWithTvl(pools, 'sell', { tiePct });
    if (!fallback) return null;
    return {
      ...fallback,
      priceInZig: fallback.priceInZig,
      diagnostics: { mode: 'fallback_mid_tvl' }
    };
  }

  return {
    ...bySim,
    priceInZig: bySim.sim.price, // executable zig per token for amt
    diagnostics: {
      mode: 'simulated',
      amountIn: amt,
      fromIsZig: false,
      reserves: { zig: bySim.zigReserve, token: bySim.tokenReserve },
      out: bySim.sim.out,
      impact: bySim.sim.impact,
      fee: bySim.fee
    }
  };
}

/* ------------------------------- route ----------------------------------- */
/**
 * GET /swap?from=<ref>&to=<ref>&amt=<number>&minTvl=<zig>&tiePct=<fraction>
 *
 * - UZIG -> Token : BUY (simulate ZIG→Token, pick max token out)
 * - Token -> UZIG : SELL (simulate Token→ZIG, pick max ZIG out)
 * - TokenA -> TokenB : via UZIG; chain the simulations
 */
router.get('/', async (req, res) => {
  try {
    const fromRef = req.query.from;
    const toRef   = req.query.to;
    if (!fromRef || !toRef) return res.status(400).json({ success:false, error:'missing from/to' });

    const zigUsd     = await getZigUsd();
    const amt        = req.query.amt ? Number(req.query.amt) : undefined;
    const minTvlZig  = req.query.minTvl ? Number(req.query.minTvl) : 0;
    const tiePct     = req.query.tiePct ? Number(req.query.tiePct) : 0.004;

    const from = await resolveRef(fromRef);
    const to   = await resolveRef(toRef);
    if (!from) return res.status(404).json({ success:false, error:'from token not found' });
    if (!to)   return res.status(404).json({ success:false, error:'to token not found' });

    // UZIG -> Token (BUY)
    if (from.type === 'uzig' && to.type === 'token') {
      const best = await bestBuyPool(to.token.token_id, { amountIn: amt, minTvlZig, tiePct, zigUsd });
      if (!best) {
        return res.json({ success:true, data:{
          route:['uzig', to.token.denom || to.token.symbol], pairs:[], price_native:null, price_usd:null, source:'direct_uzig'
        }});
      }
      const priceNative = best.priceInZig;
      const priceUsd = priceNative * zigUsd;

      return res.json({
        success: true,
        data: {
          route: ['uzig', to.token.denom || to.token.symbol || String(to.token.token_id)],
          pairs: [{ poolId: best.poolId, pairContract: best.pairContract, pairType: best.pairType }],
          price_native: priceNative,
          price_usd: priceUsd,
          source: 'direct_uzig',
          diagnostics: {
            side: 'buy',
            selection: best.diagnostics?.mode || 'simulated',
            poolId: best.poolId,
            pairType: best.pairType,
            tvl_zig: best.tvlZig,
            reserves: { zig: best.zigReserve, token: best.tokenReserve },
            sim: best.sim || null,
            params: { amt: amt ?? null, minTvlZig, tiePct }
          }
        }
      });
    }

    // Token -> UZIG (SELL)
    if (from.type === 'token' && to.type === 'uzig') {
      const best = await bestSellPool(from.token.token_id, { amountIn: amt, minTvlZig, tiePct, zigUsd });
      if (!best) {
        return res.json({ success:true, data:{
          route:[from.token.denom || from.token.symbol, 'uzig'], pairs:[], price_native:null, price_usd:null, source:'direct_uzig'
        }});
      }
      const priceNative = best.priceInZig;
      const priceUsd = priceNative * zigUsd;

      return res.json({
        success: true,
        data: {
          route: [from.token.denom || from.token.symbol || String(from.token.token_id), 'uzig'],
          pairs: [{ poolId: best.poolId, pairContract: best.pairContract, pairType: best.pairType }],
          price_native: priceNative,
          price_usd: priceUsd,
          source: 'direct_uzig',
          diagnostics: {
            side: 'sell',
            selection: best.diagnostics?.mode || 'simulated',
            poolId: best.poolId,
            pairType: best.pairType,
            tvl_zig: best.tvlZig,
            reserves: { zig: best.zigReserve, token: best.tokenReserve },
            sim: best.sim || null,
            params: { amt: amt ?? null, minTvlZig, tiePct }
          }
        }
      });
    }

    // TokenA -> TokenB (via UZIG)
    if (from.type === 'token' && to.type === 'token') {
      // Leg 1: A -> UZIG
      const sellA = await bestSellPool(from.token.token_id, { amountIn: amt, minTvlZig, tiePct, zigUsd });

      let zigOut = null;
      if (sellA?.sim) zigOut = sellA.sim.out;

      // Leg 2: UZIG -> B (use zigOut as input if we have it)
      const buyB = await bestBuyPool(to.token.token_id, { amountIn: zigOut || undefined, minTvlZig, tiePct, zigUsd });

      if (!sellA || !buyB) {
        return res.json({
          success: true,
          data: {
            route: [
              from.token.denom || from.token.symbol || String(from.token.token_id),
              'uzig',
              to.token.denom || to.token.symbol || String(to.token.token_id)
            ],
            pairs: [],
            price_native: null,
            price_usd: null,
            source: 'via_uzig',
            diagnostics: { sellA: !!sellA, buyB: !!buyB }
          }
        });
      }

      // Cross-rate (B per 1 A)
      const bPerA = sellA.priceInZig / Math.max(buyB.priceInZig, 1e-18);
      const zigPerA = sellA.priceInZig;
      const usdPerA = zigPerA * zigUsd;

      return res.json({
        success: true,
        data: {
          route: [
            from.token.denom || from.token.symbol || String(from.token.token_id),
            'uzig',
            to.token.denom || to.token.symbol || String(to.token.token_id)
          ],
          pairs: [
            { poolId: sellA.poolId, pairContract: sellA.pairContract, pairType: sellA.pairType },
            { poolId: buyB.poolId,  pairContract: buyB.pairContract,  pairType: buyB.pairType  }
          ],
          price_native: bPerA,
          cross: { zig_per_from: zigPerA, usd_per_from: usdPerA },
          price_usd: null,
          source: 'via_uzig',
          diagnostics: {
            sell_leg: {
              side: 'sell',
              poolId: sellA.poolId,
              pairType: sellA.pairType,
              tvl_zig: sellA.tvlZig,
              reserves: { zig: sellA.zigReserve, token: sellA.tokenReserve },
              sim: sellA.sim || null
            },
            buy_leg: {
              side: 'buy',
              poolId: buyB.poolId,
              pairType: buyB.pairType,
              tvl_zig: buyB.tvlZig,
              reserves: { zig: buyB.zigReserve, token: buyB.tokenReserve },
              sim: buyB.sim || null
            }
          }
        }
      });
    }

    return res.status(400).json({ success:false, error:'unsupported route (check from/to)' });
  } catch (e) {
    console.error('[swap] error:', e);
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
