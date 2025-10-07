// api/routes/swap.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { resolveTokenId, getZigUsd } from '../util/resolve-token.js';

const router = express.Router();

// ---- helpers ---------------------------------------------------------------

const UZIG_KEYWORDS = new Set(['uzig', 'zig', 'uZIG', 'UZIG']);

function isUzigRef(s) {
  if (!s) return false;
  const x = String(s).trim().toLowerCase();
  return UZIG_KEYWORDS.has(x);
}

/**
 * Resolve a token reference (id/symbol/denom) OR uzig special.
 * Returns: { type:'uzig' } or { type:'token', token:{ token_id, denom, symbol, exponent, name } }
 */
async function resolveRef(ref) {
  if (isUzigRef(ref)) return { type: 'uzig' };
  const tok = await resolveTokenId(ref);
  if (!tok) return null;
  return { type: 'token', token: tok };
}

/**
 * Rank pools for BEST BUY (lowest price_in_zig, tiebreak higher 24h TVL)
 */
async function pickBestBuyPool(tokenId) {
  const prices = await DB.query(`
    SELECT pr.pool_id, pr.price_in_zig, pr.updated_at, p.pair_contract, p.pair_type
    FROM prices pr
    JOIN pools p ON p.pool_id=pr.pool_id
    WHERE pr.token_id=$1 AND p.is_uzig_quote=TRUE
    ORDER BY pr.updated_at DESC
    LIMIT 32
  `, [tokenId]);
  if (!prices.rows.length) return null;

  const ids = prices.rows.map(r => r.pool_id);
  const tvls = await DB.query(
    `SELECT pool_id, tvl_zig FROM pool_matrix WHERE bucket='24h' AND pool_id = ANY($1)`,
    [ids]
  );
  const tvlMap = new Map(tvls.rows.map(r => [String(r.pool_id), Number(r.tvl_zig || 0)]));

  const sorted = prices.rows.sort((a,b) => {
    const pa = Number(a.price_in_zig), pb = Number(b.price_in_zig);
    if (pa !== pb) return pa - pb;                  // BUY: lower price better
    const ta = tvlMap.get(String(a.pool_id)) || 0;
    const tb = tvlMap.get(String(b.pool_id)) || 0;
    return tb - ta;                                  // tie-break higher TVL
  });

  const top = sorted[0];
  return {
    poolId: String(top.pool_id),
    pairContract: top.pair_contract,
    pairType: top.pair_type,
    priceInZig: Number(top.price_in_zig)
  };
}

/**
 * Rank pools for BEST SELL (highest price_in_zig, tiebreak higher 24h TVL)
 */
async function pickBestSellPool(tokenId) {
  const prices = await DB.query(`
    SELECT pr.pool_id, pr.price_in_zig, pr.updated_at, p.pair_contract, p.pair_type
    FROM prices pr
    JOIN pools p ON p.pool_id=pr.pool_id
    WHERE pr.token_id=$1 AND p.is_uzig_quote=TRUE
    ORDER BY pr.updated_at DESC
    LIMIT 32
  `, [tokenId]);
  if (!prices.rows.length) return null;

  const ids = prices.rows.map(r => r.pool_id);
  const tvls = await DB.query(
    `SELECT pool_id, tvl_zig FROM pool_matrix WHERE bucket='24h' AND pool_id = ANY($1)`,
    [ids]
  );
  const tvlMap = new Map(tvls.rows.map(r => [String(r.pool_id), Number(r.tvl_zig || 0)]));

  const sorted = prices.rows.sort((a,b) => {
    const pa = Number(a.price_in_zig), pb = Number(b.price_in_zig);
    if (pa !== pb) return pb - pa;                  // SELL: higher price better
    const ta = tvlMap.get(String(a.pool_id)) || 0;
    const tb = tvlMap.get(String(b.pool_id)) || 0;
    return tb - ta;                                 // tie-break higher TVL
  });

  const top = sorted[0];
  return {
    poolId: String(top.pool_id),
    pairContract: top.pair_contract,
    pairType: top.pair_type,
    priceInZig: Number(top.price_in_zig)
  };
}

// ---- route -----------------------------------------------------------------

/**
 * GET /swap?from=<ref>&to=<ref>
 *
 * Behavior:
 * - UZIG -> Token : BUY Token (use best BUY pool: min price_in_zig)
 * - Token -> UZIG : SELL Token (use best SELL pool: max price_in_zig)
 * - TokenA -> TokenB : route via UZIG:
 *     - A -> UZIG uses best SELL on A (max price_in_zig)
 *     - UZIG -> B uses best BUY on B (min price_in_zig)
 *   effective cross price = (price_sell_A / price_buy_B)  [B per 1 A]
 */
router.get('/', async (req, res) => {
  try {
    const fromRef = req.query.from;
    const toRef   = req.query.to;
    if (!fromRef || !toRef) {
      return res.status(400).json({ success:false, error: 'missing from/to' });
    }

    const zigUsd = await getZigUsd();

    const from = await resolveRef(fromRef);
    const to   = await resolveRef(toRef);
    if (!from) return res.status(404).json({ success:false, error:'from token not found' });
    if (!to)   return res.status(404).json({ success:false, error:'to token not found' });

    // UZIG <-> Token (direct, one hop)
    if (from.type === 'uzig' && to.type === 'token') {
      // BUY Token with UZIG: want the lowest ZIG per Token
      const best = await pickBestBuyPool(to.token.token_id);
      if (!best) return res.json({ success:true, data:{ route:['uzig', to.token.denom || to.token.symbol], pairs:[], price_native:null, price_usd:null, source:'direct_uzig' }});

      const priceNative = best.priceInZig;                // ZIG per 1 TO token
      const priceUsd    = priceNative * zigUsd;

      return res.json({
        success: true,
        data: {
          route: ['uzig', to.token.denom || to.token.symbol || String(to.token.token_id)],
          pairs: [{ poolId: best.poolId, pairContract: best.pairContract , pairType: best.pairType }],
          price_native: priceNative,
          price_usd: priceUsd,
          source: 'direct_uzig',
          diagnostics: { side: 'buy', rationale: 'min price_in_zig (tiebreak: highest TVL)', poolId: best.poolId }
        }
      });
    }

    if (from.type === 'token' && to.type === 'uzig') {
      // SELL Token for UZIG: want the highest ZIG per Token
      const best = await pickBestSellPool(from.token.token_id);
      if (!best) return res.json({ success:true, data:{ route:[from.token.denom || from.token.symbol, 'uzig'], pairs:[], price_native:null, price_usd:null, source:'direct_uzig' }});

      const priceNative = best.priceInZig;                // ZIG per 1 FROM token
      const priceUsd    = priceNative * zigUsd;

      return res.json({
        success: true,
        data: {
          route: [from.token.denom || from.token.symbol || String(from.token.token_id), 'uzig'],
          pairs: [{ poolId: best.poolId, pairContract: best.pairContract, pairType: best.pairType }],
          price_native: priceNative,
          price_usd: priceUsd,
          source: 'direct_uzig',
          diagnostics: { side: 'sell', rationale: 'max price_in_zig (tiebreak: highest TVL)', poolId: best.poolId }
        }
      });
    }

    // Token A <-> Token B (two hops via UZIG)
    if (from.type === 'token' && to.type === 'token') {
      // Hop 1: A -> UZIG using best SELL on A
      const sellA = await pickBestSellPool(from.token.token_id);
      // Hop 2: UZIG -> B using best BUY on B
      const buyB  = await pickBestBuyPool(to.token.token_id);

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

      // Cross-rate (TokenB per 1 TokenA)
      // sellA.priceInZig  = ZIG per 1 A (we receive this much ZIG per A)
      // buyB.priceInZig   = ZIG per 1 B (we pay this much ZIG per B)
      // B per 1 A = (ZIG per A) / (ZIG per B) = sellA / buyB
      const bPerA = sellA.priceInZig / buyB.priceInZig;

      // For convenience also compute ZIG per 1 A and USD per 1 A using best SELL leg
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
            { poolId: sellA.poolId, pairContract: sellA.pairContract }, // A->UZIG
            { poolId: buyB.poolId,  pairContract: buyB.pairContract  }  // UZIG->B
          ],
          // price_native here is B per 1 A (so the UI can show a direct A/B rate)
          price_native: bPerA,
          // If you prefer showing ZIG per A for cross, expose both:
          cross: {
            zig_per_from: zigPerA,
            usd_per_from: usdPerA
          },
          price_usd: null, // undefined for token-to-token; keep null unless you want to convert to USD via B's zig (needs extra step)
          source: 'via_uzig',
          diagnostics: {
            sell_leg: { side: 'sell', token_id: from.token.token_id, poolId: sellA.poolId, price_in_zig: sellA.priceInZig, pairType: sellA.pairType },
            buy_leg:  { side: 'buy',  token_id: to.token.token_id,   poolId: buyB.poolId,  price_in_zig: buyB.priceInZig, pairType: buyB.pairType },
            formula: 'B_per_A = price_sell_A / price_buy_B'
          }
        }
      });
    }

    // UZIG -> UZIG (no-op) or invalid combos
    return res.status(400).json({ success:false, error:'unsupported route (check from/to)' });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
