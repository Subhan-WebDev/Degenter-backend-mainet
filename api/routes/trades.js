// api/routes/trades.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { getZigUsd, resolveTokenId } from '../util/resolve-token.js';

const router = express.Router();

// -------- helpers ------------------------------------------------------------

function toNum(x) { return x == null ? null : Number(x); }

function classify(ref, unit) {
  if (ref == null) return null;
  // tweak thresholds here if you like
  const v = Number(ref);
  if (unit === 'zig') {
    if (v < 1_000)   return 'shrimp';
    if (v <= 10_000) return 'shark';
    return 'whale';
  } else {
    if (v < 1_000)   return 'shrimp';
    if (v <= 10_000) return 'shark';
    return 'whale';
  }
}

function dirFilterSQL(direction) {
  if (!direction) return '';
  const d = String(direction).toLowerCase();
  if (d === 'buy' || d === 'sell') return `AND t.direction='${d}'`;
  return '';
}

// For any denom, fetch exponent (fallback 6 for uzig/native)
function expForDenom(row, field, fallback = 6) {
  // when joined alias is present use it, else fallback
  const x = row[field];
  return x == null ? fallback : Number(x);
}

// Convert a trade row into values: scaled amounts, valueNative (zig), valueUsd
function shapeTradeRow(r, unit, zigUsd) {
  const qexp   = Number(r.qexp ?? 6);
  const offExp = expForDenom(r, 'offer_exp', r.offer_asset_denom === 'uzig' ? 6 : 6);
  const askExp = expForDenom(r, 'ask_exp',   r.ask_asset_denom   === 'uzig' ? 6 : 6);

  const offerDisp  = r.offer_amount_base != null ? Number(r.offer_amount_base)  / 10 ** offExp : null;
  const askDisp    = r.ask_amount_base   != null ? Number(r.ask_amount_base)    / 10 ** askExp : null;
  const returnDisp = r.return_amount_base!= null ? Number(r.return_amount_base) / 10 ** qexp   : null; // NOTE: return is always quote units on xy pools

  // Compute trade "value" in ZIG even if pool isn't a UZIG quote:
  // If pool is UZIG-quoted, value is already ZIG. Else multiply quote by its price_in_zig (pq_price_in_zig).
  let valueZig = null;
  if (r.is_uzig_quote) {
    if (r.direction === 'buy')  valueZig = r.offer_amount_base  != null ? Number(r.offer_amount_base)  / 10 ** qexp : null;
    else                        valueZig = r.return_amount_base != null ? Number(r.return_amount_base) / 10 ** qexp : null;
  } else {
    const qPrice = r.pq_price_in_zig != null ? Number(r.pq_price_in_zig) : null; // quote token -> ZIG
    if (qPrice != null) {
      if (r.direction === 'buy')  valueZig = (r.offer_amount_base  != null ? Number(r.offer_amount_base)  / 10 ** qexp : null) * qPrice;
      else                        valueZig = (r.return_amount_base != null ? Number(r.return_amount_base) / 10 ** qexp : null) * qPrice;
    }
  }

  const valueUsd = valueZig != null ? valueZig * zigUsd : null;
  const ref = unit === 'zig' ? valueZig : valueUsd;
  const kls = classify(ref, unit);

  return {
    time: r.created_at,
    txHash: r.tx_hash,
    pairContract: r.pair_contract,
    signer: r.signer,
    direction: r.direction,
    offerDenom: r.offer_asset_denom,
    offerAmountBase: r.offer_amount_base,
    offerAmount: offerDisp,      // scaled by exponent
    askDenom: r.ask_asset_denom,
    askAmountBase: r.ask_amount_base,
    askAmount: askDisp,          // scaled
    returnAmountBase: r.return_amount_base,
    returnAmount: returnDisp,    // scaled (quote side)
    valueNative: valueZig,
    valueUsd,
    class: kls
  };
}

// Common WHERE window builder: respects tf OR from/to/days
function windowWhere({ tf, from, to, days }) {
  if (from && to) {
    return { where: `t.created_at >= $X::timestamptz AND t.created_at < $Y::timestamptz`, mode: 'range' };
  }
  if (days) {
    const d = Math.max(1, Math.min(parseInt(days, 10) || 1, 60));
    return { where: `t.created_at >= now() - INTERVAL '${d} days'`, mode: 'rel' };
  }
  const mins = { '1h':60, '4h':240, '24h':1440, '7d':10080 }[String(tf || '24h').toLowerCase()] || 1440;
  return { where: `t.created_at >= now() - INTERVAL '${mins} minutes'`, mode: 'rel' };
}

// -------- routes ------------------------------------------------------------

/** GET /trades?tf=1h|4h|24h|7d&class=shrimp|shark|whale&unit=usd|zig&direction=buy|sell
 *  Supports: from, to, days, limit, offset
 */
router.get('/', async (req, res) => {
  try {
    const unit  = (req.query.unit || 'usd').toLowerCase();
    const limit = Math.min(Math.max(parseInt(req.query.limit || '500', 10), 1), 5000);
    const offset= Math.max(parseInt(req.query.offset || '0', 10), 0);
    const zigUsd= await getZigUsd();
    const { where, mode } = windowWhere({ tf: req.query.tf, from: req.query.from, to: req.query.to, days: req.query.days });
    const hasRange = mode === 'range';

    // params: when range, add from/to at the end
    const params = [];
    if (hasRange) {
      params.push(req.query.from);
      params.push(req.query.to);
    }

    const directionSQL = dirFilterSQL(req.query.direction);

    const sql = `
      WITH base AS (
        SELECT
          t.*,
          p.pair_contract, p.is_uzig_quote,
          qtk.exponent AS qexp,
          -- price of the QUOTE token in ZIG (for non-uzig pairs)
          (SELECT price_in_zig FROM prices WHERE token_id=p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
          -- exponents for offer/ask denoms
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp,
          COUNT(*) OVER() AS total
        FROM trades t
        JOIN pools p ON p.pool_id = t.pool_id
        JOIN tokens qtk ON qtk.token_id = p.quote_token_id
        LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
        LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
        WHERE t.action='swap'
          ${directionSQL}
          AND ${where.replace('$X', hasRange ? (params.length)     : '').replace('$Y', hasRange ? (params.length+1) : '')}
        ORDER BY t.created_at DESC
        LIMIT ${limit} OFFSET ${offset}
      )
      SELECT * FROM base;
    `;

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeTradeRow(r, unit, zigUsd));

    const klass = (req.query.class || '').toLowerCase();
    if (klass) data = data.filter(x => x.class === klass);

    const total = rows[0]?.total ? Number(rows[0].total) : data.length;
    res.json({ success:true, data, meta: { unit, tf: req.query.tf || '24h', limit, offset, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/token/:id?tf=…&class=…&unit=usd|zig&direction=buy|sell&limit=&offset= */
router.get('/token/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const unit  = (req.query.unit || 'usd').toLowerCase();
    const limit = Math.min(Math.max(parseInt(req.query.limit || '500', 10), 1), 5000);
    const offset= Math.max(parseInt(req.query.offset || '0', 10), 0);
    const zigUsd= await getZigUsd();
    const { where, mode } = windowWhere({ tf: req.query.tf, from: req.query.from, to: req.query.to, days: req.query.days });
    const hasRange = mode === 'range';
    const directionSQL = dirFilterSQL(req.query.direction);

    const params = [tok.token_id];
    if (hasRange) { params.push(req.query.from); params.push(req.query.to); }

    const sql = `
      WITH base AS (
        SELECT
          t.*,
          p.pair_contract, p.is_uzig_quote,
          qtk.exponent AS qexp,
          (SELECT price_in_zig FROM prices WHERE token_id=p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp,
          COUNT(*) OVER() AS total
        FROM trades t
        JOIN pools p  ON p.pool_id=t.pool_id
        JOIN tokens b ON b.token_id=p.base_token_id
        JOIN tokens qtk ON qtk.token_id=p.quote_token_id
        LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
        LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
        WHERE t.action='swap'
          AND b.token_id=$1
          ${directionSQL}
          AND ${hasRange ? `t.created_at >= $2::timestamptz AND t.created_at < $3::timestamptz` : where}
        ORDER BY t.created_at DESC
        LIMIT ${limit} OFFSET ${offset}
      )
      SELECT * FROM base;
    `;

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeTradeRow(r, unit, zigUsd));

    const klass = (req.query.class || '').toLowerCase();
    if (klass) data = data.filter(x => x.class === klass);

    const total = rows[0]?.total ? Number(rows[0].total) : data.length;
    res.json({ success:true, data, meta: { unit, tf: req.query.tf || '24h', limit, offset, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/pool/:ref   (ref = poolId or pair contract)
 *  same filters + pagination
 */
router.get('/pool/:ref', async (req, res) => {
  try {
    const ref = req.params.ref;
    const unit  = (req.query.unit || 'usd').toLowerCase();
    const limit = Math.min(Math.max(parseInt(req.query.limit || '500', 10), 1), 5000);
    const offset= Math.max(parseInt(req.query.offset || '0', 10), 0);
    const zigUsd= await getZigUsd();
    const { where, mode } = windowWhere({ tf: req.query.tf, from: req.query.from, to: req.query.to, days: req.query.days });
    const hasRange = mode === 'range';
    const directionSQL = dirFilterSQL(req.query.direction);

    // Resolve pool by pair_contract or numeric id
    const poolRow = await DB.query(`
      SELECT pool_id, pair_contract FROM pools
      WHERE pair_contract=$1 OR pool_id::text=$1
      LIMIT 1
    `, [ref]);
    if (!poolRow.rows.length) return res.status(404).json({ success:false, error:'pool not found' });

    const poolId = poolRow.rows[0].pool_id;

    const params = [poolId];
    if (hasRange) { params.push(req.query.from); params.push(req.query.to); }

    const sql = `
      WITH base AS (
        SELECT
          t.*,
          p.pair_contract, p.is_uzig_quote,
          qtk.exponent AS qexp,
          (SELECT price_in_zig FROM prices WHERE token_id=p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp,
          COUNT(*) OVER() AS total
        FROM trades t
        JOIN pools p ON p.pool_id=t.pool_id
        JOIN tokens qtk ON qtk.token_id=p.quote_token_id
        LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
        LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
        WHERE t.action='swap'
          AND p.pool_id=$1
          ${directionSQL}
          AND ${hasRange ? `t.created_at >= $2::timestamptz AND t.created_at < $3::timestamptz` : where}
        ORDER BY t.created_at DESC
        LIMIT ${limit} OFFSET ${offset}
      )
      SELECT * FROM base;
    `;

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeTradeRow(r, unit, zigUsd));

    const klass = (req.query.class || '').toLowerCase();
    if (klass) data = data.filter(x => x.class === klass);

    const total = rows[0]?.total ? Number(rows[0].total) : data.length;
    res.json({ success:true, data, meta: { unit, tf: req.query.tf || '24h', limit, offset, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/wallet/:address?tf|days|from&to&unit&tokenId=&poolId=&pair=&direction=&limit=&offset= */
router.get('/wallet/:address', async (req, res) => {
  try {
    const addr  = req.params.address;
    const unit  = (req.query.unit || 'usd').toLowerCase();
    const limit = Math.min(Math.max(parseInt(req.query.limit || '1000', 10), 1), 5000);
    const offset= Math.max(parseInt(req.query.offset || '0', 10), 0);
    const zigUsd= await getZigUsd();
    const { where, mode } = windowWhere({ tf: req.query.tf, from: req.query.from, to: req.query.to, days: req.query.days });
    const hasRange = mode === 'range';
    const directionSQL = dirFilterSQL(req.query.direction);

    let tokenClause = '';
    const params = [addr];

    if (req.query.tokenId) {
      const tok = await resolveTokenId(req.query.tokenId);
      if (tok) {
        tokenClause = 'AND b.token_id=$Z';
        params.push(tok.token_id);
      }
    }

    if (req.query.pair) {
      tokenClause += (tokenClause ? ' ' : '') + 'AND p.pair_contract=$P';
      params.push(req.query.pair);
    } else if (req.query.poolId) {
      tokenClause += (tokenClause ? ' ' : '') + 'AND p.pool_id=$P';
      params.push(req.query.poolId);
    }

    if (hasRange) { params.push(req.query.from); params.push(req.query.to); }

    const sql = `
      WITH base AS (
        SELECT
          t.*,
          p.pair_contract, p.is_uzig_quote,
          qtk.exponent AS qexp,
          (SELECT price_in_zig FROM prices WHERE token_id=p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp,
          COUNT(*) OVER() AS total
        FROM trades t
        JOIN pools p  ON p.pool_id=t.pool_id
        JOIN tokens b ON b.token_id=p.base_token_id
        JOIN tokens qtk ON qtk.token_id=p.quote_token_id
        LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
        LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
        WHERE t.action='swap'
          AND t.signer=$1
          ${directionSQL}
          ${tokenClause.replace('$Z', String(params.length - (hasRange ? 2 : 0))).replace(/\$P/g, String(params.length - (hasRange ? 2 : 0) + 1))}
          AND ${hasRange ? `t.created_at >= $${params.length-1}::timestamptz AND t.created_at < $${params.length}::timestamptz` : where}
        ORDER BY t.created_at DESC
        LIMIT ${limit} OFFSET ${offset}
      )
      SELECT * FROM base;
    `;

    const { rows } = await DB.query(sql, params);
    const data = rows.map(r => shapeTradeRow(r, unit, zigUsd));

    const total = rows[0]?.total ? Number(rows[0].total) : data.length;
    res.json({ success:true, data, meta:{ unit, tf: req.query.tf || '1d', limit, offset, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/large?bucket=30m|1h|4h|24h&unit=zig|usd&minValue=&maxValue=  (deduped) */
router.get('/large', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const unit   = (req.query.unit || 'zig').toLowerCase();
    const minV   = req.query.minValue != null ? Number(req.query.minValue) : null;
    const maxV   = req.query.maxValue != null ? Number(req.query.maxValue) : null;
    const zigUsd = await getZigUsd();

    const { rows } = await DB.query(`
      SELECT DISTINCT ON (tx_hash, pool_id, direction)
             lt.pool_id, lt.tx_hash, lt.signer, lt.direction, lt.value_zig, lt.created_at,
             p.pair_contract
      FROM large_trades lt
      JOIN pools p ON p.pool_id=lt.pool_id
      WHERE lt.bucket=$1
      ORDER BY lt.tx_hash, lt.pool_id, lt.direction, lt.created_at DESC
      LIMIT 1000
    `, [bucket]);

    let data = rows.map(r => ({
      pairContract: r.pair_contract,
      txHash: r.tx_hash,
      signer: r.signer,
      direction: r.direction,
      valueNative: Number(r.value_zig),
      valueUsd: Number(r.value_zig) * zigUsd,
      createdAt: r.created_at
    }));

    if (minV != null) data = data.filter(x => (unit === 'usd' ? x.valueUsd : x.valueNative) >= minV);
    if (maxV != null) data = data.filter(x => (unit === 'usd' ? x.valueUsd : x.valueNative) <= maxV);

    res.json({ success:true, data, meta: { bucket, unit, minValue: minV ?? undefined, maxValue: maxV ?? undefined } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
