// api/routes/trades.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { getZigUsd, resolveTokenId } from '../util/resolve-token.js';

const router = express.Router();

/* ---------------- helpers ---------------- */

const DIRS = new Set(['buy','sell','provide','withdraw']);

function normDir(d) {
  const x = String(d || '').toLowerCase();
  return DIRS.has(x) ? x : null;
}

function classify(v, unit) {
  if (v == null) return null;
  const x = Number(v);
  if (unit === 'zig') {
    if (x < 1_000) return 'shrimp';
    if (x <= 10_000) return 'shark';
    return 'whale';
  } else {
    if (x < 1_000) return 'shrimp';
    if (x <= 10_000) return 'shark';
    return 'whale';
  }
}

function windowWhere({ tf, from, to, days }) {
  if (from && to) return { where: `t.created_at >= $X::timestamptz AND t.created_at < $Y::timestamptz`, mode: 'range' };
  if (days) return { where: `t.created_at >= now() - ($D || ' days')::interval`, mode: 'days' };
  const mins = { '1h':60, '4h':240, '24h':1440, '7d':10080 }[String(tf||'24h').toLowerCase()] ?? 1440;
  return { where: `t.created_at >= now() - INTERVAL '${mins} minutes'`, mode: 'rel' };
}

// scale base amount using exponent; uzig => 6 by default
function scale(base, exp, fallback = 6) {
  if (base == null) return null;
  const e = (exp == null ? fallback : Number(exp));
  return Number(base) / 10 ** e;
}

/** Build SELECT … FROM … WHERE … common block.
 *  includeLiquidity=1 will include provide/withdraw otherwise only swaps.
 *  direction: buy|sell|provide|withdraw (optional)
 *  target: "all" | "token" | "pool" | "wallet"
 */
function makeSQL({ where, hasRangeOrDays, actionFilter, direction, extraJoins, extraWheres, limit, offset }) {
  // NOTE: we fetch quote exponent, and the quote->zig price for non-uzig pairs
  // also exponents for offer/ask denoms (so amounts are scaled correctly)
  const dirSQL = direction ? `AND t.direction=$DIR` : '';
  const actSQL = actionFilter === 'all' ? `AND t.action IN ('swap','provide','withdraw')`
                                        : `AND t.action='swap'`;

  const timeSQL = hasRangeOrDays === 'range'
    ? `AND t.created_at >= $F::timestamptz AND t.created_at < $T::timestamptz`
    : (hasRangeOrDays === 'days'
        ? `AND t.created_at >= now() - ($D || ' days')::interval`
        : where);

  return `
    WITH base AS (
      SELECT
        t.*,
        p.pair_contract,
        p.is_uzig_quote,
        q.exponent AS qexp,
        -- latest price of QUOTE token in ZIG (needed when pair is NOT uzig-quoted)
        (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
        toff.exponent AS offer_exp,
        task.exponent AS ask_exp,
        COUNT(*) OVER() AS total
      FROM trades t
      JOIN pools p   ON p.pool_id=t.pool_id
      JOIN tokens q  ON q.token_id=p.quote_token_id
      LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
      LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
      ${extraJoins || ''}
      WHERE ${actSQL} ${dirSQL}
        ${extraWheres || ''}
        ${timeSQL}
      ORDER BY t.created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    )
    SELECT * FROM base
  `;
}

function shapeRow(r, unit, zigUsd) {
  const offerScaled  = scale(r.offer_amount_base,  r.offer_exp,  r.offer_asset_denom === 'uzig' ? 6 : 6);
  const askScaled    = scale(r.ask_amount_base,    r.ask_exp,    r.ask_asset_denom   === 'uzig' ? 6 : 6);
  const returnScaled = scale(r.return_amount_base, r.qexp, 6); // return is on quote side in xyk

  // Value in ZIG (quote side); convert if quote != uzig
  let valueZig = null;
  if (r.is_uzig_quote) {
    // swaps: buy => offer is quote; sell => return is quote
    if (r.direction === 'buy')      valueZig = scale(r.offer_amount_base,  r.qexp, 6);
    else if (r.direction === 'sell')valueZig = scale(r.return_amount_base, r.qexp, 6);
    else { // provide/withdraw: pick whichever leg is quote
      // prefer non-null quote-side amount
      valueZig = scale(
        (r.offer_asset_denom === 'uzig' ? r.offer_amount_base :
         r.ask_asset_denom   === 'uzig' ? r.ask_amount_base   :
                                           r.return_amount_base),
        r.qexp, 6
      );
    }
  } else {
    const qPrice = r.pq_price_in_zig != null ? Number(r.pq_price_in_zig) : null;
    if (qPrice != null) {
      const quoteAmt =
        r.direction === 'buy'
          ? scale(r.offer_amount_base,  r.qexp, 6)
          : (r.direction === 'sell'
              ? scale(r.return_amount_base, r.qexp, 6)
              : scale(
                  (r.offer_asset_denom === r.ask_asset_denom  // try offer/ask first
                    ? r.offer_amount_base
                    : (r.offer_asset_denom === r.ask_asset_denom ? r.ask_amount_base
                      : (r.return_amount_base))), r.qexp, 6));
      valueZig = quoteAmt != null ? quoteAmt * qPrice : null;
    }
  }

  const valueUsd = valueZig != null ? valueZig * zigUsd : null;
  const klass = classify(unit === 'usd' ? valueUsd : valueZig, unit);

  return {
    time: r.created_at,
    txHash: r.tx_hash,
    pairContract: r.pair_contract,
    signer: r.signer,
    direction: r.direction,
    offerDenom: r.offer_asset_denom,
    offerAmountBase: r.offer_amount_base,
    offerAmount: offerScaled,              // scaled
    askDenom: r.ask_asset_denom,
    askAmountBase: r.ask_amount_base,
    askAmount: askScaled,                  // scaled
    returnAmountBase: r.return_amount_base,
    returnAmount: returnScaled,            // scaled (quote)
    valueNative: valueZig,
    valueUsd,
    class: klass
  };
}

/* ---------------- routes ---------------- */

/** GET /trades
 *  Query:
 *   tf=1h|4h|24h|7d  OR  from=ISO&to=ISO  OR days=1..60
 *   unit=usd|zig
 *   class=shrimp|shark|whale
 *   direction=buy|sell|provide|withdraw
 *   includeLiquidity=1   (include provide/withdraw)
 *   limit, offset
 */
router.get('/', async (req, res) => {
  try {
    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const limit  = Math.min(Math.max(parseInt(req.query.limit || '500',10),1), 5000);
    const offset = Math.max(parseInt(req.query.offset || '0',10), 0);
    const dir    = normDir(req.query.direction);
    const actionFilter = req.query.includeLiquidity === '1' ? 'all' : 'swaps';

    const zigUsd = await getZigUsd();
    const { where, mode } = windowWhere({ tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days });

    const sql = makeSQL({
      where,
      hasRangeOrDays: mode === 'range' ? 'range' : (mode === 'days' ? 'days' : 'rel'),
      actionFilter,
      direction: dir,
      limit, offset
    }).replace('$DIR', dir ? `'${dir}'` : 'NULL')
      .replace('$F', dir ? '$3' : '$2')
      .replace('$T', dir ? '$4' : '$3')
      .replace('$D', dir ? '$2' : '$1');

    const params = [];
    if (mode === 'range') {
      if (dir) params.push(dir);
      params.push(req.query.from, req.query.to);
    } else if (mode === 'days') {
      if (dir) params.push(dir);
      params.push(String(Math.min(60, Math.max(1, parseInt(req.query.days||'1',10)))));
    } else {
      if (dir) params.push(dir);
    }

    const { rows } = await DB.query(sql, params);
    const data = rows.map(r => shapeRow(r, unit, zigUsd));
    const total = rows[0]?.total ? Number(rows[0].total) : data.length;
    res.json({ success:true, data, meta:{ unit, tf:req.query.tf||'24h', limit, offset, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/token/:id  (same filters + pagination) */
router.get('/token/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const limit  = Math.min(Math.max(parseInt(req.query.limit || '500',10),1), 5000);
    const offset = Math.max(parseInt(req.query.offset || '0',10), 0);
    const dir    = normDir(req.query.direction);
    const actionFilter = req.query.includeLiquidity === '1' ? 'all' : 'swaps';

    const zigUsd = await getZigUsd();
    const { where, mode } = windowWhere({ tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days });

    const extraJoins  = `JOIN tokens b ON b.token_id=p.base_token_id`;
    const extraWheres = `AND b.token_id=$B`;

    const sql = makeSQL({
      where,
      hasRangeOrDays: mode === 'range' ? 'range' : (mode === 'days' ? 'days' : 'rel'),
      actionFilter,
      direction: dir,
      extraJoins, extraWheres,
      limit, offset
    })
      .replace('$DIR', dir ? `'${dir}'` : 'NULL')
      .replace('$B', dir ? '$3' : '$2')
      .replace('$F', dir ? '$4' : '$3')
      .replace('$T', dir ? '$5' : '$4')
      .replace('$D', dir ? '$3' : '$2');

    const params = [tok.token_id];
    if (dir) params.unshift(dir); // direction first if present
    if (mode === 'range') { params.push(req.query.from, req.query.to); }
    if (mode === 'days')  { params.push(String(Math.min(60, Math.max(1, parseInt(req.query.days||'1',10))))); }

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeRow(r, unit, zigUsd));

    const klass = String(req.query.class || '').toLowerCase();
    if (klass) data = data.filter(x => x.class === klass);

    const total = rows[0]?.total ? Number(rows[0].total) : data.length;
    res.json({ success:true, data, meta:{ unit, tf:req.query.tf||'24h', limit, offset, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/pool/:ref  (ref = pool_id or pair contract) */
router.get('/pool/:ref', async (req, res) => {
  try {
    const ref = req.params.ref;

    const pool = await DB.query(
      `SELECT pool_id, pair_contract FROM pools WHERE pair_contract=$1 OR pool_id::text=$1 LIMIT 1`,
      [ref]
    );
    if (!pool.rows.length) return res.status(404).json({ success:false, error:'pool not found' });

    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const limit  = Math.min(Math.max(parseInt(req.query.limit || '500',10),1), 5000);
    const offset = Math.max(parseInt(req.query.offset || '0',10), 0);
    const dir    = normDir(req.query.direction);
    const actionFilter = req.query.includeLiquidity === '1' ? 'all' : 'swaps';

    const zigUsd = await getZigUsd();
    const { where, mode } = windowWhere({ tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days });

    const extraWheres = `AND p.pool_id=$PID`;

    const sql = makeSQL({
      where,
      hasRangeOrDays: mode === 'range' ? 'range' : (mode === 'days' ? 'days' : 'rel'),
      actionFilter,
      direction: dir,
      extraWheres,
      limit, offset
    })
      .replace('$DIR', dir ? `'${dir}'` : 'NULL')
      .replace('$PID', dir ? '$3' : '$2')
      .replace('$F', dir ? '$4' : '$3')
      .replace('$T', dir ? '$5' : '$4')
      .replace('$D', dir ? '$3' : '$2');

    const params = [pool.rows[0].pool_id];
    if (dir) params.unshift(dir);
    if (mode === 'range') { params.push(req.query.from, req.query.to); }
    if (mode === 'days')  { params.push(String(Math.min(60, Math.max(1, parseInt(req.query.days||'1',10))))); }

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeRow(r, unit, zigUsd));

    const klass = String(req.query.class || '').toLowerCase();
    if (klass) data = data.filter(x => x.class === klass);

    const total = rows[0]?.total ? Number(rows[0].total) : data.length;
    res.json({ success:true, data, meta:{ unit, tf:req.query.tf||'24h', limit, offset, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/wallet/:address  (filters + tokenId/pair/poolId) */
router.get('/wallet/:address', async (req, res) => {
  try {
    const addr = req.params.address;

    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const limit  = Math.min(Math.max(parseInt(req.query.limit || '1000',10),1), 5000);
    const offset = Math.max(parseInt(req.query.offset || '0',10), 0);
    const dir    = normDir(req.query.direction);
    const actionFilter = req.query.includeLiquidity === '1' ? 'all' : 'swaps';

    const zigUsd = await getZigUsd();
    const { where, mode } = windowWhere({ tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days });

    let extraJoins = `JOIN tokens b ON b.token_id=p.base_token_id`;
    let extraWheres = `AND t.signer=$WAL`;
    const params = [addr];

    if (req.query.tokenId) {
      const tok = await resolveTokenId(req.query.tokenId);
      if (tok) {
        extraWheres += ` AND b.token_id=$TOK`;
        params.push(tok.token_id);
      }
    }

    if (req.query.pair) {
      extraWheres += ` AND p.pair_contract=$PAIR`;
      params.push(req.query.pair);
    } else if (req.query.poolId) {
      extraWheres += ` AND p.pool_id=$PID`;
      params.push(req.query.poolId);
    }

    const sql = makeSQL({
      where,
      hasRangeOrDays: mode === 'range' ? 'range' : (mode === 'days' ? 'days' : 'rel'),
      actionFilter,
      direction: dir,
      extraJoins, extraWheres,
      limit, offset
    })
      .replace('$DIR', dir ? `'${dir}'` : 'NULL')
      .replace('$WAL', '$1')
      .replace('$TOK', params.length >= 2 ? `$${params.indexOf(params.find((_,i)=>i===1))+1}` : '$2')
      .replace('$PAIR', `$${params.length - (mode==='range'?2:mode==='days'?1:0)}`)
      .replace('$PID',  `$${params.length - (mode==='range'?2:mode==='days'?1:0)}`)
      .replace('$F',    `$${params.length+1}`)
      .replace('$T',    `$${params.length+2}`)
      .replace('$D',    `$${params.length+1}`);

    if (mode === 'range') { params.push(req.query.from, req.query.to); }
    if (mode === 'days')  { params.push(String(Math.min(60, Math.max(1, parseInt(req.query.days||'1',10))))); }

    const { rows } = await DB.query(sql, params);
    const data = rows.map(r => shapeRow(r, unit, zigUsd));
    const total = rows[0]?.total ? Number(rows[0].total) : data.length;
    res.json({ success:true, data, meta:{ unit, tf:req.query.tf||'1d', limit, offset, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/large?bucket=30m|1h|4h|24h&unit=zig|usd&minValue=&maxValue= */
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
      LIMIT 2000
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

    res.json({ success:true, data, meta:{ bucket, unit, minValue:minV ?? undefined, maxValue:maxV ?? undefined } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
