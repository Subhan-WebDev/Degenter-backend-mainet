// api/routes/trades.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { getZigUsd, resolveTokenId } from '../util/resolve-token.js';

const router = express.Router();

/* ---------------- helpers ---------------- */

const VALID_DIR = new Set(['buy','sell','provide','withdraw']);

function normDir(d) {
  const x = String(d || '').toLowerCase();
  return VALID_DIR.has(x) ? x : null;
}

function clampInt(v, { min = 0, max = 1e9, def = 0 } = {}) {
  const n = Number.parseInt(v, 10);
  if (Number.isNaN(n)) return def;
  return Math.max(min, Math.min(max, n));
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

// scale base amount using exponent; uzig => 6 by default
function scale(base, exp, fallback = 6) {
  if (base == null) return null;
  const e = (exp == null ? fallback : Number(exp));
  return Number(base) / 10 ** e;
}

/** Expanded TF map */
function minutesForTf(tf) {
  const m = String(tf || '').toLowerCase();
  const map = {
    '30m': 30,
    '1h' : 60,
    '2h' : 120,
    '4h' : 240,
    '8h' : 480,
    '12h': 720,
    '24h': 1440,
    '1d' : 1440,
    '3d' : 4320,
    '5d' : 7200,
    '7d' : 10080
  };
  return map[m] || 1440; // default 24h
}

/** Build time window clause + params */
function buildWindow({ tf, from, to, days }, params) {
  const clauses = [];
  if (from && to) {
    clauses.push(`t.created_at >= $${params.length + 1}::timestamptz`);
    params.push(from);
    clauses.push(`t.created_at < $${params.length + 1}::timestamptz`);
    params.push(to);
    return { clause: clauses.join(' AND ') };
  }
  if (days) {
    const d = clampInt(days, { min: 1, max: 60, def: 1 });
    clauses.push(`t.created_at >= now() - ($${params.length + 1} || ' days')::interval`);
    params.push(String(d));
    return { clause: clauses.join(' AND ') };
  }
  // tf fallback
  const mins = minutesForTf(tf);
  clauses.push(`t.created_at >= now() - INTERVAL '${mins} minutes'`);
  return { clause: clauses.join(' AND ') };
}

/** Shared SELECT block — conditions and params are appended safely */
function buildTradesQuery({
  scope,            // 'all' | 'token' | 'pool' | 'wallet'
  scopeValue,       // token_id, {poolId}|{pairContract}, wallet
  includeLiquidity, // boolean
  direction,        // 'buy' | 'sell' | 'provide' | 'withdraw' | null
  windowOpts,       // {tf, from, to, days}
  limit, offset
}) {
  const params = [];
  const where = [];

  // action filter
  if (includeLiquidity) where.push(`t.action IN ('swap','provide','withdraw')`);
  else where.push(`t.action = 'swap'`);

  // direction filter
  if (direction) {
    where.push(`t.direction = $${params.length + 1}`);
    params.push(direction);
  }

  // scope filter (we always join base token `b` below)
  if (scope === 'token') {
    where.push(`b.token_id = $${params.length + 1}`);
    params.push(scopeValue); // token_id (BIGINT)
  } else if (scope === 'wallet') {
    where.push(`t.signer = $${params.length + 1}`);
    params.push(scopeValue); // address
  } else if (scope === 'pool') {
    if (scopeValue.poolId) {
      where.push(`p.pool_id = $${params.length + 1}`);
      params.push(scopeValue.poolId);
    } else if (scopeValue.pairContract) {
      where.push(`p.pair_contract = $${params.length + 1}`);
      params.push(scopeValue.pairContract);
    }
  }

  // window
  const { clause } = buildWindow(windowOpts, params);
  where.push(clause);

  const sql = `
    WITH base AS (
      SELECT
        t.*,
        p.pair_contract,
        p.is_uzig_quote,

        -- quote token (right side of pair)
        q.exponent AS qexp,

        -- base token (left side of pair) — used to compute price per BASE
        b.exponent AS bexp,
        b.denom    AS base_denom,

        -- latest price of QUOTE token in ZIG (when quote != uzig)
        (SELECT price_in_zig
           FROM prices
          WHERE token_id = p.quote_token_id
          ORDER BY updated_at DESC
          LIMIT 1) AS pq_price_in_zig,

        -- exponents of the trade legs
        toff.exponent AS offer_exp,
        task.exponent AS ask_exp,

        COUNT(*) OVER() AS total
      FROM trades t
      JOIN pools  p ON p.pool_id = t.pool_id
      JOIN tokens q ON q.token_id = p.quote_token_id
      JOIN tokens b ON b.token_id = p.base_token_id
      LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
      LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
      WHERE ${where.join(' AND ')}
      ORDER BY t.created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    )
    SELECT * FROM base
  `;

  return { sql, params };
}

/** shape a trade row + compute value + per-trade price (ZIG per BASE) */
function shapeRow(r, unit, zigUsd) {
  // scaled legs for response
  const offerScaled = scale(
    r.offer_amount_base,
    (r.offer_asset_denom === 'uzig') ? 6 : (r.offer_exp ?? 6),
    6
  );

  const askScaled = scale(
    r.ask_amount_base,
    (r.ask_asset_denom === 'uzig') ? 6 : (r.ask_exp ?? 6),
    6
  );

  // Two interpretations of return_amount_base
  const returnAsQuote = scale(r.return_amount_base, r.qexp ?? 6, 6); // quote units
  const returnAsBase  = scale(r.return_amount_base, r.bexp ?? 6, 6); // base units

  // ------------------------------
  // Notional value in ZIG (quote)
  // ------------------------------
  let valueZig = null;
  if (r.is_uzig_quote) {
    if (r.direction === 'buy') {
      valueZig = scale(r.offer_amount_base, r.qexp ?? 6, 6);  // paid quote
    } else if (r.direction === 'sell') {
      valueZig = scale(r.return_amount_base, r.qexp ?? 6, 6); // received quote
    } else {
      valueZig = (r.offer_asset_denom === 'uzig')
        ? scale(r.offer_amount_base, r.qexp ?? 6, 6)
        : (r.ask_asset_denom === 'uzig')
          ? scale(r.ask_amount_base, r.qexp ?? 6, 6)
          : returnAsQuote;
    }
  } else {
    const qPrice = r.pq_price_in_zig != null ? Number(r.pq_price_in_zig) : null;
    if (qPrice != null) {
      const quoteAmt =
        (r.direction === 'buy')
          ? scale(r.offer_amount_base,  r.qexp ?? 6, 6)   // paid quote
          : scale(r.return_amount_base, r.qexp ?? 6, 6);  // received quote
      valueZig = quoteAmt != null ? quoteAmt * qPrice : null;
    }
  }
  const valueUsd = valueZig != null ? valueZig * zigUsd : null;

  // ----------------------------------------
  // Execution PRICE: ZIG per 1 BASE token
  // ----------------------------------------
  // base amount:
  const baseAmt = (r.direction === 'buy')
    ? returnAsBase                             // received base
    : (r.direction === 'sell')
      ? scale(r.offer_amount_base, r.bexp ?? 6, 6) // offered base
      : null;

  // quote amount in ZIG:
  let quoteAmtZig = null;
  if (r.is_uzig_quote) {
    quoteAmtZig = (r.direction === 'buy')
      ? scale(r.offer_amount_base,  r.qexp ?? 6, 6)   // paid quote
      : (r.direction === 'sell')
        ? scale(r.return_amount_base, r.qexp ?? 6, 6) // received quote
        : null;
  } else {
    const qPrice = r.pq_price_in_zig != null ? Number(r.pq_price_in_zig) : null;
    if (qPrice != null) {
      const rawQuote = (r.direction === 'buy')
        ? scale(r.offer_amount_base,  r.qexp ?? 6, 6)
        : (r.direction === 'sell')
          ? scale(r.return_amount_base, r.qexp ?? 6, 6)
          : null;
      if (rawQuote != null) quoteAmtZig = rawQuote * qPrice;
    }
  }

  const priceNative = (quoteAmtZig != null && baseAmt != null && baseAmt !== 0)
    ? (quoteAmtZig / baseAmt)   // ZIG per 1 BASE
    : null;
  const priceUsd = priceNative != null ? priceNative * zigUsd : null;

  const klass = classify(unit === 'usd' ? valueUsd : valueZig, unit);

  return {
    time: r.created_at,
    txHash: r.tx_hash,
    pairContract: r.pair_contract,
    signer: r.signer,
    direction: r.direction,

    offerDenom: r.offer_asset_denom,
    offerAmountBase: r.offer_amount_base,
    offerAmount: offerScaled,

    askDenom: r.ask_asset_denom,
    askAmountBase: r.ask_amount_base,
    askAmount: askScaled,

    returnAmountBase: r.return_amount_base,
    // For BUY this is base; for SELL this is quote
    returnAmount: (r.direction === 'buy') ? returnAsBase : returnAsQuote,

    // per-trade execution price
    priceNative,     // ZIG per 1 BASE
    priceUsd,        // USD per 1 BASE

    // trade notional
    valueNative: valueZig,
    valueUsd,

    class: klass
  };
}

/* ---------------- routes ---------------- */

/** GET /trades
 *  tf=30m|1h|2h|4h|8h|12h|24h|1d|3d|5d|7d OR from&to OR days=1..60
 *  unit=usd|zig
 *  class=shrimp|shark|whale
 *  direction=buy|sell|provide|withdraw
 *  includeLiquidity=1
 *  limit, offset
 */
router.get('/', async (req, res) => {
  try {
    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const limit  = clampInt(req.query.limit,  { min:1, max:5000, def:500 });
    const offset = clampInt(req.query.offset, { min:0, max:1e9,  def:0   });
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';

    const zigUsd = await getZigUsd();

    const { sql, params } = buildTradesQuery({
      scope: 'all',
      includeLiquidity,
      direction: dir,
      windowOpts: { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days },
      limit, offset
    });

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

/** GET /trades/token/:id */
router.get('/token/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const limit  = clampInt(req.query.limit,  { min:1, max:5000, def:500 });
    const offset = clampInt(req.query.offset, { min:0, max:1e9,  def:0   });
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';

    const zigUsd = await getZigUsd();

    const { sql, params } = buildTradesQuery({
      scope: 'token',
      scopeValue: tok.token_id,  // BIGINT
      includeLiquidity,
      direction: dir,
      windowOpts: { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days },
      limit, offset
    });

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
    const row = await DB.query(
      `SELECT pool_id, pair_contract FROM pools WHERE pair_contract=$1 OR pool_id::text=$1 LIMIT 1`,
      [ref]
    );
    if (!row.rows.length) return res.status(404).json({ success:false, error:'pool not found' });
    const poolId = row.rows[0].pool_id;

    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const limit  = clampInt(req.query.limit,  { min:1, max:5000, def:500 });
    const offset = clampInt(req.query.offset, { min:0, max:1e9,  def:0   });
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';

    const zigUsd = await getZigUsd();

    const { sql, params } = buildTradesQuery({
      scope: 'pool',
      scopeValue: { poolId },
      includeLiquidity,
      direction: dir,
      windowOpts: { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days },
      limit, offset
    });

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

/** GET /trades/wallet/:address
 *  Window: tf=30m|1h|2h|4h|8h|12h|24h|1d|3d|5d|7d OR from&to OR days=1..60
 *  unit=usd|zig
 *  direction=buy|sell|provide|withdraw
 *  includeLiquidity=1
 *  Optional scope: tokenId=<id|symbol|denom> OR pair=<pair_contract> OR poolId=<id>
 *  Pagination: limit (<=5000), offset
 */
router.get('/wallet/:address', async (req, res) => {
  try {
    const address = req.params.address;
    const unit    = String(req.query.unit || 'usd').toLowerCase();
    const limit   = clampInt(req.query.limit,  { min:1, max:5000, def:1000 });
    const offset  = clampInt(req.query.offset, { min:0, max:1e9,  def:0    });
    const dir     = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';

    const zigUsd = await getZigUsd();

    // WHERE + params (no duplicate joins)
    const where = [];
    const params = [];

    where.push(`t.signer = $${params.length + 1}`);
    params.push(address);

    if (includeLiquidity) where.push(`t.action IN ('swap','provide','withdraw')`);
    else where.push(`t.action = 'swap'`);

    if (dir) {
      where.push(`t.direction = $${params.length + 1}`);
      params.push(dir);
    }

    const { clause: timeClause } = buildWindow(
      { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days },
      params
    );
    where.push(timeClause);

    // optional scoping by token / pair / pool (use alias b already present in SELECT)
    if (req.query.tokenId) {
      const tok = await resolveTokenId(req.query.tokenId);
      if (tok) {
        where.push(`b.token_id = $${params.length + 1}`);
        params.push(tok.token_id);
      }
    }

    if (req.query.pair) {
      where.push(`p.pair_contract = $${params.length + 1}`);
      params.push(String(req.query.pair));
    } else if (req.query.poolId) {
      where.push(`p.pool_id = $${params.length + 1}`);
      params.push(String(req.query.poolId));
    }

    const sql = `
      WITH base AS (
        SELECT
          t.*,
          p.pair_contract,
          p.is_uzig_quote,
          q.exponent AS qexp,
          b.exponent AS bexp,
          b.denom    AS base_denom,
          (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
          toff.exponent AS offer_exp,
          task.exponent AS ask_exp,
          COUNT(*) OVER() AS total
        FROM trades t
        JOIN pools  p ON p.pool_id = t.pool_id
        JOIN tokens q ON q.token_id = p.quote_token_id
        JOIN tokens b ON b.token_id = p.base_token_id
        LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
        LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
        WHERE ${where.join(' AND ')}
        ORDER BY t.created_at DESC
        LIMIT ${limit} OFFSET ${offset}
      )
      SELECT * FROM base
    `;

    const { rows } = await DB.query(sql, params);
    const data = rows.map(r => shapeRow(r, unit, zigUsd));
    const total = rows[0]?.total ? Number(rows[0].total) : data.length;

    res.json({ success:true, data, meta:{ unit, tf:req.query.tf || '24h', limit, offset, total } });
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

/** GET /trades/recent
 *  Dashboard feed.
 *  Filters:
 *    - tokenId=<id|symbol|denom>  (optional; across all its pools)
 *    - pair=<pair_contract>       (optional)
 *    - poolId=<id>                (optional)
 *    - direction=buy|sell|provide|withdraw (optional)
 *    - includeLiquidity=1
 *    - unit=usd|zig
 *    - minValue, maxValue
 *    - tf=30m|1h|2h|4h|8h|12h|24h|1d|3d|5d|7d OR from&to OR days=1..60
 *    - limit (<=2000, default 200), offset
 */
router.get('/recent', async (req, res) => {
  try {
    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const limit  = clampInt(req.query.limit,  { min:1, max:2000, def:200 });
    const offset = clampInt(req.query.offset, { min:0, max:1e9,  def:0   });
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const minV   = req.query.minValue != null ? Number(req.query.minValue) : null;
    const maxV   = req.query.maxValue != null ? Number(req.query.maxValue) : null;

    const zigUsd = await getZigUsd();

    const where = [];
    const params = [];

    if (includeLiquidity) where.push(`t.action IN ('swap','provide','withdraw')`);
    else where.push(`t.action = 'swap'`);

    if (dir) {
      where.push(`t.direction = $${params.length + 1}`);
      params.push(dir);
    }

    const { clause: timeClause } = buildWindow(
      { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days },
      params
    );
    where.push(timeClause);

    // optional scope: token / pair / pool (use alias b already present in SELECT)
    if (req.query.tokenId) {
      const tok = await resolveTokenId(req.query.tokenId);
      if (tok) {
        where.push(`b.token_id = $${params.length + 1}`);
        params.push(tok.token_id);
      }
    }
    if (req.query.pair) {
      where.push(`p.pair_contract = $${params.length + 1}`);
      params.push(String(req.query.pair));
    } else if (req.query.poolId) {
      where.push(`p.pool_id = $${params.length + 1}`);
      params.push(String(req.query.poolId));
    }

    const sql = `
      SELECT
        t.*,
        p.pair_contract,
        p.is_uzig_quote,
        q.exponent AS qexp,
        b.exponent AS bexp,
        b.denom    AS base_denom,
        (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
        toff.exponent AS offer_exp,
        task.exponent AS ask_exp
      FROM trades t
      JOIN pools  p ON p.pool_id = t.pool_id
      JOIN tokens q ON q.token_id = p.quote_token_id
      JOIN tokens b ON b.token_id = p.base_token_id
      LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
      LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
      WHERE ${where.join(' AND ')}
      ORDER BY t.created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    `;

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeRow(r, unit, zigUsd));

    if (minV != null) data = data.filter(x => (unit === 'usd' ? x.valueUsd : x.valueNative) >= minV);
    if (maxV != null) data = data.filter(x => (unit === 'usd' ? x.valueUsd : x.valueNative) <= maxV);

    res.json({ success:true, data, meta:{ unit, limit, offset, tf: req.query.tf || '24h', minValue:minV ?? undefined, maxValue:maxV ?? undefined } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
