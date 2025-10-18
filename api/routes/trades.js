// api/routes/trades.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { getZigUsd, resolveTokenId } from '../util/resolve-token.js';

const router = express.Router();

/* ---------------- helpers ---------------- */

const VALID_DIR = new Set(['buy','sell','provide','withdraw']);
const VALID_CLASS = new Set(['shrimp','shark','whale']);
const VALID_LIMITS = new Set([100, 500, 1000]);

function normDir(d) {
  const x = String(d || '').toLowerCase();
  return VALID_DIR.has(x) ? x : null;
}

function clampInt(v, { min = 0, max = 1e9, def = 0 } = {}) {
  const n = Number.parseInt(v, 10);
  if (Number.isNaN(n)) return def;
  return Math.max(min, Math.min(max, n));
}

function parseLimit(q) {
  const n = Number(q);
  if (VALID_LIMITS.has(n)) return n;
  return 100; // default
}
function parsePage(q) {
  const n = Number(q);
  return Number.isFinite(n) && n >= 1 ? Math.floor(n) : 1;
}

/** Fetch enough rows to cover (page * limit) before post-filters/combining */
function computeSqlFetchLimit(page, limit, hardMax = 5000) {
  const OVERSAMPLE = 5;                 // try 5–10 if needed
  return Math.min(page * limit * OVERSAMPLE, hardMax);
}

function classifyByWorth(value /* number|null */) {
  if (value == null) return null;
  const x = Number(value);
  if (x < 1000) return 'shrimp';
  if (x <= 10000) return 'shark';
  return 'whale';
}

function applyClassFilter(data, unit, klass) {
  if (!klass) return data;
  if (!VALID_CLASS.has(klass)) return data;
  return data.filter(x => {
    const worth = unit === 'usd' ? x.valueUsd : x.valueNative;
    return classifyByWorth(worth) === klass;
  });
}

function paginate(data, page, limit) {
  const total = data.length;
  const pages = Math.max(1, Math.ceil(total / limit));
  const p = Math.min(page, pages);
  const start = (p - 1) * limit;
  const end = start + limit;
  return {
    items: data.slice(start, end),
    total,
    page: p,
    pages,
    limit
  };
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
  sqlLimit, sqlOffset = 0
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
      task.exponent AS ask_exp
    FROM trades t
    JOIN pools  p ON p.pool_id = t.pool_id
    JOIN tokens q ON q.token_id = p.quote_token_id
    JOIN tokens b ON b.token_id = p.base_token_id
    LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
    LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
    WHERE ${where.join(' AND ')}
    ORDER BY t.created_at DESC
    LIMIT ${sqlLimit} OFFSET ${sqlOffset}
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

  const cls = classifyByWorth((unit === 'usd') ? valueUsd : valueZig);

  return {
    time: r.created_at,
    txHash: r.tx_hash,
    pairContract: r.pair_contract,
    signer: r.signer,
    direction: r.direction,

    is_router: r.is_router === true, // passthrough

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

    class: cls
  };
}

/** Combine router legs by txHash, summing worth */
function combineRouterTrades(items, unit /* 'usd'|'zig' */) {
  const byTx = new Map();
  for (const t of items) {
    const key = t.txHash;
    const cur = byTx.get(key);
    if (!cur) {
      byTx.set(key, { ...t, legs: [t] });
    } else {
      cur.legs.push(t);
      // accumulate worth
      cur.valueNative = (cur.valueNative ?? 0) + (t.valueNative ?? 0);
      cur.valueUsd    = (cur.valueUsd ?? 0) + (t.valueUsd ?? 0);
      // earliest time
      cur.time = cur.time < t.time ? cur.time : t.time;
      // mark router
      cur.is_router = cur.is_router || t.is_router;
    }
  }
  return Array.from(byTx.values()).map(x => {
    if (!x.is_router) return x; // not a router aggregate — leave as-is
    const first = x.legs[0];
    return {
      ...x,
      pairContract: 'router',
      direction: first.direction,
      class: classifyByWorth(unit === 'usd' ? x.valueUsd : x.valueNative)
    };
  });
}

/* ---------------- routes ---------------- */

/** GET /trades
 *  New pagination: page=1..N (default 1), limit=100|500|1000 (default 100)
 *  Optional: class=shrimp|shark|whale, combineRouter=1
 *  Window: tf=30m|1h|2h|4h|8h|12h|24h|1d|3d|5d|7d OR from&to OR days=1..60
 *  unit=usd|zig, direction=buy|sell|provide|withdraw, includeLiquidity=1
 */
router.get('/', async (req, res) => {
  try {
    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const page   = parsePage(req.query.page);
    const limit  = parseLimit(req.query.limit);
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const combine = req.query.combineRouter === '1';

    const zigUsd = await getZigUsd();

    const sqlLimit = computeSqlFetchLimit(page, limit, 5000);

    const { sql, params } = buildTradesQuery({
      scope: 'all',
      includeLiquidity,
      direction: dir,
      windowOpts: { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days },
      sqlLimit, sqlOffset: 0
    });

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeRow(r, unit, zigUsd));

    if (combine) data = combineRouterTrades(data, unit);

    const klass = String(req.query.class || '').toLowerCase();
    if (klass) data = applyClassFilter(data, unit, klass);

    const { items, total, pages, page: p } = paginate(data, page, limit);
    res.json({ success:true, data: items, meta:{ unit, tf:req.query.tf||'24h', limit, page: p, pages, total } });
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
    const page   = parsePage(req.query.page);
    const limit  = parseLimit(req.query.limit);
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const combine = req.query.combineRouter === '1';

    const zigUsd = await getZigUsd();

    const sqlLimit = computeSqlFetchLimit(page, limit, 5000);

    const { sql, params } = buildTradesQuery({
      scope: 'token',
      scopeValue: tok.token_id,  // BIGINT
      includeLiquidity,
      direction: dir,
      windowOpts: { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days },
      sqlLimit, sqlOffset: 0
    });

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeRow(r, unit, zigUsd));

    if (combine) data = combineRouterTrades(data, unit);

    const klass = String(req.query.class || '').toLowerCase();
    if (klass) data = applyClassFilter(data, unit, klass);

    const { items, total, pages, page: p } = paginate(data, page, limit);
    res.json({ success:true, data: items, meta:{ unit, tf:req.query.tf||'24h', limit, page: p, pages, total } });
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
    const page   = parsePage(req.query.page);
    const limit  = parseLimit(req.query.limit);
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const combine = req.query.combineRouter === '1';

    const zigUsd = await getZigUsd();

    const sqlLimit = computeSqlFetchLimit(page, limit, 5000);

    const { sql, params } = buildTradesQuery({
      scope: 'pool',
      scopeValue: { poolId },
      includeLiquidity,
      direction: dir,
      windowOpts: { tf:req.query.tf, from:req.query.from, to:req.query.to, days:req.query.days },
      sqlLimit, sqlOffset: 0
    });

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeRow(r, unit, zigUsd));

    if (combine) data = combineRouterTrades(data, unit);

    const klass = String(req.query.class || '').toLowerCase();
    if (klass) data = applyClassFilter(data, unit, klass);

    const { items, total, pages, page: p } = paginate(data, page, limit);
    res.json({ success:true, data: items, meta:{ unit, tf:req.query.tf||'24h', limit, page: p, pages, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/wallet/:address
 *  New pagination: page/limit. Optional: class=, combineRouter=1
 */
router.get('/wallet/:address', async (req, res) => {
  try {
    const address = req.params.address;
    const unit    = String(req.query.unit || 'usd').toLowerCase();
    const page    = parsePage(req.query.page);
    const limit   = parseLimit(req.query.limit);
    const dir     = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const combine = req.query.combineRouter === '1';

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

    const sqlLimit = computeSqlFetchLimit(page, limit, 5000);

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
      LIMIT ${sqlLimit}
    `;

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeRow(r, unit, zigUsd));

    if (combine) data = combineRouterTrades(data, unit);

    const klass = String(req.query.class || '').toLowerCase();
    if (klass) data = applyClassFilter(data, unit, klass);

    const { items, total, pages, page: p } = paginate(data, page, limit);
    res.json({ success:true, data: items, meta:{ unit, tf:req.query.tf || '24h', limit, page: p, pages, total } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/large
 *  Now supports: class=shrimp|shark|whale and page/limit
 */
router.get('/large', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const unit   = (req.query.unit || 'zig').toLowerCase();
    const page   = parsePage(req.query.page);
    const limit  = parseLimit(req.query.limit);
    const minV   = req.query.minValue != null ? Number(req.query.minValue) : null;
    const maxV   = req.query.maxValue != null ? Number(req.query.maxValue) : null;
    const klass  = String(req.query.class || '').toLowerCase();
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
      createdAt: r.created_at,
      class: classifyByWorth(unit === 'usd' ? (Number(r.value_zig) * zigUsd) : Number(r.value_zig))
    }));

    if (minV != null) data = data.filter(x => (unit === 'usd' ? x.valueUsd : x.valueNative) >= minV);
    if (maxV != null) data = data.filter(x => (unit === 'usd' ? x.valueUsd : x.valueNative) <= maxV);
    if (klass) data = data.filter(x => x.class === klass);

    const { items, total, pages, page: p } = paginate(data, page, limit);
    res.json({ success:true, data: items, meta:{ bucket, unit, limit, page: p, pages, total, minValue:minV ?? undefined, maxValue:maxV ?? undefined } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/recent
 *  Now supports: class=shrimp|shark|whale, combineRouter=1, and page/limit
 */
router.get('/recent', async (req, res) => {
  try {
    const unit   = String(req.query.unit || 'usd').toLowerCase();
    const page   = parsePage(req.query.page);
    const limit  = parseLimit(req.query.limit);
    const dir    = normDir(req.query.direction);
    const includeLiquidity = req.query.includeLiquidity === '1';
    const minV   = req.query.minValue != null ? Number(req.query.minValue) : null;
    const maxV   = req.query.maxValue != null ? Number(req.query.maxValue) : null;
    const klass  = String(req.query.class || '').toLowerCase();
    const combine = req.query.combineRouter === '1';

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

    const sqlLimit = computeSqlFetchLimit(page, limit, 2000);

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
      LIMIT ${sqlLimit}
    `;

    const { rows } = await DB.query(sql, params);
    let data = rows.map(r => shapeRow(r, unit, zigUsd));

    if (minV != null) data = data.filter(x => (unit === 'usd' ? x.valueUsd : x.valueNative) >= minV);
    if (maxV != null) data = data.filter(x => (unit === 'usd' ? x.valueUsd : x.valueNative) <= maxV);
    if (combine) data = combineRouterTrades(data, unit);
    if (klass) data = applyClassFilter(data, unit, klass);

    const { items, total, pages, page: p } = paginate(data, page, limit);
    res.json({ success:true, data: items, meta:{ unit, limit, page: p, pages, total, tf: req.query.tf || '24h', minValue:minV ?? undefined, maxValue:maxV ?? undefined } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
