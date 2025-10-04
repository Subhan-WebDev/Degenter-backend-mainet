// api/routes/trades.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { getZigUsd, resolveTokenId } from '../util/resolve-token.js';

const router = express.Router();

const TF_MIN = { '1h':60, '4h':240, '24h':1440, '7d':10080, '1d':1440, '5d':7200 };
const clamp60d = (d) => Math.min(Math.max(d, 1), 60); // 1..60 days

function classify(val) {
  if (val == null) return null;
  if (val < 1000) return 'shrimp';
  if (val <= 10000) return 'shark';
  return 'whale';
}

function shapeTradeRow(r, zigUsd, unit) {
  const qexp = Number(r.qexp || 6);
  const vZig = r.is_uzig_quote
    ? (r.direction === 'buy'
        ? Number(r.offer_amount_base)  / 10**qexp
        : Number(r.return_amount_base) / 10**qexp)
    : null;
  const vUsd = vZig != null ? vZig * zigUsd : null;
  const val = unit === 'zig' ? vZig : vUsd;

  return {
    time: r.created_at,
    txHash: r.tx_hash,
    pairContract: r.pair_contract,
    signer: r.signer,
    direction: r.direction,
    offerDenom: r.offer_asset_denom,
    offerAmountBase: r.offer_amount_base,
    askDenom: r.ask_asset_denom,
    askAmountBase: r.ask_amount_base,
    returnAmountBase: r.return_amount_base,
    valueNative: vZig,
    valueUsd: vUsd,
    class: classify(val)
  };
}

/* ======================= GLOBAL FEED ======================= */
router.get('/', async (req, res) => {
  try {
    const tf = (req.query.tf || '24h').toLowerCase();
    const minsDefault = TF_MIN[tf] || 1440;
    const days = clamp60d(parseInt(req.query.days || '0', 10) || 0);
    const from = req.query.from || (days ? new Date(Date.now()-days*86400*1000).toISOString() : null);
    const to   = req.query.to   || null;

    const unit  = (req.query.unit || 'usd').toLowerCase();
    const klass = (req.query.class || '').toLowerCase();

    const limit  = Math.min(parseInt(req.query.limit || '500', 10), 5000);
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));

    const zigUsd = await getZigUsd();

    const filters = [];
    const params = [];
    if (!from && !to) filters.push(`t.created_at >= now() - INTERVAL '${minsDefault} minutes'`);
    if (from) { params.push(from); filters.push(`t.created_at >= $${params.length}::timestamptz`); }
    if (to)   { params.push(to);   filters.push(`t.created_at <  $${params.length}::timestamptz`); }

    const whereTime = filters.length ? `AND ${filters.join(' AND ')}` : '';

    const rows = await DB.query(`
      SELECT t.*, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp
      FROM trades t
      JOIN pools p ON p.pool_id=t.pool_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      WHERE t.action='swap' ${whereTime}
      ORDER BY t.created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    `, params);

    let data = rows.rows.map(r => shapeTradeRow(r, zigUsd, unit));
    if (klass) data = data.filter(x => x.class === klass);

    // count (for pagination meta) – cap to 60d windows
    const countRow = await DB.query(`
      SELECT COUNT(*)::bigint AS c
      FROM trades t JOIN pools p ON p.pool_id=t.pool_id
      WHERE t.action='swap' ${whereTime}
    `, params);

    res.json({ success: true, data, meta: { unit, tf, limit, offset, total: Number(countRow.rows[0]?.c || 0) } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

/* ======================= BY TOKEN (across pools) ======================= */
router.get('/token/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const tf = (req.query.tf || '24h').toLowerCase();
    const minsDefault = TF_MIN[tf] || 1440;
    const days = clamp60d(parseInt(req.query.days || '0', 10) || 0);
    const from = req.query.from || (days ? new Date(Date.now()-days*86400*1000).toISOString() : null);
    const to   = req.query.to   || null;

    const unit  = (req.query.unit || 'usd').toLowerCase();
    const klass = (req.query.class || '').toLowerCase();
    const limit  = Math.min(parseInt(req.query.limit || '500', 10), 5000);
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));

    const zigUsd = await getZigUsd();

    const filters = [`b.token_id=$1`];
    const params = [tok.token_id];

    if (!from && !to) filters.push(`t.created_at >= now() - INTERVAL '${minsDefault} minutes'`);
    if (from) { params.push(from); filters.push(`t.created_at >= $${params.length}::timestamptz`); }
    if (to)   { params.push(to);   filters.push(`t.created_at <  $${params.length}::timestamptz`); }

    const whereSql = filters.length ? `AND ${filters.join(' AND ')}` : '';

    const rows = await DB.query(`
      SELECT t.*, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp
      FROM trades t
      JOIN pools p  ON p.pool_id=t.pool_id
      JOIN tokens b ON b.token_id=p.base_token_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      WHERE t.action='swap' ${whereSql}
      ORDER BY t.created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    `, params);

    let data = rows.rows.map(r => shapeTradeRow(r, zigUsd, unit));
    if (klass) data = data.filter(x => x.class === klass);

    const countRow = await DB.query(`
      SELECT COUNT(*)::bigint AS c
      FROM trades t
      JOIN pools p  ON p.pool_id=t.pool_id
      JOIN tokens b ON b.token_id=p.base_token_id
      WHERE t.action='swap' AND b.token_id=$1
      ${from ? `AND t.created_at >= $2::timestamptz` : ''}
      ${to   ? `AND t.created_at <  $${from ? 3 : 2}::timestamptz` : ''}
    `, from && to ? [tok.token_id, from, to] : from ? [tok.token_id, from] : to ? [tok.token_id, to] : [tok.token_id]);

    res.json({ success:true, data, meta: { unit, tf, limit, offset, total: Number(countRow.rows[0]?.c || 0) } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

/* ======================= BY POOL (poolId or pair contract) ======================= */
router.get('/pool/:ref', async (req, res) => {
  try {
    const ref = req.params.ref;
    const tf = (req.query.tf || '24h').toLowerCase();
    const minsDefault = TF_MIN[tf] || 1440;
    const days = clamp60d(parseInt(req.query.days || '0', 10) || 0);
    const from = req.query.from || (days ? new Date(Date.now()-days*86400*1000).toISOString() : null);
    const to   = req.query.to   || null;

    const unit  = (req.query.unit || 'usd').toLowerCase();
    const klass = (req.query.class || '').toLowerCase();
    const limit  = Math.min(parseInt(req.query.limit || '500', 10), 5000);
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));

    const zigUsd = await getZigUsd();

    const pool = /^\d+$/.test(ref)
      ? await DB.query(`SELECT p.pool_id, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp FROM pools p JOIN tokens q ON q.token_id=p.quote_token_id WHERE p.pool_id=$1`, [ref])
      : await DB.query(`SELECT p.pool_id, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp FROM pools p JOIN tokens q ON q.token_id=p.quote_token_id WHERE p.pair_contract=$1`, [ref]);

    if (!pool.rows[0]) return res.status(404).json({ success:false, error:'pool not found' });
    const poolId = pool.rows[0].pool_id;

    const filters = [`t.pool_id=$1`];
    const params = [poolId];

    if (!from && !to) filters.push(`t.created_at >= now() - INTERVAL '${minsDefault} minutes'`);
    if (from) { params.push(from); filters.push(`t.created_at >= $${params.length}::timestamptz`); }
    if (to)   { params.push(to);   filters.push(`t.created_at <  $${params.length}::timestamptz`); }

    const whereSql = filters.length ? `AND ${filters.join(' AND ')}` : '';

    const rows = await DB.query(`
      SELECT t.*, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp
      FROM trades t
      JOIN pools p ON p.pool_id=t.pool_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      WHERE t.action='swap' ${whereSql}
      ORDER BY t.created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    `, params);

    let data = rows.rows.map(r => shapeTradeRow(r, zigUsd, unit));
    if (klass) data = data.filter(x => x.class === klass);

    const countRow = await DB.query(`
      SELECT COUNT(*)::bigint AS c
      FROM trades t
      WHERE t.action='swap' AND t.pool_id=$1
      ${from ? `AND t.created_at >= $2::timestamptz` : ''}
      ${to   ? `AND t.created_at <  $${from ? 3 : 2}::timestamptz` : ''}
    `, from && to ? [poolId, from, to] : from ? [poolId, from] : to ? [poolId, to] : [poolId]);

    res.json({ success:true, data, meta: { unit, tf, limit, offset, total: Number(countRow.rows[0]?.c || 0) } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

/* ======================= WALLET (with token/pair filters, 60d window) ======================= */
router.get('/wallet/:address', async (req, res) => {
  try {
    const addr = req.params.address;
    const tf = (req.query.tf || '1d').toLowerCase();
    const minsDefault = TF_MIN[tf] || 1440;
    const days = clamp60d(parseInt(req.query.days || '0', 10) || 0);
    const from = req.query.from || (days ? new Date(Date.now()-days*86400*1000).toISOString() : null);
    const to   = req.query.to   || null;

    const unit  = (req.query.unit || 'usd').toLowerCase();
    const tokenRef = req.query.tokenId || null;
    const poolId   = req.query.poolId || null;
    const pair     = req.query.pair || null;

    const limit  = Math.min(parseInt(req.query.limit || '1000', 10), 5000);
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));

    const zigUsd = await getZigUsd();

    const filters = [`t.signer=$1`];
    const params = [addr];
    let idx = 2;

    if (!from && !to) filters.push(`t.created_at >= now() - INTERVAL '${minsDefault} minutes'`);
    if (from) { params.push(from); filters.push(`t.created_at >= $${idx++}::timestamptz`); }
    if (to)   { params.push(to);   filters.push(`t.created_at <  $${idx++}::timestamptz`); }

    if (tokenRef) {
      const tok = await resolveTokenId(tokenRef);
      if (!tok) return res.status(404).json({ success:false, error:'token not found' });
      params.push(tok.token_id);
      filters.push(`p.base_token_id=$${idx++}`);
    }
    if (poolId) { params.push(poolId); filters.push(`t.pool_id=$${idx++}`); }
    if (pair)   { params.push(pair);   filters.push(`p.pair_contract=$${idx++}`); }

    const whereSql = filters.length ? `AND ${filters.join(' AND ')}` : '';

    const rows = await DB.query(`
      SELECT t.*, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp
      FROM trades t
      JOIN pools p ON p.pool_id=t.pool_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      WHERE t.action='swap' ${whereSql}
      ORDER BY t.created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    `, params);

    const data = rows.rows.map(r => shapeTradeRow(r, zigUsd, unit));

    const countRow = await DB.query(`
      SELECT COUNT(*)::bigint AS c
      FROM trades t JOIN pools p ON p.pool_id=t.pool_id
      WHERE t.action='swap' ${whereSql}
    `, params);

    res.json({ success:true, data, meta:{ unit, tf, limit, offset, total: Number(countRow.rows[0]?.c || 0) } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

/* ======================= LARGE TRADES (min & max) ======================= */
router.get('/large', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const unit   = (req.query.unit || 'zig').toLowerCase();
    const minVal = Number(req.query.minValue || 0);
    const maxVal = req.query.maxValue != null ? Number(req.query.maxValue) : null;
    const zigUsd = await getZigUsd();

    const { rows } = await DB.query(`
      SELECT DISTINCT ON (tx_hash, pool_id, direction)
             pool_id, tx_hash, signer, direction, value_zig, created_at
      FROM large_trades
      WHERE bucket=$1
      ORDER BY tx_hash, pool_id, direction, created_at DESC
      LIMIT 10000
    `, [bucket]);

    let data = rows.map(r => {
      const vZig = Number(r.value_zig);
      const vUsd = vZig * zigUsd;
      return {
        pairContract: (/* resolve once */ null),
        // we'll join once for pair contract:
        poolId: String(r.pool_id), txHash: r.tx_hash, signer: r.signer, direction: r.direction,
        valueNative: vZig, valueUsd: vUsd, createdAt: r.created_at
      };
    });

    // attach pairContract (single round trip)
    if (data.length) {
      const poolIds = [...new Set(data.map(d => Number(d.poolId)))];
      const m = await DB.query(`SELECT pool_id, pair_contract FROM pools WHERE pool_id = ANY($1)`, [poolIds]);
      const pc = new Map(m.rows.map(x => [String(x.pool_id), x.pair_contract]));
      data = data.map(d => ({ ...d, pairContract: pc.get(d.poolId) }));
    }

    // apply min/max in chosen unit
    data = data.filter(t => {
      const val = unit === 'usd' ? (t.valueUsd ?? 0) : (t.valueNative ?? 0);
      if (val < minVal) return false;
      if (maxVal != null && val > maxVal) return false;
      return true;
    });

    res.json({ success:true, data, meta: { bucket, unit, minValue: minVal, maxValue: maxVal } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

/* ======================= RECENT SNAPSHOT (pairable with WS) ======================= */
router.get('/recent', async (req, res) => {
  try {
    const tokenRef = req.query.tokenId || null;
    const poolId   = req.query.poolId || null;
    const pair     = req.query.pair || null;
    const unit     = (req.query.unit || 'usd').toLowerCase();
    const minValue = Number(req.query.minValue || 0);
    const maxValue = req.query.maxValue != null ? Number(req.query.maxValue) : null;
    const limit    = Math.min(parseInt(req.query.limit || '200', 10), 500);
    const offset   = Math.max(0, parseInt(req.query.offset || '0', 10));
    const zigUsd   = await getZigUsd();

    const extra = [];
    const params = [];
    let idx = 1;
    if (tokenRef) {
      const tok = await resolveTokenId(tokenRef);
      if (!tok) return res.status(404).json({ success:false, error:'token not found' });
      extra.push(`p.base_token_id=$${idx++}`); params.push(tok.token_id);
    }
    if (poolId) { extra.push(`t.pool_id=$${idx++}`); params.push(poolId); }
    if (pair)   { extra.push(`p.pair_contract=$${idx++}`); params.push(pair); }
    const whereExtra = extra.length ? ` AND ${extra.join(' AND ')}` : '';

    const rows = await DB.query(`
      SELECT t.*, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp
      FROM trades t
      JOIN pools p ON p.pool_id=t.pool_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      WHERE t.action='swap' ${whereExtra}
      ORDER BY t.created_at DESC
      LIMIT ${limit} OFFSET ${offset}
    `, params);

    let data = rows.rows.map(r => shapeTradeRow(r, zigUsd, unit));
    data = data.filter(t => {
      const val = unit === 'usd' ? (t.valueUsd ?? 0) : (t.valueNative ?? 0);
      if (val < minValue) return false;
      if (maxValue != null && val > maxValue) return false;
      return true;
    });

    const countRow = await DB.query(`
      SELECT COUNT(*)::bigint AS c
      FROM trades t JOIN pools p ON p.pool_id=t.pool_id
      WHERE t.action='swap' ${whereExtra}
    `, params);

    res.json({ success:true, data, meta: { unit, minValue, maxValue, limit, offset, total: Number(countRow.rows[0]?.c || 0) } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

export default router;
