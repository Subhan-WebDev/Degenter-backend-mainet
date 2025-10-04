// api/routes/trades.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { getZigUsd, resolveTokenId } from '../util/resolve-token.js';

const router = express.Router();

const TF_MIN = { '1h':60, '4h':240, '24h':1440, '7d':10080, '1d':1440, '5d':7200 };

function classifyBy(val) {
  if (val == null) return null;
  if (val < 1000) return 'shrimp';
  if (val <= 10000) return 'shark';
  return 'whale';
}

/* ------- existing: global feed -------- */
router.get('/', async (req, res) => {
  try {
    const tf = (req.query.tf || '24h').toLowerCase();
    const mins = TF_MIN[tf] || 1440;
    const klass = (req.query.class || '').toLowerCase();
    const unit  = (req.query.unit || 'usd').toLowerCase();
    const zigUsd = await getZigUsd();

    const { rows } = await DB.query(`
      SELECT t.*, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp
      FROM trades t
      JOIN pools p ON p.pool_id=t.pool_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      WHERE t.action='swap' AND t.created_at >= now() - INTERVAL '${mins} minutes'
      ORDER BY t.created_at DESC
      LIMIT 500
    `);

    let data = rows.map(r => {
      const qexp = Number(r.qexp || 6);
      const vZig = r.is_uzig_quote
        ? (r.direction === 'buy'
            ? Number(r.offer_amount_base)/10**qexp
            : Number(r.return_amount_base)/10**qexp)
        : null;
      const vUsd = vZig != null ? vZig * zigUsd : null;
      const val = unit === 'zig' ? vZig : vUsd;
      const group = classifyBy(val);
      return {
        time: r.created_at,
        txHash: r.tx_hash,
        poolId: r.pool_id,
        pairContract: r.pair_contract,
        signer: r.signer,
        direction: r.direction,
        valueNative: vZig,
        valueUsd: vUsd,
        class: group
      };
    });
    if (klass) data = data.filter(x => x.class === klass);

    res.json({ success: true, data, meta: { unit, tf } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

/* ------- by token (across pools) ------- */
router.get('/token/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });
    const tf = (req.query.tf || '24h').toLowerCase();
    const mins = TF_MIN[tf] || 1440;
    const klass = (req.query.class || '').toLowerCase();
    const unit  = (req.query.unit || 'usd').toLowerCase();
    const limit = Math.min(parseInt(req.query.limit || '500', 10), 1000);
    const zigUsd = await getZigUsd();

    const { rows } = await DB.query(`
      SELECT t.*, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp
      FROM trades t
      JOIN pools p  ON p.pool_id=t.pool_id
      JOIN tokens b ON b.token_id=p.base_token_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      WHERE t.action='swap' AND b.token_id=$1
        AND t.created_at >= now() - INTERVAL '${mins} minutes'
      ORDER BY t.created_at DESC
      LIMIT $2
    `, [tok.token_id, limit]);

    let data = rows.map(r => {
      const qexp = Number(r.qexp || 6);
      const vZig = r.is_uzig_quote
        ? (r.direction === 'buy'
            ? Number(r.offer_amount_base)/10**qexp
            : Number(r.return_amount_base)/10**qexp)
        : null;
      const vUsd = vZig != null ? vZig * zigUsd : null;
      const val = unit === 'zig' ? vZig : vUsd;
      const group = classifyBy(val);
      return {
        time: r.created_at,
        txHash: r.tx_hash,
        poolId: r.pool_id,
        pairContract: r.pair_contract,
        signer: r.signer,
        direction: r.direction,
        valueNative: vZig,
        valueUsd: vUsd,
        class: group
      };
    });
    if (klass) data = data.filter(x => x.class === klass);

    res.json({ success:true, data, meta: { unit, tf } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

/* ------- NEW: trades by pool (poolId or pair contract) ------- */
router.get('/pool/:ref', async (req, res) => {
  try {
    const ref = req.params.ref;
    const tf = (req.query.tf || '24h').toLowerCase();
    const mins = TF_MIN[tf] || 1440;
    const unit  = (req.query.unit || 'usd').toLowerCase();
    const klass = (req.query.class || '').toLowerCase();
    const zigUsd = await getZigUsd();

    const pool = /^\d+$/.test(ref)
      ? await DB.query(`SELECT p.pool_id, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp FROM pools p JOIN tokens q ON q.token_id=p.quote_token_id WHERE p.pool_id=$1`, [ref])
      : await DB.query(`SELECT p.pool_id, p.is_uzig_quote, p.pair_contract, q.exponent AS qexp FROM pools p JOIN tokens q ON q.token_id=p.quote_token_id WHERE p.pair_contract=$1`, [ref]);

    if (!pool.rows[0]) return res.status(404).json({ success:false, error:'pool not found' });
    const poolId = pool.rows[0].pool_id;
    const qexp   = Number(pool.rows[0].qexp || 6);
    const isUzig = !!pool.rows[0].is_uzig_quote;

    const rows = await DB.query(`
      SELECT t.*, $1::int AS qexp, $2::boolean AS is_uzig_quote
      FROM trades t
      WHERE t.action='swap' AND t.pool_id=$3
        AND t.created_at >= now() - INTERVAL '${mins} minutes'
      ORDER BY t.created_at DESC
      LIMIT 500
    `, [qexp, isUzig, poolId]);

    let data = rows.rows.map(r => {
      const vZig = isUzig
        ? (r.direction === 'buy'
            ? Number(r.offer_amount_base)/10**qexp
            : Number(r.return_amount_base)/10**qexp)
        : null;
      const vUsd = vZig != null ? vZig * zigUsd : null;
      const val = unit === 'zig' ? vZig : vUsd;
      const group = classifyBy(val);
      return {
        time: r.created_at,
        txHash: r.tx_hash,
        poolId: r.pool_id,
        signer: r.signer,
        direction: r.direction,
        valueNative: vZig,
        valueUsd: vUsd,
        class: group
      };
    });
    if (klass) data = data.filter(x => x.class === klass);

    res.json({ success:true, data, meta: { unit, tf, poolId } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

/* ------- wallet trades with filters ------- */
router.get('/wallet/:address', async (req, res) => {
  try {
    const addr = req.params.address;
    const tf = (req.query.tf || '1d').toLowerCase();
    const mins = TF_MIN[tf] || 1440;
    const unit  = (req.query.unit || 'usd').toLowerCase();
    const zigUsd = await getZigUsd();

    const tokenRef = req.query.tokenId || null;
    const poolId   = req.query.poolId || null;
    const pair     = req.query.pair || null;
    const from     = req.query.from || null;
    const to       = req.query.to   || null;

    let extra = [];
    let params = [addr];
    let idx = 2;

    if (tokenRef) {
      const tok = await resolveTokenId(tokenRef);
      if (!tok) return res.status(404).json({ success:false, error:'token not found' });
      extra.push(`p.base_token_id=$${idx++}`); params.push(tok.token_id);
    }
    if (poolId) { extra.push(`t.pool_id=$${idx++}`); params.push(poolId); }
    if (pair)   { extra.push(`p.pair_contract=$${idx++}`); params.push(pair); }
    if (from)   { extra.push(`t.created_at >= $${idx++}::timestamptz`); params.push(from); }
    if (to)     { extra.push(`t.created_at <  $${idx++}::timestamptz`); params.push(to); }

    const whereExtra = extra.length ? ` AND ${extra.join(' AND ')}` : '';

    const rows = await DB.query(`
      SELECT t.*, p.is_uzig_quote, q.exponent AS qexp
      FROM trades t
      JOIN pools p ON p.pool_id=t.pool_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      WHERE t.action='swap' AND t.signer=$1
        AND t.created_at >= now() - INTERVAL '${mins} minutes'
        ${whereExtra}
      ORDER BY t.created_at DESC
      LIMIT 1000
    `, params);

    const data = rows.rows.map(r => {
      const qexp = Number(r.qexp || 6);
      const vZig = r.is_uzig_quote
        ? (r.direction === 'buy'
            ? Number(r.offer_amount_base)/10**qexp
            : Number(r.return_amount_base)/10**qexp)
        : null;
      const vUsd = vZig != null ? vZig * zigUsd : null;
      return {
        time: r.created_at, txHash: r.tx_hash, poolId: r.pool_id, direction: r.direction,
        valueNative: vZig, valueUsd: vUsd
      };
    });

    res.json({ success:true, data, meta:{ unit, tf } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

/* ------- large trades with thresholds ------- */
router.get('/large', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h').toLowerCase();
    const unit   = (req.query.unit || 'zig').toLowerCase();
    const minVal = Number(req.query.minValue || 0);
    const zigUsd = await getZigUsd();

    const { rows } = await DB.query(`
      SELECT DISTINCT ON (tx_hash, pool_id, direction)
             pool_id, tx_hash, signer, direction, value_zig, created_at
      FROM large_trades
      WHERE bucket=$1
      ORDER BY tx_hash, pool_id, direction, created_at DESC
      LIMIT 500
    `, [bucket]);

    let data = rows.map(r => {
      const vZig = Number(r.value_zig);
      const vUsd = vZig * zigUsd;
      return {
        poolId: String(r.pool_id), txHash: r.tx_hash, signer: r.signer, direction: r.direction,
        valueNative: vZig, valueUsd: vUsd, createdAt: r.created_at
      };
    });

    data = data.filter(t => (unit === 'usd' ? t.valueUsd >= minVal : t.valueNative >= minVal));

    res.json({ success:true, data, meta: { bucket, unit, minValue: minVal } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

/* ------- recent trades (snapshot) ------- */
router.get('/recent', async (req, res) => {
  try {
    const tokenRef = req.query.tokenId || null;
    const poolId   = req.query.poolId || null;
    const pair     = req.query.pair || null;
    const unit     = (req.query.unit || 'usd').toLowerCase();
    const minValue = Number(req.query.minValue || 0);
    const limit    = Math.min(parseInt(req.query.limit || '200', 10), 500);
    const zigUsd   = await getZigUsd();

    let extra = [];
    let params = [];
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
      LIMIT ${limit}
    `, params);

    let data = rows.rows.map(r => {
      const qexp = Number(r.qexp || 6);
      const vZig = r.is_uzig_quote
        ? (r.direction === 'buy'
            ? Number(r.offer_amount_base)/10**qexp
            : Number(r.return_amount_base)/10**qexp)
        : null;
      const vUsd = vZig != null ? vZig * zigUsd : null;
      return {
        time: r.created_at, txHash: r.tx_hash, poolId: r.pool_id, pairContract: r.pair_contract,
        direction: r.direction, valueNative: vZig, valueUsd: vUsd, signer: r.signer
      };
    });

    data = data.filter(t => (unit === 'usd' ? (t.valueUsd ?? 0) >= minValue : (t.valueNative ?? 0) >= minValue));
    res.json({ success:true, data, meta: { unit, minValue, limit } });
  } catch (e) { res.status(500).json({ success:false, error: e.message }); }
});

export default router;
