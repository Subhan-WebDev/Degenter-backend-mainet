// api/routes/trades.js
import express from 'express';
import { DB } from '../../lib/db.js';
import { getZigUsd } from '../util/resolve-token.js';
import { resolveTokenId } from '../util/resolve-token.js';

const router = express.Router();

function classifyBy(ref, unit) {
  if (ref == null) return null;
  if (unit === 'zig') {
    if (ref < 1000) return 'shrimp';
    if (ref <= 10000) return 'shark';
    return 'whale';
  } else {
    if (ref < 1000) return 'shrimp';
    if (ref <= 10000) return 'shark';
    return 'whale';
  }
}

/** GET /trades?tf=1h|4h|24h|7d&class=shrimp|shark|whale&unit=usd|zig */
router.get('/', async (req, res) => {
  try {
    const tf = (req.query.tf || '24h').toLowerCase();
    const mins = { '1h':60, '4h':240, '24h':1440, '7d':10080 }[tf] || 1440;
    const klass = (req.query.class || '').toLowerCase();
    const unit  = (req.query.unit || 'usd').toLowerCase();
    const zigUsd = await getZigUsd();

    const { rows } = await DB.query(`
      SELECT t.*, p.is_uzig_quote, qtk.exponent AS qexp
      FROM trades t
      JOIN pools p ON p.pool_id=t.pool_id
      JOIN tokens qtk ON qtk.token_id=p.quote_token_id
      WHERE t.action='swap'
        AND t.created_at >= now() - INTERVAL '${mins} minutes'
      ORDER BY t.created_at DESC
      LIMIT 500
    `);

    let data = rows.map(r => {
      const qexp = Number(r.qexp || 6);
      const valueNative = r.is_uzig_quote
        ? (r.direction === 'buy'
            ? Number(r.offer_amount_base)/10**qexp
            : Number(r.return_amount_base)/10**qexp)
        : null;
      const valueUsd = valueNative != null ? valueNative * zigUsd : null;
      const ref = unit === 'zig' ? valueNative : valueUsd;
      const group = classifyBy(ref, unit);

      return {
        time: r.created_at,
        txHash: r.tx_hash,
        poolId: r.pool_id,
        signer: r.signer,
        direction: r.direction,
        offerDenom: r.offer_asset_denom,
        offerAmountBase: r.offer_amount_base,
        askDenom: r.ask_asset_denom,
        askAmountBase: r.ask_amount_base,
        returnAmountBase: r.return_amount_base,
        valueNative,
        valueUsd,
        class: group
      };
    });

    if (klass) data = data.filter(x => x.class === klass);

    res.json({ success:true, data, meta: { unit } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/token/:id?tf=…&class=…&unit=usd|zig&limit=… */
router.get('/token/:id', async (req, res) => {
  try {
    const tok = await resolveTokenId(req.params.id);
    if (!tok) return res.status(404).json({ success:false, error:'token not found' });

    const tf = (req.query.tf || '24h').toLowerCase();
    const mins = { '1h':60, '4h':240, '24h':1440, '7d':10080 }[tf] || 1440;
    const klass = (req.query.class || '').toLowerCase();
    const unit  = (req.query.unit || 'usd').toLowerCase();
    const limit = Math.min(parseInt(req.query.limit || '500', 10), 1000);
    const zigUsd = await getZigUsd();

    const { rows } = await DB.query(`
      SELECT t.*, p.is_uzig_quote, qtk.exponent AS qexp
      FROM trades t
      JOIN pools p  ON p.pool_id=t.pool_id
      JOIN tokens b ON b.token_id=p.base_token_id
      JOIN tokens qtk ON qtk.token_id=p.quote_token_id
      WHERE t.action='swap'
        AND b.token_id=$1
        AND t.created_at >= now() - INTERVAL '${mins} minutes'
      ORDER BY t.created_at DESC
      LIMIT $2
    `, [tok.token_id, limit]);

    let data = rows.map(r => {
      const qexp = Number(r.qexp || 6);
      const valueNative = r.is_uzig_quote
        ? (r.direction === 'buy'
            ? Number(r.offer_amount_base)/10**qexp
            : Number(r.return_amount_base)/10**qexp)
        : null;
      const valueUsd = valueNative != null ? valueNative * zigUsd : null;
      const ref = unit === 'zig' ? valueNative : valueUsd;
      const group = classifyBy(ref, unit);

      return {
        time: r.created_at,
        txHash: r.tx_hash,
        poolId: r.pool_id,
        signer: r.signer,
        direction: r.direction,
        offerDenom: r.offer_asset_denom,
        offerAmountBase: r.offer_amount_base,
        askDenom: r.ask_asset_denom,
        askAmountBase: r.ask_amount_base,
        returnAmountBase: r.return_amount_base,
        valueNative,
        valueUsd,
        class: group
      };
    });

    if (klass) data = data.filter(x => x.class === klass);

    res.json({ success:true, data, meta: { unit, tf } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/wallet/:address?tf=1d&unit=usd|zig */
router.get('/wallet/:address', async (req, res) => {
  try {
    const addr = req.params.address;
    const tf = (req.query.tf || '1d').toLowerCase();
    const mins = { '1h':60, '4h':240, '1d':1440, '5d':7200 }[tf] || 1440;
    const unit  = (req.query.unit || 'usd').toLowerCase();
    const zigUsd = await getZigUsd();

    const { rows } = await DB.query(`
      SELECT t.*, p.is_uzig_quote, qtk.exponent AS qexp
      FROM trades t
      JOIN pools p ON p.pool_id=t.pool_id
      JOIN tokens qtk ON qtk.token_id=p.quote_token_id
      WHERE t.action='swap' AND t.signer=$1
        AND t.created_at >= now() - INTERVAL '${mins} minutes'
      ORDER BY t.created_at DESC
      LIMIT 1000
    `, [addr]);

    const data = rows.map(r => {
      const qexp = Number(r.qexp || 6);
      const valueNative = r.is_uzig_quote
        ? (r.direction === 'buy'
            ? Number(r.offer_amount_base)/10**qexp
            : Number(r.return_amount_base)/10**qexp)
        : null;
      const valueUsd = valueNative != null ? valueNative * zigUsd : null;
      return {
        time: r.created_at, txHash: r.tx_hash, poolId: r.pool_id, direction: r.direction,
        offerDenom: r.offer_asset_denom,
        offerAmountBase: r.offer_amount_base,
        askDenom: r.ask_asset_denom,
        askAmountBase: r.ask_amount_base,
        returnAmountBase: r.return_amount_base,
        valueNative, valueUsd
      };
    });

    res.json({ success:true, data, meta:{ unit, tf } });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/leaderboard/profitable */
router.get('/leaderboard/profitable', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h');
    const { rows } = await DB.query(`
      SELECT address, trades_count, volume_zig, gross_pnl_zig, updated_at
      FROM leaderboard_traders
      WHERE bucket=$1
      ORDER BY gross_pnl_zig DESC
      LIMIT 100
    `, [bucket]);
    res.json({ success:true, data: rows.map(r => ({
      address: r.address,
      trades: Number(r.trades_count),
      volumeNative: Number(r.volume_zig),
      pnlNative: Number(r.gross_pnl_zig),
      updatedAt: r.updated_at
    })) });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

/** GET /trades/large  (de-duped view) */
router.get('/large', async (req, res) => {
  try {
    const bucket = (req.query.bucket || '24h');
    const { rows } = await DB.query(`
      SELECT DISTINCT ON (tx_hash, pool_id, direction)
             pool_id, tx_hash, signer, direction, value_zig, created_at
      FROM large_trades
      WHERE bucket=$1
      ORDER BY tx_hash, pool_id, direction, created_at DESC
      LIMIT 200
    `, [bucket]);
    res.json({ success:true, data: rows.map(r => ({
      poolId: String(r.pool_id), txHash: r.tx_hash, signer: r.signer, direction: r.direction,
      valueNative: Number(r.value_zig), createdAt: r.created_at
    })) });
  } catch (e) {
    res.status(500).json({ success:false, error: e.message });
  }
});

export default router;
