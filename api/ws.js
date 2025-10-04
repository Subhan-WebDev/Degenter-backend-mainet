// api/ws.js
import { WebSocketServer } from 'ws';
import { info } from '../lib/log.js';
import { DB } from '../lib/db.js';
import { getZigUsd, resolveTokenId } from './util/resolve-token.js';
import { getCandles, ensureTf } from './util/ohlcv-agg.js';
import { resolvePoolSelection } from './util/pool-select.js';

export function attachWs(httpServer) {
  const wss = new WebSocketServer({ noServer: true });
  const channels = new Map(); // key -> Set(ws)

  function sub(ws, key) {
    if (!channels.has(key)) channels.set(key, new Set());
    channels.get(key).add(ws);
  }
  function unsub(ws) {
    for (const set of channels.values()) set.delete(ws);
  }
  function broadcast(key, payload) {
    const set = channels.get(key);
    if (!set) return;
    const msg = JSON.stringify(payload);
    for (const ws of set) if (ws.readyState === 1) ws.send(msg);
  }

  httpServer.on('upgrade', (req, socket, head) => {
    const { url } = req;
    if (!url.startsWith('/ws/')) {
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  });

  wss.on('connection', (ws, req) => {
    const url = new URL(req.url, 'http://localhost');
    const path = url.pathname;

    ws.on('message', async (raw) => {
      try {
        const msg = JSON.parse(String(raw || '{}'));

        // ----- Candles -----
        if (path === '/ws/candles') {
          const { tokenId: ref, tf = '1m', priceSource = 'best', poolId, useAll, mode = 'price', unit = 'native', fill = 'prev', from, to } = msg;
          const tok = await resolveTokenId(ref);
          if (!tok) return ws.send(JSON.stringify({ type: 'error', error: 'token not found' }));
          const zigUsd = await getZigUsd();

          let selectedPoolId = null;
          if (!useAll) {
            const { pool } = await resolvePoolSelection(tok.token_id, { priceSource, poolId });
            selectedPoolId = pool?.pool_id ?? null;
          }

          const circRow = await DB.query('SELECT total_supply_base, exponent FROM tokens WHERE token_id=$1', [tok.token_id]);
          const exp = Number(circRow.rows[0]?.exponent || 6);
          const circ = circRow.rows[0]?.total_supply_base != null ? Number(circRow.rows[0].total_supply_base) / (10 ** exp) : null;

          const bars = await getCandles({
            tokenId: tok.token_id,
            poolId: selectedPoolId,
            useAll: !!useAll,
            tf: ensureTf(tf),
            from, to,
            zigUsd,
            mode, unit, fill, priceSource,
            circ,
          });

          const key = `candles:${tok.token_id}:${ensureTf(tf)}:${priceSource}:${selectedPoolId || 'all'}`;
          sub(ws, key);
          ws.send(JSON.stringify({ type: 'snapshot', key, data: bars, meta: { tf: ensureTf(tf), priceSource, poolId: selectedPoolId, fill } }));
        }

        // ----- Recent trades feed -----
        if (path === '/ws/trades') {
          const { tokenId, poolId, pair, unit = 'usd', minValue = 0 } = msg;
          const key = `trades:${tokenId || '-'}:${poolId || '-'}:${pair || '-'}:${unit}:${minValue}`;
          sub(ws, key);
          ws.send(JSON.stringify({ type: 'subscribed', key }));
        }
      } catch (e) {
        ws.send(JSON.stringify({ type: 'error', error: e.message }));
      }
    });

    ws.on('close', () => unsub(ws));
  });

  // Very simple pollers — replace with NOTIFY/LISTEN or queue in prod.

  // 1) push last minute candle close snapshots (per active candle channel)
  setInterval(async () => {
    for (const key of channels.keys()) {
      if (!key.startsWith('candles:')) continue;
      const [, tokenId, tf, priceSource, poolKey] = key.split(':');
      const tokId = Number(tokenId);
      const zigUsd = await getZigUsd();
      const useAll = poolKey === 'all';
      let poolId = null;

      if (!useAll) {
        poolId = Number(poolKey);
      }
      const circRow = await DB.query('SELECT total_supply_base, exponent FROM tokens WHERE token_id=$1', [tokId]);
      const exp = Number(circRow.rows[0]?.exponent || 6);
      const circ = circRow.rows[0]?.total_supply_base != null ? Number(circRow.rows[0].total_supply_base) / (10 ** exp) : null;

      const now = new Date();
      const from = new Date(now.getTime() - 1000 * 60 * 60 * 24); // last 24h snapshot
      const bars = await getCandles({
        tokenId: tokId,
        poolId,
        useAll,
        tf,
        from: from.toISOString(),
        to: now.toISOString(),
        zigUsd,
        mode: 'price',
        unit: 'native',
        fill: 'prev',
        priceSource,
        circ,
      });
      broadcast(key, { type: 'snapshot', key, data: bars.slice(-200) });
    }
  }, 10_000);

  // 2) recent trades pusher (every 3s)
  setInterval(async () => {
    const since = new Date(Date.now() - 30_000).toISOString();
    const rows = await DB.query(`
      SELECT t.*, p.pair_contract, p.is_uzig_quote, q.exponent AS qexp, b.symbol AS base_symbol, q.symbol AS quote_symbol
      FROM trades t
      JOIN pools p ON p.pool_id=t.pool_id
      JOIN tokens b ON b.token_id=p.base_token_id
      JOIN tokens q ON q.token_id=p.quote_token_id
      WHERE t.action='swap' AND t.created_at >= $1
      ORDER BY t.created_at DESC
      LIMIT 500
    `, [since]);

    for (const r of rows.rows) {
      const payload = {
        type: 'trade',
        data: {
          ts: r.created_at,
          txHash: r.tx_hash,
          poolId: r.pool_id,
          pairContract: r.pair_contract,
          baseSymbol: r.base_symbol,
          quoteSymbol: r.quote_symbol,
          direction: r.direction,
          offerDenom: r.offer_asset_denom,
          askDenom: r.ask_asset_denom,
          signer: r.signer,
        }
      };
      for (const [key, set] of channels.entries()) {
        if (!key.startsWith('trades:')) continue;
        // naive matching: you can refine to actually filter token/pool
        for (const ws of set) if (ws.readyState === 1) ws.send(JSON.stringify(payload));
      }
    }
  }, 3_000);

  info('ws attached: /ws/candles, /ws/trades');
}
