// api/ws.js
import { WebSocketServer } from 'ws';
import { DB } from '../lib/db.js';
import { resolveTokenId, getZigUsd } from './util/resolve-token.js';

/* ---------- helpers shared with REST shaping ---------- */

function scale(base, exp, fallback = 6) {
  if (base == null) return null;
  const e = (exp == null ? fallback : Number(exp));
  return Number(base) / 10 ** e;
}

function shapeTradeRow(r, zigUsd) {
  const offerScaled  = scale(r.offer_amount_base,  r.offer_exp,  r.offer_asset_denom === 'uzig' ? 6 : 6);
  const askScaled    = scale(r.ask_amount_base,    r.ask_exp,    r.ask_asset_denom   === 'uzig' ? 6 : 6);
  const returnScaled = scale(r.return_amount_base, r.qexp, 6);

  let valueZig = null;
  if (r.is_uzig_quote) {
    if (r.direction === 'buy')      valueZig = scale(r.offer_amount_base,  r.qexp, 6);
    else if (r.direction === 'sell')valueZig = scale(r.return_amount_base, r.qexp, 6);
    else {
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
                  (r.offer_asset_denom === r.ask_asset_denom
                    ? r.offer_amount_base
                    : (r.offer_asset_denom === 'uzig' ? r.offer_amount_base
                      : (r.ask_asset_denom === 'uzig' ? r.ask_amount_base
                        : r.return_amount_base))),
                  r.qexp, 6
                ));
      valueZig = quoteAmt != null ? quoteAmt * qPrice : null;
    }
  }

  const valueUsd = valueZig != null ? valueZig * zigUsd : null;

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
    returnAmount: returnScaled,
    valueNative: valueZig,
    valueUsd
  };
}

/* ---------- topic hub ---------- */
// topic strings we support now:
// - 'trades.stream'
// - 'trades.stream.token:{idOrSymbolOrDenom}'
// - 'trades.stream.pair:{pairContract}'

export function startWS(server, { path = '/ws' } = {}) {
  const wss = new WebSocketServer({ server, path });
  const subs = new Map(); // topic -> Set<WebSocket>

  function addSub(ws, topic) {
    if (!subs.has(topic)) subs.set(topic, new Set());
    subs.get(topic).add(ws);
    ws._topics = ws._topics || new Set();
    ws._topics.add(topic);
  }

  function removeSub(ws, topic) {
    const set = subs.get(topic);
    if (set) { set.delete(ws); if (set.size === 0) subs.delete(topic); }
    if (ws._topics) ws._topics.delete(topic);
  }

  function broadcast(topic, msg) {
    const set = subs.get(topic);
    if (!set || set.size === 0) return;
    const data = JSON.stringify(msg);
    for (const ws of set) {
      if (ws.readyState === ws.OPEN) ws.send(data);
    }
  }

  // ping keepalive
  const HEARTBEAT_MS = 25000;
  setInterval(() => {
    for (const client of wss.clients) {
      if (client.readyState === client.OPEN) {
        try { client.ping(); } catch {}
      }
    }
  }, HEARTBEAT_MS);

  wss.on('connection', (ws) => {
    ws.send(JSON.stringify({ ok: true, hello: 'degenter-ws', path }));

    ws.on('message', async (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); }
      catch { return ws.send(JSON.stringify({ ok:false, error:'invalid_json' })); }

      const op = String(msg.op || '').toLowerCase();
      const topic = String(msg.topic || '');

      if (op === 'subscribe') {
        addSub(ws, topic);
        ws.send(JSON.stringify({ ok:true, subscribed: topic }));
        return;
      }
      if (op === 'unsubscribe') {
        removeSub(ws, topic);
        ws.send(JSON.stringify({ ok:true, unsubscribed: topic }));
        return;
      }
      ws.send(JSON.stringify({ ok:false, error:'unknown_op' }));
    });

    ws.on('close', () => {
      if (!ws._topics) return;
      for (const t of ws._topics) removeSub(ws, t);
    });
  });

  /* ---------- light poller to push recent trades ---------- */
  let lastSeenIso = null; // track latest created_at we’ve sent
  async function pumpTrades() {
    try {
      const zigUsd = await getZigUsd();
      // grab the freshest 200 trades since lastSeenIso (or last 10 minutes)
      const tsClause = lastSeenIso
        ? `t.created_at > $1::timestamptz`
        : `t.created_at >= now() - INTERVAL '10 minutes'`;

      const params = lastSeenIso ? [lastSeenIso] : [];
      const { rows } = await DB.query(`
        SELECT t.*, p.pair_contract, p.is_uzig_quote, q.exponent AS qexp,
               (SELECT price_in_zig FROM prices WHERE token_id = p.quote_token_id ORDER BY updated_at DESC LIMIT 1) AS pq_price_in_zig,
               toff.exponent AS offer_exp,
               task.exponent AS ask_exp
        FROM trades t
        JOIN pools  p ON p.pool_id = t.pool_id
        JOIN tokens q ON q.token_id = p.quote_token_id
        LEFT JOIN tokens toff ON toff.denom = t.offer_asset_denom
        LEFT JOIN tokens task ON task.denom = t.ask_asset_denom
        WHERE ${tsClause}
          AND t.action IN ('swap','provide','withdraw')
        ORDER BY t.created_at ASC
        LIMIT 200
      `, params);

      if (rows.length) {
        for (const r of rows) {
          const shaped = shapeTradeRow(r, zigUsd);
          // global
          broadcast('trades.stream', { type:'trade', data: shaped });

          // per-token (base token inferred via pair—do a quick lookup once)
          // We can optionally cache pair->base_token_id if needed.
          // Quick light join to discover base token for per-token topic:
          const bt = await DB.query(
            `SELECT b.symbol, b.denom, b.token_id
             FROM pools p
             JOIN tokens b ON b.token_id = p.base_token_id
             WHERE p.pair_contract=$1
             LIMIT 1`, [r.pair_contract]
          );
          if (bt.rows[0]) {
            const tok = bt.rows[0];
            const ids = new Set([String(tok.token_id), tok.symbol, tok.denom].filter(Boolean));
            for (const ref of ids) {
              broadcast(`trades.stream.token:${ref}`, { type:'trade', data: shaped });
            }
          }

          // per-pair
          broadcast(`trades.stream.pair:${r.pair_contract}`, { type:'trade', data: shaped });

          lastSeenIso = r.created_at; // advance watermark
        }
      }
    } catch (e) {
      // soft-log to console; keep loop alive
      // console.error('pumpTrades error', e);
    } finally {
      setTimeout(pumpTrades, 2000); // run again
    }
  }
  pumpTrades();

  return wss;
}
