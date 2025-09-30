// lib/pg_notify.js
import { DB } from './db.js';
import { info, warn } from './log.js';

// allow only simple channel names (identifiers) to avoid injection via identifier context
const CHAN_RX = /^[a-z_][a-z0-9_]*$/i;

/**
 * Send a Postgres NOTIFY <channel> with JSON payload (safe & parametric).
 */
export async function pgNotify(channel, payload) {
  try {
    if (!CHAN_RX.test(channel)) throw new Error(`invalid channel: ${channel}`);
    // Use the function form to keep both channel and payload parameterized.
    const json = JSON.stringify(payload ?? {});
    await DB.query('SELECT pg_notify($1, $2)', [channel, json]);
    // optional: uncomment if you want to see every notify
    // info('[pgNotify] sent', channel, json);
  } catch (e) {
    warn('[pgNotify]', channel, e.message);
  }
}

/**
 * Listen on a NOTIFY channel. Returns a connected client you should keep alive.
 * If the connection drops, it auto-retries.
 */
export async function pgListen(channel, onMessage) {
  if (!CHAN_RX.test(channel)) throw new Error(`invalid channel: ${channel}`);

  async function connectAndListen() {
    const client = await DB.connect();
    client.on('notification', (msg) => {
      if (msg.channel !== channel) return;
      try {
        const data = msg.payload ? JSON.parse(msg.payload) : null;
        onMessage?.(data);
      } catch (e) {
        warn('[pgListen] payload parse', e.message);
      }
    });
    client.on('error', (e) => warn('[pgListen/client]', e.message));
    // Identifiers cannot be parameterized; we validated the channel above.
    await client.query(`LISTEN ${channel}`);
    info('[pgListen] listening on', channel);
    return client;
  }

  let client = await connectAndListen();

  // Keep-alive / auto-reconnect loop
  (async function keepAlive() {
    while (true) {
      try {
        await client.query('SELECT 1'); // ping
      } catch (e) {
        warn('[pgListen] reconnecting…', e.message);
        try { client.release?.(); } catch {}
        client = await connectAndListen();
      }
      await new Promise(r => setTimeout(r, 10_000));
    }
  })().catch(()=>{});

  return client;
}
