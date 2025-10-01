// jobs/token-security.js
import { DB } from '../lib/db.js';
import { info, warn } from '../lib/log.js';
import { fetch } from 'undici';

const SECURITY_SCAN_SEC = parseInt(process.env.SECURITY_SCAN_SEC || '180', 10);
const ZIGSCAN_BASE = process.env.ZIGSCAN_BASE || 'https://public-zigchain-lcd.numia.xyz/';

async function ensureSchema() {
  await DB.query(`
    CREATE TABLE IF NOT EXISTS token_security (
      token_id               BIGINT PRIMARY KEY REFERENCES tokens(token_id),
      denom                  TEXT NOT NULL UNIQUE,
      is_mintable            BOOLEAN,
      can_change_minting_cap BOOLEAN,
      max_supply_base        NUMERIC(78,0),
      total_supply_base      NUMERIC(78,0),
      creator_address        TEXT,
      creator_balance_base   NUMERIC(78,0),
      creator_pct_of_max     NUMERIC(20,8),
      top10_pct_of_max       NUMERIC(20,8),
      holders_count          BIGINT,
      first_seen_at          TIMESTAMPTZ,
      risk_flags             JSONB,
      checked_at             TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_token_security_checked ON token_security(checked_at DESC);
  `);
}

function digitsOrZero(x) {
  const s = String(x ?? '0');
  return /^\d+$/.test(s) ? s : '0';
}

async function getFactoryDenom(denom) {
  const url = `${ZIGSCAN_BASE}/zigchain/factory/denom/${encodeURIComponent(denom)}`;
  const r = await fetch(url, { headers: { accept: 'application/json' } });
  if (!r.ok) throw new Error(`zigscan ${r.status}`);
  return r.json();
}

async function getCreatorBalance(creatorAddr, denom) {
  const url = `${ZIGSCAN_BASE}/cosmos/bank/v1beta1/balances/${encodeURIComponent(creatorAddr)}/by_denom?denom=${encodeURIComponent(denom)}`;
  const r = await fetch(url, { headers: { accept: 'application/json' } });
  if (!r.ok) throw new Error(`bank/balances ${r.status}`);
  return r.json();
}

async function top10ShareOfMax(tokenId, denom, maxSupplyBase) {
  const maxN = Number(maxSupplyBase || 0);
  if (!maxN) return { top10Pct: 0, holdersCount: 0 };

  const [{ rows: holdersCountRows }, { rows: topRows }] = await Promise.all([
    DB.query(`SELECT holders_count FROM token_holders_stats WHERE token_id=$1`, [tokenId]),
    DB.query(`
      SELECT balance_base::NUMERIC AS bal FROM holders
      WHERE token_id=$1 AND balance_base::NUMERIC > 0
      ORDER BY balance_base::NUMERIC DESC
      LIMIT 10
    `, [tokenId])
  ]);

  const holdersCount = Number(holdersCountRows?.[0]?.holders_count || 0);
  const topSum = topRows.reduce((acc, r) => acc + Number(r.bal || 0), 0);
  const top10Pct = maxN > 0 ? (topSum / maxN) * 100 : 0;

  return { top10Pct, holdersCount };
}

async function firstSeenAtFromHolders(tokenId) {
  const { rows } = await DB.query(`
    SELECT MIN(updated_at) AS first_seen FROM holders WHERE token_id=$1
  `, [tokenId]);
  return rows[0]?.first_seen || null;
}

function deriveRiskFlags({ isMintable, canChangeCap, creatorPct, top10Pct }) {
  return {
    creator_gt_50: creatorPct >= 50,
    top10_gt_50: top10Pct >= 50,
    can_mint_more: !!isMintable,
    can_change_mint_cap: !!canChangeCap
  };
}

/** ➕ One-shot for fast-track */
export async function scanTokenOnce(tokenId, denom) {
  if (!tokenId || !denom) return;
  await ensureSchema();
  try {
    const fd = await getFactoryDenom(denom).catch(() => null);

    const isMintable = !!(fd && (Number(fd.max_supply || 0) > Number(fd.total_supply || 0)));
    const canChangeCap = !!fd?.can_change_minting_cap;
    const maxSupplyBase = digitsOrZero(fd?.max_supply);
    const totalSupplyBase = digitsOrZero(fd?.total_supply);
    const creatorAddr = fd?.creator || null;

    let creatorBalBase = '0';
    if (creatorAddr) {
      const cb = await getCreatorBalance(creatorAddr, denom).catch(() => null);
      creatorBalBase = digitsOrZero(cb?.balance?.amount);
    }
    const creatorPct = Number(maxSupplyBase) > 0
      ? (Number(creatorBalBase) / Number(maxSupplyBase)) * 100
      : 0;

    const { top10Pct, holdersCount } = await top10ShareOfMax(tokenId, denom, maxSupplyBase);
    const firstSeenAt = await firstSeenAtFromHolders(tokenId);
    const riskFlags = deriveRiskFlags({ isMintable, canChangeCap, creatorPct, top10Pct });

    await DB.query(`
      INSERT INTO token_security (
        token_id, denom, is_mintable, can_change_minting_cap,
        max_supply_base, total_supply_base,
        creator_address, creator_balance_base,
        creator_pct_of_max, top10_pct_of_max,
        holders_count, first_seen_at, risk_flags, checked_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, now()
      )
      ON CONFLICT (token_id) DO UPDATE SET
        is_mintable            = EXCLUDED.is_mintable,
        can_change_minting_cap = EXCLUDED.can_change_minting_cap,
        max_supply_base        = EXCLUDED.max_supply_base,
        total_supply_base      = EXCLUDED.total_supply_base,
        creator_address        = EXCLUDED.creator_address,
        creator_balance_base   = EXCLUDED.creator_balance_base,
        creator_pct_of_max     = EXCLUDED.creator_pct_of_max,
        top10_pct_of_max       = EXCLUDED.top10_pct_of_max,
        holders_count          = EXCLUDED.holders_count,
        first_seen_at          = COALESCE(token_security.first_seen_at, EXCLUDED.first_seen_at),
        risk_flags             = EXCLUDED.risk_flags,
        checked_at             = now()
    `, [
      tokenId, denom, isMintable, canChangeCap,
      maxSupplyBase, totalSupplyBase, creatorAddr, creatorBalBase,
      creatorPct, top10Pct, holdersCount, firstSeenAt, riskFlags
    ]);

    info('[security/once]', denom, {
      mintable: isMintable,
      changeCap: canChangeCap,
      creatorPct: Number(creatorPct.toFixed(4)),
      top10Pct: Number(top10Pct.toFixed(4)),
      holders: holdersCount
    });
  } catch (e) {
    warn('[security/once]', denom, e.message);
  }
}

export function startTokenSecurityScanner() {
  (async function loop() {
    await ensureSchema();
    while (true) {
      try {
        const { rows: toks } = await DB.query(`
          SELECT token_id, denom
          FROM tokens
          ORDER BY token_id DESC
        `);
        for (const t of toks) {
          await scanTokenOnce(t.token_id, t.denom);
        }
      } catch (e) {
        warn('[security-scan]', e.message);
      }
      await new Promise(r => setTimeout(r, SECURITY_SCAN_SEC * 1000));
    }
  })().catch(() => {});
}
