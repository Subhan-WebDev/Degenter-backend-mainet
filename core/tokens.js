// core/tokens.js
import { DB } from '../lib/db.js';
import { lcdDenomsMetadata, lcdFactoryDenom, lcdIbcDenomTrace } from '../lib/lcd.js';
import { warn } from '../lib/log.js';

export async function upsertTokenMinimal(denom) {
  // New tokens start with exponent=0 (table default might be 6, we override explicitly)
  const { rows } = await DB.query(
    `INSERT INTO tokens(denom, exponent) VALUES ($1, 0)
     ON CONFLICT (denom) DO NOTHING
     RETURNING token_id`,
    [denom]
  );
  if (rows[0]) return rows[0].token_id;
  const r2 = await DB.query(`SELECT token_id FROM tokens WHERE denom=$1`, [denom]);
  return r2.rows[0]?.token_id || null;
}

// Heuristic fallback (used only when metadata/display absent):
// If denom looks like "uusdc" / "uatom", set display/symbol and exponent=0 (keep base as default).
function deriveFromBaseDenom(base) {
  if (typeof base !== 'string') return null;
  const m = base.match(/^u([a-z0-9]+)$/i);
  if (m) {
    const core = m[1];
    return { symbol: core.toUpperCase(), display: core.toLowerCase(), exponent: 0 };
  }
  return { symbol: base.toUpperCase(), display: base.toLowerCase(), exponent: 0 };
}

// Strict rule: exponent is the exponent of the denom unit that equals `display`,
// or whose aliases include `display`. If not found, return null.
function expFromDisplay(meta) {
  if (!meta || !meta.display || !Array.isArray(meta.denom_units)) return null;

  const dus = meta.denom_units;
  // 1) exact denom match
  const byDenom = dus.find(u => u?.denom === meta.display && typeof u.exponent === 'number');
  if (byDenom) return byDenom.exponent;

  // 2) alias match (rare, but honor if present)
  const byAlias = dus.find(u =>
    Array.isArray(u.aliases) && u.aliases.includes(meta.display) && typeof u.exponent === 'number'
  );
  if (byAlias) return byAlias.exponent;

  return null;
}

/**
 * setTokenMetaFromLCD:
 * - If denom is IBC (ibc/...), resolve trace to base denom for metadata lookup
 * - Exponent = exponent of denom unit whose denom/alias equals `metadata.display` (CAN be 0)
 * - If metadata/display missing → derive heuristically (exponent 0)
 * - Also updates name/symbol/display/uri and supply (if factory info exists)
 */
export async function setTokenMetaFromLCD(denom) {
  try {
    let lookupDenom = denom;
    let isIbc = false;
    let baseFromTrace = null;

    // Resolve IBC trace to base denom for metadata lookup; mark token type=ibc
    if (typeof denom === 'string' && denom.startsWith('ibc/')) {
      isIbc = true;
      const trace = await lcdIbcDenomTrace(denom).catch(() => null);
      baseFromTrace = trace?.denom?.base || null;
      if (baseFromTrace) lookupDenom = baseFromTrace;
      await DB.query(`UPDATE tokens SET type='ibc' WHERE denom=$1`, [denom]).catch(() => {});
    }

    const meta = await lcdDenomsMetadata(lookupDenom).catch(() => null);
    const m = meta?.metadata;

    let name    = m?.name    ?? null;
    let symbol  = m?.symbol  ?? null;
    let display = m?.display ?? null;

    // >>> THE RULE: pull exponent from the unit that matches `display` (or alias)
    let exponent = expFromDisplay(m);

    // Fallbacks when missing metadata or display or exponent:
    if (exponent == null) {
      const baseForDerive = baseFromTrace || lookupDenom;
      const d = deriveFromBaseDenom(baseForDerive);
      if (d) {
        if (!symbol)  symbol  = d.symbol;
        if (!display) display = d.display;
        exponent = d.exponent; // heuristic default is 0
      }
    }

    // If still null (super edge cases), store 0 (never synthesize 6 here).
    if (exponent == null) exponent = 0;

    // For IBC: if display is still null, show the base denom from trace for transparency
    if (!display && isIbc && baseFromTrace) display = baseFromTrace;

    await DB.query(`
      UPDATE tokens
      SET name=$2, symbol=$3, display=$4, exponent=$5, image_uri=COALESCE($6, image_uri),
          type = CASE WHEN $7::boolean THEN 'ibc' ELSE type END
      WHERE denom=$1
    `, [
      denom,
      name,
      symbol,
      display,
      exponent,        // can be 0 or 6 (or other), exactly per display rule
      m?.uri || null,
      isIbc
    ]);

    // Factory stats if available (usually N/A for IBC)
    const fact = await lcdFactoryDenom(lookupDenom).catch(()=>null);
    if (fact && (fact.total_supply || fact.total_minted)) {
      await DB.query(
        `UPDATE tokens
           SET max_supply_base   = $2::NUMERIC,
               total_supply_base = $3::NUMERIC
         WHERE denom=$1`,
        [denom, fact.max_supply || fact.minting_cap || null, fact.total_supply || fact.total_minted || null]
      );
    }
  } catch (e) {
    warn(`[tokenMeta] ${denom} → ${e.message}`);
  }
}
