// core/tokens.js
import { DB } from '../lib/db.js';
import { lcdDenomsMetadata, lcdFactoryDenom, lcdIbcDenomTrace } from '../lib/lcd.js';
import { warn, debug } from '../lib/log.js';
import { fetch } from 'undici';

// Optional: registry toggle (defaults ON if the package is installed)
const USE_CHAIN_REGISTRY = (process.env.USE_CHAIN_REGISTRY || '1') === '1';

// CJS default export; we’ll only import if USE_CHAIN_REGISTRY is true
let REGISTRY = null;
let ZIG_ASSET_LIST = null;

/* ──────────────────────────────────────────────────────────────────────────────
 * Tiny utils
 * ────────────────────────────────────────────────────────────────────────────── */

export async function upsertTokenMinimal(denom) {
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

function deriveFromBaseDenom(base) {
  if (typeof base !== 'string') return null;
  const m = base.match(/^u([a-z0-9]+)$/i);
  if (m) {
    const core = m[1];
    return { symbol: core.toUpperCase(), display: core.toLowerCase(), exponent: 0 };
  }
  return { symbol: base.toUpperCase(), display: base.toLowerCase(), exponent: 0 };
}

// from LCD metadata shape
function expFromDisplay(meta) {
  if (!meta || !meta.display || !Array.isArray(meta.denom_units)) return null;
  const dus = meta.denom_units;
  const byDenom = dus.find(u => u?.denom === meta.display && typeof u.exponent === 'number');
  if (byDenom) return byDenom.exponent;
  const byAlias = dus.find(
    u => Array.isArray(u.aliases) && u.aliases.includes(meta.display) && typeof u.exponent === 'number'
  );
  if (byAlias) return byAlias.exponent;
  return null;
}

function looksLikeJsonUrl(u = '') {
  try { return /\.json$/i.test(new URL(u).pathname); } catch { return false; }
}
function pickIcon(obj) { return obj?.icon || obj?.image || obj?.logo || null; }
function normString(x) { if (typeof x !== 'string') return null; const s = x.trim(); return s || null; }
function normUrl(x) { return normString(x); }

async function resolveUriPayload(uri) {
  if (!uri) return { image_uri: null, website: null, twitter: null, telegram: null, description: null, kind: null };
  try {
    const r = await fetch(uri, { headers: { accept: 'application/json, image/*;q=0.9, */*;q=0.5' } });
    const ct = String(r.headers.get('content-type') || '').toLowerCase();

    if (ct.startsWith('image/')) {
      return { image_uri: uri, website: null, twitter: null, telegram: null, description: null, kind: 'image' };
    }
    if (ct.includes('application/json') || looksLikeJsonUrl(uri)) {
      const j = await r.json().catch(() => null);
      if (j && typeof j === 'object') {
        return {
          image_uri: normUrl(pickIcon(j)),
          website: normUrl(j.website),
          twitter: normUrl(j.twitter),
          telegram: normUrl(j.telegram),
          description: normString(j.description),
          kind: 'json'
        };
      }
    }
    return { image_uri: null, website: null, twitter: null, telegram: null, description: null, kind: 'other' };
  } catch {
    return { image_uri: null, website: null, twitter: null, telegram: null, description: null, kind: 'error' };
  }
}

/* ──────────────────────────────────────────────────────────────────────────────
 * Chain Registry (ZigChain) helpers
 * ────────────────────────────────────────────────────────────────────────────── */

function getUnitsFromRegistryAsset(a) {
  // registry uses snake_case: denom_units
  if (Array.isArray(a?.denom_units)) return a.denom_units;
  // (just in case) some shapes may vary
  if (Array.isArray(a?.denomUnits)) return a.denomUnits;
  return [];
}

function exponentFromRegistryDisplay(a) {
  const display = a?.display ?? null;
  const units = getUnitsFromRegistryAsset(a);
  if (!display || units.length === 0) return null;
  const match =
    units.find(u => (u?.denom === display) && (u?.exponent != null)) ||
    units.find(u => Array.isArray(u?.aliases) && u.aliases.includes(display) && (u?.exponent != null));
  const exp = match?.exponent;
  const n = typeof exp === 'string' ? Number(exp) : exp;
  return Number.isFinite(n) ? n : null;
}

function firstRegistryImage(a) {
  return (
    a?.logo_URIs?.png ||
    a?.logo_URIs?.svg ||
    a?.images?.[0]?.png ||
    a?.images?.[0]?.svg ||
    null
  );
}

async function ensureRegistryLoaded() {
  if (!USE_CHAIN_REGISTRY || ZIG_ASSET_LIST) return;
  try {
    const mod = await import('chain-registry');
    REGISTRY = mod?.default ?? mod ?? {};
    const lists = Array.isArray(REGISTRY.assetLists) ? REGISTRY.assetLists : [];
    // try exact chain_name: 'zigchain'
    ZIG_ASSET_LIST =
      lists.find(l => String(l?.chain_name ?? '').toLowerCase() === 'zigchain') ||
      // fallback: the one that contains base 'uzig' or symbol includes 'zig'
      lists.find(l => Array.isArray(l?.assets) && l.assets.some(a => a?.base === 'uzig')) ||
      lists.find(l => Array.isArray(l?.assets) && l.assets.some(a => {
        const sym = String(a?.symbol ?? '').toLowerCase();
        const disp = String(a?.display ?? '').toLowerCase();
        return sym.includes('zig') || disp.includes('zig');
      })) ||
      null;
    if (!ZIG_ASSET_LIST) {
      debug('[registry] ZigChain asset list not found in installed chain-registry');
    }
  } catch (e) {
    debug('[registry] not available:', e.message);
  }
}

function findRegistryAsset(denomOrBase) {
  if (!ZIG_ASSET_LIST || !denomOrBase) return null;
  const q = String(denomOrBase).toLowerCase();
  const list = ZIG_ASSET_LIST.assets || [];

  // 1) exact base match (handles ibc/<HASH> & native base like uzig)
  let a = list.find(x => String(x?.base ?? '').toLowerCase() === q);
  if (a) return a;

  // 2) any denom_unit.denom or alias == q (e.g. 'stzig', 'usdc', the ibc hash, etc.)
  a = list.find(x => getUnitsFromRegistryAsset(x).some(du => {
    if (String(du?.denom ?? '').toLowerCase() === q) return true;
    if (Array.isArray(du?.aliases) && du.aliases.map(y => String(y).toLowerCase()).includes(q)) return true;
    return false;
  }));
  if (a) return a;

  // 3) symbol match
  a = list.find(x => String(x?.symbol ?? '').toLowerCase() === q);
  return a || null;
}

/* ──────────────────────────────────────────────────────────────────────────────
 * MAIN: setTokenMetaFromLCD → now merges Registry + LCD (supplies from LCD)
 * ────────────────────────────────────────────────────────────────────────────── */

/**
 * setTokenMetaFromLCD:
 *  - IBC trace resolution
 *  - Merge sources:
 *     * Registry (ZigChain) for name/symbol/display/denom_units/exponent/image/socials/description
 *     * LCD metadata for same (fallback) + LCD uri-json (preferred socials/desc)
 *     * Supplies (max/total) from LCD factory only
 *  - Keep your existing non-overwrite behavior (no null clobber)
 */
export async function setTokenMetaFromLCD(denom) {
  try {
    // Ensure description column exists (idempotent)
    await DB.query(`ALTER TABLE IF EXISTS tokens ADD COLUMN IF NOT EXISTS description TEXT`).catch(() => {});

    // ── IBC handling ──────────────────────────────────────────────────────────
    let lookupDenom = denom;
    let isIbc = false;
    let baseFromTrace = null;

    if (typeof denom === 'string' && denom.startsWith('ibc/')) {
      isIbc = true;
      const trace = await lcdIbcDenomTrace(denom).catch(() => null);
      baseFromTrace = trace?.denom?.base || null;  // e.g. 'uatom' for ATOM
      if (baseFromTrace) lookupDenom = baseFromTrace;
      await DB.query(`UPDATE tokens SET type='ibc' WHERE denom=$1`, [denom]).catch(() => {});
    }

    // ── LCD metadata ─────────────────────────────────────────────────────────
    const meta = await lcdDenomsMetadata(lookupDenom).catch(() => null);
    const m = meta?.metadata;

    let lcd_name    = m?.name ?? null;
    let lcd_symbol  = m?.symbol ?? null;
    let lcd_display = m?.display ?? null;
    let lcdDesc     = m?.description ?? null;
    let lcd_uri     = m?.uri ?? null;

    // decide exponent from LCD once
    let lcd_exponent = expFromDisplay(m);

    // IBC fallback: if LCD exponent missing, default to 6
    if (isIbc && typeof lcd_exponent !== 'number') lcd_exponent = 6;

    // Non-IBC fallback heuristics
    if (!isIbc && lcd_exponent == null) {
      const d = deriveFromBaseDenom(lookupDenom);
      if (d) {
        if (!lcd_symbol)  lcd_symbol  = d.symbol;
        if (!lcd_display) lcd_display = d.display;
        lcd_exponent = d.exponent;
      }
      if (lcd_exponent == null) lcd_exponent = 0;
    }

    // IBC: if no display at all, show traced base visibly
    if (!lcd_display && isIbc && baseFromTrace) lcd_display = baseFromTrace;

    // resolve LCD uri payload (preferred socials/description if present)
    let imageFromUri = null, siteFromUri = null, twFromUri = null, tgFromUri = null, descFromUri = null;
    if (lcd_uri) {
      const r = await resolveUriPayload(lcd_uri);
      imageFromUri = r.image_uri || (r.kind === 'image' ? lcd_uri : null);
      siteFromUri  = r.website;
      twFromUri    = r.twitter;
      tgFromUri    = r.telegram;
      descFromUri  = r.description;
    }

    // ── Registry metadata (ZigChain AssetList) ───────────────────────────────
    let reg_name = null, reg_symbol = null, reg_display = null, reg_exponent = null;
    let reg_image = null, reg_desc = null, reg_site = null, reg_twitter = null, reg_telegram = null;

    if (USE_CHAIN_REGISTRY) {
      await ensureRegistryLoaded();
      if (ZIG_ASSET_LIST) {
        // If IBC, prefer to match the exact ibc/<HASH>; otherwise match base or unit by denom.
        const key = isIbc ? denom : lookupDenom;
        const a = findRegistryAsset(key) || (isIbc && baseFromTrace ? findRegistryAsset(baseFromTrace) : null);
        if (a) {
          reg_name    = a?.name ?? null;
          reg_symbol  = a?.symbol ?? null;
          reg_display = a?.display ?? null;
          reg_exponent = exponentFromRegistryDisplay(a);
          reg_image   = firstRegistryImage(a);

          // Registry descriptions/socials
          reg_desc    = a?.description || a?.extended_description || null;
          const soc   = a?.socials || null;
          reg_site    = soc?.website || null;
          reg_twitter = soc?.twitter || null;
          reg_telegram= soc?.telegram || null;
        }
      }
    }

    // ── Merge policy ─────────────────────────────────────────────────────────
    // name/symbol/display: prefer registry if non-empty else LCD.
    // Special casing: if both registry.display and LCD.display exist and differ only by case -> keep LCD casing.
    function sameLettersDiffCase(a, b) {
      if (!a || !b) return false;
      return a.toLowerCase() === b.toLowerCase() && a !== b;
    }

    let name   = reg_name   || lcd_name || null;
    let symbol = (reg_symbol || lcd_symbol || null);
    if (symbol) symbol = String(symbol).toUpperCase();

    let display = reg_display || lcd_display || null;
    if (reg_display && lcd_display && sameLettersDiffCase(reg_display, lcd_display)) {
      display = lcd_display; // LCD casing wins when text is same
    }

    // exponent: prefer registry (curated), else LCD; keep your defaults already applied above
    let exponent = (reg_exponent != null) ? reg_exponent : lcd_exponent;

    // image: prefer registry logo; if none, take LCD uri image
    const image_uri = reg_image || imageFromUri || null;

    // description: LCD uri json first, then registry desc, then LCD description
    const finalDesc = normString(descFromUri) || normString(reg_desc) || normString(lcdDesc) || null;

    // socials: LCD uri json first, then registry socials
    const website  = normUrl(siteFromUri)  || normUrl(reg_site)  || null;
    const twitter  = normUrl(twFromUri)    || normUrl(reg_twitter) || null;
    const telegram = normUrl(tgFromUri)    || normUrl(reg_telegram) || null;

    // ── Final DB update (no null clobbering) ─────────────────────────────────
    await DB.query(`
      UPDATE tokens
      SET name        = COALESCE($2,  name),
          symbol      = COALESCE($3,  symbol),
          display     = COALESCE($4,  display),
          exponent    = COALESCE($5,  exponent),
          image_uri   = COALESCE($6,  image_uri),
          description = COALESCE($7,  description),
          website     = COALESCE($8,  website),
          twitter     = COALESCE($9,  twitter),
          telegram    = COALESCE($10, telegram),
          type        = CASE WHEN $11::boolean THEN 'ibc' ELSE type END
      WHERE denom=$1
    `, [
      denom,
      name,
      symbol,
      display,
      exponent,
      image_uri,
      finalDesc,
      website,
      twitter,
      telegram,
      isIbc
    ]);

    // ── Supplies from factory (when available) ───────────────────────────────
    const fact = await lcdFactoryDenom(lookupDenom).catch(() => null);
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
