// core/block-processor.js
import { getBlock, getBlockResults, unwrapBlock, unwrapBlockResults } from '../lib/rpc.js';
import { info, warn } from '../lib/log.js';
import { upsertPool, poolWithTokens } from './pools.js';
import { setTokenMetaFromLCD } from './tokens.js';
import { insertTrade } from './trades.js';
import { upsertPoolState } from './pool_state.js';
import { upsertOHLCV1m } from './ohlcv.js';
import {
  digitsOrNull, wasmByAction, byType, buildMsgSenderMap, normalizePair,
  classifyDirection, parseReservesKV, parseAssetsList, toDisp, sha256hex
} from './parse.js';

// === Tunables (env overrides) ===
const FACTORY_ADDR = process.env.FACTORY_ADDR || '';
const ROUTER_ADDR  = process.env.ROUTER_ADDR || null;
const BLOCK_PROC_CONCURRENCY = Number(process.env.BLOCK_PROC_CONCURRENCY || 12); // 8–16 is healthy
const MAX_PENDING_TASKS      = Number(process.env.BLOCK_PROC_MAX_TASKS || 5000); // soft back-pressure per block

// === Simple bounded-concurrency runner ===
async function runWithConcurrency(tasks, limit = BLOCK_PROC_CONCURRENCY) {
  const results = [];
  let i = 0;
  const workers = Array(Math.min(limit, tasks.length)).fill(0).map(async () => {
    while (i < tasks.length) {
      const idx = i++;
      try {
        results[idx] = await tasks[idx]();
      } catch (e) {
        results[idx] = e;
      }
    }
  });
  await Promise.all(workers);
  return results;
}

// === Lightweight per-process caches ===
const poolsByContract = new Map();         // pairContract -> { pool_id, base_denom, quote_denom, base_exp, quote_exp, is_uzig_quote }
const metaFetchedDenoms = new Set();       // avoid spamming LCD for token meta

async function getPoolCached(pairContract) {
  if (!pairContract) return null;
  if (poolsByContract.has(pairContract)) return poolsByContract.get(pairContract);
  const p = await poolWithTokens(pairContract);
  if (p) poolsByContract.set(pairContract, p);
  return p;
}

function rememberTokenMetaOnce(denom) {
  if (!denom || metaFetchedDenoms.has(denom)) return null;
  metaFetchedDenoms.add(denom);
  return () => setTokenMetaFromLCD(denom);
}

export async function processHeight(h) {
  info('PROCESS BLOCK →', h);

  const [blkJson, resJson] = await Promise.all([ getBlock(h), getBlockResults(h) ]);
  const blk = unwrapBlock(blkJson);
  const res = unwrapBlockResults(resJson);
  if (!blk || !blk.header) throw new Error('block: missing header');

  const txs = blk.txs || [];
  const hashes = txs.map(sha256hex);
  const txResults = res.txs_results || [];
  const timestamp = blk.header.time;

  // tasks to run with bounded concurrency
  const tasks = [];
  // low-priority tasks (LCD/meta) executed after core writes are scheduled
  const lowPrioTasks = [];

  // Collect pair contracts to prefetch pools for this block
  const pairContractsToPrefetch = new Set();

  const N = Math.max(txResults.length, hashes.length);
  for (let i = 0; i < N; i++) {
    const txr = txResults[i] || { events: [] };
    const tx_hash = hashes[i] || null;

    const wasms    = byType(txr.events, 'wasm');
    const insts    = byType(txr.events, 'instantiate');
    const executes = byType(txr.events, 'execute');
    const msgs     = byType(txr.events, 'message');
    const msgSenderByIndex = buildMsgSenderMap(msgs);

    // ===== create_pair
    const cps = wasmByAction(wasms, 'create_pair');
    for (const cp of cps) {
      if ((cp.m.get('_contract_address') || '').trim() !== FACTORY_ADDR) continue;

      const pairType = String(cp.m.get('pair_type') || 'xyk');
      const { base, quote } = normalizePair(cp.m.get('pair'));

      const reg = wasms.find(w => w.m.get('action') === 'register' && w.m.get('_contract_address') === FACTORY_ADDR);
      const poolAddr = reg?.m.get('pair_contract_addr') || insts.at(-1)?.m.get('_contract_address');
      if (!poolAddr) { warn('create_pair: could not find pool addr'); continue; }

      const signer = msgSenderByIndex.get(Number(cp.m.get('msg_index'))) || null;

      tasks.push(async () => {
        await upsertPool({
          pairContract: poolAddr,
          baseDenom: base, quoteDenom: quote,
          pairType, createdAt: timestamp, height: h, txHash: tx_hash, signer
        });
        // Prime the pool cache on creation to avoid an extra round-trip later in the block
        const p = await poolWithTokens(poolAddr);
        if (p) poolsByContract.set(poolAddr, p);
      });

      const f1 = rememberTokenMetaOnce(base);
      const f2 = rememberTokenMetaOnce(quote);
      if (f1) lowPrioTasks.push(f1);
      if (f2) lowPrioTasks.push(f2);
    }

    // ===== swaps
    const swaps = wasmByAction(wasms, 'swap');
    for (let idx = 0; idx < swaps.length; idx++) {
      const s = swaps[idx];
      const pairContract = s.m.get('_contract_address');
      if (!pairContract) continue;
      pairContractsToPrefetch.add(pairContract);

      // parse once; DB ops & price calc deferred to tasks
      const offer = s.m.get('offer_asset') || s.m.get('offer_asset_denom');
      const ask   = s.m.get('ask_asset')   || s.m.get('ask_asset_denom');

      const offerAmt = digitsOrNull(s.m.get('offer_amount'));
      const askAmt   = digitsOrNull(s.m.get('ask_amount'));
      const retAmt   = digitsOrNull(s.m.get('return_amount'));

      // reserves (two possible encodings)
      let res1d = s.m.get('reserve_asset1_denom') || s.m.get('asset1_denom') || null;
      let res1a = digitsOrNull(s.m.get('reserve_asset1_amount') || s.m.get('asset1_amount'));
      let res2d = s.m.get('reserve_asset2_denom') || s.m.get('asset2_denom') || null;
      let res2a = digitsOrNull(s.m.get('reserve_asset2_amount') || s.m.get('asset2_amount'));
      const reservesStr = s.m.get('reserves');
      if ((!res1d || !res1a || !res2d || !res2a) && reservesStr) {
        const kv = parseReservesKV(reservesStr);
        if (kv?.[0]) { res1d = res1d ?? kv[0].denom; res1a = res1a ?? digitsOrNull(kv[0].amount_base); }
        if (kv?.[1]) { res2d = res2d ?? kv[1].denom; res2a = res2a ?? digitsOrNull(kv[1].amount_base); }
      }

      const msgIndex = Number(s.m.get('msg_index') || idx);
      const signerEOA = msgSenderByIndex.get(msgIndex) || null;

      const poolSwapSender = s.m.get('sender') || null;
      const routerExec = !!ROUTER_ADDR && executes.some(e => e.m.get('_contract_address') === ROUTER_ADDR && Number(e.m.get('msg_index')||-1) === msgIndex);
      const isRouter = !!(ROUTER_ADDR && (poolSwapSender === ROUTER_ADDR || routerExec));

      tasks.push(async () => {
        const pool = await getPoolCached(pairContract);
        if (!pool) { warn(`[swap] unknown pool ${pairContract}`); return; }

        await insertTrade({
          pool_id: pool.pool_id, pair_contract: pairContract,
          action: 'swap', direction: classifyDirection(offer, pool.quote_denom),
          offer_asset_denom: offer, offer_amount_base: offerAmt,
          ask_asset_denom: ask,   ask_amount_base: askAmt,
          return_amount_base: retAmt, is_router: isRouter,
          reserve_asset1_denom: res1d, reserve_asset1_amount_base: res1a,
          reserve_asset2_denom: res2d, reserve_asset2_amount_base: res2a,
          height: h, tx_hash, signer: signerEOA, msg_index: msgIndex, created_at: timestamp
        });

        await upsertPoolState(
          pool.pool_id, pool.base_denom, pool.quote_denom, res1d, res1a, res2d, res2a
        );

        // OHLCV (native UZIG quote only)
        if (pool.is_uzig_quote && res1d && res2d) {
          let Rb=0, Rq=0;
          if (res1d === pool.base_denom) { Rb = Number(res1a || 0); Rq = Number(res2a || 0); }
          else if (res2d === pool.base_denom) { Rb = Number(res2a || 0); Rq = Number(res1a || 0); }
          if (Rb>0 && Rq>0) {
            const price = (Rq / Math.pow(10, pool.quote_exp)) / (Rb / Math.pow(10, pool.base_exp));
            const volZig = (offer === pool.quote_denom)
              ? toDisp(offerAmt, pool.quote_exp)
              : toDisp(retAmt,   pool.quote_exp);
            const bucket = new Date(Math.floor(new Date(timestamp).getTime() / 60000) * 60000);
            await upsertOHLCV1m({ pool_id: pool.pool_id, bucket_start: bucket, price, vol_zig: volZig, trade_inc: 1 });
          }
        }
      });
    }

    // ===== liquidity (provide / withdraw)
    const provides = wasmByAction(wasms, 'provide_liquidity');
    const withdraws = wasmByAction(wasms, 'withdraw_liquidity');
    const liqs = [...provides, ...withdraws];

    for (let li = 0; li < liqs.length; li++) {
      const le = liqs[li];
      const pairContract = le.m.get('_contract_address');
      if (!pairContract) continue;
      pairContractsToPrefetch.add(pairContract);

      let res1d = le.m.get('reserve_asset1_denom');
      let res1a = digitsOrNull(le.m.get('reserve_asset1_amount'));
      let res2d = le.m.get('reserve_asset2_denom');
      let res2a = digitsOrNull(le.m.get('reserve_asset2_amount'));
      if ((!res1d || !res1a || !res2d || !res2a) && le.m.get('assets')) {
        const parsed = parseAssetsList(le.m.get('assets'));
        if (parsed?.a1) { res1d = res1d ?? parsed.a1.denom; res1a = res1a ?? digitsOrNull(parsed.a1.amount_base); }
        if (parsed?.a2) { res2d = res2d ?? parsed.a2.denom; res2a = res2a ?? digitsOrNull(parsed.a2.amount_base); }
      }

      const msgIndex = Number(le.m.get('msg_index') || li);
      const signerEOA = msgSenderByIndex.get(msgIndex) || null;

      tasks.push(async () => {
        const pool = await getPoolCached(pairContract);
        if (!pool) return;

        const action = (le.m.get('action') === 'provide_liquidity' ? 'provide' : 'withdraw');
        await insertTrade({
          pool_id: pool.pool_id, pair_contract: pairContract,
          action, direction: action,
          offer_asset_denom: null, offer_amount_base: null,
          ask_asset_denom: null,   ask_amount_base: null,
          return_amount_base: digitsOrNull(le.m.get('share')),
          is_router: false,
          reserve_asset1_denom: res1d, reserve_asset1_amount_base: res1a,
          reserve_asset2_denom: res2d, reserve_asset2_amount_base: res2a,
          height: h, tx_hash, signer: signerEOA, msg_index: msgIndex, created_at: timestamp
        });

        await upsertPoolState(
          pool.pool_id, pool.base_denom, pool.quote_denom, res1d, res1a, res2d, res2a
        );
      });
    }

    // cheap back-pressure: if this tx added too many tasks, flush a chunk early
    if (tasks.length >= MAX_PENDING_TASKS) {
      await runWithConcurrency(tasks.splice(0));
    }
  }

  // Prefetch pools discovered in this block to avoid N round-trips inside task lambdas
  if (pairContractsToPrefetch.size > 0) {
    await runWithConcurrency(
      Array.from(pairContractsToPrefetch, (pc) => async () => { if (!poolsByContract.has(pc)) { const p = await poolWithTokens(pc); if (p) poolsByContract.set(pc, p); } }),
      Math.min(BLOCK_PROC_CONCURRENCY, 24)
    );
  }

  // Execute core DB tasks with bounded concurrency
  if (tasks.length > 0) {
    await runWithConcurrency(tasks);
  }

  // Execute low-priority LCD/meta touches (also bounded)
  if (lowPrioTasks.length > 0) {
    await runWithConcurrency(lowPrioTasks, Math.min(4, BLOCK_PROC_CONCURRENCY)); // keep light
  }
}
