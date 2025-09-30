// jobs/partitions.js
import { DB } from '../lib/db.js';
import { warn, debug } from '../lib/log.js';

const PARTITIONS_SEC = parseInt(process.env.PARTITIONS_SEC || '1800', 10); // 30m
const PARTITION_MONTHS_AHEAD = parseInt(process.env.PARTITION_MONTHS_AHEAD || '3', 10);

function monthRange(ym) {
  const [y, m] = ym.split('-').map(Number);
  const from = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0));
  const to = new Date(Date.UTC(y, m, 1, 0, 0, 0));
  return { from, to };
}
function fmtYYYYMM(d) {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}`;
}

async function ensureMonthlyPartition(parent, ym) {
  const { from, to } = monthRange(ym);
  const child = `${parent}_${ym.replace('-','_')}`;
  const sql = `
    DO $$
    BEGIN
      IF to_regclass('${child}') IS NULL THEN
        EXECUTE format(
          'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
          '${child}', '${parent}', to_timestamp(${from.getTime()/1000}), to_timestamp(${to.getTime()/1000})
        );
      END IF;
    EXCEPTION WHEN undefined_table THEN
      NULL;
    END$$;
  `;
  await DB.query(sql);
}

export function startPartitionsMaintainer() {
  (async function loop () {
    while (true) {
      try {
        const now = new Date();
        for (let k = 0; k <= PARTITION_MONTHS_AHEAD; k++) {
          const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth()+k, 1));
          const ym = fmtYYYYMM(d);
          await Promise.all([
            ensureMonthlyPartition('trades', ym),
            ensureMonthlyPartition('price_ticks', ym),
            ensureMonthlyPartition('ohlcv_1m', ym),
            ensureMonthlyPartition('leaderboard_traders', ym),
          ]);
        }
        debug('[partitions] ensured');
      } catch (e) { warn('[partitions]', e.message); }
      await new Promise(r => setTimeout(r, PARTITIONS_SEC * 1000));
    }
  })().catch(()=>{});
}
