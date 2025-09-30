// core/checkpoint.js
import { DB } from '../lib/db.js';

export async function readCheckpoint() {
  const { rows } = await DB.query(`SELECT last_height FROM index_state WHERE id='block'`);
  return rows[0]?.last_height || null;
}

export async function writeCheckpoint(h) {
  await DB.query(`
    INSERT INTO index_state(id, last_height) VALUES ('block', $1)
    ON CONFLICT (id) DO UPDATE SET last_height = EXCLUDED.last_height, updated_at = now()`,
    [h]
  );
}
