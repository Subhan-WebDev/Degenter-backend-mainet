// api/server.js
import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import { info } from '../lib/log.js';

import tokensRouter from './routes/tokens.js';
import tradesRouter from './routes/trades.js';
import swapRouter from './routes/swap.js';
import watchlistRouter from './routes/watchlist.js';
import alertsRouter from './routes/alerts.js';

// NEW: ws
import { attachWs } from './ws.js';

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(morgan('dev'));

app.get('/health', (req, res) => res.json({ ok: true }));

app.use('/tokens', tokensRouter);     // GET /tokens, /tokens/:id, /:id/pools, /:id/ohlcv, /swap-list, ...
app.use('/trades', tradesRouter);     // GET /trades, /trades/token/:id, /trades/pool/:ref, /trades/large, /trades/recent, ...
app.use('/swap', swapRouter);
app.use('/watchlist', watchlistRouter);
app.use('/alerts', alertsRouter);

const PORT = parseInt(process.env.API_PORT || '8003', 10);
const server = app.listen(PORT, () => info(`api listening on :${PORT}`));

// WebSockets
attachWs(server);
