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

const app = express();
app.use(cors());
app.use(express.json());
app.use(morgan('dev'));

app.get('/health', (req, res) => res.json({ ok: true }));

app.use('/tokens', tokensRouter);            // GET /tokens, /tokens/:id, ...
app.use('/trades', tradesRouter);            // GET /trades, /trades/wallet/:addr, /trades/large, /leaderboard/profitable
app.use('/swap', swapRouter);                // GET /swap?from=...&to=...
app.use('/watchlist', watchlistRouter);      // GET/POST/DELETE watchlist
app.use('/alerts', alertsRouter);            // CRUD alerts

const PORT = parseInt(process.env.API_PORT || '8003', 10);
app.listen(PORT, () => info(`api listening on :${PORT}`));
