const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const path = require('path');
const fs = require('fs');
const https = require('https');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PAIRS_FILE = path.join(__dirname, 'pairs.json');
let pairs = [];
try { pairs = JSON.parse(fs.readFileSync(PAIRS_FILE, 'utf8')); } catch { pairs = []; }

function savePairs() { fs.writeFileSync(PAIRS_FILE, JSON.stringify(pairs, null, 2)); }

const prices = {};
const history = {};
const MAX_HISTORY = 5000;

function addHistory(pairId, p1Price, p2Price) {
  if (!history[pairId]) history[pairId] = [];
  history[pairId].push({ ts: Date.now(), p1: p1Price, p2: p2Price });
  if (history[pairId].length > MAX_HISTORY) history[pairId].splice(0, history[pairId].length - MAX_HISTORY);
}

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

// ── Yahoo Finance (polling every 5s) ─────────────────────────────
function fetchYahooPrice(symbol) {
  return new Promise((resolve, reject) => {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1m&range=1d`;
    const options = {
      hostname: 'query1.finance.yahoo.com',
      path: `/v8/finance/chart/${symbol}?interval=1m&range=1d`,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          const result = json?.chart?.result?.[0];
          const price = result?.meta?.regularMarketPrice;
          if (price) resolve(parseFloat(price));
          else reject(new Error('No price in response'));
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.setTimeout(5000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

// Poll Yahoo for all yahoo symbols every 5 seconds
const yahooSymbols = {}; // symbol -> [pairId, ...]

function buildYahooSymbolMap() {
  Object.keys(yahooSymbols).forEach(k => delete yahooSymbols[k]);
  pairs.forEach(p => {
    if (!yahooSymbols[p.p1Symbol]) yahooSymbols[p.p1Symbol] = [];
    yahooSymbols[p.p1Symbol].push(p.id);
  });
}

async function pollYahoo() {
  const symbols = Object.keys(yahooSymbols);
  for (const sym of symbols) {
    try {
      const price = await fetchYahooPrice(sym);
      updateP1Price(sym, price);
      console.log(`[Yahoo] ${sym} = ${price}`);
    } catch (e) {
      console.error(`[Yahoo] ${sym} error:`, e.message);
    }
  }
}

function updateP1Price(sym, price) {
  const pairIds = yahooSymbols[sym] || [];
  pairIds.forEach(pid => {
    if (!prices[pid]) prices[pid] = {};
    prices[pid].p1 = { price, ts: Date.now() };
    const p2 = prices[pid].p2;
    if (p2) {
      addHistory(pid, price, p2.price);
      broadcast({ type: 'price', pairId: pid, p1: price, p2: p2.price, ts: Date.now() });
    }
  });
}

setInterval(pollYahoo, 5000);

// ── Hyperliquid WebSocket ─────────────────────────────────────────
let hlWs = null;
let hlReady = false;
const hlSubscribed = new Set();

function connectHyperliquid() {
  if (hlWs) { try { hlWs.terminate(); } catch {} }
  hlWs = new WebSocket('wss://api.hyperliquid.xyz/ws');

  hlWs.on('open', () => {
    hlReady = true;
    console.log('[HL] connected');
    const syms = new Set(pairs.map(p => p.p2Symbol));
    syms.forEach(subscribeHL);
  });

  hlWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.channel === 'trades' && msg.data) {
        const list = Array.isArray(msg.data) ? msg.data : [msg.data];
        list.forEach(t => { if (t.coin && t.px) updateP2Price(t.coin, parseFloat(t.px)); });
      }
      if (msg.channel === 'allMids' && msg.data && msg.data.mids) {
        Object.entries(msg.data.mids).forEach(([coin, px]) => updateP2Price(coin, parseFloat(px)));
      }
    } catch {}
  });

  hlWs.on('close', () => {
    hlReady = false;
    console.log('[HL] disconnected, reconnect in 3s');
    setTimeout(connectHyperliquid, 3000);
  });

  hlWs.on('error', e => console.error('[HL] error:', e.message));
}

function subscribeHL(sym) {
  if (!hlReady || !hlWs || hlWs.readyState !== WebSocket.OPEN) return;
  if (hlSubscribed.has(sym)) return;
  hlSubscribed.add(sym);
  hlWs.send(JSON.stringify({ method: 'subscribe', subscription: { type: 'trades', coin: sym } }));
  console.log('[HL] subscribed:', sym);
}

function updateP2Price(sym, price) {
  if (!price || isNaN(price)) return;
  pairs.filter(p => p.p2Symbol === sym).forEach(p => {
    if (!prices[p.id]) prices[p.id] = {};
    prices[p.id].p2 = { price, ts: Date.now() };
    const p1 = prices[p.id].p1;
    if (p1) {
      addHistory(p.id, p1.price, price);
      broadcast({ type: 'price', pairId: p.id, p1: p1.price, p2: price, ts: Date.now() });
    }
  });
}

// ── REST API ──────────────────────────────────────────────────────
app.get('/api/pairs', (req, res) => res.json(pairs));

app.post('/api/pairs', (req, res) => {
  const { name, p1Symbol, p2Symbol } = req.body;
  if (!name || !p1Symbol || !p2Symbol) return res.status(400).json({ error: 'Missing fields' });
  const id = Date.now().toString();
  const pair = { id, name, p1Symbol: p1Symbol.toUpperCase(), p2Symbol: p2Symbol.toUpperCase() };
  pairs.push(pair);
  savePairs();
  buildYahooSymbolMap();
  if (hlReady) subscribeHL(pair.p2Symbol);
  broadcast({ type: 'pairs', pairs });
  res.json(pair);
});

app.delete('/api/pairs/:id', (req, res) => {
  pairs = pairs.filter(p => p.id !== req.params.id);
  delete prices[req.params.id];
  delete history[req.params.id];
  savePairs();
  buildYahooSymbolMap();
  broadcast({ type: 'pairs', pairs });
  res.json({ ok: true });
});

app.get('/api/history/:pairId', (req, res) => res.json(history[req.params.pairId] || []));
app.get('/api/prices', (req, res) => res.json(prices));
app.post('/api/alert', (req, res) => res.json({ ok: true }));

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'pairs', pairs }));
  ws.send(JSON.stringify({ type: 'prices', prices }));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server on port ${PORT}`);
  buildYahooSymbolMap();
  connectHyperliquid();
  setTimeout(pollYahoo, 2000);
});
