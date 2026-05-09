const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const path = require('path');
const fs = require('fs');

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

// ── Bybit WebSocket ───────────────────────────────────────────────
let bybitWs = null;
let bybitReady = false;
const bybitSubscribed = new Set();
const bybitSymbolMap = {}; // symbol -> [pairId]

function buildBybitSymbolMap() {
  Object.keys(bybitSymbolMap).forEach(k => delete bybitSymbolMap[k]);
  pairs.forEach(p => {
    if (!bybitSymbolMap[p.p1Symbol]) bybitSymbolMap[p.p1Symbol] = [];
    bybitSymbolMap[p.p1Symbol].push(p.id);
  });
}

function connectBybit() {
  if (bybitWs) { try { bybitWs.terminate(); } catch {} }
  bybitWs = new WebSocket('wss://stream.bybit.com/v5/public/linear');

  bybitWs.on('open', () => {
    bybitReady = true;
    console.log('[Bybit] connected');
    const syms = new Set(pairs.map(p => p.p1Symbol));
    syms.forEach(subscribeBybit);
    // Heartbeat every 20s
    setInterval(() => {
      if (bybitWs && bybitWs.readyState === WebSocket.OPEN) {
        bybitWs.send(JSON.stringify({ op: 'ping' }));
      }
    }, 20000);
  });

  bybitWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.topic && msg.topic.startsWith('tickers.') && msg.data) {
        const sym = msg.topic.replace('tickers.', '');
        const price = parseFloat(msg.data.lastPrice);
        if (price) updateP1Price(sym, price);
      }
    } catch {}
  });

  bybitWs.on('close', () => {
    bybitReady = false;
    bybitSubscribed.clear();
    console.log('[Bybit] disconnected, reconnect in 3s');
    setTimeout(connectBybit, 3000);
  });

  bybitWs.on('error', e => console.error('[Bybit] error:', e.message));
}

function subscribeBybit(sym) {
  if (!bybitReady || !bybitWs || bybitWs.readyState !== WebSocket.OPEN) return;
  if (bybitSubscribed.has(sym)) return;
  bybitSubscribed.add(sym);
  bybitWs.send(JSON.stringify({ op: 'subscribe', args: [`tickers.${sym}`] }));
  console.log('[Bybit] subscribed:', sym);
}

function updateP1Price(sym, price) {
  const pairIds = bybitSymbolMap[sym] || [];
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
  buildBybitSymbolMap();
  if (bybitReady) subscribeBybit(pair.p1Symbol);
  if (hlReady) subscribeHL(pair.p2Symbol);
  broadcast({ type: 'pairs', pairs });
  res.json(pair);
});

app.delete('/api/pairs/:id', (req, res) => {
  pairs = pairs.filter(p => p.id !== req.params.id);
  delete prices[req.params.id];
  delete history[req.params.id];
  savePairs();
  buildBybitSymbolMap();
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
  buildBybitSymbolMap();
  connectBybit();
  connectHyperliquid();
});
