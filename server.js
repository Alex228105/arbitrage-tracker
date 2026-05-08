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

function addHistory(pairId, utexPrice, hlPrice) {
  if (!history[pairId]) history[pairId] = [];
  history[pairId].push({ ts: Date.now(), utex: utexPrice, hl: hlPrice });
  if (history[pairId].length > MAX_HISTORY) history[pairId].splice(0, history[pairId].length - MAX_HISTORY);
}

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

// ── UTEX ─────────────────────────────────────────────────────────
// Real message format observed:
// t=7, d: { i: subId, d: { moment: ts, markPrice: "4241000000", price: "4241000000", ... } }
// Prices are in microdollars (× 10^6)
// Heartbeat: t=5, reply t=6

let utexWs = null;
let utexReady = false;
let utexSubId = 1;
const utexSubIds = {};   // sym -> id
const utexIdSymbol = {}; // id -> sym
let utexSymbolMap = {};  // sym -> [pairId, ...]

function buildUtexSymbolMap() {
  utexSymbolMap = {};
  pairs.forEach(p => {
    if (!utexSymbolMap[p.utexSymbol]) utexSymbolMap[p.utexSymbol] = [];
    utexSymbolMap[p.utexSymbol].push(p.id);
  });
}

function connectUtex() {
  if (utexWs) { try { utexWs.terminate(); } catch {} }
  utexWs = new WebSocket('wss://ususdt-api-margin.utex.io/ws');

  utexWs.on('open', () => {
    utexReady = true;
    console.log('[UTEX] connected');
    Object.keys(utexSymbolMap).forEach(sym => subscribeUtex(sym));
  });

  utexWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      // Heartbeat
      if (msg.t === 5) {
        utexWs.send(JSON.stringify({ t: 6, d: msg.d }));
        return;
      }
      // Price update (t=7) or snapshot (t=1)
      // d: { i: subscriptionId, d: { markPrice: "...", price: "...", moment: ... } }
      if ((msg.t === 7 || msg.t === 1) && msg.d && msg.d.i !== undefined) {
        const sym = utexIdSymbol[msg.d.i];
        if (!sym) return;
        const inner = msg.d.d || {};
        // markPrice and price are in microdollars
        const raw = inner.markPrice || inner.price;
        if (raw) {
          const price = parseFloat(raw) / 1e6;
          if (price > 0) updateUtexPrice(sym, price);
        }
      }
    } catch (e) { console.error('[UTEX] parse error:', e.message); }
  });

  utexWs.on('close', () => {
    utexReady = false;
    console.log('[UTEX] disconnected, reconnect in 3s');
    setTimeout(connectUtex, 3000);
  });

  utexWs.on('error', e => console.error('[UTEX] error:', e.message));
}

function subscribeUtex(sym) {
  if (!utexReady || !utexWs || utexWs.readyState !== WebSocket.OPEN) return;
  if (utexSubIds[sym]) return;
  const id = utexSubId++;
  utexSubIds[sym] = id;
  utexIdSymbol[id] = sym;
  // t=3 = subscribe request based on observed protocol
  const msg = { t: 3, d: { i: id, s: sym } };
  console.log('[UTEX] subscribe:', JSON.stringify(msg));
  utexWs.send(JSON.stringify(msg));
}

function updateUtexPrice(sym, price) {
  const pairIds = utexSymbolMap[sym] || [];
  pairIds.forEach(pid => {
    if (!prices[pid]) prices[pid] = {};
    prices[pid].utex = { price, ts: Date.now() };
    const hl = prices[pid].hl;
    if (hl) {
      addHistory(pid, price, hl.price);
      broadcast({ type: 'price', pairId: pid, utex: price, hl: hl.price, ts: Date.now() });
    }
  });
}

// ── Hyperliquid ───────────────────────────────────────────────────
let hlWs = null;
let hlReady = false;
const hlSubscribed = new Set();

function connectHyperliquid() {
  if (hlWs) { try { hlWs.terminate(); } catch {} }
  hlWs = new WebSocket('wss://api.hyperliquid.xyz/ws');

  hlWs.on('open', () => {
    hlReady = true;
    console.log('[HL] connected');
    const syms = new Set(pairs.map(p => p.hlSymbol));
    syms.forEach(subscribeHL);
  });

  hlWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      // Trade feed: { channel: 'trades', data: [{coin, px, ...}] }
      if (msg.channel === 'trades' && msg.data) {
        const list = Array.isArray(msg.data) ? msg.data : [msg.data];
        list.forEach(t => { if (t.coin && t.px) updateHLPrice(t.coin, parseFloat(t.px)); });
      }
      // AllMids: { channel: 'allMids', data: { mids: { BTC: '95000', ... } } }
      if (msg.channel === 'allMids' && msg.data && msg.data.mids) {
        Object.entries(msg.data.mids).forEach(([coin, px]) => updateHLPrice(coin, parseFloat(px)));
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

function updateHLPrice(sym, price) {
  if (!price || isNaN(price)) return;
  pairs.filter(p => p.hlSymbol === sym).forEach(p => {
    if (!prices[p.id]) prices[p.id] = {};
    prices[p.id].hl = { price, ts: Date.now() };
    const utex = prices[p.id].utex;
    if (utex) {
      addHistory(p.id, utex.price, price);
      broadcast({ type: 'price', pairId: p.id, utex: utex.price, hl: price, ts: Date.now() });
    }
  });
}

// ── REST API ──────────────────────────────────────────────────────
app.get('/api/pairs', (req, res) => res.json(pairs));

app.post('/api/pairs', (req, res) => {
  const { name, utexSymbol, hlSymbol } = req.body;
  if (!name || !utexSymbol || !hlSymbol) return res.status(400).json({ error: 'Missing fields' });
  const id = Date.now().toString();
  const pair = { id, name, utexSymbol: utexSymbol.toUpperCase(), hlSymbol: hlSymbol.toUpperCase() };
  pairs.push(pair);
  savePairs();
  buildUtexSymbolMap();
  if (utexReady) subscribeUtex(pair.utexSymbol);
  if (hlReady) subscribeHL(pair.hlSymbol);
  broadcast({ type: 'pairs', pairs });
  res.json(pair);
});

app.delete('/api/pairs/:id', (req, res) => {
  pairs = pairs.filter(p => p.id !== req.params.id);
  delete prices[req.params.id];
  delete history[req.params.id];
  savePairs();
  buildUtexSymbolMap();
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
  buildUtexSymbolMap();
  connectUtex();
  connectHyperliquid();
});
