// DEAD PINES — servidor online (salas de 2 jogadores + mundo salvo)
// Sem dependências: só precisa do Node.js (12 ou mais novo).
// Uso: node server.js            (porta 8080)
//      PORT=9000 node server.js  (outra porta)
'use strict';
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = +(process.env.PORT || 8080);
const DATA = process.env.DATA_DIR || path.join(__dirname, 'mundos');
const MAX_MSG = 3 * 1024 * 1024;      // 3 MB por mensagem (o save do mundo é a maior)
const MAX_ROOMS = 300;
const MAX_CONN_PER_IP = 12;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

try { fs.mkdirSync(DATA, { recursive: true }); } catch (e) {}

const rooms = new Map();   // código -> { code, host, guests[], pid, slot, created }
const MAX_GUESTS = 3;      // dono + 3 amigos = 4 jogadores
function others(r, c) { const l = []; if (r.host && r.host !== c) l.push(r.host); for (const g of r.guests) if (g !== c) l.push(g); return l; }
const ipCount = new Map();
let nextId = 1;

function log() { const a = Array.prototype.slice.call(arguments); console.log(new Date().toISOString().slice(0, 19).replace('T', ' '), a.join(' ')); }

// ---------- WebSocket mínimo (RFC 6455) ----------
function wsFrame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeUInt32BE(Math.floor(len / 4294967296), 2); head.writeUInt32BE(len % 4294967296, 6); }
  head[0] = 0x80 | op;
  return Buffer.concat([head, payload]);
}

function makeClient(sock, ip) {
  const c = { id: nextId++, sock, ip, buf: Buffer.alloc(0), frag: null, fragOp: 0, alive: true, room: null, role: '', closed: false, pid: '', name: '' };
  c.send = function (obj) {
    if (c.closed) return;
    try { sock.write(wsFrame(1, Buffer.from(typeof obj === 'string' ? obj : JSON.stringify(obj)))); } catch (e) {}
  };
  c.sendRaw = function (text) { if (c.closed) return; try { sock.write(wsFrame(1, Buffer.from(text))); } catch (e) {} };
  c.close = function (code) {
    if (c.closed) return;
    try { const b = Buffer.alloc(2); b.writeUInt16BE(code || 1000, 0); sock.write(wsFrame(8, b)); } catch (e) {}
    c.closed = true; try { sock.end(); } catch (e) {}
    setTimeout(function () { try { sock.destroy(); } catch (e) {} }, 1500);
    onClose(c);
  };
  return c;
}

function parseFrames(c, chunk) {
  c.buf = c.buf.length ? Buffer.concat([c.buf, chunk]) : chunk;
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = b.readUInt32BE(2) * 4294967296 + b.readUInt32BE(6); off = 10; }
    if (len > MAX_MSG) { c.close(1009); return; }
    if (!masked) { c.close(1002); return; }
    if (b.length < off + 4 + len) return;
    const mask = b.slice(off, off + 4), data = Buffer.alloc(len);
    for (let i = 0; i < len; i++) data[i] = b[off + 4 + i] ^ mask[i & 3];
    c.buf = b.slice(off + 4 + len);
    if (op === 8) { c.close(1000); return; }
    if (op === 9) { try { c.sock.write(wsFrame(10, data)); } catch (e) {} continue; }
    if (op === 10) { c.alive = true; continue; }
    if (op === 0) {
      if (!c.frag) continue;
      c.frag.push(data);
      if (c.frag.reduce(function (a, x) { return a + x.length; }, 0) > MAX_MSG) { c.close(1009); return; }
      if (fin) { const all = Buffer.concat(c.frag); c.frag = null; if (c.fragOp === 1) onText(c, all.toString('utf8')); }
      continue;
    }
    if (op === 1 || op === 2) {
      if (!fin) { c.frag = [data]; c.fragOp = op; continue; }
      if (op === 1) onText(c, data.toString('utf8'));
    }
  }
}

// ---------- salas ----------
function newCode() {
  for (let t = 0; t < 50; t++) {
    let s = '';
    const r = crypto.randomBytes(5);
    for (let i = 0; i < 5; i++) s += CODE_CHARS[r[i] % CODE_CHARS.length];
    if (!rooms.has(s)) return s;
  }
  return null;
}
const okPid = function (p) { return typeof p === 'string' && /^[a-z0-9]{8,40}$/.test(p); };
const okSlot = function (n) { return n === 1 || n === 2 || n === 3; };
const worldFile = function (pid, slot) { return path.join(DATA, pid + '_' + slot + '.json'); };
function worldInfo(pid, slot) {
  try { const st = fs.statSync(worldFile(pid, slot)); return { savedAt: st.mtimeMs, size: st.size }; } catch (e) { return null; }
}
function saveWorld(pid, slot, world) {
  const f = worldFile(pid, slot), tmp = f + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify({ v: 1, savedAt: Date.now(), world: world }));
  fs.renameSync(tmp, f);
}

function onText(c, text) {
  c.alive = true;
  let o;
  try { o = JSON.parse(text); } catch (e) { return; }
  if (!o || typeof o.t !== 'string') return;
  const room = c.room;

  // repasse entre os dois jogadores (o mais comum: vai direto, sem reprocessar)
  if (o.t === 'r') {
    if (!room) return;
    // marca quem mandou (f) e repassa para todos os outros da sala
    const out = '{"f":' + c.id + ',' + text.slice(1);
    for (const x of others(room, c)) if (!x.closed) x.sendRaw(out);
    return;
  }
  if (o.t === 'host') {
    if (room) return c.send({ t: 'err', m: 'Você já está numa sala.' });
    if (!okPid(o.pid) || !okSlot(o.slot)) return c.send({ t: 'err', m: 'Dados inválidos.' });
    // dono voltando depois de cair a internet: retoma a mesma sala (mesmo código)
    if (o.resume) {
      const rr = rooms.get(String(o.resume));
      if (rr && rr.pid === o.pid && !rr.host) {
        clearTimeout(rr.graceT); rr.host = c; c.room = rr; c.role = 'host'; c.pid = o.pid; c.name = String(o.name || 'Dono').slice(0, 24);
        log('dono voltou para a sala', rr.code);
        c.send({ t: 'room', code: rr.code, world: worldInfo(rr.pid, rr.slot), resumed: true });
        for (const g of rr.guests) { g.send({ t: 'hostBack', id: c.id }); c.send({ t: 'peer', id: g.id, name: g.name }); }
        return;
      }
      return c.send({ t: 'err', m: 'O servidor fechou enquanto você estava sem internet. Crie de novo.' });
    }
    rooms.forEach(function (r) { if (r.pid === o.pid && !r.host) closeRoom(r); });
    if (rooms.size >= MAX_ROOMS) return c.send({ t: 'err', m: 'Servidor lotado. Tente mais tarde.' });
    // se esse jogador já tinha uma sala aberta (caiu a internet), fecha a antiga
    rooms.forEach(function (r) { if (r.pid === o.pid && r.host && r.host !== c) { r.host.leaving = true; r.host.close(4000); } });
    const code = newCode();
    if (!code) return c.send({ t: 'err', m: 'Tente de novo.' });
    const r = { code: code, host: c, guests: [], pid: o.pid, slot: o.slot, created: Date.now(), ver: String(o.ver || '').slice(0, 12), pub: o.pub !== false, day: Math.max(0, Math.min(99999, +o.day || 0)) };
    rooms.set(code, r); c.room = r; c.role = 'host'; c.pid = o.pid; c.name = String(o.name || 'Dono').slice(0, 24);
    log('sala aberta', code, 'por', c.name, c.ip);
    return c.send({ t: 'room', code: code, world: worldInfo(o.pid, o.slot) });
  }
  if (o.t === 'join') {
    if (room) return c.send({ t: 'err', m: 'Você já está numa sala.' });
    const code = String(o.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
    const r = rooms.get(code);
    if (!r) return c.send({ t: 'err', m: 'Servidor não encontrado. Confira o código (o dono precisa estar com o jogo aberto).' });
    if (r.guests.length >= MAX_GUESTS) return c.send({ t: 'err', m: 'Esse servidor está cheio (4 jogadores).' });
    if (!r.host) return c.send({ t: 'err', m: 'O dono desse servidor está reconectando. Tente de novo em instantes.' });
    r.guests.push(c); c.room = r; c.role = 'guest'; c.name = String(o.name || 'Amigo').slice(0, 24);
    log('entrou na sala', code, c.name, c.ip, '(' + (1 + r.guests.length) + ' jogadores)');
    c.send({ t: 'joined', code: code, host: r.host.name, hostId: r.host.id, id: c.id, ver: r.ver, peers: others(r, c).map(function (x) { return { id: x.id, name: x.name }; }) });
    for (const x of others(r, c)) x.send({ t: 'peer', id: c.id, name: c.name });
    return;
  }
  if (o.t === 'save') {
    // só o dono da sala salva o mundo dele
    if (!room || c.role !== 'host' || o.pid !== room.pid || !o.world || typeof o.world !== 'object') return;
    try { saveWorld(room.pid, room.slot, o.world); c.send({ t: 'saved', at: Date.now(), n: o.n || 0 }); }
    catch (e) { log('erro ao salvar', e.message); c.send({ t: 'err', m: 'Não consegui salvar o mundo no servidor.' }); }
    return;
  }
  if (o.t === 'load') {
    if (!okPid(o.pid) || !okSlot(o.slot)) return;
    let w = null;
    try { w = JSON.parse(fs.readFileSync(worldFile(o.pid, o.slot), 'utf8')); } catch (e) {}
    return c.send({ t: 'world', savedAt: w ? w.savedAt : 0, world: w ? w.world : null });
  }
  if (o.t === 'list') {
    // lista de servidores públicos com vaga (para o amigo só tocar e entrar)
    const out = [];
    rooms.forEach(function (r) { if (r.pub && r.guests.length < MAX_GUESTS && r.host && !r.host.closed) out.push({ code: r.code, name: r.host.name, ver: r.ver, day: r.day, n: 1 + r.guests.length, max: MAX_GUESTS + 1, age: Math.round((Date.now() - r.created) / 1000) }); });
    out.sort(function (a, b) { return a.age - b.age; });
    let total = 0; rooms.forEach(function (r) { total += (r.host ? 1 : 0) + r.guests.length; });
    return c.send({ t: 'list', rooms: out.slice(0, 60), online: total });
  }
  if (o.t === 'leave') { c.leaving = true; c.close(1000); return; }
  if (o.t === 'ping') return c.send({ t: 'pong' });
}

function closeRoom(r) {
  // dono saiu: a sala fecha (o mundo já foi salvo pelo jogo antes de sair)
  clearTimeout(r.graceT); rooms.delete(r.code);
  for (const g of r.guests) { g.send({ t: 'closed', m: 'O dono do servidor saiu. O servidor foi fechado e o mundo dele foi salvo.' }); g.room = null; }
  r.guests = [];
  log('sala fechada', r.code);
}
function onClose(c) {
  const n = (ipCount.get(c.ip) || 1) - 1; if (n <= 0) ipCount.delete(c.ip); else ipCount.set(c.ip, n);
  const r = c.room; c.room = null;
  if (!r) return;
  if (c.role === 'host') {
    if (r.host !== c) return;
    if (c.leaving) { closeRoom(r); return; }
    // caiu a internet do dono: segura a sala por 45 s esperando ele voltar
    r.host = null;
    for (const g of r.guests) g.send({ t: 'hostLost' });
    log('dono caiu, esperando', r.code);
    r.graceT = setTimeout(function () { if (!r.host) closeRoom(r); }, 45000);
  } else if (c.role === 'guest' && r.guests.indexOf(c) >= 0) {
    r.guests.splice(r.guests.indexOf(c), 1);
    for (const x of others(r, c)) x.send({ t: 'peerLeft', id: c.id });
    log('saiu da sala', r.code, c.name);
  }
}

// ---------- HTTP + upgrade ----------
const server = http.createServer(function (req, res) {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  let players = 0; rooms.forEach(function (r) { players += (r.host ? 1 : 0) + r.guests.length; });
  res.end('DEAD PINES — servidor online\nSalas abertas: ' + rooms.size + '\nJogadores: ' + players + '\n');
});

server.on('upgrade', function (req, sock) {
  // atrás de proxy (Render etc.) o IP real vem no X-Forwarded-For
  const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || (req.socket.remoteAddress || '').replace('::ffff:', '');
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  if ((ipCount.get(ip) || 0) >= MAX_CONN_PER_IP) { sock.destroy(); return; }
  ipCount.set(ip, (ipCount.get(ip) || 0) + 1);
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = makeClient(sock, ip);
  sock.on('data', function (d) { try { parseFrames(c, d); } catch (e) { c.close(1011); } });
  sock.on('close', function () { if (!c.closed) { c.closed = true; onClose(c); } });
  sock.on('error', function () {});
  c.send({ t: 'hello', srv: 'dead-pines', v: 1 });
  allClients.add(c);
  sock.on('close', function () { allClients.delete(c); });
});

// manter conexões vivas e derrubar quem sumiu
const allClients = new Set();
setInterval(function () {
  allClients.forEach(function (c) {
    if (c.closed) { allClients.delete(c); return; }
    if (!c.alive) { c.close(1001); return; }
    c.alive = false;
    try { c.sock.write(wsFrame(9, Buffer.alloc(0))); } catch (e) {}
  });
}, 25000);

server.listen(PORT, '0.0.0.0', function () { log('DEAD PINES servidor ouvindo na porta', PORT, '— mundos em', DATA); });
