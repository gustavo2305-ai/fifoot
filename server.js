const http = require('http');
const path = require('path');
const express = require('express');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const ROOM_TTL_MS = 6 * 60 * 60 * 1000;
const HOST_GRACE_MS = 45000;

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '512kb' }));

const rooms = new Map();

function summarizePublicRooms() {
  const list = [];
  for (const entry of rooms.values()) {
    const snap = entry.snapshot;
    if (!snap || snap.stage !== 'lobby') continue;
    if (!snap.rules || snap.rules.privacy !== 'PUBLIC') continue;
    const hostMgr = Array.isArray(snap.managers)
      ? snap.managers.find((m) => m.id === snap.hostId)
      : null;
    list.push({
      code: entry.code,
      title: snap.title || 'Sala Fifoot',
      universe: snap.rules.universe || 'BRASIL',
      format: snap.rules.format || 'GROUPS_KNOCKOUT',
      hostName: hostMgr ? hostMgr.name : 'Host',
      hostClubId: hostMgr ? hostMgr.clubId : 'spfc',
      count: Array.isArray(snap.managers) ? snap.managers.length : 1
    });
  }
  return list;
}

function sendSafe(ws, payload) {
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify(payload)); } catch (err) {}
  }
}

function broadcastPublicLobbyDirectory(wss) {
  const payload = { type: 'PUBLIC_ROOMS_UPDATED', rooms: summarizePublicRooms() };
  for (const client of wss.clients) {
    if (!client._fifootRoomCode) sendSafe(client, payload);
  }
}

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    activeRooms: rooms.size,
    uptimeSeconds: Math.round(process.uptime())
  });
});

app.get('/api/rooms', (_req, res) => {
  res.json({ rooms: summarizePublicRooms() });
});

app.use((_req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 256 * 1024 });

function clearHostGrace(entry) {
  if (entry && entry.hostGraceTimer) {
    clearTimeout(entry.hostGraceTimer);
    entry.hostGraceTimer = null;
  }
}

function closeRoomAndNotify(code, reason) {
  const entry = rooms.get(code);
  if (!entry) return;
  clearHostGrace(entry);
  for (const gWs of entry.guests.values()) {
    sendSafe(gWs, { packet: { type: 'ROOM_CLOSED', code, reason } });
  }
  rooms.delete(code);
  broadcastPublicLobbyDirectory(wss);
}

function detachSocketFromRoom(ws, explicitLeave = false) {
  const code = ws._fifootRoomCode;
  const clientId = ws._fifootClientId;
  if (!code || !rooms.has(code)) return;

  const entry = rooms.get(code);
  ws._fifootRoomCode = null;

  if (entry.hostClientId === clientId && entry.hostWs === ws) {
    entry.hostWs = null;
    if (explicitLeave) {
      closeRoomAndNotify(code, 'O host encerrou a sala.');
      return;
    }

    // Notify guests that Host temporarily dropped & wait 45s for HOST_RECONNECT
    for (const gWs of entry.guests.values()) {
      sendSafe(gWs, { packet: { type: 'HOST_TEMPORARILY_OFFLINE', code } });
    }
    clearHostGrace(entry);
    entry.hostGraceTimer = setTimeout(() => {
      const current = rooms.get(code);
      if (!current || (current.hostWs && current.hostWs.readyState === 1)) return;

      const nextGuestEntry = [...current.guests.entries()].find(([, gWs]) => gWs && gWs.readyState === 1);
      if (nextGuestEntry && current.snapshot && current.snapshot.stage === 'lobby') {
        const [newHostId, newHostWs] = nextGuestEntry;
        current.guests.delete(newHostId);
        current.hostClientId = newHostId;
        current.hostWs = newHostWs;
        current.snapshot.managers = (current.snapshot.managers || []).filter((m) => m.id !== clientId);
        current.snapshot.hostId = newHostId;
        const newHostMgr = current.snapshot.managers.find((m) => m.id === newHostId);
        const notice = (newHostMgr ? newHostMgr.name : 'Novo técnico') + ' assumiu como host da sala.';

        sendSafe(newHostWs, { type: 'HOST_PROMOTED', code, newHostId, notice });
        const syncPacket = { type: 'ROOM_SYNC', code, room: current.snapshot, toastMsg: notice };
        sendSafe(newHostWs, { packet: syncPacket });
        for (const gWs of current.guests.values()) {
          sendSafe(gWs, { type: 'HOST_PROMOTED', code, newHostId, notice });
          sendSafe(gWs, { packet: syncPacket });
        }
        broadcastPublicLobbyDirectory(wss);
      } else {
        closeRoomAndNotify(code, 'O host não reconectou após 45 segundos.');
      }
    }, HOST_GRACE_MS);
    return;
  }

  if (entry.guests.get(clientId) === ws) {
    entry.guests.delete(clientId);
    if (entry.hostWs && entry.hostWs.readyState === 1) {
      sendSafe(entry.hostWs, { packet: { type: 'GUEST_LEAVE', code, clientId } });
    }
    if (entry.snapshot && entry.snapshot.stage === 'lobby') {
      broadcastPublicLobbyDirectory(wss);
    }
  }
}

wss.on('connection', (ws) => {
  ws._isAlive = true;
  ws.on('pong', () => { ws._isAlive = true; });

  ws.on('message', (raw) => {
    let msg = null;
    try { msg = JSON.parse(String(raw)); } catch (err) { return; }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'HOST_CREATE_ROOM') {
      detachSocketFromRoom(ws, true);
      const code = String(msg.code || '').trim().toUpperCase();
      if (!code || !msg.room) return;
      ws._fifootRoomCode = code;
      ws._fifootClientId = msg.clientId;
      rooms.set(code, {
        code,
        hostClientId: msg.clientId,
        hostWs: ws,
        hostGraceTimer: null,
        guests: new Map(),
        snapshot: msg.room,
        updatedAt: Date.now()
      });
      broadcastPublicLobbyDirectory(wss);
      return;
    }

    if (msg.type === 'HOST_RECONNECT') {
      const code = String(msg.code || '').trim().toUpperCase();
      const entry = rooms.get(code);
      if (entry && entry.hostClientId === msg.clientId) {
        clearHostGrace(entry);
        ws._fifootRoomCode = code;
        ws._fifootClientId = msg.clientId;
        entry.hostWs = ws;
        if (msg.room) entry.snapshot = msg.room;
        entry.updatedAt = Date.now();
        for (const gWs of entry.guests.values()) {
          sendSafe(gWs, { packet: { type: 'HOST_RECONNECTED', code } });
          sendSafe(gWs, { packet: { type: 'ROOM_SYNC', code, room: entry.snapshot } });
        }
      } else if (code && msg.room && msg.clientId) {
        // Server restarted (e.g. cold wake): re-seed the room transparently!
        ws._fifootRoomCode = code;
        ws._fifootClientId = msg.clientId;
        rooms.set(code, {
          code,
          hostClientId: msg.clientId,
          hostWs: ws,
          hostGraceTimer: null,
          guests: new Map(),
          snapshot: msg.room,
          updatedAt: Date.now()
        });
        broadcastPublicLobbyDirectory(wss);
      }
      return;
    }

    if (msg.type === 'CLIENT_JOIN_ROOM') {
      detachSocketFromRoom(ws, true);
      const code = String(msg.code || '').trim().toUpperCase();
      const entry = rooms.get(code);
      if (!entry) {
        sendSafe(ws, { type: 'ROOM_NOT_FOUND', code, reason: 'Sala ' + code + ' não encontrada ou encerrada.' });
        return;
      }
      ws._fifootRoomCode = code;
      ws._fifootClientId = msg.clientId;
      entry.guests.set(msg.clientId, ws);
      entry.updatedAt = Date.now();
      if (entry.hostWs && entry.hostWs.readyState === 1) {
        sendSafe(entry.hostWs, { packet: msg.packet });
      } else {
        sendSafe(ws, { packet: { type: 'HOST_TEMPORARILY_OFFLINE', code } });
      }
      return;
    }

    if (msg.type === 'HOST_BROADCAST') {
      const code = String(msg.code || '').trim().toUpperCase();
      const entry = rooms.get(code);
      if (!entry || entry.hostWs !== ws) return;
      const prevStage = entry.snapshot ? entry.snapshot.stage : 'lobby';
      const prevCount = entry.snapshot && entry.snapshot.managers ? entry.snapshot.managers.length : 0;
      if (msg.room) entry.snapshot = msg.room;
      entry.updatedAt = Date.now();

      if (msg.packet && msg.packet.targetId) {
        const targetWs = entry.guests.get(msg.packet.targetId);
        sendSafe(targetWs, { packet: msg.packet });
      } else {
        for (const gWs of entry.guests.values()) {
          sendSafe(gWs, { packet: msg.packet });
        }
      }

      const newStage = entry.snapshot ? entry.snapshot.stage : 'lobby';
      const newCount = entry.snapshot && entry.snapshot.managers ? entry.snapshot.managers.length : 0;
      if (prevStage === 'lobby' || newStage === 'lobby' || prevCount !== newCount) {
        broadcastPublicLobbyDirectory(wss);
      }
      return;
    }

    if (msg.type === 'CLIENT_TO_HOST') {
      const code = String(msg.code || '').trim().toUpperCase();
      const entry = rooms.get(code);
      // Validate sender is an authenticated guest of this exact room
      if (!entry || ws._fifootRoomCode !== code || entry.guests.get(ws._fifootClientId) !== ws) return;
      if (!entry.hostWs || entry.hostWs.readyState !== 1) return;
      entry.updatedAt = Date.now();
      if (msg.packet) msg.packet.clientId = ws._fifootClientId;
      sendSafe(entry.hostWs, { packet: msg.packet });
      return;
    }

    if (msg.type === 'LEAVE_ROOM') {
      detachSocketFromRoom(ws, true);
    }
  });

  ws.on('close', () => detachSocketFromRoom(ws, false));
  ws.on('error', () => detachSocketFromRoom(ws, false));
});

const heartbeatInterval = setInterval(() => {
  const cutoff = Date.now() - ROOM_TTL_MS;
  for (const [code, entry] of rooms.entries()) {
    if (entry.updatedAt < cutoff) closeRoomAndNotify(code, 'Sala expirada por inatividade.');
  }
  for (const ws of wss.clients) {
    if (ws._isAlive === false) {
      detachSocketFromRoom(ws, false);
      ws.terminate();
      continue;
    }
    ws._isAlive = false;
    try { ws.ping(); } catch (err) {}
  }
}, 25000);

wss.on('close', () => clearInterval(heartbeatInterval));

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[fifoot] Servidor oficial online na porta ${PORT}`);
});
