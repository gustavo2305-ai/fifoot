// Fifoot Online — Vercel Serverless Multiplayer Sync API (Zero Dependencies)
// Uses Upstash Redis REST API (via Vercel Marketplace) when configured,
// with automatic in-memory fallback for single-instance testing.

const REDIS_URL = (process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL || '').replace(/\/+$/, '');
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN || '';
const HAS_REDIS = !!(REDIS_URL && REDIS_TOKEN);

const ROOM_TTL_SEC = 7200; // 2 hours
const LOBBY_STALE_MS = 20 * 60 * 1000; // 20 minutes
const CODE_RE = /^FIFO-[A-Z0-9]{4,8}$/;
const MAX_BODY_CHARS = 512 * 1024;

// In-memory fallback store (used if Upstash Redis is not yet connected in Vercel Storage)
if (!globalThis.__FIFOOT_MEM__) {
  globalThis.__FIFOOT_MEM__ = {
    roomsHash: new Map(), // code -> json string
    kv: new Map(),        // key -> { val, exp }
    lists: new Map()      // key -> { items: [], exp }
  };
}
const MEM = globalThis.__FIFOOT_MEM__;

function memPrune(map, key) {
  const entry = map.get(key);
  if (!entry) return null;
  if (entry.exp && Date.now() > entry.exp) {
    map.delete(key);
    return null;
  }
  return entry;
}

async function redisPipeline(commands) {
  if (!HAS_REDIS) {
    return commands.map((cmd) => {
      const op = String(cmd[0] || '').toUpperCase();
      if (op === 'GET') {
        const e = memPrune(MEM.kv, cmd[1]);
        return e ? e.val : null;
      }
      if (op === 'SET') {
        const exp = cmd[3] === 'EX' && cmd[4] ? Date.now() + Number(cmd[4]) * 1000 : 0;
        MEM.kv.set(cmd[1], { val: String(cmd[2]), exp });
        return 'OK';
      }
      if (op === 'DEL') {
        MEM.kv.delete(cmd[1]);
        MEM.lists.delete(cmd[1]);
        return 1;
      }
      if (op === 'EXPIRE') {
        const ttl = Number(cmd[2]) * 1000;
        const e1 = MEM.kv.get(cmd[1]);
        if (e1) e1.exp = Date.now() + ttl;
        const e2 = MEM.lists.get(cmd[1]);
        if (e2) e2.exp = Date.now() + ttl;
        return 1;
      }
      if (op === 'RPUSH') {
        let e = memPrune(MEM.lists, cmd[1]);
        if (!e) {
          e = { items: [], exp: Date.now() + ROOM_TTL_SEC * 1000 };
          MEM.lists.set(cmd[1], e);
        }
        e.items.push(String(cmd[2]));
        return e.items.length;
      }
      if (op === 'LLEN') {
        const e = memPrune(MEM.lists, cmd[1]);
        return e ? e.items.length : 0;
      }
      if (op === 'LRANGE') {
        const e = memPrune(MEM.lists, cmd[1]);
        if (!e) return [];
        const start = Math.max(0, Number(cmd[2]) || 0);
        const end = Number(cmd[3]);
        if (end < 0) return e.items.slice(start);
        return e.items.slice(start, end + 1);
      }
      if (op === 'HSET') {
        MEM.roomsHash.set(String(cmd[2]), String(cmd[3]));
        return 1;
      }
      if (op === 'HDEL') {
        MEM.roomsHash.delete(String(cmd[2]));
        return 1;
      }
      if (op === 'HGETALL') {
        const out = [];
        for (const [k, v] of MEM.roomsHash.entries()) {
          out.push(k, v);
        }
        return out;
      }
      return null;
    });
  }

  const res = await fetch(REDIS_URL + '/pipeline', {
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + REDIS_TOKEN,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(commands)
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error('Redis pipeline failed (' + res.status + '): ' + txt.slice(0, 120));
  }
  const data = await res.json();
  if (!Array.isArray(data)) throw new Error('Invalid Redis pipeline response');
  return data.map((item) => (item && typeof item === 'object' && 'result' in item ? item.result : null));
}

function keysFor(code) {
  const p = 'fifoot:v1:room:' + code;
  return {
    meta: p + ':meta',
    state: p + ':state',
    c2h: p + ':c2h',
    h2c: p + ':h2c',
    roomsHash: 'fifoot:v1:rooms'
  };
}

function safeJsonParse(str, fallback = null) {
  if (!str || typeof str !== 'string') return fallback;
  try { return JSON.parse(str); } catch (_) { return fallback; }
}

function buildPublicSummary(code, roomObj, hostId) {
  const mgrs = Array.isArray(roomObj.managers) ? roomObj.managers : [];
  const hostMgr = mgrs.find((m) => m.id === hostId) || mgrs[0] || {};
  return {
    code,
    name: String(roomObj.name || ('Copa ' + code)).slice(0, 48),
    hostName: String(hostMgr.name || 'Host').slice(0, 24),
    clubId: hostMgr.clubId || 'flamengo',
    mode: (roomObj.rules && roomObj.rules.universe) || 'BRASIL',
    privacy: roomObj.privacy || 'PUBLIC',
    stage: roomObj.stage || 'lobby',
    playerCount: mgrs.length,
    updatedAt: Date.now()
  };
}

async function readBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  if (typeof req.body === 'string') {
    if (req.body.length > MAX_BODY_CHARS) throw new Error('Payload too large');
    return safeJsonParse(req.body, {});
  }
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk;
      if (raw.length > MAX_BODY_CHARS) {
        reject(new Error('Payload too large'));
      }
    });
    req.on('end', () => resolve(safeJsonParse(raw, {})));
    req.on('error', reject);
  });
}

function sendJson(res, status, obj) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.end(JSON.stringify(obj));
}

function parseQuery(req) {
  if (req.query && typeof req.query === 'object') return req.query;
  const u = new URL(req.url || '/', 'http://localhost');
  const out = {};
  u.searchParams.forEach((v, k) => { out[k] = v; });
  return out;
}

module.exports = async function handler(req, res) {
  const storage = HAS_REDIS ? 'redis' : 'memory';

  try {
    if (req.method === 'OPTIONS') {
      res.statusCode = 204;
      res.end();
      return;
    }

    const q = parseQuery(req);

    // -------------------------------------------------------------------------
    // GET OPERATIONS: health | rooms | poll
    // -------------------------------------------------------------------------
    if (req.method === 'GET') {
      const op = String(q.op || 'health');

      if (op === 'health') {
        return sendJson(res, 200, { ok: true, mode: 'vercel', storage, ts: Date.now() });
      }

      if (op === 'rooms') {
        const [rawHash] = await redisPipeline([['HGETALL', 'fifoot:v1:rooms']]);
        const list = [];
        const staleCodes = [];
        const now = Date.now();

        if (Array.isArray(rawHash)) {
          for (let i = 0; i < rawHash.length; i += 2) {
            const code = rawHash[i];
            const item = safeJsonParse(rawHash[i + 1]);
            if (!item || (now - (item.updatedAt || 0) > LOBBY_STALE_MS) || item.stage !== 'lobby' || item.privacy !== 'PUBLIC') {
              staleCodes.push(code);
            } else {
              list.push(item);
            }
          }
        } else if (rawHash && typeof rawHash === 'object') {
          for (const [code, val] of Object.entries(rawHash)) {
            const item = safeJsonParse(val);
            if (!item || (now - (item.updatedAt || 0) > LOBBY_STALE_MS) || item.stage !== 'lobby' || item.privacy !== 'PUBLIC') {
              staleCodes.push(code);
            } else {
              list.push(item);
            }
          }
        }

        if (staleCodes.length > 0) {
          await redisPipeline(staleCodes.map((c) => ['HDEL', 'fifoot:v1:rooms', c])).catch(() => {});
        }

        list.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
        return sendJson(res, 200, { ok: true, rooms: list.slice(0, 30), storage });
      }

      if (op === 'poll') {
        const code = String(q.code || '').trim().toUpperCase();
        const role = String(q.role || 'guest');
        const cursor = Math.max(0, parseInt(q.cursor || '0', 10) || 0);
        const clientStateVer = Math.max(0, parseInt(q.stateVersion || '0', 10) || 0);

        if (!CODE_RE.test(code)) {
          return sendJson(res, 400, { ok: false, error: 'INVALID_CODE' });
        }

        const k = keysFor(code);
        if (role === 'host') {
          const [metaRaw, rawEvents] = await redisPipeline([
            ['GET', k.meta],
            ['LRANGE', k.c2h, cursor, -1]
          ]);
          const meta = safeJsonParse(metaRaw);
          if (!meta) {
            return sendJson(res, 200, { ok: false, error: 'ROOM_NOT_FOUND', closed: true, storage });
          }
          const items = Array.isArray(rawEvents) ? rawEvents : [];
          const events = items.map((s) => safeJsonParse(s)).filter(Boolean).map((x) => x.packet || x);
          return sendJson(res, 200, {
            ok: true,
            events,
            nextCursor: cursor + items.length,
            stateVersion: meta.stateVersion || 1,
            storage
          });
        }

        // Guest poll
        const [metaRaw, rawEvents] = await redisPipeline([
          ['GET', k.meta],
          ['LRANGE', k.h2c, cursor, -1]
        ]);
        const meta = safeJsonParse(metaRaw);
        const items = Array.isArray(rawEvents) ? rawEvents : [];
        const events = items.map((s) => safeJsonParse(s)).filter(Boolean);

        if (!meta) {
          return sendJson(res, 200, {
            ok: false,
            error: 'ROOM_NOT_FOUND',
            closed: true,
            events,
            nextCursor: cursor + items.length,
            storage
          });
        }

        const latestVersion = meta.stateVersion || 1;
        const needsRoom = latestVersion > clientStateVer || events.some((e) => e && e.useLatestRoom);
        let latestRoom = null;
        if (needsRoom) {
          const [stateRaw] = await redisPipeline([['GET', k.state]]);
          latestRoom = safeJsonParse(stateRaw);
        }

        return sendJson(res, 200, {
          ok: true,
          events,
          nextCursor: cursor + items.length,
          latestVersion,
          latestRoom,
          storage
        });
      }

      return sendJson(res, 400, { ok: false, error: 'UNKNOWN_OP' });
    }

    // -------------------------------------------------------------------------
    // POST OPERATIONS: host_create | host_broadcast | client_join | client_to_host | leave
    // -------------------------------------------------------------------------
    if (req.method === 'POST') {
      const body = await readBody(req);
      const op = String(body.op || '');
      const code = String(body.code || '').trim().toUpperCase();

      if (!CODE_RE.test(code)) {
        return sendJson(res, 400, { ok: false, error: 'INVALID_CODE' });
      }
      const k = keysFor(code);

      if (op === 'host_create') {
        const hostId = String(body.hostId || '');
        const roomObj = body.room;
        if (!hostId || !roomObj || typeof roomObj !== 'object') {
          return sendJson(res, 400, { ok: false, error: 'INVALID_PAYLOAD' });
        }

        const summary = buildPublicSummary(code, roomObj, hostId);
        const meta = {
          code,
          hostId,
          stage: summary.stage,
          privacy: summary.privacy,
          stateVersion: 1,
          updatedAt: Date.now()
        };

        const cmds = [
          ['DEL', k.c2h],
          ['DEL', k.h2c],
          ['SET', k.meta, JSON.stringify(meta), 'EX', ROOM_TTL_SEC],
          ['SET', k.state, JSON.stringify(roomObj), 'EX', ROOM_TTL_SEC]
        ];
        if (summary.stage === 'lobby' && summary.privacy === 'PUBLIC') {
          cmds.push(['HSET', k.roomsHash, code, JSON.stringify(summary)]);
        } else {
          cmds.push(['HDEL', k.roomsHash, code]);
        }

        await redisPipeline(cmds);
        return sendJson(res, 200, { ok: true, c2hCursor: 0, stateVersion: 1, storage });
      }

      if (op === 'host_broadcast') {
        const hostId = String(body.hostId || '');
        const packet = body.packet || {};
        const roomObj = body.room || (packet && packet.room) || null;

        const [metaRaw] = await redisPipeline([['GET', k.meta]]);
        let meta = safeJsonParse(metaRaw);

        if (!meta) {
          if (!roomObj) return sendJson(res, 404, { ok: false, error: 'ROOM_NOT_FOUND' });
          meta = {
            code,
            hostId,
            stage: roomObj.stage || 'lobby',
            privacy: roomObj.privacy || 'PUBLIC',
            stateVersion: 0,
            updatedAt: Date.now()
          };
        } else if (meta.hostId && hostId && meta.hostId !== hostId) {
          return sendJson(res, 403, { ok: false, error: 'FORBIDDEN_HOST' });
        }

        const cmds = [];
        if (roomObj && typeof roomObj === 'object') {
          meta.stateVersion = (meta.stateVersion || 0) + 1;
          meta.stage = roomObj.stage || meta.stage;
          meta.privacy = roomObj.privacy || meta.privacy;
          meta.updatedAt = Date.now();
          cmds.push(['SET', k.state, JSON.stringify(roomObj), 'EX', ROOM_TTL_SEC]);

          const summary = buildPublicSummary(code, roomObj, meta.hostId);
          if (summary.stage === 'lobby' && summary.privacy === 'PUBLIC') {
            cmds.push(['HSET', k.roomsHash, code, JSON.stringify(summary)]);
          } else {
            cmds.push(['HDEL', k.roomsHash, code]);
          }
        } else {
          meta.updatedAt = Date.now();
        }
        cmds.push(['SET', k.meta, JSON.stringify(meta), 'EX', ROOM_TTL_SEC]);

        // Store compact event in h2c (avoid duplicating full 35KB room snapshot per list item)
        let compactPacket = null;
        if (packet && packet.type) {
          if (packet.type === 'ROOM_SYNC') {
            compactPacket = {
              type: 'ROOM_SYNC_REF',
              code,
              toastMsg: packet.toastMsg || '',
              stateVersion: meta.stateVersion
            };
          } else if (packet.type === 'JOIN_ACCEPTED' || packet.type === 'KICKOFF_SYNC') {
            compactPacket = Object.assign({}, packet);
            delete compactPacket.room;
            compactPacket.useLatestRoom = true;
            compactPacket.stateVersion = meta.stateVersion;
          } else {
            compactPacket = packet;
          }
          cmds.push(['RPUSH', k.h2c, JSON.stringify(compactPacket)]);
          cmds.push(['EXPIRE', k.h2c, ROOM_TTL_SEC]);
        }

        await redisPipeline(cmds);
        return sendJson(res, 200, { ok: true, stateVersion: meta.stateVersion, storage });
      }

      if (op === 'client_join') {
        const clientId = String(body.clientId || '');
        const packet = body.packet;
        if (!clientId || !packet || typeof packet !== 'object') {
          return sendJson(res, 400, { ok: false, error: 'INVALID_PAYLOAD' });
        }

        const [metaRaw, h2cLen] = await redisPipeline([
          ['GET', k.meta],
          ['LLEN', k.h2c]
        ]);
        const meta = safeJsonParse(metaRaw);
        if (!meta) {
          return sendJson(res, 200, {
            ok: false,
            error: 'ROOM_NOT_FOUND',
            reason: 'Sala ' + code + ' não encontrada ou host offline.',
            storage
          });
        }

        await redisPipeline([
          ['RPUSH', k.c2h, JSON.stringify({ clientId, packet, ts: Date.now() })],
          ['EXPIRE', k.c2h, ROOM_TTL_SEC]
        ]);

        return sendJson(res, 200, {
          ok: true,
          h2cCursor: Number(h2cLen) || 0,
          stateVersion: 0,
          storage
        });
      }

      if (op === 'client_to_host') {
        const clientId = String(body.clientId || '');
        const packet = body.packet;
        if (!clientId || !packet || typeof packet !== 'object') {
          return sendJson(res, 400, { ok: false, error: 'INVALID_PAYLOAD' });
        }

        const [metaRaw] = await redisPipeline([['GET', k.meta]]);
        if (!metaRaw) {
          return sendJson(res, 200, { ok: false, error: 'ROOM_NOT_FOUND', storage });
        }

        await redisPipeline([
          ['RPUSH', k.c2h, JSON.stringify({ clientId, packet, ts: Date.now() })],
          ['EXPIRE', k.c2h, ROOM_TTL_SEC]
        ]);
        return sendJson(res, 200, { ok: true, storage });
      }

      if (op === 'leave') {
        const clientId = String(body.clientId || '');
        const isHost = !!body.isHost;
        const [metaRaw] = await redisPipeline([['GET', k.meta]]);
        const meta = safeJsonParse(metaRaw);
        if (!meta) return sendJson(res, 200, { ok: true, storage });

        if (isHost && meta.hostId === clientId) {
          await redisPipeline([
            ['HDEL', k.roomsHash, code],
            ['RPUSH', k.h2c, JSON.stringify({ type: 'ROOM_CLOSED', code, reason: 'O host encerrou a sala.' })],
            ['EXPIRE', k.h2c, 60],
            ['DEL', k.meta],
            ['DEL', k.state],
            ['DEL', k.c2h]
          ]);
        } else if (clientId) {
          await redisPipeline([
            ['RPUSH', k.c2h, JSON.stringify({ clientId, packet: { type: 'GUEST_LEAVE', code, clientId }, ts: Date.now() })],
            ['EXPIRE', k.c2h, ROOM_TTL_SEC]
          ]);
        }
        return sendJson(res, 200, { ok: true, storage });
      }

      return sendJson(res, 400, { ok: false, error: 'UNKNOWN_OP' });
    }

    return sendJson(res, 405, { ok: false, error: 'METHOD_NOT_ALLOWED' });
  } catch (err) {
    return sendJson(res, 500, {
      ok: false,
      error: 'SERVER_ERROR',
      detail: err && err.message ? err.message : 'Unexpected error',
      storage
    });
  }
};
