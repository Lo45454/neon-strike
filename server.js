// NEON STRIKE — serveur de jeu
// - Duels en ligne : relais de messages entre deux joueurs d'une même salle.
// - Profils, liste d'amis, clavardage et invitations directes.
// - Comptes avec pseudo et mot de passe : chaque joueur a sa propre progression.
// - Sans compte, la progression est liée à l'appareil (en-tête X-Visitor-Id).
// Socket.IO utilise WebSocket et retombe sur HTTP long-polling si nécessaire.
const path = require('path');
const fs = require('fs');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 5000;
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: true, credentials: true },
  pingInterval: 10000,
  pingTimeout: 15000,
  maxHttpBufferSize: 64 * 1024,
});

app.use(express.static(path.join(__dirname, 'public')));

// ===================== SECRETS =====================
let SECRET = {};
try { SECRET = require('./creator-secret.json'); } catch (e) {}
const CREATOR_HASH = process.env.CREATOR_PASSWORD_SHA256 || SECRET.sha256 || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || SECRET.adminToken || '';

function safeEqual(a, b) {
  const A = Buffer.from(String(a)), B = Buffer.from(String(b));
  return A.length === B.length && A.length > 0 && crypto.timingSafeEqual(A, B);
}

// ===================== DONNÉES =====================
// db = { v: 2, saves: { visitorId: { data, creator, profile:{name,code}, friends:[], requests:[], unread:{} } }, chats: { "A|B": [{f,x,t}] } }
// Supabase est utilisé si SUPABASE_URL et SUPABASE_SECRET_KEY sont configurés.
// Sinon, le serveur conserve le mode fichier local (non persistant sur Render Free).
const DATA_DIR = process.env.DATA_DIR || __dirname;
fs.mkdirSync(DATA_DIR, { recursive: true });
const DATA_FILE = path.join(DATA_DIR, 'data.json');
const RESTORE_FILE = path.join(DATA_DIR, 'restore.json');
// db.accounts = { cléPseudo: { id, name, salt, hash, t } } ; db.saves[id du compte] = progression du compte
// db.sessions = { jeton: { a, t } } ; db.remember = { appareil: id du compte } ; db.devSeen = { appareil: [{ a, t }] }
let db = null;

function normalize(raw) {
  const base = { v: 3, saves: {}, chats: {}, accounts: {}, sessions: {}, remember: {}, devSeen: {} };
  if (!raw || typeof raw !== 'object') return base;
  if (raw.v === 2 || raw.v === 3) {
    for (const k of Object.keys(base)) if (k !== 'v' && raw[k] && typeof raw[k] === 'object') base[k] = raw[k];
    return base;
  }
  base.saves = raw; // ancien format : objet de sauvegardes
  return base;
}
db = normalize(null);
try {
  if (fs.existsSync(DATA_FILE)) db = normalize(JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')));
  else if (fs.existsSync(RESTORE_FILE)) { db = normalize(JSON.parse(fs.readFileSync(RESTORE_FILE, 'utf8'))); console.log('[data] restauré depuis restore.json'); }
} catch (e) { console.log('[data] lecture impossible', e.message); }

let writeTimer = null;
let persistChain = Promise.resolve();
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/$/, '');
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY || '';
const useSupabase = !!(SUPABASE_URL && SUPABASE_SECRET_KEY);
async function supabaseRequest(pathname, options = {}) {
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${pathname}`, {
    ...options,
    headers: {
      apikey: SUPABASE_SECRET_KEY,
      Authorization: `Bearer ${SUPABASE_SECRET_KEY}`,
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  });
  if (!response.ok) throw new Error(`Supabase HTTP ${response.status}: ${(await response.text()).slice(0, 300)}`);
  if (response.status === 204) return null;
  const text = await response.text();
  return text ? JSON.parse(text) : null;
}
async function loadSupabaseData() {
  if (!useSupabase) return false;
  try {
    const rows = await supabaseRequest('game_state?select=payload&id=eq.main&limit=1');
    if (Array.isArray(rows) && rows[0] && rows[0].payload) {
      db = normalize(rows[0].payload);
      // Rebuild indexes after loading persisted account/profile data.
      accIndex.clear();
      for (const a of Object.values(db.accounts)) accIndex.set(a.id, a);
      rebuildIndex();
      console.log('[data] sauvegardes chargées depuis Supabase');
    } else {
      console.log('[data] première sauvegarde Supabase; migration des données locales disponibles');
      await writeSupabaseSnapshot(JSON.stringify(db));
    }
    return true;
  } catch (e) {
    console.error('[data] connexion Supabase impossible:', e.message);
    return false;
  }
}
async function writeSupabaseSnapshot(snapshot) {
  await supabaseRequest('game_state?on_conflict=id', {
    method: 'POST',
    headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
    body: JSON.stringify({ id: 'main', payload: JSON.parse(snapshot), updated_at: new Date().toISOString() }),
  });
}
function persist() {
  clearTimeout(writeTimer);
  writeTimer = setTimeout(() => {
    const snapshot = JSON.stringify(db);
    persistChain = persistChain.then(async () => {
      if (useSupabase) {
        try {
          await writeSupabaseSnapshot(snapshot);
          console.log('[data] sauvegarde Supabase réussie');
          return;
        } catch (e) {
          console.error('[data] écriture Supabase échouée:', e.message);
        }
      }
      // Copie locale de secours; Render Free ne garantit pas sa conservation.
      const tmp = DATA_FILE + '.tmp';
      try { await fs.promises.writeFile(tmp, snapshot); await fs.promises.rename(tmp, DATA_FILE); }
      catch (e) { console.error('[data] écriture locale échouée:', e.message); }
    }).catch(e => console.error('[data] sauvegarde échouée:', e.message));
  }, 400);
}

const CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const codeIndex = new Map(); // code ami -> visitorId
function rebuildIndex() {
  codeIndex.clear();
  for (const [vid, s] of Object.entries(db.saves)) if (s.profile && s.profile.code) codeIndex.set(s.profile.code, vid);
}
rebuildIndex();

// ===================== COMPTES =====================
const accIndex = new Map(); // id du compte -> compte
for (const a of Object.values(db.accounts)) accIndex.set(a.id, a);
function nameKey(n) { return cleanName(n).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, ''); }
function hashPass(pass, salt) { return crypto.scryptSync(String(pass), salt, 32).toString('hex'); }
function newSession(accId) {
  const tok = crypto.randomBytes(24).toString('hex');
  db.sessions[tok] = { a: accId, t: Date.now() };
  const cutoff = Date.now() - 30 * 24 * 3600 * 1000;
  for (const [k, v] of Object.entries(db.sessions)) if (v.t < cutoff) delete db.sessions[k];
  return tok;
}
function markSeen(dev, accId) {
  const now = Date.now();
  const list = (db.devSeen[dev] || []).filter(e => e.a !== accId && now - e.t < 30 * 24 * 3600 * 1000);
  list.push({ a: accId, t: now });
  db.devSeen[dev] = list.slice(-6);
}
function isShared(dev) { return (db.devSeen[dev] || []).length >= 2; }
function hasProgress(r) { return !!(r && !r.claimedBy && (r.data || r.creator || (r.profile && r.profile.code))); }

function rec(vid) {
  if (!db.saves[vid]) db.saves[vid] = {};
  const r = db.saves[vid];
  if (!Array.isArray(r.friends)) r.friends = [];
  if (!Array.isArray(r.requests)) r.requests = [];
  if (!r.unread || typeof r.unread !== 'object') r.unread = {};
  return r;
}
function newFriendCode() {
  for (let i = 0; i < 5000; i++) {
    let c = '';
    for (let k = 0; k < 6; k++) c += CHARS[crypto.randomInt(CHARS.length)];
    if (!codeIndex.has(c)) return c;
  }
  return null;
}
function cleanName(n) {
  return String(n || '').replace(/[\u0000-\u001f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, 16);
}
function cleanCode(c) { return String(c || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6); }
function chatKey(a, b) { return a < b ? a + '|' + b : b + '|' + a; }

// ===================== HTTP =====================
function visitor(req) { return String(req.headers['x-visitor-id'] || 'local').slice(0, 100); }

app.get('/api/save', (req, res) => {
  const v = db.saves[visitor(req)];
  res.json(v ? { ok: true, data: v.data || null, creator: !!v.creator } : { ok: true, data: null, creator: false });
});
app.post('/api/save', express.text({ type: '*/*', limit: '20kb' }), (req, res) => {
  let data = null;
  try { data = JSON.parse(req.body); } catch (e) { return res.status(400).json({ ok: false }); }
  const r = rec(visitor(req));
  r.data = data; r.t = Date.now();
  persist();
  res.json({ ok: true });
});

// Mode créateur : mot de passe vérifié côté serveur (empreinte SHA-256)
const attempts = new Map();
app.post('/api/creator', express.text({ type: '*/*', limit: '1kb' }), (req, res) => {
  const id = visitor(req);
  const now = Date.now();
  const a = attempts.get(id) || { n: 0, t: now };
  if (now - a.t > 10 * 60 * 1000) { a.n = 0; a.t = now; }
  if (a.n >= 8) return res.status(429).json({ ok: false, error: "Trop d'essais. Réessaie dans 10 minutes." });
  a.n++; attempts.set(id, a);
  const pass = typeof req.body === 'string' ? req.body.trim() : '';
  const ok = !!CREATOR_HASH && safeEqual(crypto.createHash('sha256').update(pass).digest('hex'), CREATOR_HASH);
  if (ok) { attempts.delete(id); rec(id).creator = true; persist(); }
  res.json(ok ? { ok: true } : { ok: false, error: 'Mot de passe incorrect.' });
});
app.post('/api/creator-off', (req, res) => {
  const r = db.saves[visitor(req)];
  if (r) { r.creator = false; persist(); }
  res.json({ ok: true });
});

// Sauvegarde complète (pour conserver la progression lors des mises à jour)
app.get('/api/admin/export', (req, res) => {
  const tok = req.headers['x-admin-token'] || req.query.token || '';
  if (!ADMIN_TOKEN || !safeEqual(tok, ADMIN_TOKEN)) return res.status(403).json({ ok: false });
  res.json(db);
});

app.get('/health', (req, res) => res.json({ ok: true, rooms: rooms.size, players: io.engine.clientsCount }));
io.engine.on('connection_error', (err) => console.log('[socket] erreur', err.code, err.message));

// ===================== SALLES DE DUEL =====================
const rooms = new Map(); // code -> { host, guest, created }
function newRoomCode() {
  for (let tries = 0; tries < 1000; tries++) {
    let c = '';
    for (let i = 0; i < 5; i++) c += CHARS[crypto.randomInt(CHARS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}
function other(room, id) { return room ? (room.host === id ? room.guest : room.host) : null; }
function leaveRoom(socket, notify = true) {
  const code = socket.data.room;
  if (!code) return;
  const room = rooms.get(code);
  socket.data.room = null;
  socket.leave(code);
  if (!room) return;
  const peer = other(room, socket.id);
  rooms.delete(code);
  if (peer && notify) {
    const ps = io.sockets.sockets.get(peer);
    if (ps) { ps.emit('peer-left'); ps.data.room = null; ps.leave(code); }
  }
  notifyFriendsOf(socket.data.vid);
  if (peer) { const ps = io.sockets.sockets.get(peer); if (ps) notifyFriendsOf(ps.data.vid); }
}

// ===================== PRÉSENCE / AMIS =====================
const online = new Map(); // visitorId -> Set(socketId)
function socketsOf(vid) { return [...(online.get(vid) || [])].map(id => io.sockets.sockets.get(id)).filter(Boolean); }
function emitTo(vid, ev, data) { for (const s of socketsOf(vid)) s.emit(ev, data); }
function isBusy(vid) {
  return socketsOf(vid).some(s => { const r = rooms.get(s.data.room); return r && r.guest; });
}
function notifyFriendsOf(vid) {
  const r = db.saves[vid];
  if (!r || !r.profile) return;
  for (const code of r.friends || []) { const fv = codeIndex.get(code); if (fv) emitTo(fv, 'social'); }
}
function socialState(vid) {
  const r = rec(vid);
  const mk = (code) => {
    const fv = codeIndex.get(code);
    const fr = fv && db.saves[fv];
    return { code, name: (fr && fr.profile && fr.profile.name) || '???', online: !!(fv && online.get(fv) && online.get(fv).size), busy: !!(fv && isBusy(fv)), unread: r.unread[code] || 0 };
  };
  return {
    ok: true,
    profile: r.profile || null,
    friends: r.friends.map(mk).sort((a, b) => (b.online - a.online) || a.name.localeCompare(b.name)),
    requests: r.requests.map(mk),
  };
}

io.on('connection', (socket) => {
  const hv = socket.handshake.headers['x-visitor-id'];
  const q = socket.handshake.query || {};
  // En production l'en-tête X-Visitor-Id est toujours présent ; ?vid= sert seulement aux tests locaux.
  const dev = String(hv || ('local-' + String(q.vid || 'x').slice(0, 40))).slice(0, 100);
  // Identité : compte connecté (jeton de session ou « rester connecté »), sinon l'appareil
  let vid = dev;
  socket.data.token = null;
  const tok = String((socket.handshake.auth || {}).token || '').slice(0, 80);
  if (tok && db.sessions[tok] && accIndex.has(db.sessions[tok].a)) {
    vid = db.sessions[tok].a; socket.data.token = tok; db.sessions[tok].t = Date.now();
  } else if (db.remember[dev] && accIndex.has(db.remember[dev])) {
    vid = db.remember[dev]; socket.data.token = newSession(vid); persist();
  }
  socket.data.vid = vid;
  socket.data.dev = dev;
  socket.data.room = null;
  socket.data.chatTimes = [];
  if (!online.has(vid)) online.set(vid, new Set());
  const wasOffline = online.get(vid).size === 0;
  online.get(vid).add(socket.id);
  if (wasOffline) notifyFriendsOf(vid);

  const myRec = () => rec(vid);
  const myCode = () => (myRec().profile || {}).code;

  // ----- Comptes -----
  function switchTo(newId) {
    if (newId === vid) return;
    const set = online.get(vid);
    if (set) { set.delete(socket.id); if (!set.size) { online.delete(vid); notifyFriendsOf(vid); } }
    vid = newId; socket.data.vid = vid;
    if (!online.has(vid)) online.set(vid, new Set());
    const was = online.get(vid).size === 0;
    online.get(vid).add(socket.id);
    if (was) notifyFriendsOf(vid);
  }
  function authState(extra = {}) {
    const acc = accIndex.get(vid);
    const r = db.saves[vid] || {};
    return {
      ok: true, account: acc ? { name: acc.name } : null, token: acc ? socket.data.token : null,
      data: r.data || null, creator: !!r.creator, shared: isShared(dev),
      guestProgress: !acc && hasProgress(db.saves[dev]), ...extra,
    };
  }
  function login(accId, remember) {
    switchTo(accId);
    socket.data.token = newSession(accId);
    markSeen(dev, accId);
    if (remember) db.remember[dev] = accId;
    else if (db.remember[dev] === accId) delete db.remember[dev];
    persist();
  }
  const fails = (k) => { const now = Date.now(); const a = attempts.get(k) || { n: 0, t: now }; if (now - a.t > 10 * 60 * 1000) { a.n = 0; a.t = now; } attempts.set(k, a); return a; };

  socket.on('auth:status', (ack) => { if (typeof ack === 'function') ack(authState()); });

  socket.on('auth:register', (d, ack) => {
    if (typeof ack !== 'function' || !d) return;
    const name = cleanName(d.name), key = nameKey(d.name), pass = String(d.pass || '');
    if (name.length < 3) return ack({ ok: false, error: 'Ton pseudo doit avoir au moins 3 caractères.' });
    if (!/^[\p{L}\p{N} _.-]+$/u.test(name)) return ack({ ok: false, error: 'Ton pseudo peut contenir des lettres, des chiffres, des espaces, « _ », « - » et « . ».' });
    if (pass.length < 4 || pass.length > 64) return ack({ ok: false, error: 'Ton mot de passe doit avoir entre 4 et 64 caractères.' });
    if (db.accounts[key]) return ack({ ok: false, error: 'Ce pseudo est déjà pris. Choisis-en un autre.' });
    const f = fails('reg:' + dev);
    if (f.n >= 6) return ack({ ok: false, error: 'Trop de comptes créés sur cet appareil. Réessaie dans 10 minutes.' });
    f.n++;
    const id = 'acc_' + crypto.randomBytes(9).toString('hex');
    const salt = crypto.randomBytes(16).toString('hex');
    const acc = { id, name, salt, hash: hashPass(pass, salt), t: Date.now() };
    db.accounts[key] = acc; accIndex.set(id, acc);
    const r = rec(id);
    const g = db.saves[dev];
    let migrated = false;
    if (d.migrate && accIndex.get(vid) === undefined && hasProgress(g)) { // on garde la progression de l'appareil
      r.data = g.data || null; r.creator = !!g.creator;
      if (g.profile && g.profile.code) {
        r.profile = { name, code: g.profile.code };
        r.friends = g.friends || []; r.requests = g.requests || []; r.unread = g.unread || {};
        codeIndex.set(g.profile.code, id);
      }
      db.saves[dev] = { claimedBy: id };
      migrated = true;
    }
    if (!r.profile) { const code = newFriendCode(); r.profile = { name, code }; codeIndex.set(code, id); }
    login(id, !!d.remember);
    notifyFriendsOf(id);
    ack(authState({ migrated }));
  });

  socket.on('auth:login', (d, ack) => {
    if (typeof ack !== 'function' || !d) return;
    const key = nameKey(d.name);
    const f1 = fails('dev:' + dev), f2 = fails('acc:' + key);
    if (f1.n >= 12 || f2.n >= 8) return ack({ ok: false, error: "Trop d'essais. Réessaie dans 10 minutes." });
    const acc = db.accounts[key];
    if (!acc || !safeEqual(hashPass(String(d.pass || ''), acc.salt), acc.hash)) {
      f1.n++; f2.n++;
      return ack({ ok: false, error: 'Pseudo ou mot de passe incorrect.' });
    }
    attempts.delete('acc:' + key);
    login(acc.id, !!d.remember);
    ack(authState());
  });

  socket.on('auth:logout', (ack) => {
    if (socket.data.token) delete db.sessions[socket.data.token];
    if (db.remember[dev] === vid) delete db.remember[dev];
    socket.data.token = null;
    leaveRoom(socket);
    switchTo(dev);
    persist();
    if (typeof ack === 'function') ack(authState());
  });

  socket.on('auth:delete', (d, ack) => {
    if (typeof ack !== 'function') return;
    const acc = accIndex.get(vid);
    if (!acc || !socket.data.token) return ack({ ok: false, error: 'Connecte-toi pour supprimer ton compte.' });
    const key = nameKey(acc.name);
    // Retire le compte, sa progression, ses sessions et ses connexions mémorisées.
    delete db.accounts[key];
    accIndex.delete(acc.id);
    delete db.saves[acc.id];
    for (const [token, session] of Object.entries(db.sessions)) if (session.a === acc.id) delete db.sessions[token];
    for (const [device, remembered] of Object.entries(db.remember)) if (remembered === acc.id) delete db.remember[device];
    for (const [device, seen] of Object.entries(db.devSeen)) {
      db.devSeen[device] = seen.filter(entry => entry.a !== acc.id);
      if (!db.devSeen[device].length) delete db.devSeen[device];
    }
    socket.data.token = null;
    switchTo(dev);
    persist();
    ack(authState({ deleted: true }));
  });

  socket.on('auth:changePass', (d, ack) => {
    if (typeof ack !== 'function' || !d) return;
    const acc = accIndex.get(vid);
    if (!acc) return ack({ ok: false, error: "Tu n'es pas connecté." });
    if (!safeEqual(hashPass(String(d.old || ''), acc.salt), acc.hash)) return ack({ ok: false, error: 'Ancien mot de passe incorrect.' });
    const np = String(d.pass || '');
    if (np.length < 4 || np.length > 64) return ack({ ok: false, error: 'Ton mot de passe doit avoir entre 4 et 64 caractères.' });
    acc.salt = crypto.randomBytes(16).toString('hex'); acc.hash = hashPass(np, acc.salt);
    for (const [k, v] of Object.entries(db.sessions)) if (v.a === acc.id && k !== socket.data.token) delete db.sessions[k];
    persist();
    ack({ ok: true });
  });

  // ----- Sauvegarde de la progression (compte ou appareil) -----
  socket.on('save:set', (data, ack) => {
    let ok = false;
    try { ok = !!data && typeof data === 'object' && JSON.stringify(data).length < 20000; } catch (e) {}
    if (ok) { const r = myRec(); r.data = data; r.t = Date.now(); persist(); }
    if (typeof ack === 'function') ack({ ok });
  });

  socket.on('creator:on', (pass, ack) => {
    if (typeof ack !== 'function') return;
    const a = fails('creator:' + dev);
    if (a.n >= 8) return ack({ ok: false, error: "Trop d'essais. Réessaie dans 10 minutes." });
    a.n++;
    const ok = !!CREATOR_HASH && safeEqual(crypto.createHash('sha256').update(String(pass || '').trim()).digest('hex'), CREATOR_HASH);
    if (ok) { attempts.delete('creator:' + dev); myRec().creator = true; persist(); }
    ack(ok ? { ok: true } : { ok: false, error: 'Mot de passe incorrect.' });
  });
  socket.on('creator:off', () => { const r = db.saves[vid]; if (r) { r.creator = false; persist(); } });

  // ----- Profil & amis -----
  socket.on('social:get', (ack) => { if (typeof ack === 'function') ack(socialState(vid)); });

  socket.on('social:setName', (name, ack) => {
    if (typeof ack !== 'function') return;
    if (!accIndex.get(vid)) return ack({ ok: false, error: 'Connecte-toi à ton compte pour ajouter des amis.' });
    return ack({ ok: false, error: 'Ton pseudo est celui de ton compte.' });
    const n = cleanName(name);
    const r = myRec();
    if (!r.profile) {
      const code = newFriendCode();
      if (!code) return ack({ ok: false, error: 'Erreur du serveur, réessaie.' });
      r.profile = { name: n, code };
      codeIndex.set(code, vid);
    } else r.profile.name = n;
    persist();
    notifyFriendsOf(vid);
    ack(socialState(vid));
  });

  socket.on('social:add', (rawCode, ack) => {
    if (typeof ack !== 'function') return;
    const me = myRec();
    if (!me.profile) return ack({ ok: false, error: "Choisis d'abord un pseudo." });
    let code = cleanCode(String(rawCode || '').replace(/^#/, ''));
    let tv = String(rawCode || '').replace(/^#/, '').trim().length === 6 ? codeIndex.get(code) : null;
    if (!tv) { // recherche par pseudo de compte
      const acc = db.accounts[nameKey(rawCode)];
      const ar = acc && db.saves[acc.id];
      if (ar && ar.profile) { tv = acc.id; code = ar.profile.code; }
    }
    if (!tv) return ack({ ok: false, error: `Aucun joueur trouvé avec « ${cleanName(rawCode) || '?'} ».` });
    if (tv === vid) return ack({ ok: false, error: "C'est ton propre code." });
    if (me.friends.includes(code)) return ack({ ok: false, error: 'Vous êtes déjà amis.' });
    const them = rec(tv);
    if (me.requests.includes(code)) { // il m'avait déjà demandé : on devient amis
      me.requests = me.requests.filter(c => c !== code);
      me.friends.push(code);
      if (!them.friends.includes(me.profile.code)) them.friends.push(me.profile.code);
      them.requests = them.requests.filter(c => c !== me.profile.code);
      persist(); emitTo(tv, 'social');
      return ack({ ...socialState(vid), msg: `${them.profile.name} est maintenant ton ami.` });
    }
    if (!them.requests.includes(me.profile.code)) them.requests.push(me.profile.code);
    if (them.requests.length > 50) them.requests.shift();
    persist(); emitTo(tv, 'social');
    ack({ ...socialState(vid), msg: `Demande envoyée à ${them.profile.name}.` });
  });

  socket.on('social:respond', (d, ack) => {
    if (typeof ack !== 'function' || !d) return;
    const me = myRec();
    const code = cleanCode(d.code);
    if (!me.profile || !me.requests.includes(code)) return ack(socialState(vid));
    me.requests = me.requests.filter(c => c !== code);
    const tv = codeIndex.get(code);
    if (d.accept && tv) {
      const them = rec(tv);
      if (!me.friends.includes(code)) me.friends.push(code);
      if (!them.friends.includes(me.profile.code)) them.friends.push(me.profile.code);
      emitTo(tv, 'social');
    }
    persist();
    ack(socialState(vid));
  });

  socket.on('social:remove', (rawCode, ack) => {
    if (typeof ack !== 'function') return;
    const me = myRec();
    const code = cleanCode(rawCode);
    me.friends = me.friends.filter(c => c !== code);
    delete me.unread[code];
    const tv = codeIndex.get(code);
    if (tv && me.profile) { const them = rec(tv); them.friends = them.friends.filter(c => c !== me.profile.code); emitTo(tv, 'social'); }
    persist();
    ack(socialState(vid));
  });

  // ----- Clavardage -----
  socket.on('chat:history', (rawCode, ack) => {
    if (typeof ack !== 'function') return;
    const me = myRec(), code = cleanCode(rawCode);
    if (!me.profile || !me.friends.includes(code)) return ack({ ok: false, messages: [] });
    me.unread[code] = 0; persist();
    const msgs = (db.chats[chatKey(me.profile.code, code)] || []).slice(-60);
    ack({ ok: true, me: me.profile.code, messages: msgs });
  });

  socket.on('chat:read', (rawCode) => {
    const me = myRec(), code = cleanCode(rawCode);
    if (me.unread[code]) { me.unread[code] = 0; persist(); }
  });

  socket.on('chat:send', (d, ack) => {
    if (typeof ack !== 'function' || !d) return;
    const me = myRec(), code = cleanCode(d.to);
    const text = String(d.text || '').replace(/[\u0000-\u0008\u000b-\u001f]/g, '').trim().slice(0, 300);
    if (!me.profile || !me.friends.includes(code)) return ack({ ok: false, error: "Ce joueur n'est pas dans tes amis." });
    if (!text) return ack({ ok: false });
    const now = Date.now();
    socket.data.chatTimes = socket.data.chatTimes.filter(t => now - t < 5000);
    if (socket.data.chatTimes.length >= 6) return ack({ ok: false, error: 'Doucement, attends un peu avant de renvoyer un message.' });
    socket.data.chatTimes.push(now);
    const key = chatKey(me.profile.code, code);
    const msg = { f: me.profile.code, x: text, t: now };
    (db.chats[key] = db.chats[key] || []).push(msg);
    if (db.chats[key].length > 100) db.chats[key].splice(0, db.chats[key].length - 100);
    const tv = codeIndex.get(code);
    if (tv) {
      const them = rec(tv);
      them.unread[me.profile.code] = (them.unread[me.profile.code] || 0) + 1;
      emitTo(tv, 'chat', { with: me.profile.code, name: me.profile.name, msg });
    }
    for (const s of socketsOf(vid)) if (s.id !== socket.id) s.emit('chat', { with: code, msg });
    persist();
    ack({ ok: true, msg });
  });

  // ----- Invitations directes (sans partager de code) -----
  socket.on('invite:send', (d, ack) => {
    if (typeof ack !== 'function' || !d) return;
    const me = myRec(), code = cleanCode(d.to);
    if (!me.profile || !me.friends.includes(code)) return ack({ ok: false, error: "Ce joueur n'est pas dans tes amis." });
    const tv = codeIndex.get(code);
    const room = rooms.get(socket.data.room);
    if (!room || room.host !== socket.id || room.guest) return ack({ ok: false, error: 'Partie introuvable.' });
    if (!tv || !socketsOf(tv).length) return ack({ ok: false, error: "Ton ami n'est pas en ligne." });
    if (isBusy(tv)) return ack({ ok: false, error: 'Ton ami est déjà en duel.' });
    emitTo(tv, 'invite', { from: me.profile.code, name: me.profile.name, room: socket.data.room, mode: String(d.mode || '').slice(0, 20) });
    ack({ ok: true });
  });

  socket.on('invite:reply', (d) => {
    if (!d) return;
    const me = myRec(), code = cleanCode(d.to);
    const tv = codeIndex.get(code);
    if (tv && me.profile) emitTo(tv, 'invite:reply', { from: me.profile.code, name: me.profile.name, accept: !!d.accept });
  });

  // ----- Salles de duel -----
  socket.on('create', (ack) => {
    if (typeof ack !== 'function') return;
    leaveRoom(socket);
    const code = newRoomCode();
    if (!code) return ack({ ok: false, error: 'Serveur plein, réessaie.' });
    rooms.set(code, { host: socket.id, guest: null, created: Date.now() });
    socket.data.room = code;
    socket.join(code);
    ack({ ok: true, code });
  });

  socket.on('join', (rawCode, ack) => {
    if (typeof ack !== 'function') return;
    const code = String(rawCode || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 5);
    const room = rooms.get(code);
    if (!room) return ack({ ok: false, error: `Aucune partie trouvée avec le code ${code}.` });
    if (room.guest) return ack({ ok: false, error: 'Cette partie est déjà complète.' });
    if (room.host === socket.id) return ack({ ok: false, error: 'Tu ne peux pas rejoindre ta propre partie.' });
    leaveRoom(socket);
    room.guest = socket.id;
    socket.data.room = code;
    socket.join(code);
    ack({ ok: true, code });
    const hs = io.sockets.sockets.get(room.host);
    if (hs) hs.emit('peer-joined', { role: 'host' });
    socket.emit('peer-joined', { role: 'guest' });
    notifyFriendsOf(vid);
    if (hs) notifyFriendsOf(hs.data.vid);
  });

  socket.on('m', (data) => {
    const room = rooms.get(socket.data.room);
    const peer = other(room, socket.id);
    if (peer) io.to(peer).emit('m', data);
  });

  socket.on('leave', () => leaveRoom(socket));
  socket.on('disconnect', () => {
    leaveRoom(socket);
    const set = online.get(vid);
    if (set) { set.delete(socket.id); if (!set.size) { online.delete(vid); notifyFriendsOf(vid); } }
  });
});

// Nettoyage des salles abandonnées sans adversaire (> 30 min)
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (!room.guest && now - room.created > 30 * 60 * 1000) {
      const hs = io.sockets.sockets.get(room.host);
      if (hs) hs.data.room = null;
      rooms.delete(code);
    }
  }
}, 60 * 1000);

(async () => {
  if (useSupabase) await loadSupabaseData();
  else console.log('[data] Supabase non configuré; stockage local actif');
  server.listen(PORT, '0.0.0.0', () => console.log(`NEON STRIKE serveur sur le port ${PORT}${useSupabase ? ' (Supabase)' : ''}`));
})();
