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
const RELEASE_ID = 'neon-2026-10-28-multishot-boss-return-v29';
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: true, credentials: true },
  pingInterval: 10000,
  pingTimeout: 15000,
  maxHttpBufferSize: 420 * 1024,
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
  const base = { v: 4, saves: {}, chats: {}, accounts: {}, sessions: {}, remember: {}, devSeen: {}, bans: {}, audit: [], feedback: [], announcements: { text: '', updatedAt: 0 }, ranked: {}, dailyQuest: null, weeklyLevel: null, moderationNotices: {}, bossTuning: {enabled:false,audience:'test',presetId:'B2',scope:'all',multiplier:1,updatedAt:0}, infiniteTuning: {enabled:false,audience:'test',presetId:'I2',multiplier:1,updatedAt:0} };
  if (!raw || typeof raw !== 'object') return base;
  if (raw.v === 2 || raw.v === 3 || raw.v === 4) {
    for (const k of Object.keys(base)) if (k !== 'v' && raw[k] && typeof raw[k] === 'object') base[k] = raw[k];
    for (const record of Object.values(base.ranked || {})) if (record && typeof record === 'object') delete record.weeks;
    for (const save of Object.values(base.saves || {})) if (save && save.data && typeof save.data === 'object') delete save.data.tournamentTitles;
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

// ===================== CLASSEMENT & QUÊTES =====================
const rankTickets = new Map();
const rankLastSubmit = new Map();
function weekKey(at = new Date()) {
  const d = new Date(Date.UTC(at.getUTCFullYear(), at.getUTCMonth(), at.getUTCDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const week = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return `${d.getUTCFullYear()}-W${String(week).padStart(2, '0')}`;
}
const DAILY_QUESTS = [
  {id:'flight',name:'Patrouille prolongée',description:'Cumule 5 minutes de vol en campagne ou dans l’Abysse.',metric:'seconds',target:300,reward:150},
  {id:'score',name:'Chasseur de points',description:'Gagne 4 000 points pendant tes parties.',metric:'points',target:4000,reward:150},
  {id:'campaign',name:'Route de campagne',description:'Termine un niveau de campagne.',metric:'levels',target:1,reward:175},
  {id:'boss',name:'Briseur de cuirasse',description:'Vaincs un boss.',metric:'bosses',target:1,reward:200},
  {id:'abyss',name:'Dans les profondeurs',description:'Survis 2 minutes dans l’Abysse infini.',metric:'abyssSeconds',target:120,reward:175},
  {id:'kills',name:'Nettoyage orbital',description:'Détruis 25 ennemis.',metric:'kills',target:25,reward:150},
  {id:'ships',name:'Pilote polyvalent',description:'Joue avec 2 vaisseaux différents.',metric:'ships',target:2,reward:175},
  {id:'coop',name:'Escadron uni',description:'Termine un niveau en coop.',metric:'coopLevels',target:1,reward:200},
  {id:'hard',name:'À l’épreuve du vide',description:'Termine un niveau en difficulté difficile.',metric:'hardLevels',target:1,reward:225},
  {id:'mystery',name:'Signaux mystérieux',description:'Découvre un indice NEON MYSTERY.',metric:'clues',target:1,reward:175},
];
const DAILY_QUEST_POOL = [
  ['seconds','Veille orbitale','Cumule {n} secondes de vol.',180,160],['points','Collecte de données','Gagne {n} points pendant tes parties.',2500,165],['levels','Exploration guidée','Termine {n} niveau(x) de campagne.',2,190],['bosses','Chasseur de colosses','Vaincs {n} boss.',1,210],['abyssSeconds','Plongée dans l’Abysse','Survis {n} secondes dans l’Abysse infini.',150,190],['kills','Nettoyage du secteur','Détruis {n} ennemis.',35,165],['ships','Essai de flotte','Joue avec {n} vaisseaux différents.',2,185],['coopLevels','Escadron en mission','Termine {n} niveau(x) en coop.',1,210],['hardLevels','Pilote intrépide','Termine {n} niveau(x) en difficulté difficile.',1,230],['clues','Chasse aux signaux','Découvre {n} indice(s) mystérieux.',1,190]
];
for(let i=0;i<30;i++){const [metric,name,description,target,reward]=DAILY_QUEST_POOL[i%DAILY_QUEST_POOL.length];const cycle=Math.floor(i/DAILY_QUEST_POOL.length);DAILY_QUESTS.push({id:`daily-${String(i+11).padStart(2,'0')}`,name:cycle?`${name} ${['II','III'][cycle-1]}`:name,description:description.replace('{n}',String(target+cycle)),metric,target:target+cycle,reward:reward+cycle*10});}
function dayKey(at=new Date()) { return at.toISOString().slice(0,10); }
function activeDailyQuest() {
  const key=dayKey();
  if(!db.dailyQuest||db.dailyQuest.day!==key) { const n=(Math.floor(Date.parse(key+'T00:00:00Z')/86400000))%DAILY_QUESTS.length; db.dailyQuest={day:key,questId:DAILY_QUESTS[(n+DAILY_QUESTS.length)%DAILY_QUESTS.length].id,source:'rotation',updatedAt:Date.now()};persist(); }
  const quest=DAILY_QUESTS.find(q=>q.id===db.dailyQuest.questId)||DAILY_QUESTS[0];
  const start=new Date(key+'T00:00:00.000Z');
  return {...quest,day:key,startsAt:start.toISOString(),endsAt:new Date(start.getTime()+86400000).toISOString(),selectedByAdmin:db.dailyQuest.source==='admin'};
}
const WEEKLY_LEVELS = [
  {id:'rift-01',name:'Faille de l’Aurore',description:'Traverse la faille et vaincs le boss sans aide de difficulté.',levelIndex:2,mods:['gravity','asteroids'],cosmetic:'aura-aurora'},
  {id:'rift-02',name:'Pluie de cendres',description:'Affronte des vagues serrées sous un bombardement continu.',levelIndex:4,mods:['storm','swarm'],cosmetic:'aura-ember'},
  {id:'rift-03',name:'Station fantôme',description:'Survis aux mines dans un couloir orbital étroit.',levelIndex:6,mods:['mines','station'],cosmetic:'aura-phantom'},
  {id:'rift-04',name:'Périgée glacé',description:'Garde le contrôle dans une zone glacée et balayée par les vents.',levelIndex:8,mods:['ice','drift'],cosmetic:'aura-glacier'},
  {id:'rift-05',name:'Orbite instable',description:'Le trou noir courbe les tirs au milieu d’une pluie d’astéroïdes.',levelIndex:10,mods:['gravity','asteroids'],cosmetic:'aura-prism'},
  {id:'rift-06',name:'Essaim rouge',description:'Des ennemis rapides arrivent en formations plus denses.',levelIndex:12,mods:['speed','swarm'],cosmetic:'aura-comet'},
  {id:'rift-07',name:'Tempête ionique',description:'Bats le boss dans une tempête et un champ de mines.',levelIndex:14,mods:['storm','mines'],cosmetic:'aura-ion'},
  {id:'rift-08',name:'Nébuleuse d’élite',description:'Élimine les ennemis d’élite au milieu du brouillard.',levelIndex:16,mods:['elite','fog'],cosmetic:'aura-nebula'},
  {id:'rift-09',name:'Cœur en rotation',description:'Combine gravité et ennemis renforcés.',levelIndex:18,mods:['gravity','elite'],cosmetic:'aura-singularity'},
  {id:'rift-10',name:'Dernier signal',description:'Une épreuve complète avec vagues serrées et tempête.',levelIndex:19,mods:['storm','swarm','elite'],cosmetic:'aura-crown'},
];
const WEEKLY_LEVEL_POOL = [
 ['Nécropole orbitale','Survis aux vagues de l’ancien cimetière.', ['fog','mines']],['Glacier de minuit','Garde le contrôle sous les rafales glacées.', ['ice','drift']],['Forge des comètes','Esquive les roches et la chaleur du réacteur.', ['asteroids','storm']],['Portail cramoisi','Affronte un essaim agressif dans la faille.', ['swarm','speed']],['Jardin des pulsars','Évite les impulsions et les ennemis d’élite.', ['storm','elite']],['Prison stellaire','Les mines bloquent chaque sortie.', ['mines','gravity']],['Récif quantique','Traverse un champ d’astéroïdes instable.', ['asteroids','gravity']],['Courant solaire','La tempête accélère les vagues.', ['storm','speed']],['Forteresse d’obsidienne','Un secteur défendu par des unités blindées.', ['elite','fog']],['Voie des spectres','Des apparitions rapides masquent les tirs.', ['fog','swarm']],['Arsenal fantôme','Les projectiles ennemis quadrillent la zone.', ['mines','storm']],['Anneau de Saturne','Les roches et la dérive réduisent l’espace sûr.', ['asteroids','drift']],['Noyau instable','La gravité change au milieu des vagues.', ['gravity','elite']],['Éclipse éternelle','Combat dans une obscurité chargée de mines.', ['fog','mines']],['Cratère des anciens','Une pluie de roches accompagne le boss.', ['asteroids','elite']],['Balise perdue','Reste mobile dans le brouillard ionique.', ['fog','speed']],['Mer de particules','Esquive les formations rapides et denses.', ['swarm','drift']],['Cité des machines','La station active ses défenses automatiques.', ['station','mines']],['Couloir des mirages','Le brouillard cache des vagues renforcées.', ['fog','elite']],['Frontière zéro','Dernière traversée : tempête, gravité et élites.', ['storm','gravity','elite']]
];
for(let i=0;i<90;i++){const [baseName,baseDescription,baseMods]=WEEKLY_LEVEL_POOL[i%WEEKLY_LEVEL_POOL.length],cycle=Math.floor(i/WEEKLY_LEVEL_POOL.length),mods=[...baseMods];if(cycle>0&&!mods.includes(['gravity','elite','storm','mines'][cycle%4]))mods.push(['gravity','elite','storm','mines'][cycle%4]);WEEKLY_LEVELS.push({id:`rift-${String(i+11).padStart(2,'0')}`,name:cycle?`${baseName} · Cycle ${cycle+1}`:baseName,description:cycle?`${baseDescription} Variante ${cycle+1} : vagues plus denses et boss renforcé.`:baseDescription,levelIndex:(25+i*7)%100,mods,cosmetic:`aura-weekly-${i+11}`});}
function activeWeeklyLevel(){
  const key=weekKey();
  if(!db.weeklyLevel||db.weeklyLevel.week!==key){const n=Number(key.split('-W')[1])%WEEKLY_LEVELS.length;db.weeklyLevel={week:key,levelId:WEEKLY_LEVELS[n].id,source:'rotation',updatedAt:Date.now()};persist();}
  const item=WEEKLY_LEVELS.find(x=>x.id===db.weeklyLevel.levelId)||WEEKLY_LEVELS[0];
  const m=key.match(/^(\d+)-W(\d+)$/),year=Number(m[1]),week=Number(m[2]),jan4=new Date(Date.UTC(year,0,4)),start=new Date(jan4);start.setUTCDate(jan4.getUTCDate()-((jan4.getUTCDay()+6)%7)+(week-1)*7);
  return {...item,week:key,startsAt:start.toISOString(),endsAt:new Date(start.getTime()+7*86400000).toISOString(),rewardCredits:5000,rewardTitle:'Vainqueur de la Faille',selectedByAdmin:db.weeklyLevel.source==='admin'};
}
function leaderboard(category = 'overall') {
  const rows = [];
  for (const [id, record] of Object.entries(db.ranked || {})) {
    const score = Number(record[category] || 0);
    if (score <= 0 || !accIndex.has(id)) continue;
    const save = db.saves[id] || {};
    if (save.data && (save.data.devAccount || save.data.devTest)) continue;
    rows.push({ id, name: accIndex.get(id).name, avatar: (save.profile && save.profile.avatar) || '🚀', score, stats: record.stats || {}, titles: record.titles || [] });
  }
  return rows.sort((a,b) => b.score - a.score || a.name.localeCompare(b.name)).map((p,i) => ({ ...p, rank: i + 1, reward: i === 0 ? 'Couronne du champion' : i < 3 ? 'Effet du top 3' : i < 10 ? 'Badge du top 10' : '' }));
}
function rankPayload(id) {
  const top=leaderboard('overall'),mine=top.find(p=>p.id===id)||null,ranked=db.ranked[id]||{};
  return {ok:true,account:accIndex.get(id)?.name||null,rankings:{overall:top.slice(0,100),endless:leaderboard('endless').slice(0,100),campaign:leaderboard('campaign').slice(0,100),me:mine,personal:ranked}};
}
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
  // Les privilèges développeur sont conservés côté serveur même si le client sauvegarde sa progression.
  if (r.data && r.data.devAccount) { data.devAccount = true; data.unlockedLevel = 999; data.credits = 2147483647; }
  if (r.data && (r.data.devAccount || r.data.devTest)) { data.devAccount = true; data.devTest = true; data.credits = 2147483647; data.unlockedLevel = 999; data.ownedShips = Array.from(new Set([...(Array.isArray(data.ownedShips)?data.ownedShips:[]), 'default','speeder','tanker','multi','sniper','destroyer','phantom','nova','vanguard','eclipse'])); data.ownedUpgrades = Array.from(new Set([...(Array.isArray(data.ownedUpgrades)?data.ownedUpgrades:[]), 'speed1','fire1','life1','dmg1','speed2','fire2','life2','dmg2'])); data.chestLast6h=0; data.chestLast24h=0; data.chest6hAt=0; data.chest24hAt=0; }
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

// Tableau de modération : seules les requêtes avec ADMIN_TOKEN valide sont autorisées.
function adminAllowed(req) {
  const tok = req.headers['x-admin-token'] || '';
  return !!ADMIN_TOKEN && safeEqual(tok, ADMIN_TOKEN);
}
function adminResolvePlayer(input) {
  const raw = String(input || '').trim();
  if (!raw) return null;
  if (accIndex.has(raw) || db.saves[raw]) return raw;
  const wanted = nameKey(raw);
  for (const [id, save] of Object.entries(db.saves)) {
    const n = (save.profile && save.profile.name) || (accIndex.get(id) || {}).name || '';
    if (nameKey(n) === wanted) return id;
  }
  for (const a of Object.values(db.accounts)) if (nameKey(a.name) === wanted) return a.id;
  return null;
}
// Import legacy local reports after Supabase has loaded, so they are not
// overwritten by the cloud snapshot during startup.
function migrateLegacyFeedback() {
  try {
    const oldFeedback = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'feedback.json'), 'utf8'));
    if (!Array.isArray(oldFeedback)) return;
    const known = new Set(db.feedback.map(x => x.id));
    const additions = oldFeedback.filter(x => x && x.id && !known.has(x.id));
    if (additions.length) { db.feedback.push(...additions); persist(); }
  } catch (e) {}
}
app.post('/api/feedback', express.json({limit:'4kb'}), (req,res) => {
  const type = String(req.body?.type || 'suggestion').slice(0,30);
  const message = String(req.body?.message || '').trim().slice(0,1500);
  if (message.length < 5) return res.status(400).json({ok:false,error:'Message trop court.'});
  db.feedback.push({id:crypto.randomBytes(8).toString('hex'),type,message,createdAt:new Date().toISOString()});
  persist(); res.json({ok:true});
});
app.get('/api/admin/feedback', (req,res) => {
  const tok = req.headers['x-admin-token'] || req.query.token || '';
  if (!ADMIN_TOKEN || !safeEqual(tok, ADMIN_TOKEN)) return res.status(403).json({ok:false});
  res.json({ok:true,items:[...db.feedback].reverse()});
});

app.get('/api/admin/dashboard', (req, res) => {
  if (!adminAllowed(req)) return res.status(403).json({ ok: false, error: 'Accès refusé.' });
  const accounts = Object.values(db.accounts).map(a => {
    const r = db.saves[a.id] || {};
    const sockets = online.get(a.id);
    const liveSockets = sockets ? [...sockets].map(id => io.sockets.sockets.get(id)).filter(Boolean) : [];
    const inRoom = liveSockets.some(s => !!s.data.room);
    const data = r.data || {};
    return {
      id: a.id, name: a.name, createdAt: a.t || null,
      credits: Number(data.credits) || 0,
      level: Math.max(0, Number(data.unlockedLevel) || 0) + 1,
      ships: Array.isArray(data.ownedShips) ? data.ownedShips : ['default'],
      equippedShip: data.equippedShip || 'default',
      bestScores: data.bestScores || {},
      online: liveSockets.length > 0, connections: liveSockets.length,
      lastOnlineAt: r.lastOnlineAt || null,
      lastSessionSeconds: Number(r.lastSessionSeconds) || 0,
      activity: liveSockets.length ? (liveSockets.map(s => s.data.activity || (s.data.room ? 'En partie multijoueur' : 'Connecté au jeu')).join(' · ')) : 'Hors ligne',
      location: liveSockets.map(s => s.data.location || (s.data.room ? 'Salle multijoueur' : 'Menu / activité inconnue')).join(' · '),
      room: liveSockets.map(s => s.data.room || '').filter(Boolean).join(', '),
      lastActivityAt: liveSockets.reduce((m,s)=>Math.max(m,s.data.lastActivityAt||0),0) || null,
      lastSave: r.t || null,
    };
  }).sort((a,b) => Number(b.online)-Number(a.online) || a.name.localeCompare(b.name));
  const guestIds = new Set([...Object.keys(db.saves), ...online.keys()]);
  const guests = [...guestIds].filter(id => {
    const save = db.saves[id] || {};
    return !accIndex.has(id) && !save.claimedBy && (save.data || (online.get(id) && online.get(id).size));
  }).map(id => {
    const r = db.saves[id] || {}, savedData = r.data || {};
    const sockets = online.get(id), liveSockets = sockets ? [...sockets].map(sid => io.sockets.sockets.get(sid)).filter(Boolean) : [];
    return {
      id, name: (r.profile && r.profile.name) || 'Invité', credits: Number(savedData.credits)||0,
      level: Math.max(0, Number(savedData.unlockedLevel)||0)+1,
      online: liveSockets.length > 0,
      activity: liveSockets.map(socket => socket.data.activity || 'Connecté au jeu').join(' · ') || 'Hors ligne',
      location: liveSockets.map(socket => socket.data.location || 'Menu / activité inconnue').join(' · ') || '—',
      ships: Array.isArray(savedData.ownedShips) ? savedData.ownedShips : ['default'],
      equippedShip: savedData.equippedShip || 'default', lastSave: r.t || null,
      lastOnlineAt: r.lastOnlineAt || null, lastSessionSeconds: Number(r.lastSessionSeconds)||0,
    };
  });
  res.json({ ok:true, accounts, guests, onlineCount:[...online.values()].reduce((n,set)=>n+(set.size?1:0),0), totalAccounts:accounts.length });
});
app.post('/api/admin/credits/:id', express.json({limit:'2kb'}), (req,res) => {
  if (!adminAllowed(req)) return res.status(403).json({ok:false,error:'Accès refusé.'});
  const id = adminResolvePlayer(req.params.id);
  if (!id) return res.status(404).json({ok:false,error:'Joueur introuvable. Vérifie le pseudo ou l’ID.'});
  const amount = Number(req.body && req.body.amount);
  if (!Number.isSafeInteger(amount) || amount === 0 || Math.abs(amount) > 1000000) return res.status(400).json({ok:false,error:'Montant invalide (entre -1 000 000 et 1 000 000, sans zéro).'});
  const r = rec(id); if (!r.data || typeof r.data !== 'object') r.data = {};
  const current = Math.max(0, Number(r.data.credits)||0);
  r.data.credits = Math.max(0, Math.min(2147483647, current + amount)); r.t=Date.now(); audit('credits','admin',id,{amount,balance:r.data.credits});
  for (const socket of socketsOf(id)) socket.emit('admin:credits-updated', { credits:r.data.credits });
  res.json({ok:true,credits:r.data.credits});
});


function audit(action, actor='admin', target='', detail={}) {
  db.audit = Array.isArray(db.audit) ? db.audit : [];
  db.audit.unshift({ id: crypto.randomBytes(6).toString('hex'), at: Date.now(), action, actor, target, detail });
  if (db.audit.length > 500) db.audit.length = 500;
  persist();
}

app.get('/api/daily-quest', (req,res) => res.json({ok:true,quest:activeDailyQuest()}));
app.get('/api/admin/daily-quest', (req,res) => { if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});res.json({ok:true,quest:activeDailyQuest(),quests:DAILY_QUESTS}); });
app.post('/api/admin/daily-quest', express.json({limit:'2kb'}), (req,res) => { if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});const quest=DAILY_QUESTS.find(q=>q.id===String(req.body?.questId||''));if(!quest)return res.status(400).json({ok:false,error:'Choisis une quête dans la liste.'});const day=dayKey();db.dailyQuest={day,questId:quest.id,source:'admin',updatedAt:Date.now()};audit('daily-quest','admin','all',{day,questId:quest.id});res.json({ok:true,quest:activeDailyQuest()}); });
const BOSS_TUNING_PRESETS=[
  {id:'B0',name:'Très accessible',multiplier:.3,detail:'Profil le plus doux : vie réduite et attaques nettement ralenties.'},
  {id:'B1',name:'Accessible',multiplier:.45,detail:'Boss plus indulgents, avec des attaques moins pressantes.'},
  {id:'B2',name:'Équilibré',multiplier:.6,detail:'Réglage conseillé : difficile, mais nettement moins punitif.'},
  {id:'B3',name:'Soutenu',multiplier:.75,detail:'Un défi exigeant sans le profil extrême précédent.'},
  {id:'B4',name:'Difficile',multiplier:.9,detail:'Près de la base, avec une pression accrue.'},
  {id:'B5',name:'Maximum',multiplier:1.05,detail:'Valeur maximale du nouveau dial.'}
];
function currentBossTuning(){const raw=db.bossTuning||{},preset=BOSS_TUNING_PRESETS.find(x=>x.id===raw.presetId)||BOSS_TUNING_PRESETS[2],scope=['all','health','attacks'].includes(raw.scope)?raw.scope:'all',multiplier=raw.presetId==='CUSTOM'?Math.max(.3,Math.min(1.05,Number(raw.multiplier)||.6)):preset.multiplier;return {enabled:raw.enabled===true,audience:raw.audience==='everyone'?'everyone':'test',presetId:raw.presetId==='CUSTOM'?'CUSTOM':preset.id,scope,multiplier,updatedAt:Number(raw.updatedAt)||0};}
app.get('/api/boss-tuning',(req,res)=>res.json({ok:true,tuning:currentBossTuning()}));
app.get('/api/admin/boss-tuning',(req,res)=>{if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});res.json({ok:true,tuning:currentBossTuning(),presets:BOSS_TUNING_PRESETS});});
app.post('/api/admin/boss-tuning',express.json({limit:'2kb'}),(req,res)=>{if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});const presetId=String(req.body?.presetId||'B2'),scope=String(req.body?.scope||'all'),audience=String(req.body?.audience||'test'),enabled=req.body?.enabled===true;if(!['test','everyone'].includes(audience))return res.status(400).json({ok:false,error:'Choisis les comptes test ou tous les joueurs.'});if(!['all','health','attacks'].includes(scope))return res.status(400).json({ok:false,error:'Mode de réglage invalide.'});let multiplier;if(presetId==='CUSTOM'){multiplier=Number(req.body?.multiplier);if(!Number.isFinite(multiplier)||multiplier<.3||multiplier>1.05)return res.status(400).json({ok:false,error:'La valeur personnalisée doit être entre 0,30 et 1,05.'});multiplier=Math.round(multiplier*100)/100;}else{const preset=BOSS_TUNING_PRESETS.find(x=>x.id===presetId);if(!preset)return res.status(400).json({ok:false,error:'Choisis un profil B0 à B5 ou une valeur personnalisée.'});multiplier=preset.multiplier;}db.bossTuning={enabled,audience,presetId,scope,multiplier,updatedAt:Date.now()};persist();audit('boss-tuning','admin',audience==='everyone'?'all-players':'test-accounts',{enabled,audience,presetId,scope,multiplier});res.json({ok:true,tuning:currentBossTuning()});});const INFINITE_TUNING_PRESETS=[
  {id:'I0',name:'Doux',multiplier:.5,detail:'Apparitions plus espacées et progression ralentie.'},
  {id:'I1',name:'Accessible',multiplier:.75,detail:'Progression modérée.'},
  {id:'I2',name:'Équilibré',multiplier:1,detail:'Courbe de référence.'},
  {id:'I3',name:'Exigeant',multiplier:1.35,detail:'Ennemis plus résistants et cadence accrue.'},
  {id:'I4',name:'Extrême',multiplier:1.8,detail:'Progression nettement plus agressive.'},
  {id:'I5',name:'Sans pitié',multiplier:2.4,detail:'Réglage maximal du laboratoire.'}
];
function currentInfiniteTuning(){const raw=db.infiniteTuning||{},preset=INFINITE_TUNING_PRESETS.find(x=>x.id===raw.presetId)||INFINITE_TUNING_PRESETS[2],multiplier=raw.presetId==='CUSTOM'?Math.max(.5,Math.min(2.5,Number(raw.multiplier)||1)):preset.multiplier;return {enabled:raw.enabled===true,audience:raw.audience==='everyone'?'everyone':'test',presetId:raw.presetId==='CUSTOM'?'CUSTOM':preset.id,multiplier,updatedAt:Number(raw.updatedAt)||0};}
app.get('/api/infinite-tuning',(req,res)=>res.json({ok:true,tuning:currentInfiniteTuning()}));
app.get('/api/admin/infinite-tuning',(req,res)=>{if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});res.json({ok:true,tuning:currentInfiniteTuning(),presets:INFINITE_TUNING_PRESETS});});
app.post('/api/admin/infinite-tuning',express.json({limit:'2kb'}),(req,res)=>{if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});const presetId=String(req.body?.presetId||'I2'),audience=String(req.body?.audience||'test'),enabled=req.body?.enabled===true;if(!['test','everyone'].includes(audience))return res.status(400).json({ok:false,error:'Choisis les comptes test ou tous les joueurs.'});let multiplier;if(presetId==='CUSTOM'){multiplier=Number(req.body?.multiplier);if(!Number.isFinite(multiplier)||multiplier<.5||multiplier>2.5)return res.status(400).json({ok:false,error:'La difficulté personnalisée doit être entre 0,50 et 2,50.'});multiplier=Math.round(multiplier*100)/100;}else{const preset=INFINITE_TUNING_PRESETS.find(x=>x.id===presetId);if(!preset)return res.status(400).json({ok:false,error:'Choisis un profil I0 à I5 ou une valeur personnalisée.'});multiplier=preset.multiplier;}db.infiniteTuning={enabled,audience,presetId,multiplier,updatedAt:Date.now()};persist();audit('infinite-tuning','admin',audience==='everyone'?'all-players':'test-accounts',{enabled,audience,presetId,multiplier});res.json({ok:true,tuning:currentInfiniteTuning()});});
app.get('/api/weekly-level', (req,res) => res.json({ok:true,level:activeWeeklyLevel()}));
app.get('/api/admin/weekly-level', (req,res) => { if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});res.json({ok:true,level:activeWeeklyLevel(),levels:WEEKLY_LEVELS.map(x=>({...x,rewardCredits:5000,rewardTitle:'Vainqueur de la Faille'}))}); });
app.post('/api/admin/weekly-level', express.json({limit:'2kb'}), (req,res) => { if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});const level=WEEKLY_LEVELS.find(x=>x.id===String(req.body?.levelId||''));if(!level)return res.status(400).json({ok:false,error:'Choisis un niveau dans la liste.'});const week=weekKey();db.weeklyLevel={week,levelId:level.id,source:'admin',updatedAt:Date.now()};audit('weekly-level','admin','all',{week,levelId:level.id});res.json({ok:true,level:activeWeeklyLevel()}); });

app.get('/api/announcement', (req,res) => res.json({ok:true, announcement: db.announcements || {text:'',updatedAt:0}}));
app.post('/api/admin/announcement', express.json({limit:'4kb'}), (req,res) => {
  if (!adminAllowed(req)) return res.status(403).json({ok:false,error:'Accès refusé.'});
  const text = String(req.body?.text || '').replace(/[\u0000-\u0008\u000b-\u001f]/g,'').trim().slice(0,1000);
  db.announcements = { text, updatedAt: Date.now() }; audit('announcement','admin','all',{text}); res.json({ok:true,announcement:db.announcements});
});
app.post('/api/admin/message/:id', express.json({limit:'3kb'}), (req,res) => {
  if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});
  const id=adminResolvePlayer(req.params.id);if(!id)return res.status(404).json({ok:false,error:'Joueur introuvable.'});
  const text=String(req.body?.text||'').replace(/[\u0000-\u0008\u000b-\u001f]/g,'').trim().slice(0,1000);if(text.length<2)return res.status(400).json({ok:false,error:'Le message doit contenir au moins 2 caractères.'});
  const waitSeconds=req.body?.waitTenSeconds===true?10:0,notice={id:crypto.randomBytes(10).toString('hex'),text,at:Date.now(),waitSeconds};db.moderationNotices[id]=[notice];persist();for(const sock of socketsOf(id))sock.emit('admin:notice',notice);audit('moderation-message','admin',id,{noticeId:notice.id,waitSeconds});res.json({ok:true,online:socketsOf(id).length>0});
});
app.get('/api/admin/live-frames', (req,res) => {
  if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});const cutoff=Date.now()-8000,frames=[];for(const [id,frame] of liveFrames){if(frame.at<cutoff){liveFrames.delete(id);continue;}const {image,...summary}=frame;frames.push(summary);}res.json({ok:true,frames});
});
app.get('/api/admin/live-frames/:id', (req,res) => {if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});const id=adminResolvePlayer(req.params.id),frame=id&&liveFrames.get(id);if(!frame||Date.now()-frame.at>8000)return res.status(404).json({ok:false,error:'Aucune image récente de la fenêtre Neon Strike.'});res.json({ok:true,frame});});
app.post('/api/admin/live-frames/:id/watch', express.json({limit:'1kb'}), (req,res) => {if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});const id=adminResolvePlayer(req.params.id);if(!id)return res.status(404).json({ok:false,error:'Joueur introuvable.'});const peers=socketsOf(id);if(!peers.length)return res.status(409).json({ok:false,error:'Le joueur doit être en ligne dans Neon Strike pour ouvrir la vue du jeu.'});liveFrames.delete(id);for(const sock of peers){sock.data.screenWatch=true;sock.data.lastFrameAt=0;sock.emit('admin:screen-watch',{enabled:true});}res.json({ok:true});});
app.post('/api/admin/live-frames/:id/stop', express.json({limit:'1kb'}), (req,res) => {if(!adminAllowed(req))return res.status(403).json({ok:false,error:'Accès refusé.'});const id=adminResolvePlayer(req.params.id);if(!id)return res.status(404).json({ok:false,error:'Joueur introuvable.'});for(const sock of socketsOf(id)){sock.data.screenWatch=false;sock.emit('admin:screen-watch',{enabled:false});}liveFrames.delete(id);res.json({ok:true});});
app.get('/api/admin/chats', (req,res) => {
  if (!adminAllowed(req)) return res.status(403).json({ok:false,error:'Accès refusé.'});
  const conversations = Object.entries(db.chats || {}).map(([key, messages]) => {
    const [a,b] = key.split('|');
    const profile = id => (rec(id).profile || {}).name || (accIndex.get(id)||{}).name || id;
    return { key, a, b, names: [profile(a), profile(b)], messages: (Array.isArray(messages)?messages:[]).slice(-100).map(m=>({from:String(m.f||''),text:String(m.x||''),at:Number(m.t)||0})) };
  }).sort((a,b)=>(b.messages.at(-1)?.at||0)-(a.messages.at(-1)?.at||0)).slice(0,200);
  res.json({ok:true,conversations});
});
app.get('/api/admin/audit', (req,res) => {
  if (!adminAllowed(req)) return res.status(403).json({ok:false,error:'Accès refusé.'});
  res.json({ok:true, entries:(db.audit||[]).slice(0,200)});
});
app.post('/api/admin/ban/:id', express.json({limit:'2kb'}), (req,res) => {
  if (!adminAllowed(req)) return res.status(403).json({ok:false,error:'Accès refusé.'});
  const id=adminResolvePlayer(req.params.id), reason=String(req.body?.reason||'Modération').slice(0,200);
  if (!id) return res.status(404).json({ok:false,error:'Joueur introuvable. Vérifie le pseudo ou l’ID.'});
  db.bans=db.bans||{}; db.bans[id]={reason,at:Date.now()}; audit('ban','admin',id,{reason});
  for(const sock of socketsOf(id)){sock.emit('account:banned',{reason}); setTimeout(()=>{ if(sock.connected) sock.disconnect(true); },250);}
  res.json({ok:true});
});
app.post('/api/admin/unban/:id', express.json({limit:'1kb'}), (req,res) => {
  if (!adminAllowed(req)) return res.status(403).json({ok:false,error:'Accès refusé.'});
  const id=adminResolvePlayer(req.params.id); if(!id) return res.status(404).json({ok:false,error:'Joueur introuvable. Vérifie le pseudo ou l’ID.'}); if(db.bans) delete db.bans[id]; audit('unban','admin',id,{}); res.json({ok:true});
});
app.post('/api/admin/grant-dev/:id', express.json({limit:'1kb'}), (req,res) => {
  if (!adminAllowed(req)) return res.status(403).json({ok:false,error:'Accès refusé.'});
  const id=adminResolvePlayer(req.params.id);
  if(!id) return res.status(404).json({ok:false,error:'Joueur introuvable. Vérifie le pseudo ou l’ID.'});
  const r=rec(id); r.data={...(r.data||{}), credits:2147483647, unlockedLevel:999,
    ownedShips:Array.from(new Set([...(Array.isArray(r.data?.ownedShips)?r.data.ownedShips:[]), 'default','speeder','tanker','multi','sniper','destroyer','phantom','nova','vanguard','eclipse'])),
    ownedUpgrades:Array.from(new Set([...(Array.isArray(r.data?.ownedUpgrades)?r.data.ownedUpgrades:[]), 'speed1','fire1','life1','dmg1','speed2','fire2','life2','dmg2'])),
    devAccount:true, devTest:true, chestLast6h:0, chestLast24h:0, chest6hAt:0, chest24hAt:0};
  r.t=Date.now(); audit('dev-grant','admin',id,{credits:'max',level:999,ships:'all configured',chests:'no cooldown'}); persist();
  for(const sock of socketsOf(id)) sock.emit('admin:test-updated',{ok:true});
  res.json({ok:true,id,name:(accIndex.get(id)||{}).name||((r.profile||{}).name)||id, data:r.data});
});

// Réinitialise uniquement la progression d'un compte, sans supprimer son pseudo ni ses amis.
app.post('/api/admin/reset-account/:id', express.json({limit:'1kb'}), (req,res) => {
  if (!adminAllowed(req)) return res.status(403).json({ok:false,error:'Accès refusé.'});
  const id=adminResolvePlayer(req.params.id);
  const acc=id && accIndex.get(id);
  if(!acc) return res.status(404).json({ok:false,error:'Compte enregistré introuvable. Les invités ne peuvent pas être réinitialisés ici.'});
  const r=rec(id);
  r.data={credits:0,unlockedLevel:0,ownedShips:['default'],equippedShip:'default',ownedUpgrades:[],upgradeState:{},shipUpgradeStates:{},shipColors:{},bestScores:{},chestLast6h:0,chestLast24h:0,chest6hAt:0,chest24hAt:0};
  r.t=Date.now(); audit('account-reset','admin',id,{name:acc.name}); persist();
  for(const sock of socketsOf(id)) sock.emit('admin:account-reset',{ok:true});
  res.json({ok:true,name:acc.name});
});

// Supprime définitivement le compte et libère le pseudo. Les autres comptes restent intacts.
app.post('/api/admin/delete-account/:id', express.json({limit:'1kb'}), (req,res) => {
  if (!adminAllowed(req)) return res.status(403).json({ok:false,error:'Accès refusé.'});
  const id=adminResolvePlayer(req.params.id);
  const acc=id && accIndex.get(id);
  if(!acc) return res.status(404).json({ok:false,error:'Compte enregistré introuvable. Les invités ne peuvent pas être supprimés ici.'});
  const key=nameKey(acc.name);
  for(const sock of socketsOf(id)){ sock.emit('admin:account-deleted',{ok:true}); sock.disconnect(true); }
  delete db.accounts[key]; accIndex.delete(id); delete db.saves[id];
  for(const [tok,session] of Object.entries(db.sessions||{})) if(session.a===id) delete db.sessions[tok];
  for(const [device,remembered] of Object.entries(db.remember||{})) if(remembered===id) delete db.remember[device];
  for(const [device,seen] of Object.entries(db.devSeen||{})){db.devSeen[device]=seen.filter(e=>e.a!==id);if(!db.devSeen[device].length)delete db.devSeen[device];}
  if(db.bans) delete db.bans[id];
  for(const r of Object.values(db.saves)){
    const code=(r.profile||{}).code;
    if(code){r.friends=(r.friends||[]).filter(c=>c!==code);r.requests=(r.requests||[]).filter(c=>c!==code);delete r.unread?.[code];}
  }
  for(const k of Object.keys(db.chats||{})) if(k.split('|').includes(id)) delete db.chats[k];
  for(const [code,owner] of codeIndex.entries()) if(owner===id) codeIndex.delete(code);
  audit('account-delete','admin',id,{name:acc.name,pseudoReleased:true}); persist();
  res.json({ok:true,name:acc.name,pseudoReleased:true});
});
app.post('/api/admin/friends/:id', express.json({limit:'2kb'}), (req,res) => {
  if (!adminAllowed(req)) return res.status(403).json({ok:false,error:'Accès refusé.'});
  const id=adminResolvePlayer(req.params.id), otherId=adminResolvePlayer(req.body?.otherId);
  if(!id || !otherId) return res.status(404).json({ok:false,error:'Un des deux joueurs est introuvable. Vérifie les pseudos ou IDs.'});
  const a=rec(id), b=rec(otherId);
  if(!a.profile || !b.profile || id===otherId) return res.status(400).json({ok:false,error:'Les deux joueurs doivent avoir un profil distinct.'});
  a.friends=[...new Set([...(a.friends||[]),b.profile.code])]; b.friends=[...new Set([...(b.friends||[]),a.profile.code])];
  audit('force-friend','admin',id,{otherId}); persist(); emitTo(id,'social',socialState(id)); emitTo(otherId,'social',socialState(otherId)); res.json({ok:true});
});

app.get('/health', (req, res) => res.json({ ok: true, release: RELEASE_ID, rooms: rooms.size, players: io.engine.clientsCount }));
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
const liveFrames = new Map();
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
  socket.data.connectedAt = Date.now();
  socket.data.dev = dev;
  if (db.bans && db.bans[vid]) { socket.emit('account:banned',{reason:db.bans[vid].reason||'Compte suspendu'}); return socket.disconnect(true); }
  socket.data.room = null;
  socket.data.activity = 'Connecté au jeu'; socket.data.location = 'Menu principal'; socket.data.lastActivityAt = Date.now();
  socket.data.chatTimes = []; socket.data.screenWatch=false; socket.data.lastFrameAt=0;
  for(const notice of (db.moderationNotices[vid]||[])) socket.emit('admin:notice',notice);
  socket.on('admin:notice:ack',noticeId=>{const queue=db.moderationNotices[vid]||[];db.moderationNotices[vid]=queue.filter(n=>n.id!==String(noticeId||''));if(!db.moderationNotices[vid].length)delete db.moderationNotices[vid];persist();});
  socket.on('screen:frame',payload=>{const now=Date.now(),image=payload&&payload.version===9?payload.image:null;if(!socket.data.screenWatch||now-socket.data.lastFrameAt<300||typeof image!=='string'||image.length>390000||!/^data:image\/jpeg;base64,[A-Za-z0-9+/=]+$/.test(image))return;socket.data.lastFrameAt=now;liveFrames.set(vid,{id:vid,name:(accIndex.get(vid)||{}).name||(rec(vid).profile||{}).name||'Invité',at:now,image});});
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
    if (set) { set.delete(socket.id); if (!set.size) { online.delete(vid); liveFrames.delete(vid); notifyFriendsOf(vid); } }
    vid = newId; socket.data.vid = vid;
  socket.data.connectedAt = Date.now();
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
  socket.on('rank:get', (ack) => { if (typeof ack === 'function') ack(rankPayload(vid)); });
  socket.on('rank:find', (query, ack) => {
    if (typeof ack !== 'function') return; const q=String(query||'').trim().slice(0,16).toLocaleLowerCase();
    if(q.length<2)return ack({ok:true,items:[]});
    const items=['overall','endless','campaign'].flatMap(category=>leaderboard(category).filter(p=>p.name.toLocaleLowerCase().includes(q)).slice(0,20).map(p=>({...p,category})));
    ack({ok:true,items:items.slice(0,30)});
  });

  socket.on('rank:start', (d, ack) => {
    if (typeof ack !== 'function') return;
    if (!accIndex.has(vid)) return ack({ok:false,error:'Connecte-toi à un compte pour participer au classement.'});
    const save=db.saves[vid]||{};if(save.data&&(save.data.devAccount||save.data.devTest))return ack({ok:false,error:'Les comptes en mode test ne participent pas au classement.'});
    const mode = d && d.mode === 'endless' ? 'endless' : d && d.mode === 'campaign' ? 'campaign' : null;
    if (!mode) return ack({ok:false,error:'Mode classé inconnu.'});
    const ticket = crypto.randomBytes(18).toString('hex');
    rankTickets.set(ticket,{id:vid,mode,started:Date.now(),lastPulse:Date.now(),activeMs:0});
    for (const [k,v] of rankTickets) if (Date.now()-v.started > 3*3600000) rankTickets.delete(k);
    ack({ok:true,ticket});
  });
  socket.on('rank:pulse', (d) => {
    const t=d&&rankTickets.get(String(d.ticket||'')); if(!t||t.id!==vid)return;
    const now=Date.now(),delta=Math.max(0,Math.min(3000,now-t.lastPulse));
    if(d.active===true)t.activeMs+=delta; t.lastPulse=now;
  });
  socket.on('rank:submit', (d, ack) => {
    if (typeof ack !== 'function') return;
    const t = d && rankTickets.get(String(d.ticket||''));
    if (!t || t.id !== vid) return ack({ok:false,error:'Cette partie ne peut pas être validée.'});
    rankTickets.delete(String(d.ticket));
    const elapsed = Date.now()-t.started, activeMs=Number(t.activeMs)||0, now=Date.now(), last=rankLastSubmit.get(vid)||0;
    const save=db.saves[vid]||{};if(!accIndex.has(vid)||(save.data&&(save.data.devAccount||save.data.devTest))||activeMs<12000||elapsed>3*3600000||now-last<5000)return ack({ok:false,error:'Résultat invalide, compte test ou durée de partie insuffisante.'});
    rankLastSubmit.set(vid,now);
    // Le score classé est calculé par le serveur à partir du temps écoulé; aucun score client n'est accepté.
    const score = Math.floor(activeMs/1000)*100;
    const r=db.ranked[vid]||(db.ranked[vid]={id:vid,overall:0,endless:0,campaign:0,stats:{runs:0,bestTime:0},titles:[]});
    const improved=score>Number(r[t.mode]||0);
    r[t.mode]=Math.max(Number(r[t.mode]||0),score); r.overall=Math.max(Number(r.overall)||0,r.endless+r.campaign);
    r.stats.runs=(Number(r.stats.runs)||0)+1; r.stats.bestTime=Math.max(Number(r.stats.bestTime)||0,Math.floor(activeMs/1000)); r.updatedAt=Date.now();
    persist(); ack({ok:true,improved,score,rank:rankPayload(vid)});
  });


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
    if (ok) {
      const r = myRec();
      // Ne jamais laisser une sauvegarde client effacer les privilèges accordés par la console.
      if (r.data && (r.data.devAccount || r.data.devTest)) {
        data = { ...data, devAccount: true, devTest: true, credits: 2147483647,
          unlockedLevel: 999,
          ownedShips: Array.from(new Set([...(Array.isArray(data.ownedShips) ? data.ownedShips : []), ...(Array.isArray(r.data.ownedShips) ? r.data.ownedShips : []), 'default','speeder','tanker','multi','sniper','destroyer','phantom','nova','vanguard','eclipse'])),
          ownedUpgrades: Array.from(new Set([...(Array.isArray(data.ownedUpgrades) ? data.ownedUpgrades : []), ...(Array.isArray(r.data.ownedUpgrades) ? r.data.ownedUpgrades : []), 'speed1','fire1','life1','dmg1','speed2','fire2','life2','dmg2'])),
          chestLast6h: 0, chestLast24h: 0, chest6hAt: 0, chest24hAt: 0 };
      }
      r.data = data; r.t = Date.now(); persist();
    }
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
    const rawText = String(d.text || '').trim();
    const pay = rawText.match(/^\/pay\s+(\d+)$/i);
    if (pay) {
      const amount = Number(pay[1]), friendCode = code;
      if (!me.profile || !me.friends.includes(friendCode)) return ack({ok:false,error:'Ajoute ce joueur comme ami avant de lui envoyer des crédits.'});
      if (!Number.isSafeInteger(amount) || amount < 1 || amount > 1000000) return ack({ok:false,error:'Montant invalide (1 à 1 000 000).'});
      const targetId=codeIndex.get(friendCode); if(!targetId) return ack({ok:false,error:'Joueur introuvable.'});
      const from=rec(vid), to=rec(targetId); from.data=from.data||{}; to.data=to.data||{}; const fromTest=!!(from.data.devAccount||from.data.devTest),toTest=!!(to.data.devAccount||to.data.devTest); if(fromTest&&!toTest)return ack({ok:false,error:'Les comptes test peuvent envoyer des crédits uniquement à d’autres comptes test.'});
      const balance=Math.max(0,Number(from.data.credits)||0);
      if(balance<amount) return ack({ok:false,error:'Tu n’as pas assez de crédits.'});
      from.data.credits=balance-amount; to.data.credits=Math.min(2147483647,(Number(to.data.credits)||0)+amount);
      from.t=to.t=Date.now(); audit('credit-transfer',myCode()||vid,targetId,{amount,from:vid,to:targetId});
      const msg={f:me.profile.code,x:`💸 ${me.profile.name} t’a envoyé ${amount} crédits !`,t:Date.now()};
      const key=chatKey(me.profile.code,friendCode); (db.chats[key]=db.chats[key]||[]).push(msg);
      emitTo(targetId,'chat',{with:me.profile.code,name:me.profile.name,msg});
      for(const ss of socketsOf(vid)) ss.emit('admin:credits-updated',{credits:from.data.credits});
      for(const ss of socketsOf(targetId)) ss.emit('admin:credits-updated',{credits:to.data.credits});
      persist(); return ack({ok:true,msg,transfer:{amount,balance:from.data.credits}});
    }
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
    if (data && typeof data === 'object') {
      if (data.t === 'spectate-consent' || data.t === 'spectate-frame') return;
      socket.data.lastActivityAt = Date.now();
      if (data.t === 'hello' || data.t === 'start') { socket.data.activity = 'Préparation / lancement de partie'; socket.data.location = 'Salle multijoueur'; }
      else if (data.t === 'coop-state') { socket.data.activity = data.alive ? 'En mission coop' : 'Éliminé en coop'; socket.data.location = `Coop · niveau ${(Number(data.level)||0)+1}, vague ${Number(data.wave)||1}`; }
      else if (data.t === 'coop-world') { socket.data.activity = 'En mission coop'; socket.data.location = `Coop · niveau ${(Number(data.level)||0)+1}, vague ${Number(data.wave)||1}`; }
      else if (data.t === 'bye') { socket.data.activity = 'Fin de partie'; socket.data.location = 'Menu / sortie de partie'; }
      else if (data.t === 'ship') { socket.data.activity = 'Sélection de vaisseau'; }
    }
    const room = rooms.get(socket.data.room);
    const peer = other(room, socket.id);
    if (peer) io.to(peer).emit('m', data);
  });

  socket.on('leave', () => leaveRoom(socket));
  socket.on('disconnect', () => {
    socket.data.screenWatch=false;
    const sessionRec = rec(vid); sessionRec.lastOnlineAt = Date.now(); sessionRec.lastSessionSeconds = Math.max(0, Math.round((Date.now() - (socket.data.connectedAt || Date.now())) / 1000)); persist();
    leaveRoom(socket);
    const set = online.get(vid);
    if (set) { set.delete(socket.id); if (!set.size) { online.delete(vid); liveFrames.delete(vid); notifyFriendsOf(vid); } }
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
  migrateLegacyFeedback();
  server.listen(PORT, '0.0.0.0', () => console.log(`NEON STRIKE serveur sur le port ${PORT}${useSupabase ? ' (Supabase)' : ''}`));
})();

