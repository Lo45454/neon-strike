
// NEON STRIKE — serveur de jeu (Express + Socket.IO)
// Héberge le jeu et gère : duels, comptes, sauvegarde, amis, chat, invitations, mode créateur.
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const CREATOR_PASS = process.env.CREATOR_PASS || 'NEON-FKQX28';
const SOCKET_PATH = '/port/5000/socket.io';

const app = express();
const httpServer = http.createServer(app);
const io = new Server(httpServer, { path: SOCKET_PATH, cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

// ---------- Mémoire ----------
const accounts = new Map();   // name -> {pass, token, save, creator, profile, friends, requests}
const tokens = new Map();     // token -> name
const guestSaves = new Map(); // vid -> save
const guestProfiles = new Map(); // vid -> {name, code, creator, friends, requests}
const chats = new Map();      // "codeA|codeB" -> [{f,t,x}]
const rooms = new Map();      // code -> {sockets:[id,id], host:id}
const socketToRoom = new Map();

const genCode = (n) => {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c=''; for(let i=0;i<n;i++) c+=chars[Math.floor(Math.random()*chars.length)];
  return c;
};
const hash = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');
const newToken = () => crypto.randomBytes(24).toString('hex');
const chatKey = (a,b) => [a,b].sort().join('|');

function getAccountByToken(token){ if(!token) return null; const name=tokens.get(token); return name?accounts.get(name):null; }
function publicProfile(acct){
  return { name: acct.profile.name, code: acct.profile.code };
}
function socialState(acct){
  return {
    ok:true,
    profile: publicProfile(acct),
    friends: acct.friends.map(f=>{
      const other = accounts.get(f);
      const online = other && [...io.sockets.sockets.values()].some(s=>s.data.accountName===f);
      const busy = other && [...rooms.values()].some(r=>r.sockets.some(id=>{const so=io.sockets.sockets.get(id); return so&&so.data.accountName===f;}));
      return { name: other?other.profile.name:f, code:f, online, busy, unread: acct.unread&&acct.unread[f]||0 };
    }),
    requests: acct.requests.map(r=>({ name:(accounts.get(r)||{profile:{name:r}}).profile.name, code:r, online:!!accounts.get(r)&&[...io.sockets.sockets.values()].some(s=>s.data.accountName===r) }))
  };
}
function ensureGuest(vid){
  if(!guestProfiles.has(vid)){
    guestProfiles.set(vid,{ name:'Joueur-'+vid.slice(0,4).toUpperCase(), code:genCode(6), creator:false, friends:[], requests:[], unread:{} });
  }
  return guestProfiles.get(vid);
}
function guestSocial(vid){
  const g=ensureGuest(vid);
  return { ok:true, profile:{name:g.name,code:g.code}, friends:[], requests:[], guestProgress:true };
}

// ---------- Sauvegarde REST (secours) ----------
app.get('/port/5000/api/save', (req,res)=>{
  const auth=req.headers.authorization||'';
  const acct=getAccountByToken(auth.replace('Bearer ',''));
  if(acct) return res.json({ok:true, data:acct.save, creator:acct.creator});
  const vid=req.query.vid;
  if(vid&&guestSaves.has(vid)) return res.json({ok:true, data:guestSaves.get(vid), creator:ensureGuest(vid).creator});
  res.json({ok:true, data:null});
});
app.post('/port/5000/api/save', (req,res)=>{
  let body=''; req.on('data',c=>body+=c); req.on('end',()=>{
    try{
      const data=JSON.parse(body||'{}');
      const vid=req.query.vid;
      if(vid) guestSaves.set(vid,data);
      res.json({ok:true});
    }catch(e){ res.status(400).json({ok:false}); }
  });
});

// ---------- Socket.IO ----------
io.on('connection', (socket)=>{
  const vid=socket.handshake.query.vid||socket.id;
  socket.data.vid=vid;

  // Reconnexion avec token de compte
  const token=socket.handshake.auth&&socket.handshake.auth.token;
  const acct=getAccountByToken(token);
  if(acct){ socket.data.accountName=acct.profile.name; }

  const emitAuth=()=>{
    const a=getAccountByToken(socket.data.accountName?tokens.get([...tokens.entries()].find(([t,n])=>n===socket.data.accountName)[0]):null);
    if(socket.data.accountName){
      const name=socket.data.accountName, acc=accounts.get(name);
      const tok=[...tokens.entries()].find(([t,n])=>n===name)[0];
      socket.emit('m',{t:'auth:status',ok:true,token:tok,account:{name},shared:false,guestProgress:false,data:acc.save,creator:acc.creator});
    } else {
      const g=ensureGuest(vid);
      socket.emit('m',{t:'auth:status',ok:true,token:null,account:null,shared:false,guestProgress:true,data:guestSaves.get(vid)||null,creator:g.creator});
    }
  };
  socket.on('auth:status', (cb)=>{ 
    if(socket.data.accountName){
      const name=socket.data.accountName, acc=accounts.get(name);
      const tok=[...tokens.entries()].find(([t,n])=>n===name)[0];
      cb({ok:true,token:tok,account:{name},shared:false,guestProgress:false,data:acc.save,creator:acc.creator});
    } else {
      const g=ensureGuest(vid);
      cb({ok:true,token:null,account:null,shared:false,guestProgress:true,data:guestSaves.get(vid)||null,creator:g.creator});
    }
  });

  // ----- Comptes -----
  socket.on('auth:register', (p,cb)=>{
    const name=String(p.name||'').trim().toLowerCase();
    if(name.length<3||accounts.has(name)) return cb({ok:false,error:'Pseudo déjà pris ou trop court.'});
    const token=newToken();
    const acc={ pass:hash(p.pass), token, creator:false,
      save:{credits:0,ownedShips:['default'],equippedShip:'default',ownedUpgrades:[],unlockedLevel:0,bestScores:{}},
      profile:{name,code:genCode(6)}, friends:[], requests:[], unread:{} };
    if(p.migrate&&guestSaves.has(vid)) acc.save=guestSaves.get(vid);
    accounts.set(name,acc); tokens.set(token,name);
    socket.data.accountName=name;
    cb({ok:true,token,account:{name},migrated:!!(p.migrate&&guestSaves.has(vid)),data:acc.save,creator:false,guestProgress:false});
    io.emit('social');
  });
  socket.on('auth:login', (p,cb)=>{
    const name=String(p.name||'').trim().toLowerCase();
    const acc=accounts.get(name);
    if(!acc||acc.pass!==hash(p.pass)) return cb({ok:false,error:'Pseudo ou mot de passe incorrect.'});
    const token=newToken(); tokens.set(token,name); acc.token=token;
    socket.data.accountName=name;
    cb({ok:true,token,account:{name},data:acc.save,creator:acc.creator,guestProgress:false});
  });
  socket.on('auth:logout', (cb)=>{
    socket.data.accountName=null;
    const g=ensureGuest(vid);
    cb({ok:true,token:null,account:null,guestProgress:true,data:guestSaves.get(vid)||null,creator:g.creator});
  });
  socket.on('auth:changePass', (p,cb)=>{
    const acc=socket.data.accountName?accounts.get(socket.data.accountName):null;
    if(!acc) return cb({ok:false,error:'Non connecté.'});
    if(acc.pass!==hash(p.old)) return cb({ok:false,error:'Ancien mot de passe incorrect.'});
    acc.pass=hash(p.pass); cb({ok:true});
  });

  // ----- Sauvegarde -----
  socket.on('save:set', (data)=>{
    if(socket.data.accountName) accounts.get(socket.data.accountName).save=data;
    else guestSaves.set(vid,data);
  });

  // ----- Mode créateur -----
  socket.on('creator:on', (pass,cb)=>{
    if(pass!==CREATOR_PASS) return cb({ok:false,error:'Mot de passe incorrect.'});
    if(socket.data.accountName){ accounts.get(socket.data.accountName).creator=true; }
    else { ensureGuest(vid).creator=true; }
    cb({ok:true});
  });
  socket.on('creator:off', ()=>{
    if(socket.data.accountName) accounts.get(socket.data.accountName).creator=false;
    else ensureGuest(vid).creator=false;
  });

  // ----- Amis / chat / invitations -----
  socket.on('social:get', (cb)=>{
    if(socket.data.accountName) cb(socialState(accounts.get(socket.data.accountName)));
    else cb(guestSocial(vid));
  });
  socket.on('social:add', (code,cb)=>{
    const me=socket.data.accountName?accounts.get(socket.data.accountName):ensureGuest(vid);
    const target=accounts.get(String(code).toLowerCase());
    if(!target) return cb({ok:false,error:'Aucun compte trouvé avec ce pseudo ou ce code.'});
    if(target.profile.code===me.profile.code) return cb({ok:false,error:'Tu ne peux pas t\'ajouter toi-même.'});
    if(!target.requests.includes(me.profile.code)) target.requests.push(me.profile.code);
    cb({ok:true,msg:'Demande envoyée.'});
    io.emit('social');
  });
  socket.on('social:respond', (p,cb)=>{
    const me=socket.data.accountName?accounts.get(socket.data.accountName):ensureGuest(vid);
    me.requests=me.requests.filter(c=>c!==p.code);
    if(p.accept){
      if(!me.friends.includes(p.code)) me.friends.push(p.code);
      const other=accounts.get(p.code);
      if(other&&!other.friends.includes(me.profile.code)) other.friends.push(me.profile.code);
    }
    cb({ok:true,...socialState(me)});
    io.emit('social');
  });
  socket.on('social:remove', (code,cb)=>{
    const me=socket.data.accountName?accounts.get(socket.data.accountName):ensureGuest(vid);
    me.friends=me.friends.filter(c=>c!==code);
    const other=accounts.get(code);
    if(other) other.friends=other.friends.filter(c=>c!==me.profile.code);
    cb({ok:true,...socialState(me)});
    io.emit('social');
  });
  socket.on('chat:history', (code,cb)=>{
    const me=socket.data.accountName?accounts.get(socket.data.accountName):ensureGuest(vid);
    cb({ok:true,messages:chats.get(chatKey(me.profile.code,code))||[]});
  });
  socket.on('chat:send', (p,cb)=>{
    const me=socket.data.accountName?accounts.get(socket.data.accountName):ensureGuest(vid);
    const msg={f:me.profile.code,t:Date.now(),x:String(p.text||'').slice(0,300)};
    const key=chatKey(me.profile.code,p.to);
    if(!chats.has(key)) chats.set(key,[]);
    chats.get(key).push(msg);
    if(chats.get(key).length>200) chats.get(key).shift();
    cb({ok:true,msg});
    // Livrer à tous les sockets du destinataire
    for(const s of io.sockets.sockets.values()){
      const other=s.data.accountName?accounts.get(s.data.accountName):ensureGuest(s.data.vid);
      if(other&&other.profile.code===p.to){
        s.emit('chat',{with:me.profile.code,name:me.profile.name,msg});
      }
    }
  });
  socket.on('chat:read', (code)=>{
    const me=socket.data.accountName?accounts.get(socket.data.accountName):ensureGuest(vid);
    if(me.unread) me.unread[code]=0;
    io.emit('social');
  });
  socket.on('invite:send', (p,cb)=>{
    const me=socket.data.accountName?accounts.get(socket.data.accountName):ensureGuest(vid);
    const room=socketToRoom.get(socket.id);
    if(!room) return cb({ok:false,error:"Crée d'abord une partie."});
    let sent=false;
    for(const s of io.sockets.sockets.values()){
      const other=s.data.accountName?accounts.get(s.data.accountName):ensureGuest(s.data.vid);
      if(other&&other.profile.code===p.to){ s.emit('invite',{from:me.profile.code,name:me.profile.name,room,mode:p.mode}); sent=true; }
    }
    cb(sent?{ok:true}:{ok:false,error:'Joueur non trouvé ou hors ligne.'});
  });
  socket.on('invite:reply', (p)=>{
    const from=io.sockets.sockets.get(p.to);
    if(from) from.emit('invite:reply',{accept:!!p.accept,name:'Adversaire'});
  });

  // ----- Duels -----
  socket.on('create', (cb)=>{
    let code; do{ code=genCode(5); }while(rooms.has(code));
    rooms.set(code,{sockets:[socket.id],host:socket.id});
    socketToRoom.set(socket.id,code);
    cb({ok:true,code});
  });
  socket.on('join', (code,cb)=>{
    code=String(code||'').toUpperCase();
    const room=rooms.get(code);
    if(!room) return cb({ok:false,error:'Partie introuvable.'});
    if(room.sockets.length>=2) return cb({ok:false,error:'Cette partie est déjà complète.'});
    room.sockets.push(socket.id);
    socketToRoom.set(socket.id,code);
    cb({ok:true});
    room.sockets.forEach(id=>{ const s=io.sockets.sockets.get(id); if(s) s.emit('peer-joined'); });
  });
  socket.on('m', (msg)=>{
    const code=socketToRoom.get(socket.id);
    const room=code&&rooms.get(code);
    if(!room) return;
    for(const id of room.sockets){
      if(id!==socket.id){ const s=io.sockets.sockets.get(id); if(s) s.emit('m',msg); }
    }
  });
  socket.on('leave', ()=>{
    const code=socketToRoom.get(socket.id);
    const room=code&&rooms.get(code);
    if(room){
      room.sockets=room.sockets.filter(id=>id!==socket.id);
      if(room.sockets.length===0) rooms.delete(code);
      else for(const id of room.sockets){ const s=io.sockets.sockets.get(id); if(s) s.emit('peer-left'); }
    }
    socketToRoom.delete(socket.id);
  });
  socket.on('disconnect', ()=>{
    const code=socketToRoom.get(socket.id);
    const room=code&&rooms.get(code);
    if(room){
      room.sockets=room.sockets.filter(id=>id!==socket.id);
      if(room.sockets.length===0) rooms.delete(code);
      else for(const id of room.sockets){ const s=io.sockets.sockets.get(id); if(s) s.emit('peer-left'); }
    }
    socketToRoom.delete(socket.id);
  });
});

httpServer.listen(PORT, ()=>console.log('NEON STRIKE server on port '+PORT));
