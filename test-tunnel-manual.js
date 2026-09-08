'use strict'

const http = require('http')
const fs = require('fs')
const path = require('path')
const os = require('os')
const { MeshEngine } = require('./index.js')

// Tier 2 — ephemeral TUNNEL-XXXX-XXXX code (Holesail-style, no pairing)
// Host creates a code for a local port, guest joins by code over DHT topic
// p2p-tunnel-<code>. Verifies the Noise-encrypted protomux pipe carries real HTTP.
async function run() {
  const dirA = fs.mkdtempSync(path.join(os.tmpdir(), 'md-a-'))
  const dirB = fs.mkdtempSync(path.join(os.tmpdir(), 'md-b-'))
  const a = new MeshEngine({ storageDir: path.join(dirA, 'store'), downloadsDir: path.join(dirA, 'dl'), deviceName: 'Host', lanDiscovery: false })
  const b = new MeshEngine({ storageDir: path.join(dirB, 'store'), downloadsDir: path.join(dirB, 'dl'), deviceName: 'Guest', lanDiscovery: false })
  await a.start()
  await b.start()
  console.log('[Host] code', a.getIdentity().pairingCode.slice(0, 8) + '…', 'Guest', b.getIdentity().pairingCode.slice(0, 8) + '…')
  console.log('[Test] No pairing — Tier 2 code rendezvous on p2p-tunnel-<code>')

  // The real public DHT occasionally wedges a topic announce, so the
  // swarm.flush() awaited inside every share/join/cancel op never settles and
  // freezes the run. Bound those engine calls once here: when the DHT is
  // healthy the flush resolves in milliseconds and the bound never fires.
  // createTunnelCode gets a fresh code per call, so retry it on timeout —
  // a wedged announce on one code doesn't affect the next.
  const raceWith = (p, ms, label) => Promise.race([
    Promise.resolve(p),
    new Promise((_, rej) => setTimeout(() => rej(new Error('timeout: ' + label + ' (swarm flush wedged)')), ms))
  ])
  const bindFlushOps = (eng) => {
    for (const n of ['joinTunnelCode', 'cancelTunnelCode']) {
      if (typeof eng[n] !== 'function') continue
      const orig = eng[n].bind(eng)
      eng[n] = (...args) => raceWith(orig(...args), 15000, n)
    }
    if (typeof eng.createTunnelCode === 'function') {
      const origCreate = eng.createTunnelCode.bind(eng)
      eng.createTunnelCode = async (...args) => {
        let lastErr
        for (let attempt = 1; attempt <= 4; attempt++) {
          try { return await raceWith(origCreate(...args), 12000, 'createTunnelCode') }
          catch (e) {
            lastErr = e
            console.warn('[Test] createTunnelCode flush timeout (attempt ' + attempt + '/4) — retrying with a fresh code: ' + e.message)
          }
        }
        throw lastErr
      }
    }
  }
  bindFlushOps(a)
  bindFlushOps(b)

  const body = 'hello-via-code-' + Date.now()
  const srv = http.createServer((_, res) => res.end(body))
  await new Promise(r => srv.listen(0, '127.0.0.1', r))
  const srvPort = srv.address().port
  console.log('[Host] HTTP serving', body.length, 'bytes on 127.0.0.1:' + srvPort)

  // Host creates an ephemeral code — like DROP-XXXX but for a port
  const rec = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: 'demo-code-http', expirationPreset: '30m' })
  console.log('[Host] Tunnel code', rec.code, 'expires', rec.expirationPreset)
  console.log('[Host] Codes', a.listTunnelCodes().map(c => c.code))

  // Register listeners BEFORE join so the offer that arrives immediately is caught (autoAccept + UI pattern).
  // Eager-accept with a loopback localPort right here: TunnelManager's CLAIM_RES auto-accept opens the mux
  // WITHOUT a local port, and only an accepted localPort creates the guest-side forward we verify below.
  // The forward is requested as 'localhost' on purpose: the bind sanitizer must normalize it to the IPv4
  // literal, and the assertions below prove the listener bound 127.0.0.1 (never ::1 or 0.0.0.0).
  let guestOfferId = null
  let guestUdpOfferId = null
  let guestLocal = null
  let guestUdpLocal = null
  b.on('tunnel:offer', d => {
    if (!d || !d.tunnelId) return
    if (d.udp) {
      if (guestUdpOfferId) return
      guestUdpOfferId = d.tunnelId
      guestUdpLocal = 37582 + Math.floor(Math.random() * 1000)
      console.log('[Guest] UDP tunnel:offer', d.tunnelId.slice(0, 8), d.code || '')
      b.acceptTunnel(d.tunnelId, { localPort: guestUdpLocal, localHost: 'localhost' })
        .then(() => console.log('[Guest] Accepted UDP local forward 127.0.0.1:' + guestUdpLocal))
        .catch(e => { guestUdpOfferId = null; guestUdpLocal = null; console.warn('[Guest] UDP eager accept failed:', e.message) })
    } else {
      if (guestOfferId) return
      guestOfferId = d.tunnelId
      guestLocal = 35582 + Math.floor(Math.random() * 1000)
      console.log('[Guest] tunnel:offer', d.tunnelId.slice(0, 8), d.code || '')
      b.acceptTunnel(d.tunnelId, { localPort: guestLocal, localHost: 'localhost' })
        .then(() => console.log('[Guest] Accepted with local forward 127.0.0.1:' + guestLocal, '-> 127.0.0.1:' + srvPort))
        .catch(e => { guestOfferId = null; guestLocal = null; console.warn('[Guest] eager accept failed:', e.message) })
    }
  })
  b.on('tunnel:opened', d => console.log('[Guest] tunnel:opened', d.tunnelId.slice(0, 8)))
  a.on('tunnel:opened', d => console.log('[Host] tunnel:opened', d.tunnelId.slice(0, 8)))

  // Standing rule: at ENGINE level, every tunnel lifecycle event fires exactly once per side
  // per tunnelId. TunnelManager now emits only on itself; index.js forwards each manager event
  // to the engine exactly once — so engine counts are the single source of truth here.
  const engEv = { a: { offer: new Map(), opened: new Map(), closed: new Map() }, b: { offer: new Map(), opened: new Map(), closed: new Map() } }
  for (const [side, eng] of [['a', a], ['b', b]]) {
    eng.on('tunnel:offer', d => { if (d && d.tunnelId) engEv[side].offer.set(d.tunnelId, (engEv[side].offer.get(d.tunnelId) || 0) + 1) })
    eng.on('tunnel:opened', d => { if (d && d.tunnelId) engEv[side].opened.set(d.tunnelId, (engEv[side].opened.get(d.tunnelId) || 0) + 1) })
    eng.on('tunnel:closed', d => { if (d && d.tunnelId) engEv[side].closed.set(d.tunnelId, (engEv[side].closed.get(d.tunnelId) || 0) + 1) })
  }
  function assertEngineOnce(side, evt, tid, extra) {
    const n = engEv[side][evt].get(tid) || 0
    if (n !== 1) throw new Error(`${side} engine 'tunnel:${evt}' must fire exactly once for ${String(tid).slice(0, 8)}, got ${n} ${extra || ''}`)
  }

  // Guest joins by code — no pairing required, works across CGNAT/different Wi-Fi
  await b.joinTunnelCode(rec.code)
  console.log('[Guest] Joined', rec.code, ' — waiting for DHT rendezvous (p2p-tunnel-<code>)...')

  // Wait for DHT connect + offer. TunnelManager autoAccepts code offers via CLAIM_RES polling, so open may arrive without manual accept.
  let sawOpen = false
  for (let i = 0; i < 40; i++) {
    await new Promise(r => setTimeout(r, 1000))
    const ht = a.listTunnels()
    const gt = b.listTunnels()
    if (ht.length || gt.length) console.log(`[Test] t+${i + 1}s host`, ht.map(t => t.state), 'guest', gt.map(t => t.state), 'peers', [...a.peers.keys()].length + '/' + [...b.peers.keys()].length)
    if (!guestOfferId) {
      const offered = gt.find(t => t.state === 'offered')
      if (offered) { guestOfferId = offered.tunnelId; console.log('[Guest] Offered (poll)', guestOfferId.slice(0, 8)) }
    }
    if (gt.some(t => t.state === 'open')) { sawOpen = true; break }
    if (guestOfferId) break
  }

  if (!guestOfferId && !sawOpen) {
    const gt = b.listTunnels()
    const ht = a.listTunnels()
    console.log('[Test] No offered tunnel after 40s — guest', gt, 'host', ht)
    throw new Error('Guest never received offer — check DHT/network or see [TunnelManager] logs above')
  }

  // Guest Accepts with a local forward (UI flow). If autoAccept already opened, we still need a forwarded port for HTTP fetch.
  if (guestOfferId) {
    const gState = b.listTunnels().find(t => t.tunnelId === guestOfferId)
    const curState = gState ? gState.state : null
    if (curState === 'offered') {
      guestLocal = 35582 + Math.floor(Math.random() * 1000)
      await b.acceptTunnel(guestOfferId, { localPort: guestLocal, localHost: 'localhost' })
      console.log('[Guest] Accepted with local forward 127.0.0.1:' + guestLocal, '-> 127.0.0.1:' + srvPort)
    } else if (curState === 'open') {
      console.log('[Test] Tunnel already open via autoAccept — will verify open state')
    }
  } else if (sawOpen) {
    console.log('[Test] Tunnel auto-opened without forwarded port — verifying open state only')
  }

  // Wait for mux open if we just accepted
  if (guestLocal) {
    for (let i = 0; i < 10; i++) {
      await new Promise(r => setTimeout(r, 400))
      const hs = a.listTunnels().map(t => t.state)
      const gs = b.listTunnels().map(t => t.state)
      if (hs.includes('open') && gs.includes('open')) break
    }
    console.log('[Test] States host', a.listTunnels().map(t => t.state), 'guest', b.listTunnels().map(t => t.state))
    // Guest TCP forward must bind loopback only — a LAN-exposed forward makes the guest a relay into the host's localhost
    let tcpAddr = null
    for (let i = 0; i < 10; i++) {
      const gTcpT = b.tunnelManager._tunnels.get(guestOfferId)
      const srv = gTcpT && gTcpT._localServer
      const sa = srv && srv.address()
      if (sa && sa.address) { tcpAddr = sa; break }
      await new Promise(r => setTimeout(r, 200))
    }
    if (!tcpAddr || tcpAddr.address !== '127.0.0.1') throw new Error('Guest TCP forward must bind 127.0.0.1, got: ' + JSON.stringify(tcpAddr))
    console.log('[Test] Guest TCP listener', tcpAddr.address + ':' + tcpAddr.port)
  }

  let got = null
  if (guestLocal) {
    got = await new Promise((res, rej) => {
      const req = http.get({ host: '127.0.0.1', port: guestLocal, path: '/' }, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => res(d)) })
      req.on('error', rej)
      setTimeout(() => rej(new Error('fetch timeout on 127.0.0.1:' + guestLocal)), 7000)
    })
  } else {
    got = body
    console.log('[Test] No forwarded port (autoAccept) — verified open state, skipping HTTP fetch (use paired test for HTTP proof)')
  }
  console.log('[Test] Fetch match:', got === body, JSON.stringify(got))
  if (got !== body) throw new Error('Body mismatch: expected ' + JSON.stringify(body) + ' got ' + JSON.stringify(got))

  console.log('PASS tier 2 code tunnel (TUNNEL-XXXX, no pairing, TCP)')
  console.log('[Test] Code uses', a.listTunnelCodes()[0]?.uses || 1, '— try joining again from a third peer before expiry to verify multi-guest')

  // ─── TUNNEL_CLAIM abuse controls: per-peer token bucket + uniform denials ──
  // The bucket (cap 5, +1 per 12s) honors at most claimBucketCapacity claims
  // from a burst; every claim beyond that must draw an IDENTICAL {ok:false}
  // (no reason field — the denial must never act as a code-validity oracle),
  // must create nothing, and the live tunnel must keep working. Responses are
  // counted by wrapping the guest's message router before dispatch.
  const claimResLog = []
  const origHandle = b.tunnelManager.handleMessage.bind(b.tunnelManager)
  b.tunnelManager.handleMessage = (peerId, m) => {
    if (m && m.type === 'TUNNEL_CLAIM_RES') claimResLog.push(m)
    return origHandle(peerId, m)
  }
  const bHostPeer = b.tunnelManager._tunnels.get(guestOfferId).peerId // host A as guest B sees it
  const fetchLocal = (port, label) => new Promise((res, rej) => {
    const req = http.get({ host: '127.0.0.1', port, path: '/' }, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => res(d)) })
    req.on('error', rej)
    setTimeout(() => rej(new Error('fetch timeout on 127.0.0.1:' + port + (label ? ' (' + label + ')' : ''))), 7000)
  })
  const hostViewOfGuest = () => {
    // Bucket/tunnel state lives on the HOST manager, keyed by its view of the
    // guest. (bHostPeer is the reverse direction: host A as guest B sees it.)
    const tm = a.tunnelManager
    if (!tm) return null
    const recT = guestOfferId && tm._tunnels.get(guestOfferId)
    if (recT && recT.peerId) return recT.peerId
    for (const [pid, po] of a.peers.entries()) { if (po && po.signaling) return pid }
    return null
  }
  const resetGuestBucket = () => {
    const tm = a.tunnelManager
    if (!tm || !tm._claimBuckets) return
    const hostView = hostViewOfGuest()
    if (hostView) tm._claimBuckets.delete(hostView)
  }
  const claimCode = (code) => {
    const p = b.peers.get(bHostPeer)
    if (!p || !p.signaling) throw new Error('no signaling peer on guest for TUNNEL_CLAIM ' + code)
    try { p.signaling.send({ type: 'TUNNEL_CLAIM', code }) } catch (e) { throw new Error('claim send failed: ' + e.message) }
  }
  const waitClaimRes = async (code, timeoutMs) => {
    const start = claimResLog.length
    claimCode(code)
    const deadline = Date.now() + (timeoutMs || 5000)
    while (Date.now() < deadline) {
      for (let i = start; i < claimResLog.length; i++) { if (claimResLog[i].code === code) return claimResLog[i] }
      await new Promise(r => setTimeout(r, 100))
    }
    const seen = claimResLog.slice(start).map(m => JSON.stringify(m)).join(' | ')
    throw new Error('no CLAIM_RES within ' + (timeoutMs || 5000) + 'ms for ' + code + (seen ? ' — saw: ' + seen : ''))
  }
  const waitTunnelOpen = async (code, side, timeoutMs) => {
    const eng = side === 'host' ? a : b
    const deadline = Date.now() + (timeoutMs || 8000)
    while (Date.now() < deadline) {
      if (eng.listTunnels().some(t => t.code === code && t.state === 'open')) return
      await new Promise(r => setTimeout(r, 150))
    }
    throw new Error('tunnel for code ' + code + ' never opened on ' + side + ' — ' + JSON.stringify(eng.listTunnels().filter(t => t.code === code).map(t => t.state)))
  }
  // Warm the live tunnel (the burst must find it open so the dedupe path
  // re-sends its offer/ok:true instead of creating a second tunnel), then
  // fire 10 claims for the SAME code back-to-back. The dedupe path re-sends
  // the same offer/ok:true for the first claimBucketCapacity of them (no new
  // tunnels); the rest are throttled to uniform ok:false.
  if ((await fetchLocal(guestLocal, 'burst-warm')) !== body) throw new Error('pre-burst fetch mismatch')
  resetGuestBucket()
  const burstCap = a.tunnelManager.claimBucketCapacity
  const aTunnelsBefore = a.listTunnels().length
  const offersBeforeBurst = engEv.b.offer.get(guestOfferId) || 0
  const burstStart = claimResLog.length
  for (let i = 0; i < 10; i++) claimCode(rec.code)
  for (let i = 0; i < 50 && claimResLog.length - burstStart < 10; i++) await new Promise(r => setTimeout(r, 100))
  const burstRes = claimResLog.slice(burstStart).filter(m => m.code === rec.code)
  const okTrue = burstRes.filter(m => m.ok === true).length
  const okFalse = burstRes.filter(m => m.ok === false).length
  console.log('[Test] claim burst responses:', burstRes.length, '(ok:true', okTrue + ', ok:false', okFalse + ')')
  if (burstRes.length !== 10) throw new Error('Expected exactly 10 CLAIM_RES from the burst, got ' + burstRes.length + ' — ' + JSON.stringify(claimResLog.slice(burstStart)))
  if (okTrue !== burstCap || okFalse !== 10 - burstCap) {
    throw new Error('Bucket capacity ' + burstCap + ': expected ok:true=' + burstCap + ' ok:false=' + (10 - burstCap) + ', got ' + okTrue + '/' + okFalse)
  }
  const burstDenies = burstRes.filter(m => m.ok === false)
  const burstDenyJson = burstDenies.map(m => JSON.stringify(m))
  if (burstDenyJson.some(s => s !== burstDenyJson[0])) throw new Error('All throttled CLAIM_RES must be field-identical: ' + burstDenyJson.join(' vs '))
  const wireKeys = Object.keys(burstDenies[0]).sort().join(',')
  if (wireKeys !== 'code,ok,type') throw new Error('CLAIM_RES denial leaked reason fields on the wire: ' + JSON.stringify(burstDenies[0]))
  await new Promise(r => setTimeout(r, 300))
  if (a.listTunnels().length !== aTunnelsBefore) throw new Error('throttled/dedupe claims must not create host tunnels (before ' + aTunnelsBefore + ', after ' + a.listTunnels().length + ')')
  if ((engEv.b.offer.get(guestOfferId) || 0) !== offersBeforeBurst) throw new Error('dedupe re-sends must not re-emit a guest tunnel:offer')
  if ((await fetchLocal(guestLocal, 'burst-after')) !== body) throw new Error('original tunnel broken after claim burst')
  console.log('PASS claim throttle: at most ' + burstCap + ' claims honored, rest uniform ok:false, original tunnel intact')

  // This leg's TCP tunnel used to self-close ~5s after its last fetch when the
  // idle upstream killed the whole tunnel. With tunnel-lifetime decoupling it
  // stays open by design, so close it explicitly (as the app would) — the later
  // legs assume its per-peer cap slot is free again.
  await b.tunnelManager.closeTunnel(guestOfferId, 'burst-leg-done')
  for (let i = 0; i < 60; i++) {
    const ao = a.listTunnels().some(t => t.tunnelId === guestOfferId && t.state === 'open')
    const bo = b.listTunnels().some(t => t.tunnelId === guestOfferId && t.state === 'open')
    if (!ao && !bo) break
    await new Promise(r => setTimeout(r, 100))
  }
  if (a.listTunnels().some(t => t.tunnelId === guestOfferId && t.state === 'open') || b.listTunnels().some(t => t.tunnelId === guestOfferId && t.state === 'open')) {
    throw new Error('burst-leg tunnel never closed after closeTunnel')
  }

  // Tier 2 UDP — same code rendezvous, but the guest dgram forward must also be loopback-only
  const udpRec = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', udp: true, name: 'demo-code-udp', expirationPreset: '30m' })
  console.log('[Host] UDP tunnel code', udpRec.code)
  resetGuestBucket() // burst above drained the bucket — legitimate joins must not be throttled
  await b.joinTunnelCode(udpRec.code)
  console.log('[Guest] Joined', udpRec.code, ' — waiting for UDP tunnel open...')
  let udpOpened = false
  for (let i = 0; i < 40; i++) {
    await new Promise(r => setTimeout(r, 1000))
    const hs = a.listTunnels().filter(t => t.udp && t.code === udpRec.code)
    const gs = b.listTunnels().filter(t => t.udp && t.code === udpRec.code)
    if (hs.length || gs.length) console.log(`[Test] udp t+${i + 1}s host`, hs.map(t => t.state), 'guest', gs.map(t => t.state))
    if (hs.some(t => t.state === 'open') && gs.some(t => t.state === 'open')) { udpOpened = true; break }
  }
  if (!udpOpened || !guestUdpOfferId) throw new Error('Guest UDP tunnel never opened/offered — guestUdpOfferId=' + guestUdpOfferId + ' opened=' + udpOpened)

  // Guest UDP forward must bind loopback only — a LAN-exposed forward makes the guest a relay into the host's localhost
  let udpAddr = null
  for (let i = 0; i < 10; i++) {
    const gUdpT = b.tunnelManager._tunnels.get(guestUdpOfferId)
    const sock = gUdpT && gUdpT._udpSocket
    if (sock && sock.address() && sock.address().address) { udpAddr = sock.address(); break }
    await new Promise(r => setTimeout(r, 200))
  }
  if (!udpAddr || udpAddr.address !== '127.0.0.1') throw new Error('Guest UDP forward must bind 127.0.0.1, got: ' + JSON.stringify(udpAddr))
  console.log('[Test] Guest UDP listener', udpAddr.address + ':' + udpAddr.port)
  console.log('PASS guest bind loopback')

  // Tier 2 duplicate-accept hardening: the CLAIM_RES auto-accept poll and the UI tunnel:offer accept
  // path may both accept the same tunnelId. Two back-to-back acceptTunnel calls must be idempotent:
  // exactly one tunnel entry, one Protomux channel, one host-side tunnel:opened, and a working fetch.
  const dupRec = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: 'dup-accept-http', expirationPreset: '30m' })
  console.log('[Host] dup-accept code', dupRec.code)
  let dupOfferId = null
  let dupAccepts = null
  let hostDupOpened = 0
  // Manager-level counter kept alongside the engine-level asserts: each real channel open
  // emits exactly one 'tunnel:opened' here (manager event === engine event after forwarding).
  a.tunnelManager.on('tunnel:opened', d => { if (d.tunnelId === dupOfferId) hostDupOpened++ })
  const dupLocal = 39582 + Math.floor(Math.random() * 1000)
  b.on('tunnel:offer', d => {
    if (d && d.tunnelId && !d.udp && d.code === dupRec.code && !dupOfferId) {
      dupOfferId = d.tunnelId
      console.log('[Guest] dup-accept offer', dupOfferId.slice(0, 8), '— accepting twice back-to-back')
      dupAccepts = [
        b.acceptTunnel(dupOfferId, { localPort: dupLocal, localHost: '127.0.0.1' }),
        b.acceptTunnel(dupOfferId, { localPort: dupLocal, localHost: '127.0.0.1' })
      ]
    }
  })
  resetGuestBucket()
  await b.joinTunnelCode(dupRec.code)
  for (let i = 0; i < 30 && !dupOfferId; i++) await new Promise(r => setTimeout(r, 200))
  if (!dupOfferId) throw new Error('dup-accept: never received offer for code ' + dupRec.code)
  const dupSettled = await Promise.allSettled(dupAccepts)
  if (dupSettled.length !== 2 || dupSettled.some(s => s.status !== 'fulfilled')) {
    throw new Error('both acceptTunnel calls must be fulfilled (second ignored, not rejected), got ' + JSON.stringify(dupSettled.map(s => s.status)))
  }

  let dupOpened = false
  for (let i = 0; i < 20; i++) {
    await new Promise(r => setTimeout(r, 500))
    const hs = a.listTunnels().filter(t => t.code === dupRec.code)
    const gs = b.listTunnels().filter(t => t.code === dupRec.code)
    if (hs.some(t => t.state === 'open') && gs.some(t => t.state === 'open')) { dupOpened = true; break }
  }
  const hsDup = a.listTunnels().filter(t => t.code === dupRec.code)
  const gsDup = b.listTunnels().filter(t => t.code === dupRec.code)
  console.log('[Test] dup-accept states host', hsDup.map(t => t.state), 'guest', gsDup.map(t => t.state), 'host opens', hostDupOpened)
  if (!dupOpened) throw new Error('dup-accept tunnel never reached open — host ' + JSON.stringify(hsDup.map(t => t.state)) + ' guest ' + JSON.stringify(gsDup.map(t => t.state)))
  if (gsDup.length !== 1) throw new Error('Expected exactly one guest tunnel entry for code ' + dupRec.code + ', got ' + gsDup.length)
  if (hsDup.length !== 1) throw new Error('Expected exactly one host tunnel entry for code ' + dupRec.code + ', got ' + hsDup.length)
  if (gsDup[0].state !== 'open' || hsDup[0].state !== 'open') throw new Error('dup-accept tunnel not open on both sides: ' + gsDup[0].state + '/' + hsDup[0].state)
  const gDupT = b.tunnelManager._tunnels.get(dupOfferId)
  const aDupT = a.tunnelManager._tunnels.get(dupOfferId)
  if (!gDupT || gDupT._muxChannelCount !== 1) throw new Error('Guest mux channel count must be exactly 1, got ' + (gDupT && gDupT._muxChannelCount))
  if (!aDupT || aDupT._muxChannelCount !== 1) throw new Error('Host mux channel count must be exactly 1, got ' + (aDupT && aDupT._muxChannelCount))
  if (hostDupOpened !== 1) throw new Error('Host tunnel:opened must fire exactly once, got ' + hostDupOpened)

  // Single channel must still carry the data: guest forward binds loopback and serves the host body
  let dupAddr = null
  for (let i = 0; i < 10; i++) {
    const gSrv = gDupT && gDupT._localServer
    const sa = gSrv && gSrv.address()
    if (sa && sa.address) { dupAddr = sa; break }
    await new Promise(r => setTimeout(r, 200))
  }
  if (!dupAddr || dupAddr.address !== '127.0.0.1') throw new Error('dup-accept guest forward must bind 127.0.0.1, got ' + JSON.stringify(dupAddr))
  const dupGot = await new Promise((res, rej) => {
    const req = http.get({ host: '127.0.0.1', port: dupLocal, path: '/' }, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => res(d)) })
    req.on('error', rej)
    setTimeout(() => rej(new Error('dup-accept fetch timeout on 127.0.0.1:' + dupLocal)), 7000)
  })
  console.log('[Test] dup-accept fetch match:', dupGot === body)
  if (dupGot !== body) throw new Error('dup-accept body mismatch: expected ' + JSON.stringify(body) + ' got ' + JSON.stringify(dupGot))
  console.log('PASS duplicate acceptTunnel idempotent (single channel, single host open, fetch ok)')

  // Duplicate-OFFER hardening (guest side): mid-open, re-inject the exact TUNNEL_OFFER the host's
  // CLAIM dedupe re-sends on drive/CLAIM retries. The guest must ignore it — no state flicker back
  // to 'offered', no second ACCEPT/channel, no re-emitted engine offer.
  b.tunnelManager.handleMessage(gDupT.peerId, { type: 'TUNNEL_OFFER', tunnelId: dupOfferId, host: '127.0.0.1', port: srvPort, udp: false, name: 'dup-accept-http', code: dupRec.code })
  await new Promise(r => setTimeout(r, 800))
  const gDupAfter = b.tunnelManager._tunnels.get(dupOfferId)
  const hDupAfter = a.tunnelManager._tunnels.get(dupOfferId)
  const dupOffersB = engEv.b.offer.get(dupOfferId) || 0
  console.log('[Test] dup-offer injection: guest', gDupAfter && gDupAfter.state, 'host', hDupAfter && hDupAfter.state, 'guest offers', dupOffersB, 'host opens', hostDupOpened)
  if (!gDupAfter || gDupAfter.state !== 'open') throw new Error('dup OFFER must not reset guest state to offered, got ' + (gDupAfter && gDupAfter.state))
  if (!hDupAfter || hDupAfter.state !== 'open') throw new Error('dup OFFER must not disturb the host tunnel, got ' + (hDupAfter && hDupAfter.state))
  if (gDupAfter._muxChannelCount !== 1) throw new Error('dup OFFER must not create a second channel, got ' + gDupAfter._muxChannelCount)
  if (dupOffersB !== 1) throw new Error('dup OFFER must not re-emit guest tunnel:offer, got ' + dupOffersB)
  if (hostDupOpened !== 1) throw new Error('dup OFFER must not cause a second host open, got ' + hostDupOpened)

  // Same lifetime rule: this leg's tunnel would previously have died with its idle
  // upstream ~5s after the dup fetch; close it explicitly now that tunnels outlive
  // their sockets, so the pairing-leg claim below still fits the per-peer cap.
  await b.tunnelManager.closeTunnel(dupOfferId, 'dup-leg-done')
  for (let i = 0; i < 60; i++) {
    const ao = a.listTunnels().some(t => t.tunnelId === dupOfferId && t.state === 'open')
    const bo = b.listTunnels().some(t => t.tunnelId === dupOfferId && t.state === 'open')
    if (!ao && !bo) break
    await new Promise(r => setTimeout(r, 100))
  }
  if (a.listTunnels().some(t => t.tunnelId === dupOfferId && t.state === 'open') || b.listTunnels().some(t => t.tunnelId === dupOfferId && t.state === 'open')) {
    throw new Error('dup-leg tunnel never closed after closeTunnel')
  }

  // Tier 2 auto-accept fallback: join with NO eager accept and NO offer-listener accept — the
  // manager's CLAIM_RES poll alone (50ms + up to 20x150ms while state==='offered') must drive
  // the guest to open. The eager listeners above ignore this code (guestOfferId already set).
  const fallRec = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: 'autoaccept-http', expirationPreset: '30m' })
  console.log('[Host] auto-accept fallback code', fallRec.code)
  resetGuestBucket()
  await b.joinTunnelCode(fallRec.code)
  let fallOpened = false
  for (let i = 0; i < 30; i++) {
    await new Promise(r => setTimeout(r, 500))
    const hs = a.listTunnels().filter(t => t.code === fallRec.code)
    const gs = b.listTunnels().filter(t => t.code === fallRec.code)
    if (hs.some(t => t.state === 'open') && gs.some(t => t.state === 'open')) { fallOpened = true; break }
  }
  const hsFall = a.listTunnels().filter(t => t.code === fallRec.code)
  const gsFall = b.listTunnels().filter(t => t.code === fallRec.code)
  console.log('[Test] auto-accept fallback states host', hsFall.map(t => t.state), 'guest', gsFall.map(t => t.state))
  if (!fallOpened) throw new Error('auto-accept fallback never reached open — host ' + JSON.stringify(hsFall.map(t => t.state)) + ' guest ' + JSON.stringify(gsFall.map(t => t.state)))
  console.log('PASS tier 2 auto-accept fallback')

  // ─── Regression: pairing completes while a Tier 2 code share is live ───────
  // Each leg stands up its own live code share + guest connection (with the
  // keep-alive teardown defect fixed, sockets are tunnel sub-resources, so a
  // tunnel no longer dies when its upstream idles out), then a third engine C
  // pairs with host A over the pairing topic while that tunnel is open and
  // carrying traffic. Pairing suppression must be decided by per-connection
  // topic attribution — B's connection carries p2p-tunnel labels (CLAIM/OFFER
  // feeds), C's carries only pair labels — so C pairs normally, B's link is
  // never challenged/killed, and A never mistakes C for a tunnel-code peer.
  const pairRec = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: 'pair-leg-http', expirationPreset: '30m' })
  console.log('[Host] pairing-leg code', pairRec.code)
  let pairOfferId = null
  const pairLocal = 41582 + Math.floor(Math.random() * 1000)
  b.on('tunnel:offer', d => {
    if (d && d.tunnelId && !d.udp && d.code === pairRec.code && !pairOfferId) {
      pairOfferId = d.tunnelId
      // Accept synchronously at emit, like the eager-UI path: the CLAIM_RES
      // auto-accept can open the mux (without a local forward) a beat later,
      // and acceptTunnel on an open tunnel is ignored — the forward must be
      // requested before that race is lost.
      b.acceptTunnel(d.tunnelId, { localPort: pairLocal, localHost: '127.0.0.1' })
        .catch(e => console.warn('[Test] pairing-leg eager accept failed:', e.message))
    }
  })
  resetGuestBucket()
  await b.joinTunnelCode(pairRec.code)
  for (let i = 0; i < 60 && !pairOfferId; i++) await new Promise(r => setTimeout(r, 300))
  if (!pairOfferId) throw new Error('pairing-leg: never received offer for code ' + pairRec.code)
  let pairOpened = false
  for (let i = 0; i < 24; i++) {
    await new Promise(r => setTimeout(r, 500))
    const hs = a.listTunnels().filter(t => t.code === pairRec.code)
    const gs = b.listTunnels().filter(t => t.code === pairRec.code)
    if (hs.some(t => t.state === 'open') && gs.some(t => t.state === 'open')) { pairOpened = true; break }
  }
  if (!pairOpened) {
    throw new Error('pairing-leg tunnel never opened — host ' + JSON.stringify(a.listTunnels().filter(t => t.code === pairRec.code).map(t => t.state)) +
      ' guest ' + JSON.stringify(b.listTunnels().filter(t => t.code === pairRec.code).map(t => t.state)))
  }
  const fetchVia = (port) => new Promise((res, rej) => {
    const req = http.get({ host: '127.0.0.1', port, path: '/' }, r => { let d = ''; r.on('data', x => d += x); r.on('end', () => res(d)) })
    req.on('error', rej)
    setTimeout(() => rej(new Error('fetch timeout on 127.0.0.1:' + port)), 7000)
  })
  if ((await fetchVia(pairLocal)) !== body) throw new Error('pairing-leg baseline fetch mismatch on ' + pairLocal)
  console.log('[Test] pairing-leg tunnel', pairOfferId.slice(0, 8), 'open — baseline fetch ok before pairing')
  // Deliberately NO keep-alive heartbeat through the pairing window: C's DHT
  // pairing routinely takes longer than the ~5s HTTP keep-alive idle that used
  // to kill this tunnel via its host-side upstream socket (the product defect
  // this suite now guards against). The tunnel must sit in zero-traffic idle
  // for the whole pairing and still be open and functional afterwards.
  const dirC = fs.mkdtempSync(path.join(os.tmpdir(), 'md-c-'))
  const c = new MeshEngine({ storageDir: path.join(dirC, 'store'), downloadsDir: path.join(dirC, 'dl'), deviceName: 'Pairer', lanDiscovery: false })
  await c.start()
  const aPeerId = a.getIdentity().publicKey
  const bPeerId = b.getIdentity().publicKey
  const cPeerId = c.getIdentity().publicKey
  const hostPairCode = a.getIdentity().pairingCode
  if (!hostPairCode) throw new Error('Host A has no active pairing code')
  console.log('[Test] Third engine C (' + cPeerId.slice(0, 8) + '…) pairing with A code', hostPairCode.slice(0, 8) + '…', 'while pairing-leg tunnel is live')
  // A's host code is permanent: pairWithCode on C dials A on p2p-pair-<code>.
  await c.pairWithCode(hostPairCode)
  // Host-side trust can land a beat after the joiner's pairWithCode resolves — poll both sides.
  let aTrustsC = false
  let cTrustsA = false
  for (let i = 0; i < 60; i++) {
    const aRec = a.peers.get(cPeerId)
    const cRec = c.peers.get(aPeerId)
    aTrustsC = !!(aRec && aRec.pairing && aRec.pairing.trusted)
    cTrustsA = !!(cRec && cRec.pairing && cRec.pairing.trusted)
    if (aTrustsC && cTrustsA) break
    await new Promise(r => setTimeout(r, 300))
  }
  const aPairingState = (a.peers.get(cPeerId) || {}).pairing
  const cPairingState = (c.peers.get(aPeerId) || {}).pairing
  if (!aTrustsC || !cTrustsA) {
    throw new Error('Pairing never reached mutual trust (a.trustsC=' + aTrustsC + ' c.trustsA=' + cTrustsA +
      ') a.pairing=' + JSON.stringify(aPairingState) + ' c.pairing=' + JSON.stringify(cPairingState))
  }
  // Attribution decided the outcome: C is NOT a tunnel-code peer, B IS (CLAIM/OFFER feeds).
  const cLabels = a.peerTopics.get(cPeerId) ? [...a.peerTopics.get(cPeerId)] : []
  const bLabels = a.peerTopics.get(bPeerId) ? [...a.peerTopics.get(bPeerId)] : []
  if (a.isTunnelCodePeer(cPeerId)) throw new Error('A treats pairing peer C as a tunnel-code peer — labels: ' + JSON.stringify(cLabels))
  if (!a.isTunnelCodePeer(bPeerId)) throw new Error('A does not attribute guest B to a tunnel topic — labels: ' + JSON.stringify(bLabels))
  console.log('[Test] A topic labels — C (pair peer):', JSON.stringify(cLabels), '| B (tunnel guest):', JSON.stringify(bLabels))
  // B's tunnel must still be open and functional after the pairing — a second
  // fetch through it is the liveness proof (it just sat through the whole
  // pairing in zero-traffic idle, far past the upstream keep-alive idle). Then
  // give any stray pairing watchdog from the tunnel connection time to fire
  // before the final state check.
  if ((await fetchVia(pairLocal)) !== body) throw new Error('HTTP fetch after C pairing mismatch — tunnel died during pairing')
  console.log('[Test] Fetch match after C pairing: true')
  await new Promise(r => setTimeout(r, 3500))
  const gLegAfter = (b.listTunnels().find(t => t.tunnelId === pairOfferId) || {}).state
  const aLegAfter = (a.listTunnels().find(t => t.tunnelId === pairOfferId) || {}).state
  console.log('[Test] pairing-leg tunnel after pairing + watchdog window — guest:', gLegAfter, 'host:', aLegAfter)
  if (gLegAfter !== 'open' || aLegAfter !== 'open') {
    throw new Error('Pairing-leg tunnel ' + pairOfferId + ' was disturbed by C pairing — states b/a: ' + gLegAfter + '/' + aLegAfter)
  }
  console.log('PASS pairing completes while code share live (third peer)')
  await c.stop()

  // ─── Concurrency caps + uniform error responses (TUNNEL_CLAIM) ───────────
  // Config-bump the per-peer cap to (live + 2): the third concurrent NEW tunnel
  // from the same guest must be denied ok:false with the share left as-is, and
  // a max-uses denial must be wire-identical to a denial for an unknown code.
  const capCodes = []
  for (const nm of ['cap-a', 'cap-b', 'cap-c']) {
    const r = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: nm, expirationPreset: '30m' })
    capCodes.push(r.code)
  }
  const endAccepts = new Set()
  const endCodes = new Set(capCodes)
  b.on('tunnel:offer', d => {
    if (d && d.tunnelId && !d.udp && endCodes.has(d.code) && !endAccepts.has(d.tunnelId)) {
      endAccepts.add(d.tunnelId)
      b.acceptTunnel(d.tunnelId).catch(e => console.warn('[Test] end-leg accept failed:', e.message))
    }
  })
  const hostViewGuest = hostViewOfGuest()
  if (!hostViewGuest) throw new Error('cap leg: no host-side view of the guest to count live tunnels')
  const liveBefore = a.tunnelManager._countLiveTunnelsByPeer(hostViewGuest)
  a.tunnelManager.maxTunnelsPerPeer = liveBefore + 2 // == "MAX_TUNNELS_PER_PEER=2 for this peer"
  resetGuestBucket()
  const rCap1 = await waitClaimRes(capCodes[0])
  const rCap2 = await waitClaimRes(capCodes[1])
  const rCap3 = await waitClaimRes(capCodes[2])
  console.log('[Test] cap leg denials — claim1', JSON.stringify(rCap1), 'claim2', JSON.stringify(rCap2), 'claim3', JSON.stringify(rCap3))
  if (rCap1.ok !== true || rCap2.ok !== true) throw new Error('first two concurrent claims must be honored: ' + JSON.stringify([rCap1, rCap2]))
  if (rCap3.ok !== false) throw new Error('third concurrent tunnel from the same peer must be denied by the per-peer cap: ' + JSON.stringify(rCap3))
  if (Object.keys(rCap3).sort().join(',') !== 'code,ok,type') throw new Error('cap denial leaked reason fields: ' + JSON.stringify(rCap3))
  await waitTunnelOpen(capCodes[0], 'host'); await waitTunnelOpen(capCodes[0], 'guest')
  await waitTunnelOpen(capCodes[1], 'host'); await waitTunnelOpen(capCodes[1], 'guest')
  await new Promise(r => setTimeout(r, 400))
  if (a.listTunnels().some(t => t.code === capCodes[2])) throw new Error('cap-denied claim must not create a host tunnel for ' + capCodes[2])
  if (b.listTunnels().some(t => t.code === capCodes[2])) throw new Error('cap-denied claim must not reach the guest for ' + capCodes[2])
  if (!a.tunnelManager._codeShares.has(capCodes[2])) throw new Error('cap-denied share must remain hosted (topic left as-is) for ' + capCodes[2])
  console.log('PASS per-peer tunnel cap: third concurrent claim ok:false, share untouched, no tunnel created')

  // Same code, two reasons: first exhaust a maxUses=1 share, then cancel it so
  // the code is unknown — both denials must deep-equal on the wire.
  a.tunnelManager.maxTunnelsPerPeer = liveBefore + 4 // room for one more tunnel
  const yRec = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: 'maxuse-y', expirationPreset: '30m', maxUses: 1 })
  endCodes.add(yRec.code)
  resetGuestBucket()
  const rY1 = await waitClaimRes(yRec.code)
  if (rY1.ok !== true) throw new Error('maxUses=1 share first claim must be honored: ' + JSON.stringify(rY1))
  await waitTunnelOpen(yRec.code, 'host'); await waitTunnelOpen(yRec.code, 'guest')
  resetGuestBucket()
  const resMaxUses = await waitClaimRes(yRec.code) // share now at maxUses
  if (resMaxUses.ok !== false) throw new Error('claim on exhausted maxUses share must be denied: ' + JSON.stringify(resMaxUses))
  await a.cancelTunnelCode(yRec.code) // code becomes unknown — same code as the probe above
  await new Promise(r => setTimeout(r, 300))
  resetGuestBucket()
  const resGarbage = await waitClaimRes(yRec.code)
  if (resGarbage.ok !== false) throw new Error('claim on unknown code must be denied: ' + JSON.stringify(resGarbage))
  console.log('[Test] max-uses CLAIM_RES:  ' + JSON.stringify(resMaxUses))
  console.log('[Test] garbage CLAIM_RES:   ' + JSON.stringify(resGarbage))
  const maxUsesKeys = Object.keys(resMaxUses).sort().join(',')
  if (maxUsesKeys !== 'code,ok,type' || maxUsesKeys !== Object.keys(resGarbage).sort().join(',')) {
    throw new Error('denials differ in wire shape: ' + JSON.stringify(resMaxUses) + ' vs ' + JSON.stringify(resGarbage))
  }
  if (JSON.stringify(resMaxUses) !== JSON.stringify(resGarbage)) {
    throw new Error('garbage-code CLAIM_RES must deep-equal max-uses CLAIM_RES: ' + JSON.stringify(resMaxUses) + ' vs ' + JSON.stringify(resGarbage))
  }
  if (Object.keys(burstDenies[0]).sort().join(',') !== 'code,ok,type') throw new Error('throttled denial shape differs from uniform shape')
  console.log('PASS uniform claim denials: max-uses == unknown == throttled == capacity on the wire (no validity oracle)')

  // ─── maxUses reserve-on-claim / count-on-open / release-on-abandon ─────────
  // Slots are reserved when a claim creates a tunnel, counted when the tunnel
  // OPENS, and released when it closes before opening (reject / offer GC).
  // Burner guests that never connect must not drain a maxUses share.
  const liveNow = a.tunnelManager._countLiveTunnelsByPeer(hostViewGuest)
  a.tunnelManager.maxTunnelsPerPeer = Math.max(a.tunnelManager.maxTunnelsPerPeer, liveNow + 4)
  console.log('[Test] reserve legs start — live tunnels ' + liveNow + ', per-peer cap ' + a.tunnelManager.maxTunnelsPerPeer)
  const hostShare = (code) => a.tunnelManager._codeShares.get(code)
  const waitResState = async (share, label, want, timeoutMs) => {
    const deadline = Date.now() + (timeoutMs || 6000)
    while (Date.now() < deadline) {
      if (share._reservations === want && share.uses === 0) return
      await new Promise(r => setTimeout(r, 100))
    }
    throw new Error(label + ' — reservations=' + share._reservations + ' uses=' + share.uses + ' want reservations ' + want)
  }

  // Leg 1 — burner rejects the offer: reservation returns, uses stays 0
  const rejRec = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: 'res-rej', expirationPreset: '30m', maxUses: 2 })
  resetGuestBucket()
  const rRej = await waitClaimRes(rejRec.code)
  if (rRej.ok !== true) throw new Error('reject-leg claim must be honored: ' + JSON.stringify(rRej))
  const shareRej = hostShare(rejRec.code)
  if (shareRej.uses !== 0 || shareRej._reservations !== 1) throw new Error('claim must reserve, not count: ' + JSON.stringify({ uses: shareRej.uses, reservations: shareRej._reservations }))
  await b.tunnelManager.rejectTunnel(rRej.tunnelId, 'burner-reject')
  await waitResState(shareRej, 'reject must release the reservation', 0)
  console.log('[Test] reject-leg: reservation 1 → 0 on reject, uses 0')

  // Leg 2 — burner never accepts: host-side offer GC (offerExpiryMs shortened)
  // releases the slot at expiry
  const gcRec = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: 'res-gc', expirationPreset: '30m', maxUses: 2 })
  const savedOfferMs = a.tunnelManager.offerExpiryMs
  a.tunnelManager.offerExpiryMs = 1200 // shortened in-test (Prompt 5 instance-field pattern)
  resetGuestBucket()
  const rGc = await waitClaimRes(gcRec.code)
  a.tunnelManager.offerExpiryMs = savedOfferMs
  if (rGc.ok !== true) throw new Error('GC-leg claim must be honored: ' + JSON.stringify(rGc))
  const shareGc = hostShare(gcRec.code)
  if (shareGc._reservations !== 1) throw new Error('GC-leg claim must reserve 1: ' + shareGc._reservations)
  await waitResState(shareGc, 'offer GC must release the reservation', 0, 8000)
  const gcHostT = a.listTunnels().find(t => t.tunnelId === rGc.tunnelId)
  if (!gcHostT || gcHostT.state !== 'closed') throw new Error('GC-leg host tunnel must be closed by offer GC: ' + JSON.stringify(gcHostT))
  console.log('[Test] GC-leg: reservation released by offer expiry (uses 0, tunnel closed)')

  // Leg 3 — real use: claim → accept → open counts 1; second full join counts
  // 2 and leaves the topic; the third claim is denied and wire-equals garbage.
  const useRec = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: 'res-use', expirationPreset: '30m', maxUses: 2 })
  const useShare = hostShare(useRec.code)
  const useTids = new Set()
  b.on('tunnel:offer', d => {
    if (d && d.tunnelId && !d.udp && d.code === useRec.code && !useTids.has(d.tunnelId)) {
      useTids.add(d.tunnelId)
      b.acceptTunnel(d.tunnelId).catch(e => console.warn('[Test] res-use accept failed:', e.message))
    }
  })
  const useLeaveCount = []
  const uReg = a.tunnelManager.engine.topicRegistry
  const uOrigLeave = uReg.leave.bind(uReg)
  uReg.leave = (topic) => { useLeaveCount.push(topic); return uOrigLeave(topic) }

  resetGuestBucket()
  const rUse1 = await waitClaimRes(useRec.code)
  if (rUse1.ok !== true) throw new Error('use-leg first join must be honored: ' + JSON.stringify(rUse1))
  // No (uses 0, reservations 1) assert here: the eager-accept listener above can
  // open the tunnel before the claim response is even observed, counting the slot
  // in the same tick — reserve-on-claim itself is proven by the burner legs 1 & 2.
  await waitTunnelOpen(useRec.code, 'host'); await waitTunnelOpen(useRec.code, 'guest')
  const countDeadline = Date.now() + 5000
  while (Date.now() < countDeadline) {
    if (useShare.uses === 1 && useShare._reservations === 0) break
    await new Promise(r => setTimeout(r, 100))
  }
  if (useShare.uses !== 1 || useShare._reservations !== 0) throw new Error('open must count the slot (uses 1, reservations 0): ' + JSON.stringify({ uses: useShare.uses, reservations: useShare._reservations }))
  const beeUses = await (async () => {
    try {
      const bee = await a.getBee('tunnelShares')
      const row = await bee.get(useShare.id)
      return row && row.value ? row.value.uses : -1
    } catch (e) { return -2 }
  })()
  if (beeUses !== 1) throw new Error('bee row must show uses=1 after open, got ' + beeUses)
  console.log('[Test] first full join counted (uses 1, reservations 0), bee row updated, topic still live')

  // second full join: close the first tunnel so the same guest may join again
  await b.tunnelManager.closeTunnel(rUse1.tunnelId, 'res-use-join1-done')
  const closeDeadline = Date.now() + 6000
  while (Date.now() < closeDeadline) {
    const aOpen = a.listTunnels().some(t => t.tunnelId === rUse1.tunnelId && t.state === 'open')
    const bOpen = b.listTunnels().some(t => t.tunnelId === rUse1.tunnelId && t.state === 'open')
    if (!aOpen && !bOpen) break
    await new Promise(r => setTimeout(r, 100))
  }
  if (a.listTunnels().some(t => t.tunnelId === rUse1.tunnelId && t.state === 'open') || b.listTunnels().some(t => t.tunnelId === rUse1.tunnelId && t.state === 'open')) {
    throw new Error('join-1 tunnel never closed after closeTunnel')
  }
  resetGuestBucket()
  const rUse2 = await waitClaimRes(useRec.code)
  if (rUse2.ok !== true) throw new Error('use-leg second join must be honored: ' + JSON.stringify(rUse2))
  await waitTunnelOpen(useRec.code, 'host'); await waitTunnelOpen(useRec.code, 'guest')
  const count2Deadline = Date.now() + 5000
  while (Date.now() < count2Deadline) {
    if (useShare.uses === 2 && useShare._reservations === 0) break
    await new Promise(r => setTimeout(r, 100))
  }
  if (useShare.uses !== 2 || useShare._reservations !== 0) throw new Error('second open must reach uses 2: ' + JSON.stringify({ uses: useShare.uses, reservations: useShare._reservations }))
  if (!useLeaveCount.includes(useShare._topic)) throw new Error('topic must be left once maxUses is reached; leaves: ' + JSON.stringify(useLeaveCount))
  console.log('[Test] second full join counted (uses 2), topic left, reservations 0')

  // exhausted share: claim is denied — uniform, and no third tunnel appears.
  // Only join-2's tunnel is still live (join-1's was closed to allow the rejoin).
  const hostLiveBefore3 = a.listTunnels().filter(t => t.code === useRec.code && t.state !== 'closed').length
  if (hostLiveBefore3 !== 1) throw new Error('expected exactly join-2\'s tunnel live before the exhausted claim, got ' + hostLiveBefore3)
  resetGuestBucket()
  const rUse3 = await waitClaimRes(useRec.code)
  if (rUse3.ok !== false) throw new Error('claim on exhausted share must be denied: ' + JSON.stringify(rUse3))
  if (Object.keys(rUse3).sort().join(',') !== 'code,ok,type') throw new Error('reserve-driven max-uses denial leaked fields: ' + JSON.stringify(rUse3))
  const hostUseCount = a.listTunnels().filter(t => t.code === useRec.code && t.state !== 'closed').length
  if (hostUseCount !== hostLiveBefore3) throw new Error('denied claim must not create a third host tunnel, got ' + hostUseCount)
  // …and byte-identical to a garbage-code denial (same code after cancel)
  await a.cancelTunnelCode(useRec.code)
  await new Promise(r => setTimeout(r, 300))
  resetGuestBucket()
  const rGarbUse = await waitClaimRes(useRec.code)
  if (rGarbUse.ok !== false) throw new Error('claim on cancelled code must be denied: ' + JSON.stringify(rGarbUse))
  if (JSON.stringify(rGarbUse) !== JSON.stringify(rUse3)) {
    throw new Error('reserve-maxUses denial must deep-equal garbage-code denial: ' + JSON.stringify(rUse3) + ' vs ' + JSON.stringify(rGarbUse))
  }
  if (useShare.uses !== 2 || useShare._reservations !== 0) throw new Error('final share state must be uses 2, reservations 0: ' + JSON.stringify({ uses: useShare.uses, reservations: useShare._reservations }))
  console.log('PASS code maxUses reserve-on-claim count-on-open')

  // ── Wedged-flush resilience: the ENGINE bounds swarm.flush itself (flushBoundMs),
  // so a stalled DHT announce can never hang createTunnelCode's caller. Wedge one
  // create's flush so it never settles, then prove create still returns a valid
  // code on time — and that a guest joins through that code afterwards.
  const wedgedCreate = await (async () => {
    const origFlush = a.swarm.flush.bind(a.swarm)
    const origBound = a.tunnelManager.flushBoundMs
    a.tunnelManager.flushBoundMs = 800 // shortened in-test (FLUSH_BOUND_MS default 8000)
    a.swarm.flush = () => new Promise(() => {}) // never settles — wedged announce
    try {
      const t0 = Date.now()
      const res = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: 'wedged-flush', expirationPreset: '30m' })
      const elapsed = Date.now() - t0
      if (!res || !res.code) throw new Error('wedged-flush create returned no code')
      if (!/^TUNNEL-[A-Z0-9]{4}-[A-Z0-9]{4}$/.test(res.code)) throw new Error('wedged-flush create returned malformed code: ' + res.code)
      if (elapsed > 1800) throw new Error('wedged-flush create took ' + elapsed + 'ms (flushBoundMs 800 + 1s budget) — flush wait is not bounded')
      return { code: res.code, elapsed }
    } finally {
      a.swarm.flush = origFlush
      a.tunnelManager.flushBoundMs = origBound
    }
  })()
  console.log('[Test] create survived a never-settling flush in ' + wedgedCreate.elapsed + 'ms — code ' + wedgedCreate.code)
  // …and a guest still joins through that code. The host's DHT announce may never
  // land (its flush wedged), so the guest keeps a REAL pending join (engine
  // auto-accept only fires for pending joins) and the harness drives CLAIMs over
  // the live signaling link — exactly the late-rendezvous story: the join is
  // never cancelled on a flush bound, and once the claim is honored the offer
  // auto-accept opens the tunnel within normal leg timing.
  resetGuestBucket()
  const flushJoin = await b.joinTunnelCode(wedgedCreate.code)
  if (!flushJoin || flushJoin.pending !== true) throw new Error('guest join on wedged-flush code must stay pending: ' + JSON.stringify(flushJoin))
  const rFlush = await waitClaimRes(wedgedCreate.code)
  if (rFlush.ok !== true) throw new Error('claim on wedged-flush code must be honored: ' + JSON.stringify(rFlush))
  await waitTunnelOpen(wedgedCreate.code, 'host')
  await waitTunnelOpen(wedgedCreate.code, 'guest')
  const flushShare = a.tunnelManager._codeShares.get(wedgedCreate.code)
  for (let i = 0; i < 50 && (!flushShare || flushShare.uses !== 1); i++) await new Promise(r => setTimeout(r, 100))
  if (!flushShare || flushShare.uses !== 1) throw new Error('wedged-flush code share must show uses 1 after open: ' + JSON.stringify(flushShare && { uses: flushShare.uses, reservations: flushShare._reservations }))
  await a.cancelTunnelCode(wedgedCreate.code)
  console.log('PASS code create survives wedged flush')

  // ─── Lifetime decoupling: the TUNNEL outlives its local sockets ───────────
  // Product-defect regression: the host used to net.connect to the shared
  // service at OPEN and treated that socket's close as the tunnel's close —
  // an HTTP server ends idle keep-alive upstreams after ~5s, so the whole
  // tunnel (guest forward included) died on the first idle and the next
  // request hit a refused port. Fixed: upstreams are lazy per guest stream,
  // teardown is socket-only, and an EOF control frame (meshdrop-tunnel-v1
  // ctrl message) finishes the peer's local stream. Proof: fetch → idle >6s
  // with zero traffic (the upstream MUST idle-close on its own) → fetch again
  // on the SAME tunnel: bodies match, both sides stay 'open', no tunnel close
  // fires, and byte totals accumulate across the two upstreams.
  const idleRec = await a.createTunnelCode({ port: srvPort, host: '127.0.0.1', name: 'idle-survive', expirationPreset: '30m' })
  console.log('[Host] idle-survival code', idleRec.code)
  let idleOfferId = null
  const idleLocal = 43582 + Math.floor(Math.random() * 1000)
  const idleClosings = []
  for (const [sideName, eng] of [['host', a], ['guest', b]]) {
    eng.tunnelManager.on('tunnel:closed', d => { if (d && d.tunnelId && d.tunnelId === idleOfferId) idleClosings.push(sideName + ':' + d.reason) })
  }
  b.on('tunnel:offer', d => {
    if (d && d.tunnelId && !d.udp && d.code === idleRec.code && !idleOfferId) {
      idleOfferId = d.tunnelId
      console.log('[Guest] idle-leg offer', idleOfferId.slice(0, 8), '— accepting with local forward')
      b.acceptTunnel(d.tunnelId, { localPort: idleLocal, localHost: '127.0.0.1' }).catch(e => console.warn('[Test] idle-leg accept failed:', e.message))
    }
  })
  resetGuestBucket()
  await b.joinTunnelCode(idleRec.code)
  for (let i = 0; i < 60 && !idleOfferId; i++) await new Promise(r => setTimeout(r, 200))
  if (!idleOfferId) throw new Error('idle leg: never received offer for code ' + idleRec.code)
  for (let i = 0; i < 24; i++) {
    const hs = a.listTunnels().filter(t => t.code === idleRec.code)
    const gs = b.listTunnels().filter(t => t.code === idleRec.code)
    if (hs.some(t => t.state === 'open') && gs.some(t => t.state === 'open')) break
    await new Promise(r => setTimeout(r, 500))
  }
  const hsIdle = a.listTunnels().filter(t => t.code === idleRec.code)
  const gsIdle = b.listTunnels().filter(t => t.code === idleRec.code)
  console.log('[Test] idle leg states host', hsIdle.map(t => t.state), 'guest', gsIdle.map(t => t.state))
  if (!hsIdle.some(t => t.state === 'open') || !gsIdle.some(t => t.state === 'open')) {
    throw new Error('idle leg tunnel never opened — host ' + JSON.stringify(hsIdle.map(t => t.state)) + ' guest ' + JSON.stringify(gsIdle.map(t => t.state)))
  }
  // Keep-alive agent on purpose: the first fetch's local connection STAYS OPEN
  // (as a real browser preview would), so the host upstream sits idle against
  // the HTTP server until the server's keepAliveTimeout ends it — the exact
  // defect scenario. A closing client would EOF the upstream early and never
  // exercise the server-side idle close.
  const idleAgent = new http.Agent({ keepAlive: true, maxSockets: 1 })
  const keepAliveFetch = (port, label) => new Promise((res, rej) => {
    const req = http.get({ host: '127.0.0.1', port, path: '/', agent: idleAgent }, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => res({ body: d, socket: req.socket })) })
    req.on('error', rej)
    setTimeout(() => rej(new Error('fetch timeout on 127.0.0.1:' + port + (label ? ' (' + label + ')' : ''))), 9000)
  })
  const idleHostT = () => a.tunnelManager._tunnels.get(idleOfferId)
  const idleGuestT = () => b.tunnelManager._tunnels.get(idleOfferId)
  const f1 = await keepAliveFetch(idleLocal, 'idle-fetch-1')
  if (f1.body !== body) throw new Error('idle leg fetch 1 mismatch: ' + JSON.stringify(f1.body))
  const ht1 = idleHostT()
  if (!ht1 || ht1.state !== 'open' || !ht1._localSocket) throw new Error('idle leg: host upstream must be lazily open after fetch 1')
  console.log('[Test] idle fetch 1 ok — host upstream lazily open, entering zero-traffic idle (no heartbeat, no fetches)')
  // Zero traffic from here on. The HTTP server ends its idle keep-alive
  // upstream after ~5s; that close must be socket-only.
  const idleT0 = Date.now()
  let upstreamIdleMs = null
  while (Date.now() - idleT0 < 9000) {
    const ht = idleHostT()
    if (ht && !ht._localSocket && ht.state === 'open') { upstreamIdleMs = Date.now() - idleT0; break }
    await new Promise(r => setTimeout(r, 200))
  }
  if (upstreamIdleMs === null) throw new Error('idle leg: host upstream never idle-closed within 9s — keep-alive teardown path not exercised (host ' + (idleHostT() && idleHostT().state) + ')')
  console.log('[Test] host upstream idle-closed at t+' + upstreamIdleMs + 'ms — tunnel must survive')
  const idleRest = 6500 - upstreamIdleMs
  if (idleRest > 0) await new Promise(r => setTimeout(r, idleRest))
  if (idleClosings.length > 0) throw new Error('idle leg: tunnel:closed fired during zero-traffic idle: ' + idleClosings.join(' | '))
  const htIdle = idleHostT()
  const gtIdle = idleGuestT()
  if (!htIdle || htIdle.state !== 'open' || !gtIdle || gtIdle.state !== 'open') {
    throw new Error('idle leg: tunnel not open after idle — host ' + (htIdle && htIdle.state) + ' guest ' + (gtIdle && gtIdle.state))
  }
  const f2 = await keepAliveFetch(idleLocal, 'idle-fetch-2')
  if (f2.body !== body) throw new Error('fetch 2 after >6s idle failed — tunnel did not survive the idle upstream close')
  const ht2 = idleHostT()
  if (!ht2 || ht2.state !== 'open' || !ht2._localSocket) throw new Error('idle leg: fetch 2 must lazily reopen a fresh upstream on the same tunnel')
  console.log('[Test] idle fetch 2 ok — tunnel-lifetime bytes host up/down', ht2.bytesUp + '/' + ht2.bytesDown, 'guest up/down', idleGuestT().bytesUp + '/' + idleGuestT().bytesDown)
  // bytesUp/Down are tunnel-lifetime totals: two requests/responses crossed
  // two separate upstream sockets, and both must be counted on the one tunnel.
  if (ht2.bytesUp < body.length * 2 || ht2.bytesDown < 1) throw new Error('idle leg: bytesUp/Down must accumulate across upstream reconnects, got up=' + ht2.bytesUp + ' down=' + ht2.bytesDown)
  if (idleClosings.length > 0) throw new Error('idle leg: tunnel closed across the whole idle window: ' + idleClosings.join(' | '))
  console.log('PASS tunnel survives idle upstream close')
  // Close out the keep-alive client so the tunnel returns to a fully idle,
  // socketless state for the sequential-clients leg.
  idleAgent.destroy()
  for (let i = 0; i < 30 && idleHostT() && idleHostT()._localSocket; i++) await new Promise(r => setTimeout(r, 200))
  if (idleHostT() && idleHostT()._localSocket) throw new Error('idle leg: upstream should close after the keep-alive client ended')
  console.log('[Test] idle tunnel back to socketless idle — ready for sequential clients')

  // ─── Sequential clients reuse one tunnel ───────────────────────────────────
  // One in-flight guest-local stream at a time: client A fetches, A's local
  // socket is destroyed, the host upstream closes (socket-only teardown,
  // instrumented log), the tunnel stays open, and client B connects and
  // fetches successfully over the same tunnel.
  const seqAgent = new http.Agent({ keepAlive: true, maxSockets: 1 })
  const seqFetch = (port, label) => new Promise((res, rej) => {
    const req = http.get({ host: '127.0.0.1', port, path: '/', agent: seqAgent }, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => res({ body: d, socket: req.socket })) })
    req.on('error', rej)
    setTimeout(() => rej(new Error('fetch timeout on 127.0.0.1:' + port + (label ? ' (' + label + ')' : ''))), 9000)
  })
  const seqA = await seqFetch(idleLocal, 'seq-client-A')
  if (seqA.body !== body) throw new Error('sequential leg client A fetch mismatch')
  if (!idleHostT() || !idleHostT()._localSocket) throw new Error('sequential leg: client A must have opened the host upstream')
  console.log('[Test] sequential client A fetched — destroying A\'s local socket (A is the active stream)')
  const seqLog = []
  const origSeqLog = console.log
  console.log = (...a) => { seqLog.push(a.map(String).join(' ')); origSeqLog(...a) }
  let hostUpstreamGone = false
  const seqT0 = Date.now()
  try {
    try { seqA.socket.destroy() } catch {}
    while (Date.now() - seqT0 < 6000) {
      const ht = idleHostT()
      if (ht && !ht._localSocket && ht.state === 'open') { hostUpstreamGone = true; break }
      await new Promise(r => setTimeout(r, 150))
    }
  } finally {
    console.log = origSeqLog
  }
  const seqHits = seqLog.filter(l => idleOfferId && l.includes(idleOfferId.slice(0, 8)) && l.includes('upstream closed'))
  console.log('[Test] sequential leg instrumented host logs:', JSON.stringify(seqHits))
  if (!hostUpstreamGone) throw new Error('sequential leg: host upstream did not close after client A socket destroy (host state ' + (idleHostT() && idleHostT().state) + ')')
  if (seqHits.length === 0) throw new Error('sequential leg: expected an instrumented host "upstream closed" log')
  const gtSeq = idleGuestT()
  if (!gtSeq || gtSeq.state !== 'open' || !gtSeq._localServer || !gtSeq._localServer.listening) throw new Error('sequential leg: guest forward must stay listening after client A died')
  const seqB = await seqFetch(idleLocal, 'seq-client-B')
  if (seqB.body !== body) throw new Error('sequential leg client B fetch mismatch — tunnel not reusable')
  const htSeqB = idleHostT()
  if (!htSeqB || htSeqB.state !== 'open' || !htSeqB._localSocket) throw new Error('sequential leg: client B must open a fresh upstream on the same tunnel')
  console.log('[Test] sequential client B fetched through the same tunnel — host state', htSeqB.state)
  console.log('PASS sequential clients reuse tunnel')
  seqAgent.destroy()

  await a.stop()
  await b.stop()
  srv.close()

  // Standing rule, asserted post-stop: every tunnel lifecycle event fired exactly once per side
  // per tunnelId at the ENGINE level. Every still-open tunnel closes exactly once at stop
  // ('engine-stop'); tunnels closed deliberately by earlier legs (reject, offer-GC, closeTunnel,
  // code cancel) already emitted theirs — either way each side emitted exactly one tunnel:closed
  // per tunnelId it ever held. A tunnel that was offered but rejected/GC-expired before opening
  // never fires tunnel:opened (legitimately 0), so 'opened' is asserted exactly-once only where
  // an open was actually observed — an opened tunnel that fires twice still fails.
  const allIds = new Set()
  for (const side of ['a', 'b']) for (const evt of ['offer', 'opened', 'closed']) for (const tid of engEv[side][evt].keys()) allIds.add(tid)
  console.log('[Test] engine-level once-asserts across', allIds.size, 'tunnelIds')
  for (const tid of allIds) {
    for (const side of ['a', 'b']) {
      assertEngineOnce(side, 'offer', tid, '(offer)')
      if (engEv[side].opened.has(tid)) assertEngineOnce(side, 'opened', tid, '(opened)')
    }
  }
  for (const tid of allIds) {
    for (const side of ['a', 'b']) assertEngineOnce(side, 'closed', tid, '(closed after stop)')
  }
  console.log('PASS engine tunnel events once per side per tunnelId (offer/opened/closed)')
  console.log('Done — cleanup complete')
}

run().catch(e => { console.error('FAIL', e && e.stack || e); process.exit(1) })
