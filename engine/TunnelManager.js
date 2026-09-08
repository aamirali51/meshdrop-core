'use strict'

const { EventEmitter } = require('../compat.js')
const { EVENTS, MESSAGES, tunnelTopic } = require('../protocol.js')
const { generateTunnelCode, normalizeTunnelCode } = require('../crypto.js')

const Protomux = require('protomux')
const c = require('compact-encoding')
const b4a = require('b4a')
const net = require('net')
const dgram = require('dgram')

const TUNNEL_PROTOCOL = 'meshdrop-tunnel-v1'
const TUNNEL_UDP_PROTOCOL = 'meshdrop-tunnel-udp-v1'
const TUNNEL_DATA_MAX = 64 * 1024

// meshdrop-tunnel-v1 channel framing (documented in protocol.js): one
// Protomux channel per tunnelId carries two messages — data (raw stream
// bytes) and ctrl (JSON string): {type:'close',reason} tears the whole tunnel
// down, {type:'eof',reason?} means the sender's LOCAL stream ended (socket-
// only teardown; the tunnel survives and the channel is reused).

// TUNNEL_CLAIM abuse controls. Hyperswarm applies no per-topic throttling, so
// claims are metered here per peerId with a token bucket, and concurrent
// inbound tunnels are capped per peer and per share. Every denial is uniform
// { ok:false } on the wire — no reason field — so CLAIM_RES never doubles as a
// code-validity oracle; the specific reason is logged host-side only.
const CLAIM_BUCKET_CAPACITY = 5 // tokens per peerId; every claim costs 1
const CLAIM_BUCKET_REFILL_MS = 12 * 1000 // +1 token per window (no burst accumulation)
const CLAIM_BUCKET_SWEEP_MS = 5 * 60 * 1000 // idle bucket dropped after this
const MAX_TUNNELS_PER_PEER = 4 // live (offered/connecting/open) tunnels per peerId
const MAX_ACTIVE_CODE_TUNNELS = 16 // live tunnels per code share
// maxUses is accounted reserve-on-claim / count-on-open: a NEW claim holds an
// in-memory slot (share._reservations, never persisted) that is counted into
// share.uses only when the code tunnel actually opens, and is released on any
// close before open — so a burner that claims and abandons can never drain a
// maxUses share ahead of a real guest.
const OFFER_EXPIRY_MS = 60 * 1000 // code offers un-accepted after this are GC'd
const FLUSH_BOUND_MS = 8 * 1000 // swarm.flush() is raced against this, never awaited unboundedly (a wedged DHT announce must not hang the UI)

function makeTunnelId() {
  return `tun-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
}

function isAllowedHost(host) {
  const h = String(host || '').trim().toLowerCase()
  return h === '127.0.0.1' || h === 'localhost' || h === '::1' || h === '::ffff:127.0.0.1'
}

// Guest-side forward bind host. Loopback-only is the invariant; IPv4-literal binding is the
// implementation. 'localhost' stays a valid request but is normalized to the literal '127.0.0.1'
// here — Node may resolve 'localhost' to '::1', and local clients dialing the advertised
// 127.0.0.1:<port> (what the UI and logs advertise) would then get ECONNREFUSED.
function guestBindHost(requestedHost) {
  if (requestedHost && requestedHost !== '127.0.0.1' && requestedHost !== 'localhost') {
    console.warn(`[TunnelManager] guest bind host '${requestedHost}' is not loopback; coercing to 127.0.0.1`)
  }
  return '127.0.0.1'
}

function getDurationMs(preset) {
  const map = { '5m': 5 * 60 * 1000, '10m': 10 * 60 * 1000, '15m': 15 * 60 * 1000, '30m': 30 * 60 * 1000, '1h': 60 * 60 * 1000, '6h': 6 * 60 * 60 * 1000, '24h': 24 * 60 * 60 * 1000, never: 0 }
  return map[preset] ?? 30 * 60 * 1000
}

class TunnelManager extends EventEmitter {
  constructor({ engine }) {
    super()
    this.engine = engine
    this._tunnels = new Map()
    this._codeShares = new Map() // code -> { code, port, host, udp, name, expirationPreset, expiresAt, maxUses, uses, id, pendingCodes:Set, activePeerCodes:Map }
    this._activeCodeByPeer = new Map() // peerId -> Set<code>
    this._claimBuckets = new Map() // peerId -> { tokens, last } — claim throttle state
    // Caps/limits as instance fields so tests can config-bump them; claim-bucket
    // constants are also mirrored so a harness can read the capacity it asserts on.
    this.maxTunnelsPerPeer = MAX_TUNNELS_PER_PEER
    this.maxActiveCodeTunnels = MAX_ACTIVE_CODE_TUNNELS
    this.claimBucketCapacity = CLAIM_BUCKET_CAPACITY
    this.claimBucketRefillMs = CLAIM_BUCKET_REFILL_MS
    this.offerExpiryMs = OFFER_EXPIRY_MS // un-accepted code offers GC'd after this (tests shorten it)
    this.flushBoundMs = FLUSH_BOUND_MS // swarm.flush() raced against this in create/joinTunnelCode (tests shorten it)
    this._listeners = []
    this._onPeerDisconnected = (info) => {
      const peerId = (info && (info.peerId || info.id)) || ''
      if (!peerId) return
      for (const [tid, t] of this._tunnels.entries()) {
        if (t.peerId === peerId && t.state !== 'closed') this._closeTunnel(tid, 'peer-disconnected')
      }
      // code shares: drop peer's pending code offer state
      if (this._activeCodeByPeer.has(peerId)) this._activeCodeByPeer.delete(peerId)
      // Claim buckets follow the peer's connection lifetime — never key cleanup
      // to a single connection close (a peer may hold mesh + tunnel connections),
      // so drop only when no topic labels remain; the TTL sweep backstops it.
      if (this._claimBuckets.has(peerId) && (!this.engine.peerTopics || !this.engine.peerTopics.get(peerId) || this.engine.peerTopics.get(peerId).size === 0)) {
        this._claimBuckets.delete(peerId)
      }
    }
    this.engine.on(EVENTS.PEER_DISCONNECTED, this._onPeerDisconnected)
    this._listeners.push([EVENTS.PEER_DISCONNECTED, this._onPeerDisconnected])
    this._expirationTimer = setInterval(() => this._checkExpirations(), 10 * 1000)
    if (this._expirationTimer.unref) this._expirationTimer.unref()
  }

  // ─── Listing ───────────────────────────────────────────────────────────────
  listTunnels() {
    const out = []
    for (const t of this._tunnels.values()) {
      out.push({ tunnelId: t.tunnelId, peerId: t.peerId, peerName: t.peerName, host: t.host, port: t.port, udp: !!t.udp, role: t.role, state: t.state, bytesUp: t.bytesUp, bytesDown: t.bytesDown, createdAt: t.createdAt, code: t.code || null })
    }
    return out
  }

  listTunnelCodes() {
    const now = Date.now()
    const out = []
    for (const s of this._codeShares.values()) {
      out.push({ id: s.id, code: s.code, host: s.host, port: s.port, udp: !!s.udp, name: s.name, expiresAt: s.expiresAt, expirationPreset: s.expirationPreset, maxUses: s.maxUses, uses: s.uses, expired: s.expiresAt > 0 && now >= s.expiresAt })
    }
    return out.sort((a, b) => b.expiresAt - a.expiresAt)
  }

  _resolvePeerId(peerId) {
    if (typeof peerId !== 'string' || !peerId) return null
    if (this.engine.peers.has(peerId)) return peerId
    for (const [noiseKey, pObj] of this.engine.peers.entries()) {
      if (pObj.device && (pObj.device.id === peerId || pObj.device.publicKey === peerId)) return noiseKey
    }
    return null
  }

  // ─── Tier 1: paired tunnel ────────────────────────────────────────────────
  async createTunnel({ peerId, port, host = '127.0.0.1', name = '', udp = false }) {
    if (!this.engine.started) throw new Error('Engine not started')
    if (!isAllowedHost(host)) throw new Error('Tunnel host must be localhost (127.0.0.1 / ::1)')
    const p = Number(port)
    if (!Number.isInteger(p) || p < 1 || p > 65535) throw new Error('Invalid port: ' + String(port))
    const resolved = this._resolvePeerId(peerId)
    if (!resolved) throw new Error('Peer not paired/connected: ' + String(peerId).slice(0, 12))
    peerId = resolved
    const peerObj = this.engine.peers.get(peerId)
    if (!peerObj || !peerObj.pairing || !peerObj.pairing.trusted) throw new Error('Peer not paired: ' + String(peerId).slice(0, 12))
    if (!peerObj.signaling) throw new Error('Peer signaling not ready')

    const tunnelId = makeTunnelId()
    const tunnel = {
      tunnelId, peerId, peerName: (peerObj.device && peerObj.device.name) || peerId.slice(0, 12),
      host, port: p, name: String(name || '').slice(0, 80), udp: !!udp,
      role: 'host', state: 'offered', createdAt: Date.now(),
      bytesUp: 0, bytesDown: 0,
      // _localSocket = host's upstream pipe to the shared service (TCP, lazy —
      // opened per guest stream, never at tunnel open); _localServer/_localClient
      // = guest's tunnel-lifetime local forward + its single active stream.
      _localSocket: null, _udpSocket: null, _muxChannel: null, _muxMessages: null, _muxChannelCount: 0, _localServer: null, _localClient: null, _udpPeerAddr: null
    }
    this._tunnels.set(tunnelId, tunnel)

    peerObj.signaling.send({ type: MESSAGES.TUNNEL_OFFER, tunnelId, host, port: p, udp: !!udp, name: tunnel.name, from: this.engine.deviceIdentity ? this.engine.deviceIdentity.id : '' })
    // Tunnel lifecycle events emit only on this manager; index.js forwards them to the engine exactly once.
    this.emit('tunnel:offer', { tunnelId, peerId, host, port: p, udp: !!udp, name: tunnel.name, role: 'host', direction: 'offered' })

    setTimeout(() => {
      const t = this._tunnels.get(tunnelId)
      if (t && t.state === 'offered') {
        this._closeTunnel(tunnelId, 'offer-timeout')
        try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_ERROR, tunnelId, error: 'offer-timeout' }) } catch {}
      }
    }, 60 * 1000).unref?.()

    return { tunnelId, host, port: p, udp: !!udp, peerId }
  }

  async acceptTunnel(tunnelId, { localPort, localHost = '127.0.0.1' } = {}) {
    const t = this._tunnels.get(tunnelId)
    if (!t || t.role !== 'guest') return
    // Two accept paths race for the same tunnelId (UI tunnel:offer listener + code auto-accept poll).
    // Claim the tunnel synchronously so a concurrent second caller observes 'connecting' and bails.
    if (t.state !== 'offered') { console.warn('[tunnel] accept ignored, state=', t.state); return }
    if (localPort != null) {
      const lp = Number(localPort)
      if (!Number.isInteger(lp) || lp < 1 || lp > 65535) throw new Error('Invalid localPort')
      t.localPort = lp
      t.localHost = String(localHost || '127.0.0.1')
    }
    const peerObj = this.engine.peers.get(t.peerId)
    if (!peerObj || !peerObj.signaling) throw new Error('Peer not connected')
    t.state = 'connecting'
    peerObj.signaling.send({ type: MESSAGES.TUNNEL_ACCEPT, tunnelId })
    this._openProtomuxTunnel(t, peerObj)
    return { tunnelId, state: t.state }
  }

  async rejectTunnel(tunnelId, reason = 'rejected') {
    const t = this._tunnels.get(tunnelId)
    if (!t) throw new Error('Unknown tunnel: ' + tunnelId)
    if (t.state !== 'offered') throw new Error('Tunnel not offered: ' + t.state)
    const peerObj = this.engine.peers.get(t.peerId)
    if (peerObj && peerObj.signaling) { try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_REJECT, tunnelId, reason }) } catch {} }
    this._closeTunnel(tunnelId, reason)
    return { tunnelId, rejected: true }
  }

  async closeTunnel(tunnelId, reason = 'closed-by-user') {
    const t = this._tunnels.get(tunnelId)
    if (!t) throw new Error('Unknown tunnel: ' + tunnelId)
    const peerObj = this.engine.peers.get(t.peerId)
    if (peerObj && peerObj.signaling && t.state !== 'closed') { try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_CLOSE, tunnelId, reason }) } catch {} }
    this._closeTunnel(tunnelId, reason)
    return { tunnelId, closed: true }
  }

  // ─── Tier 2: ephemeral code tunnel (like DROP-XXXX, no pairing) ──────────
  // swarm.flush() wedges when a public-DHT topic announce stalls; awaiting it
  // unboundedly would hang the Share-Port wizard / Join dialog forever. Race it
  // against a bound and PROCEED on timeout: the topic join was already issued
  // before the await, rendezvous may still complete late, and nothing here is
  // cancelled — no topic leave, no refcount change, no join teardown.
  async _flushBounded(context) {
    const bound = this.flushBoundMs > 0 ? this.flushBoundMs : FLUSH_BOUND_MS
    let timer = null
    return new Promise((resolve) => {
      let done = false
      const finish = () => { if (done) return; done = true; clearTimeout(timer); resolve() }
      timer = setTimeout(() => {
        console.warn(`[TunnelManager] swarm.flush ${context} did not settle within ${bound}ms — proceeding, topic rendezvous may complete late`)
        finish()
      }, bound)
      if (timer.unref) timer.unref()
      try {
        const f = this.engine.swarm ? this.engine.swarm.flush() : Promise.resolve()
        if (f && typeof f.then === 'function') f.then(finish, finish); else finish()
      } catch { finish() }
    })
  }

  async createTunnelCode({ port, host = '127.0.0.1', name = '', udp = false, expirationPreset = '30m', maxUses = 0 }) {
    if (!this.engine.started) throw new Error('Engine not started')
    if (!isAllowedHost(host)) throw new Error('Tunnel host must be localhost')
    const p = Number(port)
    if (!Number.isInteger(p) || p < 1 || p > 65535) throw new Error('Invalid port')
    const code = generateTunnelCode()
    const id = `tun-code-${Date.now().toString(36)}`
    const duration = getDurationMs(expirationPreset)
    const createdAt = Date.now()
    const expiresAt = duration > 0 ? createdAt + duration : 0
    const share = { id, code, host, port: p, udp: !!udp, name: String(name || '').slice(0, 80), expirationPreset, createdAt, expiresAt, maxUses: Number(maxUses) > 0 ? Number(maxUses) : 0, uses: 0, _reservations: 0, _topic: tunnelTopic(code) }
    this._codeShares.set(code, share)
    this.engine.topicRegistry.join(share._topic, { client: true, server: true })
    await this._flushBounded('createTunnelCode(' + code + ')')
    // persist so reboot restores
    try {
      const bee = await this.engine.getBee('tunnelShares')
      await bee.put(id, { ...share, _topic: undefined, _reservations: undefined })
    } catch {}
    console.log(`[TunnelManager] Tunnel code created: ${code} -> ${host}:${p}${udp ? ' udp' : ''} expires ${expirationPreset}`)
    return { code, id, host, port: p, udp: !!udp, expiresAt, expirationPreset }
  }

  async joinTunnelCode(rawCode) {
    if (!this.engine.started) throw new Error('Engine not started')
    const code = normalizeTunnelCode(rawCode)
    if (!code) throw new Error('Invalid tunnel code — expected TUNNEL-XXXX-XXXX')
    const topic = tunnelTopic(code)
    // if we are also hosting this code, join as guest would loop — but host+guest same process is rare; still allow
    this.engine.topicRegistry.join(topic, { client: true, server: true })
    // Ensure the DHT announce has had a chance to flush before we expect peers —
    // bounded: a wedged flush must not hang the Join dialog (see _flushBounded)
    await this._flushBounded('joinTunnelCode(' + code + ')')
    // Pull the freshest topic attribution now — peers connected before this
    // join only gain this code's topic label when their peerInfo topics update,
    // and the flush's lookups may have just done that.
    try { this.engine.refreshPeerTopicAttribution && this.engine.refreshPeerTopicAttribution() } catch {}
    // track as pending join; signal is sent once we have a peer on that topic
    if (!this._pendingJoins) this._pendingJoins = new Map()
    this._pendingJoins.set(code, { code, topic, at: Date.now() })
    // send CLAIM immediately to any already-connected peer observed on this
    // code's tunnel topic (attribution: peerInfo topics at connection time +
    // code handshakes — never to unrelated peers on other topics)
    for (const [peerId, peerObj] of this.engine.peers.entries()) {
      if (peerObj.signaling && this._peerIsOnTopic(peerId, topic)) { try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_CLAIM, code }) } catch {} }
    }
    // Drive: re-send CLAIM every 5s until an offer arrives (DHT rendezvous may be late)
    const drive = setInterval(() => {
      if (!this._pendingJoins || !this._pendingJoins.has(code)) { clearInterval(drive); return }
      for (const [peerId, peerObj] of this.engine.peers.entries()) {
        if (peerObj.signaling && this._peerIsOnTopic(peerId, topic)) { try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_CLAIM, code }) } catch {} }
      }
    }, 5000)
    if (drive.unref) drive.unref()
    // timeout: leave topic if no host answers
    setTimeout(() => {
      clearInterval(drive)
      if (this._pendingJoins && this._pendingJoins.has(code)) {
        this._pendingJoins.delete(code)
        try { this.engine.topicRegistry.leave(topic) } catch {}
        this.emit('tunnel:error', { code, error: 'tunnel-host-not-found' })
      }
    }, 35 * 1000).unref?.()

    return { code, topic, pending: true }
  }

  async cancelTunnelCode(codeOrId) {
    let share = null
    let key = null
    for (const [k, s] of this._codeShares.entries()) { if (s.code === codeOrId || s.id === codeOrId) { share = s; key = k; break } }
    if (!share) throw new Error('Tunnel code not found: ' + codeOrId)
    try { this.engine.topicRegistry.leave(share._topic) } catch {}
    this._codeShares.delete(key)
    try { const bee = await this.engine.getBee('tunnelShares'); await bee.del(share.id) } catch {}
    // close any code-bound tunnels
    for (const [tid, t] of Array.from(this._tunnels.entries())) { if (t.code === share.code && t.state !== 'closed') this._closeTunnel(tid, 'code-cancelled') }
    return { cancelled: share.code }
  }

  listTunnelCodes() {
    const now = Date.now()
    const out = []
    for (const s of this._codeShares.values()) {
      out.push({ id: s.id, code: s.code, host: s.host, port: s.port, udp: !!s.udp, name: s.name, expiresAt: s.expiresAt, expirationPreset: s.expirationPreset, maxUses: s.maxUses, uses: s.uses, expired: s.expiresAt > 0 && now >= s.expiresAt })
    }
    return out.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0))
  }

  // auto-restore after reboot + when new peer connects on the tunnel topic
  async restoreTunnelCodes() {
    try {
      const bee = await this.engine.getBee('tunnelShares')
      const now = Date.now()
      let restored = 0
      for await (const node of bee.createReadStream()) {
        const s = node.value
        if (!s || !s.code || !s.id) continue
        if (s.expiresAt > 0 && now >= s.expiresAt) { await bee.del(node.key).catch(() => {}); continue }
        if (this._codeShares.has(s.code)) continue
        s._topic = tunnelTopic(s.code)
        s._reservations = 0 // in-memory only; reservations never survive a reboot
        this._codeShares.set(s.code, s)
        this.engine.topicRegistry.join(s._topic, { client: true, server: true })
        restored++
      }
      if (restored) console.log(`[TunnelManager] Restored ${restored} tunnel code(s) on boot`)
    } catch {}
  }

  // Topic membership checks/feeds against engine.peerTopics (per-connection
  // topic attribution maintained by connections/index.js + the code-protocol
  // handshakes below). Hyperswarm only fills peerInfo.topics from OUR lookups
  // (dialing side, ~10min refresh), so handshake feeds close the gap on the
  // accepting side.
  _peerIsOnTopic(peerId, label) {
    if (!peerId || !this.engine.peerTopics) return false
    const labels = this.engine.peerTopics.get(peerId)
    return !!(labels && labels.has(label))
  }
  _attributePeerTopic(peerId, label) {
    if (!peerId || !label || !this.engine.peerTopics) return
    let set = this.engine.peerTopics.get(peerId)
    if (!set) {
      set = new Set()
      this.engine.peerTopics.set(peerId, set)
    }
    set.add(label)
  }

  // called from connections onConnection after signaling is ready — lets code-host send CLAIM handling immediately
  _onPeerSignalingReady(peerId) {
    // if we host any codes, the guest's TUNNEL_CLAIM will arrive; nothing to do here
    // if we are pending a join for some code, re-send CLAIM to this new peer so late joiners are found —
    // only when this peer is actually on that code's tunnel topic (never to unrelated peers)
    if (this._pendingJoins) {
      for (const { code, topic } of this._pendingJoins.values()) {
        const peerObj = this.engine.peers.get(peerId)
        if (peerObj && peerObj.signaling && this._peerIsOnTopic(peerId, topic)) { try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_CLAIM, code }) } catch {} }
      }
    }
  }

  // ─── Signaling handler ────────────────────────────────────────────────────
  handleMessage(peerId, msg) {
    if (!msg || typeof msg.type !== 'string') return
    if (!msg.type.startsWith('TUNNEL_')) return

    // TUNNEL_CLAIM / CLAIM_RES are code-based and must NOT require pairing
    if (msg.type === MESSAGES.TUNNEL_CLAIM) {
      this._handleTunnelClaim(peerId, msg)
      return
    }
    if (msg.type === MESSAGES.TUNNEL_CLAIM_RES) {
      this._handleTunnelClaimRes(peerId, msg)
      return
    }

    // Paired-only guard: code-based messages are exempt (like DROP claim). Detect by msg.code OR by stored tunnel's code for that tunnelId
    const tidForGuard = typeof msg.tunnelId === 'string' ? msg.tunnelId : ''
    const storedIsCode = !!(tidForGuard && this._tunnels.get(tidForGuard) && this._tunnels.get(tidForGuard).code)
    const isCodeMsg = (typeof msg.code === 'string' && !!msg.code) || storedIsCode
    if (!isCodeMsg) {
      const peerObj2 = this.engine.peers.get(peerId)
      if (!peerObj2 || !peerObj2.pairing || !peerObj2.pairing.trusted) {
        console.warn(`[TunnelManager] Ignoring ${msg.type} from unpaired peer ${String(peerId).slice(0, 12)}`)
        return
      }
    }

    if (msg.type === MESSAGES.TUNNEL_OFFER) {
      const peerObj = this.engine.peers.get(peerId)
      const tunnelId = String(msg.tunnelId || '')
      if (!tunnelId) return
      // A code offer proves the sender hosts this code — attribute the topic so
      // our CLAIM drive and pairing suppression see the host as a tunnel-code
      // peer even when we accepted the connection (topics only land on dialers).
      const offeredCode = normalizeTunnelCode(msg.code || '')
      if (offeredCode) this._attributePeerTopic(peerId, tunnelTopic(offeredCode))
      if (this._tunnels.has(tunnelId)) return
      const host = String(msg.host || '127.0.0.1')
      const port = Number(msg.port)
      if (!Number.isInteger(port) || port < 1 || port > 65535) return
      const t = {
        tunnelId, peerId, peerName: (peerObj && peerObj.device && peerObj.device.name) || peerId.slice(0, 12),
        host, port, name: String(msg.name || '').slice(0, 80), udp: !!msg.udp,
        role: 'guest', state: 'offered', createdAt: Date.now(), code: msg.code || null,
        bytesUp: 0, bytesDown: 0, _localSocket: null, _udpSocket: null, _muxChannel: null, _muxMessages: null, _muxChannelCount: 0, _localServer: null, _localClient: null, _udpPeerAddr: null
      }
      this._tunnels.set(tunnelId, t)
      this.emit('tunnel:offer', { tunnelId, peerId, host, port, udp: !!msg.udp, name: t.name, role: 'guest', code: msg.code || null })
      setTimeout(() => {
        const cur = this._tunnels.get(tunnelId)
        if (cur && cur.state === 'offered') {
          // Tell the host the offer died on our side so it releases the
          // maxUses reservation it made for this claim.
          this._closeTunnel(tunnelId, 'offer-expired')
          try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_CLOSE, tunnelId, reason: 'offer-expired' }) } catch {}
        }
      }, this.offerExpiryMs || OFFER_EXPIRY_MS).unref?.()
      return
    }

    if (msg.type === MESSAGES.TUNNEL_ACCEPT) {
      const t = this._tunnels.get(String(msg.tunnelId || ''))
      if (!t || t.role !== 'host') return
      // Exactly one mux channel per tunnel: a duplicate TUNNEL_ACCEPT (guest auto-accept poll racing
      // the guest UI accept) while already connecting/open must not re-run _openProtomuxTunnel.
      if (t.state !== 'offered') {
        if (t.state === 'connecting' || t.state === 'open') console.warn('[tunnel] duplicate TUNNEL_ACCEPT ignored, state=', t.state)
        return
      }
      // code offers: ACCEPT is valid without pairing (code is auth), so skip trust re-check here
      if (!t.code) { // paired-only check for non-code tunnels
        const needPeer = this.engine.peers.get(peerId)
        if (!needPeer || !needPeer.pairing || !needPeer.pairing.trusted) {
          console.warn(`[TunnelManager] Ignoring TUNNEL_ACCEPT from unpaired peer ${String(peerId).slice(0,12)}`)
          return
        }
      }
      t.state = 'connecting'
      const pObj = this.engine.peers.get(t.peerId)
      if (!pObj) { this._closeTunnel(t.tunnelId, 'peer-gone'); return }
      this._openProtomuxTunnel(t, pObj)
      return
    }

    if (msg.type === MESSAGES.TUNNEL_REJECT) {
      const tid = String(msg.tunnelId || '')
      const t = this._tunnels.get(tid)
      if (!t) return
      this._closeTunnel(tid, String(msg.reason || 'rejected'))
      this.emit('tunnel:error', { tunnelId: tid, peerId, error: String(msg.reason || 'rejected') })
      return
    }

    if (msg.type === MESSAGES.TUNNEL_CLOSE || msg.type === MESSAGES.TUNNEL_ERROR) {
      const tid = String(msg.tunnelId || '')
      if (!tid) return
      this._closeTunnel(tid, String(msg.reason || msg.error || 'remote-close'))
      if (msg.type === MESSAGES.TUNNEL_ERROR) this.emit('tunnel:error', { tunnelId: tid, peerId, error: String(msg.error || 'error') })
      return
    }
  }

  // Token bucket per peerId. Every claim costs one token — valid, dedupe or
  // garbage — so fuzzing a share's existence drains the same budget as real
  // claims. Refill is 1 token per window without burst accumulation.
  _claimTakeToken(peerId) {
    const now = Date.now()
    const refillMs = this.claimBucketRefillMs || CLAIM_BUCKET_REFILL_MS
    const cap = this.claimBucketCapacity || CLAIM_BUCKET_CAPACITY
    let b = this._claimBuckets.get(peerId)
    if (!b) {
      b = { tokens: cap, last: now }
      this._claimBuckets.set(peerId, b)
    }
    const refills = Math.floor((now - b.last) / refillMs)
    if (refills > 0) {
      b.tokens = Math.min(cap, b.tokens + refills)
      b.last += refills * refillMs
    }
    if (b.tokens < 1) return false
    b.tokens--
    return true
  }

  // Uniform denial: {ok:false} only, never a reason — a differentiated error
  // string would hand probers a code-validity oracle. Specific reason is
  // logged host-side only. The echoed code is the sender's own input ('' when
  // none parses), so the echo itself leaks nothing.
  _claimDeny(peerId, peerObj, code, reason) {
    const wireCode = typeof code === 'string' ? code : ''
    console.log(`[TunnelManager] TUNNEL_CLAIM denied ${wireCode ? 'for ' + wireCode + ' ' : ''}from ${String(peerId).slice(0, 12)}: ${reason}`)
    try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_CLAIM_RES, code: wireCode, ok: false }) } catch {}
  }

  // Live = offered/connecting/open. Closed entries linger 5s post-close for UI
  // final-state rendering and must not count toward caps.
  _countLiveTunnelsByPeer(peerId) {
    let n = 0
    for (const t of this._tunnels.values()) { if (t.peerId === peerId && t.state !== 'closed') n++ }
    return n
  }
  _countLiveTunnelsByCode(code) {
    let n = 0
    for (const t of this._tunnels.values()) { if (t.code === code && t.state !== 'closed') n++ }
    return n
  }

  // maxUses count-on-open: the host side of a code tunnel consumes its
  // reserved slot when the mux channel actually opens, persists the new uses
  // count, and leaves the share's topic once maxUses is exhausted (no new
  // guests can discover it). A reservation that never opens is released by
  // _closeTunnel instead. Fires once per tunnel (_counted guards).
  _countCodeOpen(tunnel) {
    if (tunnel.role !== 'host' || !tunnel.code || tunnel._counted) return
    tunnel._counted = true
    tunnel._reservationHeld = false // a counted slot is never released
    const share = this._codeShares.get(tunnel.code)
    if (!share) return
    share.uses++
    share._reservations = Math.max(0, (share._reservations || 0) - 1)
    try { this.engine.getBee('tunnelShares').then(bee => bee.put(share.id, { ...share, _topic: undefined, _reservations: undefined })).catch(() => {}) } catch {}
    if (share.maxUses > 0 && share.uses >= share.maxUses) {
      console.log(`[TunnelManager] Tunnel code ${share.code} reached maxUses ${share.maxUses} — leaving topic`)
      try { this.engine.topicRegistry.leave(share._topic) } catch {}
    }
  }

  // maxUses release-on-abandon: a reserved-but-never-opened code tunnel closed
  // for ANY reason (guest reject, guest close while connecting, host cancel,
  // offer GC, code expiry, bind failure, peer drop) gives its slot back.
  // _reservationHeld guards against double-release from racing close paths.
  _releaseCodeReservation(tunnel) {
    if (!tunnel.code || tunnel._counted || !tunnel._reservationHeld) return
    tunnel._reservationHeld = false
    const share = this._codeShares.get(tunnel.code)
    if (share) share._reservations = Math.max(0, (share._reservations || 0) - 1)
  }

  _handleTunnelClaim(peerId, msg) {
    const peerObj = this.engine.peers.get(peerId)
    if (!peerObj || !peerObj.signaling) return
    const rawCode = typeof msg.code === 'string' ? msg.code : ''
    const code = normalizeTunnelCode(rawCode)
    // Throttle BEFORE any code resolution: every claim costs a token, even
    // invalid ones, so probing never gets cheaper than a real claim.
    if (!this._claimTakeToken(peerId)) {
      this._claimDeny(peerId, peerObj, code, 'rate-limited')
      return
    }
    if (!code) {
      this._claimDeny(peerId, peerObj, '', 'malformed-code')
      return
    }
    const share = this._codeShares.get(code)
    const now = Date.now()
    if (!share) {
      this._claimDeny(peerId, peerObj, code, 'invalid-code')
      return
    }
    if (share.expiresAt > 0 && now >= share.expiresAt) {
      this._claimDeny(peerId, peerObj, code, 'expired')
      return
    }
    // maxUses counts reserved-but-unopened slots too: a claim holds one until
    // its tunnel opens (or is abandoned), so burners can't drain the share
    // between a real guest's claim and open.
    if (share.maxUses > 0 && (share.uses + (share._reservations || 0)) >= share.maxUses) {
      this._claimDeny(peerId, peerObj, code, 'max-uses')
      return
    }
    // A valid claim proves this peer is connected on our p2p-tunnel-<code>
    // topic. Attribute it now — Hyperswarm's peerInfo.topics only land on the
    // dialing side, so without this feed the accepting host would misread the
    // guest as an unrelated peer (and pairing suppression could miss it).
    this._attributePeerTopic(peerId, share._topic)
    // Dedupe: if we already have an offered/open tunnel for this exact peer+code, don't create a second (drive retries + CLAIM retries would otherwise spam)
    for (const t of this._tunnels.values()) {
      if (t.peerId === peerId && t.code === code && (t.state === 'offered' || t.state === 'connecting' || t.state === 'open')) {
        // re-send the same offer so guest that missed it can catch up
        try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_OFFER, tunnelId: t.tunnelId, host: share.host, port: share.port, udp: !!share.udp, name: share.name, code }) } catch {}
        try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_CLAIM_RES, code, ok: true, tunnelId: t.tunnelId, host: share.host, port: share.port, udp: !!share.udp, name: share.name }) } catch {}
        return
      }
    }
    // Concurrency caps (a re-send of an existing tunnel above is exempt — it
    // adds no capacity). Denials leave the share's topic exactly as it was.
    const liveForPeer = this._countLiveTunnelsByPeer(peerId)
    if (this.maxTunnelsPerPeer > 0 && liveForPeer >= this.maxTunnelsPerPeer) {
      this._claimDeny(peerId, peerObj, code, `per-peer-cap ${liveForPeer}/${this.maxTunnelsPerPeer}`)
      return
    }
    const liveForCode = this._countLiveTunnelsByCode(code)
    if (this.maxActiveCodeTunnels > 0 && liveForCode >= this.maxActiveCodeTunnels) {
      this._claimDeny(peerId, peerObj, code, `per-share-cap ${liveForCode}/${this.maxActiveCodeTunnels}`)
      return
    }
    // allow multiple guests — each gets its own tunnelId pipe to same host port
    const tunnelId = makeTunnelId()
    // Reserve a maxUses slot for this claim (in-memory only; counted on open,
    // released on any close before open). uses is deliberately NOT incremented
    // here — a burner that claims and never opens must not consume the share.
    share._reservations = (share._reservations || 0) + 1
    // Track this guest for this code
    if (!this._activeCodeByPeer.has(peerId)) this._activeCodeByPeer.set(peerId, new Set())
    this._activeCodeByPeer.get(peerId).add(code)

    const t = {
      tunnelId, peerId, peerName: (peerObj.device && peerObj.device.name) || peerId.slice(0, 12),
      host: share.host, port: share.port, udp: !!share.udp, name: share.name, code,
      role: 'host', state: 'offered', createdAt: Date.now(),
      _counted: false, _reservationHeld: true, // maxUses slot: open counts it, any pre-open close releases it
      bytesUp: 0, bytesDown: 0, _localSocket: null, _udpSocket: null, _muxChannel: null, _muxMessages: null, _muxChannelCount: 0, _localServer: null, _localClient: null, _udpPeerAddr: null
    }
    this._tunnels.set(tunnelId, t)
    // send offer tied to this code (guest auto-accepts if it issued the claim)
    peerObj.signaling.send({ type: MESSAGES.TUNNEL_OFFER, tunnelId, host: share.host, port: share.port, udp: !!share.udp, name: share.name, code })
    // also confirm claim
    peerObj.signaling.send({ type: MESSAGES.TUNNEL_CLAIM_RES, code, ok: true, tunnelId, host: share.host, port: share.port, udp: !!share.udp, name: share.name })
    this.emit('tunnel:offer', { tunnelId, peerId, code, host: share.host, port: share.port, udp: !!share.udp, role: 'host', direction: 'code-claim' })
    // Un-accepted code offers are GC'd host-side too (mirrors the guest's own
    // offer timer) so a claim that never opens can't pin an offered tunnel —
    // or its reservation — indefinitely.
    setTimeout(() => {
      const cur = this._tunnels.get(tunnelId)
      if (cur && cur.state === 'offered') {
        this._closeTunnel(tunnelId, 'offer-expired')
        try { peerObj.signaling.send({ type: MESSAGES.TUNNEL_ERROR, tunnelId, error: 'offer-expired' }) } catch {}
      }
    }, this.offerExpiryMs || OFFER_EXPIRY_MS).unref?.()
  }

  _handleTunnelClaimRes(peerId, msg) {
    const code = normalizeTunnelCode(msg.code || '')
    if (!code || !this._pendingJoins || !this._pendingJoins.has(code)) return
    if (!msg.ok) {
      // Denials are uniform {ok:false} — the wire never says whether this was a
      // throttle, a cap, expiry or a bad code — so no single denial is proof the
      // join can never succeed. Keep the CLAIM drive running; it ends on the
      // first ok:true or the join's own 35s timeout. (Reasonless denials also
      // keep a non-hosting peer on the topic from aborting a legit join.)
      return
    }
    // success — host created a tunnelId and will send TUNNEL_OFFER (with code) shortly
    this._pendingJoins.delete(code)
    if (msg.tunnelId) {
      const tid = String(msg.tunnelId)
      let tries = 0
      const tryAccept = () => {
        const t = this._tunnels.get(tid)
        if (t && t.role === 'guest' && t.state === 'offered') {
          const pObj = this.engine.peers.get(peerId)
          // re-check right before sending: the UI accept path may have claimed the offer first
          if (pObj && pObj.signaling && t.state === 'offered') {
            t.state = 'connecting'
            try { pObj.signaling.send({ type: MESSAGES.TUNNEL_ACCEPT, tunnelId: tid }) } catch {}
            this._openProtomuxTunnel(t, pObj)
          }
          return
        }
        if (++tries < 20) setTimeout(tryAccept, 150).unref?.()
      }
      setTimeout(tryAccept, 50).unref?.()
    }
  }

  _openProtomuxTunnel(tunnel, peerObj) {
    const peerId = tunnel.peerId
    const tunnelId = tunnel.tunnelId
    // Exactly one Protomux channel per tunnel: a live channel means an accept already opened this tunnel.
    if (tunnel._muxChannel && !tunnel._muxChannel.destroyed) {
      console.log(`[TunnelManager] mux channel already open for ${tunnelId.slice(0, 8)} — skipping duplicate open`)
      return
    }
    const connection = peerObj.connection
    if (!connection) { this._closeTunnel(tunnelId, 'no-connection'); return }

    // UDP uses same mux but different data handling; reuse TCP protocol id for simplicity but track udp flag
    const protocol = tunnel.udp ? TUNNEL_UDP_PROTOCOL : TUNNEL_PROTOCOL
    const mux = Protomux.isProtomux(connection) ? connection : Protomux.from(connection)
    const channel = mux.createChannel({
      protocol,
      id: Buffer.from(tunnelId, 'utf8'),
      onopen: () => {
        if (tunnel.state === 'closed') return
        tunnel.state = 'open'
        this.emit('tunnel:opened', { tunnelId, peerId, host: tunnel.host, port: tunnel.port, udp: !!tunnel.udp, role: tunnel.role, code: tunnel.code || null })
        // Both accept paths (eager acceptTunnel and the CLAIM_RES auto-accept
        // poll) open the mux channel through this same helper, so counting the
        // maxUses slot here covers every code tunnel that actually connects.
        this._countCodeOpen(tunnel)
        // The TUNNEL is the long-lived object; local sockets are sub-resources
        // that come and go per stream. UDP binds its dgram socket at open (it
        // is connectionless); a TCP HOST opens NO upstream here — it connects
        // lazily on the first inbound guest data (_onHostData) — and the TCP
        // guest binds its tunnel-lifetime local forward listener.
        if (tunnel.udp) this._attachUdpSocket(tunnel)
        else if (tunnel.role === 'guest') this._bindGuestForward(tunnel)
      },
      onclose: () => { this._closeTunnel(tunnelId, 'mux-closed') },
      ondestroy: () => { this._closeTunnel(tunnelId, 'mux-destroyed') }
    })
    if (!channel) { this._closeTunnel(tunnelId, 'mux-create-failed'); return }

    const dataMsg = channel.addMessage({
      encoding: c.raw,
      onmessage: (buf) => {
        if (tunnel.state === 'closed') return
        const chunk = b4a.isBuffer(buf) ? buf : Buffer.from(buf)
        tunnel.bytesDown += chunk.length
        if (tunnel.udp) {
          // UDP: chunk is a framed datagram — forward to local udp socket target
          if (tunnel.role === 'host' && tunnel._udpSocket && tunnel._udpPeerAddr) {
            try { tunnel._udpSocket.send(chunk, tunnel._udpPeerAddr.port, tunnel._udpPeerAddr.address) } catch {}
          } else if (tunnel.role === 'guest' && tunnel._udpSocket) {
            // guest side: forward to whichever local udp client last sent, or broadcast to all bound clients
            try { tunnel._udpSocket.send(chunk, tunnel._lastGuestUdpAddr.port, tunnel._lastGuestUdpAddr.address) } catch {}
          }
          return
        }
        if (tunnel.role === 'host') this._onHostData(tunnel, chunk)
        else this._onGuestData(tunnel, chunk)
      }
    })

    const ctrlMsg = channel.addMessage({
      encoding: c.string,
      onmessage: (raw) => {
        if (tunnel.state === 'closed') return
        let m = null
        try { m = JSON.parse(raw) } catch {}
        if (!m || typeof m.type !== 'string') return
        if (m.type === 'close') {
          // whole-tunnel teardown requested by the peer
          this._closeTunnel(tunnelId, m.reason || 'remote-ctrl-close')
        } else if (m.type === 'eof') {
          // The peer's LOCAL stream ended (or never started): finish OUR local
          // stream with a clean FIN. Socket-only teardown — the tunnel stays
          // open; the next guest stream reuses the channel.
          if (tunnel.udp) return // UDP is connectionless — no stream to finish
          if (tunnel.role === 'host') this._endHostUpstream(tunnel)
          else this._endGuestClient(tunnel, m)
        }
      }
    })

    tunnel._muxChannel = channel
    tunnel._muxMessages = { data: dataMsg, ctrl: ctrlMsg }
    tunnel._muxChannelCount = (tunnel._muxChannelCount || 0) + 1
    channel.open()
  }

  // TCP guest: bind the local forward listener for the tunnel's life. Each
  // accepted local client is ONE in-flight stream on the mux channel
  // (single-stream scope — see TUNNEL_AUDIT.md §3.8). A second client while
  // one is active is rejected loudly, never byte-interleaved.
  _bindGuestForward(tunnel) {
    const lp = tunnel.localPort
    const tunnelId = tunnel.tunnelId
    if (!lp) return // opened via CLAIM_RES auto-accept without a local forward
    const server = net.createServer((client) => {
      if (tunnel.state === 'closed') { try { client.destroy() } catch {} return }
      if (tunnel._localClient) {
        // The channel is one byte pipe: two parallel local streams would
        // interleave with no way to demux replies. Queueing would
        // head-of-line-block behind an idle keep-alive client indefinitely,
        // so fail the newcomer fast (browsers open parallel conns and retry
        // refused ones).
        console.warn(`[TunnelManager] tunnel ${tunnelId.slice(0, 8)}: rejecting concurrent local client (single-stream tunnel)`)
        this.emit('tunnel:error', { tunnelId, error: 'concurrent-client-unsupported' })
        try { client.destroy() } catch {}
        return
      }
      tunnel._localClient = client
      client.on('data', (chunk) => {
        if (tunnel.state === 'closed' || tunnel._localClient !== client) return
        tunnel.bytesUp += chunk.length
        try { tunnel._muxMessages.data.send(chunk) } catch {}
      })
      client.on('error', () => { try { client.destroy() } catch {} })
      client.on('close', () => {
        if (tunnel._localClient !== client) return // EOF-initiated teardown already cleared it
        tunnel._localClient = null
        if (tunnel.state === 'closed') return
        // Local client ended → EOF to the host so its upstream closes too
        // (never lingers on an idle keep-alive).
        this._sendCtrl(tunnel, { type: 'eof' })
      })
    })
    server.on('error', (err) => {
      console.warn('[TunnelManager] guest listen failed:', err.message)
      this.emit('tunnel:error', { tunnelId, error: 'guest-listen-failed: ' + err.message })
    })
    // guest forward must be loopback-only; a LAN-exposed forward makes this device a relay into the host's localhost
    server.listen(lp, guestBindHost(tunnel.localHost), () => { // guestBindHost() → the '127.0.0.1' literal
      const a = server.address()
      console.log(`[TunnelManager] Tunnel ${tunnelId} listening on ${a ? a.address : '127.0.0.1'}:${lp} -> ${tunnel.host}:${tunnel.port} @ ${tunnel.peerId.slice(0, 12)}`)
    })
    tunnel._localServer = server
  }

  // Host upstream pipe: NO connect at open. The first inbound mux data with no
  // live upstream lazily opens one (Node buffers writes made before
  // 'connect', so nothing is lost). When that upstream goes away — an HTTP
  // server ending an idle keep-alive after ~5s, a service restart, a refused
  // connect — ONLY the socket is torn down, an EOF frame tells the guest to
  // finish its local client, and the tunnel survives: the next guest stream
  // simply opens a fresh upstream here.
  _connectUpstream(tunnel) {
    const sock = net.connect({ host: tunnel.host, port: tunnel.port })
    const tunnelId = tunnel.tunnelId
    tunnel._localSocket = sock
    sock.on('data', (chunk) => {
      if (tunnel.state === 'closed' || tunnel._localSocket !== sock) return
      tunnel.bytesUp += chunk.length
      try { tunnel._muxMessages.data.send(chunk) } catch {}
    })
    sock.on('error', (err) => {
      if (tunnel.state !== 'closed' && tunnel._localSocket === sock) {
        tunnel._localSocket = null
        console.warn(`[TunnelManager] tunnel ${tunnelId.slice(0, 8)} upstream ${tunnel.host}:${tunnel.port} error: ${err.message} — tunnel stays open (next guest stream reconnects)`)
        // error/EOF frame: the guest's local client must close cleanly, not hang
        this._sendCtrl(tunnel, { type: 'eof', reason: 'upstream-error: ' + err.message })
      }
      try { sock.destroy() } catch {}
    })
    sock.on('close', () => {
      if (tunnel.state === 'closed' || tunnel._localSocket !== sock) return
      tunnel._localSocket = null
      console.log(`[TunnelManager] tunnel ${tunnelId.slice(0, 8)} upstream closed (remote FIN) — tunnel stays open`)
      this._sendCtrl(tunnel, { type: 'eof' })
    })
    return sock
  }

  _onHostData(tunnel, chunk) {
    if (tunnel._localSocket) {
      const ok = tunnel._localSocket.write(chunk)
      if (!ok) tunnel._localSocket.once('drain', () => {})
      return
    }
    const sock = this._connectUpstream(tunnel)
    try { sock.write(chunk) } catch {} // net.Socket buffers writes until 'connect'
  }

  _onGuestData(tunnel, chunk) {
    const c = tunnel._localClient
    if (!c || c.destroyed) return // no active stream (EOF-finished) — nothing to route to
    const ok = c.write(chunk)
    if (!ok) c.once('drain', () => {})
  }

  _sendCtrl(tunnel, obj) {
    try { if (tunnel._muxMessages) tunnel._muxMessages.ctrl.send(JSON.stringify(obj)) } catch {}
  }

  // Host side of an EOF frame: the guest's local client ended. Close the
  // upstream socket only (flush queued writes then FIN; force-destroy if the
  // service never closes) — never the tunnel.
  _endHostUpstream(tunnel) {
    const s = tunnel._localSocket
    if (!s) return
    tunnel._localSocket = null
    console.log(`[TunnelManager] tunnel ${tunnel.tunnelId.slice(0, 8)} upstream closed (guest EOF) — tunnel stays open`)
    try { s.end() } catch {}
    if (typeof s.setTimeout === 'function') { try { s.setTimeout(2000, () => { try { s.destroy() } catch {} }) } catch {} }
  }

  // Guest side of an EOF frame: the host's upstream ended or never started
  // (idle close, crash, refused connect). Finish the local client with a clean
  // FIN so it errors out promptly instead of hanging; the local server keeps
  // listening for the next client.
  _endGuestClient(tunnel, eof) {
    const c = tunnel._localClient
    if (!c) return
    tunnel._localClient = null
    console.log(`[TunnelManager] tunnel ${tunnel.tunnelId.slice(0, 8)} upstream EOF${eof && eof.reason ? ' (' + eof.reason + ')' : ''} — finishing local client`)
    try { c.end() } catch {}
    if (typeof c.setTimeout === 'function') { try { c.setTimeout(2000, () => { try { c.destroy() } catch {} }) } catch {} }
  }

  _attachUdpSocket(tunnel) {
    const host = tunnel.host
    const port = tunnel.port
    const tunnelId = tunnel.tunnelId

    if (tunnel.role === 'host') {
      // Host side: create a UDP socket that will forward datagrams to the real local udp service and back
      const sock = dgram.createSocket('udp4')
      tunnel._udpSocket = sock
      // Remember guest's datagram target for replies
      sock.on('message', (msg) => {
        if (tunnel.state === 'closed') return
        tunnel.bytesUp += msg.length
        try { tunnel._muxMessages.data.send(msg) } catch {}
      })
      sock.on('error', (err) => {
        console.warn(`[TunnelManager] udp host socket error:`, err.message)
      })
      // Bind ephemeral and remember peer address lazily (guest sends first)
      sock.bind(0, '127.0.0.1', () => {
        // Also intercept inbound HIT from mux: we need target host:port for send — use supplied host/port
        tunnel._udpPeerAddr = { address: host, port }
      })
      // For host role, intercept mux data direction: guest->host datagrams arrive as TUNNEL_UDP data and are forwarded here (see onmessage)
      // Also forward local UDP replies back — already wired via 'message' above
    } else {
      // Guest side: bind a UDP listener on localPort (if provided), forwarding datagrams over mux
      const lp = tunnel.localPort || 0
      const sock = dgram.createSocket('udp4')
      tunnel._udpSocket = sock
      tunnel._lastGuestUdpAddr = null
      sock.on('message', (msg, rinfo) => {
        if (tunnel.state === 'closed') return
        tunnel._lastGuestUdpAddr = { address: rinfo.address, port: rinfo.port }
        tunnel.bytesUp += msg.length
        try { tunnel._muxMessages.data.send(msg) } catch {}
      })
      sock.on('error', (err) => {
        console.warn('[TunnelManager] udp guest socket error:', err.message)
        this.emit('tunnel:error', { tunnelId, error: 'udp-guest-error: ' + err.message })
      })
      const bindPort = typeof lp === 'number' ? lp : 0
      // guest forward must be loopback-only; a LAN-exposed forward makes this device a relay into the host's localhost
      sock.bind(bindPort, guestBindHost(tunnel.localHost), () => { // guestBindHost() → the '127.0.0.1' literal
        const a = sock.address()
        console.log(`[TunnelManager] UDP tunnel ${tunnelId} listening on ${a.address}:${a.port} -> ${tunnel.host}:${tunnel.port} @ ${tunnel.peerId.slice(0, 12)}`)
        tunnel.localPort = a.port
        tunnel.localHost = a.address
      })
    }
  }

  _checkExpirations() {
    const now = Date.now()
    // TTL sweep for claim-throttle buckets: drop buckets idle past the window
    // (disconnect cleanup handles the common case; this catches stragglers and
    // bounds memory for peers that never fully disconnect).
    for (const [pid, b] of Array.from(this._claimBuckets.entries())) {
      if (!b || now - b.last > CLAIM_BUCKET_SWEEP_MS) this._claimBuckets.delete(pid)
    }
    for (const [code, s] of Array.from(this._codeShares.entries())) {
      if (s.expiresAt > 0 && now >= s.expiresAt) {
        console.log(`[TunnelManager] Tunnel code ${code} expired`)
        try { this.engine.topicRegistry.leave(s._topic) } catch {}
        this._codeShares.delete(code)
        try { this.engine.getBee('tunnelShares').then(bee => bee.del(s.id)).catch(() => {}) } catch {}
        for (const [tid, t] of Array.from(this._tunnels.entries())) { if (t.code === code && t.state !== 'closed') this._closeTunnel(tid, 'code-expired') }
      }
    }
  }

  _closeTunnel(tunnelId, reason) {
    const t = this._tunnels.get(tunnelId)
    if (!t || t.state === 'closed') return
    t.state = 'closed'
    // Code tunnel closed before it opened → give its reserved maxUses slot
    // back (guest reject, remote close while connecting, host cancel, offer
    // GC, code expiry, bind failure, peer drop — all land here).
    if (t.code) this._releaseCodeReservation(t)
    try { if (t._localSocket) t._localSocket.destroy() } catch {}
    try { if (t._udpSocket) t._udpSocket.close() } catch {}
    try { if (t._localServer) t._localServer.close() } catch {}
    try { if (t._localClient) t._localClient.destroy() } catch {}
    try { if (t._muxChannel) t._muxChannel.close() } catch {}
    if (t._muxChannel) t._muxChannelCount = Math.max(0, (t._muxChannelCount || 0) - 1)
    t._localSocket = null
    t._udpSocket = null
    t._localServer = null
    t._localClient = null
    t._muxChannel = null
    t._muxMessages = null
    this.emit('tunnel:closed', { tunnelId, peerId: t.peerId, reason, role: t.role, code: t.code || null })
    setTimeout(() => this._tunnels.delete(tunnelId), 5000).unref?.()
  }

  async stop() {
    for (const [evt, fn] of this._listeners) this.engine.removeListener(evt, fn)
    this._listeners = []
    if (this._expirationTimer) { clearInterval(this._expirationTimer); this._expirationTimer = null }
    for (const tid of Array.from(this._tunnels.keys())) this._closeTunnel(tid, 'engine-stop')
    this._tunnels.clear()
    this._claimBuckets.clear()
    // Leave all code topics
    for (const s of this._codeShares.values()) { try { this.engine.topicRegistry.leave(s._topic) } catch {} }
    this._codeShares.clear()
    if (this._pendingJoins) {
      for (const { topic } of this._pendingJoins.values()) { try { this.engine.topicRegistry.leave(topic) } catch {} }
      this._pendingJoins.clear()
    }
  }
}

module.exports = { TunnelManager, TUNNEL_PROTOCOL, TUNNEL_UDP_PROTOCOL, TUNNEL_DATA_MAX }
