'use strict'

const BlindRelay = require('blind-relay')

const MAX_RELAY_SESSIONS = 8
const UNPAIRED_IDLE_TIMEOUT_MS = 10 * 1000

class BlindRelayManager {
  constructor(engine) {
    this.engine = engine
    this.relay = null
    this.dhtServer = null
    this.enabled = false
    this.sessions = new Map() // id -> { remoteKey, start, bytes, session, paired, idleTimer }
    this._sessionCounter = 0
  }

  isRunning() {
    return this.enabled && !!this._onSwarmConnection
  }

  getStats() {
    const active = this.sessions.size
    const relay = this.relay
    const serverActive = relay ? relay.stats.sessions.active : 0
    const pairings = relay ? { pending: relay.stats.pairings.pending, active: relay.stats.pairings.active, matched: relay.stats.pairings.matched } : { pending: 0, active: 0, matched: 0 }
    const list = []
    for (const [id, info] of this.sessions.entries()) {
      const remote = info.remoteKey || 'unknown'
      const device = this._resolveDeviceName(remote)
      const label = device ? `Relaying for ${device}` : 'unknown peer'
      list.push({ id, remoteKey: remote, remotePrefix: remote.slice(0, 12), deviceName: device || null, label, durationMs: Date.now() - info.start, bytes: info.bytes, paired: !!info.paired })
    }
    return { active, serverActive, pairings, sessions: list, maxSessions: MAX_RELAY_SESSIONS, running: this.isRunning() }
  }

  _resolveDeviceName(remoteHex) {
    if (!remoteHex || remoteHex === 'unknown') return null
    try {
      for (const peerObj of this.engine.peers.values()) {
        if (peerObj.device && peerObj.device.publicKey === remoteHex) return peerObj.device.name
      }
      return null
    } catch { return null }
  }

  _isPairedPeerKey(hex) {
    if (!hex || hex === 'unknown' || hex.length !== 64) return false
    try {
      const peerObj = this.engine.peers.get(hex)
      return !!(peerObj && peerObj.device && peerObj.device.isTrusted)
    } catch { return false }
  }

  async start() {
    if (this.enabled) return
    if (!this.engine.swarm || !this.engine.swarm.dht) {
      console.warn('[BlindRelay] Cannot start: swarm/dht not ready')
      return
    }
    const dht = this.engine.swarm.dht
    this.relay = new BlindRelay.Server({
      createStream(opts) {
        return dht.createRawStream({ ...opts, framed: true })
      }
    })

    this.enabled = true

    // The swarm always owns the keyPair by construction (Hyperswarm creates a
    // HyperDHT server on the keyPair at construction). Trying a dedicated
    // dht.createServer on that key will always throw KEYPAIR_ALREADY_USED,
    // so we go straight to the live path: share the swarm's single DHT key.
    this._onSwarmConnection = (socket) => this._handleRelaySocket(socket)
    this.engine.swarm.on('connection', this._onSwarmConnection)
    this.dhtServer = { _shared: true, close: async () => { try { this.engine.swarm.off('connection', this._onSwarmConnection) } catch {} } }
    console.log('[BlindRelay] Relay sharing swarm server (single DHT key) — attaching to swarm connections')

    console.log('[BlindRelay] Relay for paired devices ENABLED (cap 8 sessions)')
  }

  _handleRelaySocket(socket) {
    if (!this.enabled || !this.relay) return
    const remoteHex = socket.remotePublicKey ? Buffer.from(socket.remotePublicKey).toString('hex') : 'unknown'
    const isPaired = this._isPairedPeerKey(remoteHex)
    const active = this.relay.stats.sessions.active

    if (active >= MAX_RELAY_SESSIONS) {
      if (isPaired) {
        // Paired preemption: cap is full, but this is a paired device — destroy
        // the oldest unknown-peer session to admit it. Prevents strangers from
        // starving paired devices.
        let victimId = null
        let victimStart = Infinity
        for (const [id, info] of this.sessions.entries()) {
          if (!info.paired && info.start < victimStart) {
            victimId = id
            victimStart = info.start
          }
        }
        if (victimId) {
          const victim = this.sessions.get(victimId)
          console.warn(`[BlindRelay] Cap full (8/8) — evicting oldest unknown peer ${victim.remoteKey.slice(0, 12)}... to admit paired ${remoteHex.slice(0, 12)}...`)
          try { victim.session.destroy() } catch {}
          try { if (victim.idleTimer) clearTimeout(victim.idleTimer) } catch {}
          this.sessions.delete(victimId)
        } else {
          const remote = remoteHex.slice(0, 12)
          console.warn(`[BlindRelay] Session cap reached (8/8) — rejecting relay socket from ${remote}... (all sessions are paired)`)
          try { socket.destroy() } catch {}
          return
        }
      } else {
        const remote = remoteHex.slice(0, 12)
        console.warn(`[BlindRelay] Session cap reached (8/8) — rejecting relay socket from ${remote}...`)
        try { socket.destroy() } catch {}
        return
      }
    }

    const remotePrefix = remoteHex.slice(0, 12)
    const start = Date.now()
    const id = `${remotePrefix}-${++this._sessionCounter}`
    // Phantom probes are normal on the shared-swarm-key path — every inbound
    // connection is offered to blind-relay. Log at debug so normal peers
    // don't spam production logs with zero-byte sessions.
    const isDebug = process.env.MESH_DEBUG_RELAY === '1'
    if (isDebug) console.log(`[BlindRelay] Relay dial probe from ${remotePrefix}... (paired=${isPaired})`)

    const session = this.relay.accept(socket, { id: socket.remotePublicKey })

    const info = { remoteKey: remoteHex, start, bytes: 0, session, paired: isPaired, idleTimer: null }
    this.sessions.set(id, info)

    const clearIdle = () => { if (info.idleTimer) { clearTimeout(info.idleTimer); info.idleTimer = null } }

    // Idle/unpaired guard: a relay socket that hasn't completed its token pair
    // within UNPAIRED_IDLE_TIMEOUT_MS is destroyed (strangers cannot idle-hold
    // slots). Paired peers clear the timer on pair.
    if (!isPaired) {
      info.idleTimer = setTimeout(() => {
        if (!this.sessions.has(id)) return
        // If still unpaired (no links, no pairing activity), destroy
        try {
          const hasLinks = session._links && session._links.size > 0
          const hasPairing = session._pairing && session._pairing.size > 0
          if (!hasLinks && !hasPairing) {
            console.warn(`[BlindRelay] Idle unpaired session ${remotePrefix}... timed out (${UNPAIRED_IDLE_TIMEOUT_MS}ms) — destroying`)
            try { session.destroy() } catch {}
            try { socket.destroy() } catch {}
            this.sessions.delete(id)
          }
        } catch {}
      }, UNPAIRED_IDLE_TIMEOUT_MS)
      if (info.idleTimer.unref) info.idleTimer.unref()
    }

    const onPair = () => {
      // Token pair completed — this session is now active relay, cancel idle timer
      clearIdle()
      info.paired = true
    }
    // Blind-relay emits 'pair' on successful token match — use to clear idle guard
    session.on('pair', onPair)

    const onClose = () => {
      clearIdle()
      session.off('pair', onPair)
      if (!this.sessions.has(id)) return
      const duration = Date.now() - info.start
      // Honest log: only claim "Relaying for X" when bytes flowed.
      // Zero-byte probes log at debug as "relay dial probe (no data)".
      if (info.bytes > 0) {
        const devName = this._resolveDeviceName(info.remoteKey)
        const label = devName ? `Relaying for ${devName}` : 'unknown peer'
        console.log(`[BlindRelay] Relay session closed ${remotePrefix}... bytes=${info.bytes} duration=${duration}ms ${label}`)
      } else if (process.env.MESH_DEBUG_RELAY === '1') {
        console.log(`[BlindRelay] Relay dial probe (no data) closed ${remotePrefix}... duration=${duration}ms`)
      }
      this.sessions.delete(id)
    }
    session.on('close', onClose)
    session.on('error', (err) => {
      console.warn(`[BlindRelay] Relay session error ${remotePrefix}...:`, err.message)
    })

    // Shared-path idle heuristic: if this socket never sends a blind-relay pair,
    // it is a normal peer connection — remove from relay accounting after 3s
    // (but the idle guard above already covers unpaired strangers; this is for
    // normal peers that just look like relay sessions at first).
    setTimeout(() => {
      if (!this.sessions.has(id)) return
      try {
        if (session._pairing && session._pairing.size === 0 && session._links && session._links.size === 0) {
          if (!info.paired) {
            // Normal peer, not a relay client — stop counting it
            this.sessions.delete(id)
            clearIdle()
            session.removeListener('close', onClose)
            session.off('pair', onPair)
          }
        }
      } catch {}
    }, 3000).unref?.()
  }

  async stop() {
    if (!this.enabled) return
    this.enabled = false
    console.log('[BlindRelay] Relay for paired devices DISABLING — closing server + sockets')
    for (const [, info] of this.sessions.entries()) {
      try { if (info.idleTimer) clearTimeout(info.idleTimer) } catch {}
      try { info.session.destroy() } catch {}
    }
    this.sessions.clear()
    if (this.dhtServer) {
      try { this.engine.swarm.off('connection', this._onSwarmConnection) } catch {}
      this.dhtServer = null
    }
    this._onSwarmConnection = null
    if (this.relay) {
      try { await this.relay.close() } catch {}
      this.relay = null
    }
    console.log('[BlindRelay] Relay server closed, no relay sessions accepted')
  }
}

module.exports = { BlindRelayManager, MAX_RELAY_SESSIONS, UNPAIRED_IDLE_TIMEOUT_MS }
