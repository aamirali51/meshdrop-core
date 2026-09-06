'use strict'

const BlindRelay = require('blind-relay')

const MAX_RELAY_SESSIONS = 8

class BlindRelayManager {
  constructor(engine) {
    this.engine = engine
    this.relay = null
    this.dhtServer = null
    this.enabled = false
    this.sessions = new Map()
    this._sessionCounter = 0
  }

  isRunning() {
    return this.enabled && (!!this.dhtServer || !!this._onSwarmConnection)
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
      list.push({ id, remoteKey: remote, remotePrefix: remote.slice(0, 12), deviceName: device || null, label, durationMs: Date.now() - info.start, bytes: info.bytes })
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

    let useSharedPath = false
    try {
      this.dhtServer = dht.createServer((socket) => this._handleRelaySocket(socket))
      await this.dhtServer.listen(this.engine.swarm.keyPair)
      console.log(`[BlindRelay] Relay server listening on ${this.engine.swarm.keyPair.publicKey.toString('hex').slice(0, 12)}... (dedicated HyperDHT server)`)
    } catch (err) {
      if (/KEYPAIR_ALREADY_USED|ALREADY_LISTENING/i.test(String(err.message))) {
        useSharedPath = true
        this.dhtServer = null
        console.log('[BlindRelay] Relay sharing swarm server (single DHT key) — attaching to swarm connections')
      } else {
        console.warn('[BlindRelay] Failed to start dedicated relay server:', err.message)
        useSharedPath = true
      }
    }

    if (useSharedPath) {
      this._onSwarmConnection = (socket) => this._handleRelaySocket(socket)
      this.engine.swarm.on('connection', this._onSwarmConnection)
      this.dhtServer = { _shared: true, close: async () => { try { this.engine.swarm.off('connection', this._onSwarmConnection) } catch {} } }
    }

    console.log('[BlindRelay] Relay for paired devices ENABLED (cap 8 sessions)')
  }

  _handleRelaySocket(socket) {
    if (!this.enabled || !this.relay) return
    const active = this.relay.stats.sessions.active
    if (active >= MAX_RELAY_SESSIONS) {
      const remote = socket.remotePublicKey ? Buffer.from(socket.remotePublicKey).toString('hex').slice(0, 12) : 'unknown'
      console.warn(`[BlindRelay] Session cap reached (${active}/${MAX_RELAY_SESSIONS}) — rejecting relay socket from ${remote}...`)
      try { socket.destroy() } catch {}
      return
    }
    const remoteHex = socket.remotePublicKey ? Buffer.from(socket.remotePublicKey).toString('hex') : 'unknown'
    const remotePrefix = remoteHex.slice(0, 12)
    const start = Date.now()
    const id = `${remotePrefix}-${++this._sessionCounter}`
    console.log(`[BlindRelay] Relay session accepted from ${remotePrefix}... (active ${active + 1}/${MAX_RELAY_SESSIONS})`)

    const session = this.relay.accept(socket, { id: socket.remotePublicKey })

    this.sessions.set(id, { remoteKey: remoteHex, start, bytes: 0, session })

    const onClose = () => {
      const info = this.sessions.get(id)
      if (!info) return
      const duration = Date.now() - info.start
      const devName = this._resolveDeviceName(info.remoteKey)
      const label = devName ? `Relaying for ${devName}` : 'unknown peer'
      console.log(`[BlindRelay] Relay session closed ${remotePrefix}... bytes=${info.bytes} duration=${duration}ms ${label}`)
      this.sessions.delete(id)
    }
    session.on('close', onClose)
    session.on('error', (err) => {
      console.warn(`[BlindRelay] Relay session error ${remotePrefix}...:`, err.message)
    })

    setTimeout(() => {
      const info = this.sessions.get(id)
      if (!info) return
      try {
        if (session._pairing && session._pairing.size === 0 && session._links && session._links.size === 0) {
          this.sessions.delete(id)
          session.removeListener('close', onClose)
        }
      } catch {}
    }, 3000).unref?.()
  }

  async stop() {
    if (!this.enabled) return
    this.enabled = false
    console.log('[BlindRelay] Relay for paired devices DISABLING — closing server + sockets')
    for (const [, info] of this.sessions.entries()) {
      try { info.session.destroy() } catch {}
    }
    this.sessions.clear()
    if (this.dhtServer) {
      if (this.dhtServer._shared) {
        try { this.engine.swarm.off('connection', this._onSwarmConnection) } catch {}
      } else {
        try { await this.dhtServer.close() } catch {}
      }
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

module.exports = { BlindRelayManager, MAX_RELAY_SESSIONS }
