'use strict'

// LanDiscovery owns LAN peer discovery END-TO-END inside the core.
//
// In the legacy worker this was split in two: the Bare worker had no
// UDP stack, so Electron main owned the socket advertiser
// (electron/lan-discovery.js) and fed announcements back over IPC. The core
// has no Electron and no IPC, so the UDP advertiser (node:dgram, multicast
// 239.255.255.250:39001) and the peer-validation state are consolidated here.
//
// Only 64-hex noise (swarm) public keys are ever accepted or advertised —
// never a corestore identity key — and only a real swarm public key is handed
// to swarm.joinPeer.

let dgram = null
try {
  dgram = eval("require('dgram')")
} catch {}
const { os } = require('../compat.js')

const PEER_KEY_HEX_LEN = 64 // 32-byte noise public key, hex-encoded
const MAX_PEERS = 128
const PRUNE_MS = 30 * 1000
const DEFAULT_PORT = 39001
const DEFAULT_GROUP = '239.255.255.250'
const ANNOUNCE_INTERVAL_MS = 5000
const MAX_MESSAGE_SIZE = 512

class LanDiscovery {
  constructor({
    swarm,
    getDeviceIdentity,
    onPeerKey,
    now = Date.now,
    port = DEFAULT_PORT,
    group = DEFAULT_GROUP,
    bindAddress = '0.0.0.0',
    targets = null,
    multicast = true,
    announceIntervalMs = ANNOUNCE_INTERVAL_MS
  }) {
    this.swarm = swarm
    this.getDeviceIdentity = getDeviceIdentity || (() => null)
    this.onPeerKey = onPeerKey || (() => {})
    this.now = now
    this.port = port
    this.group = group
    this.bindAddress = bindAddress
    this.targets = targets || [group]
    this.multicast = multicast
    this.announceIntervalMs = announceIntervalMs
    this.known = new Map() // peerKeyHex -> { key, id, name, os, seenAt }
    this.started = false
    this.socket = null
    this.timer = null
    this.self = null
  }

  // The only key we may advertise or connect with: the swarm DHT keypair
  // public key (the peer identity used by secret-stream / joinPeer).
  getPublicKey() {
    const keyPair = this.swarm && this.swarm.keyPair
    if (!keyPair || !keyPair.publicKey) return null
    return Buffer.from(keyPair.publicKey).toString('hex')
  }

  getSelfAnnouncement() {
    const identity = this.getDeviceIdentity() || {}
    return {
      v: 1,
      key: this.getPublicKey(),
      id: identity.id || '',
      name: identity.name || '',
      os: identity.os || ''
    }
  }

  // Bind the UDP socket and start announcing. Never throws: a LAN-discovery
  // failure must not take down the DHT-based engine.
  start() {
    if (this.started) return
    this.started = true
    try {
      this.socket = dgram.createSocket({ type: 'udp4', reuseAddr: true })
      this.socket.on('message', (msg, rinfo) => this._handleMessage(msg, rinfo))
      this.socket.on('error', (err) =>
        console.warn('[MeshEngine] LanDiscovery socket error:', err.message)
      )
      this.socket.bind(this.port, this.bindAddress, () => {
        if (!this.started) return // stopped while binding
        this._joinGroup()
        this._announce()
        this.timer = setInterval(() => this._announce(), this.announceIntervalMs)
        if (this.timer.unref) this.timer.unref()
      })
    } catch (err) {
      console.warn('[MeshEngine] LanDiscovery failed to start:', err.message)
      this.started = false
    }
    return this.getPublicKey()
  }

  _joinGroup() {
    if (!this.multicast) return
    try {
      this.socket.addMembership(this.group, this._interfaceAddress())
    } catch (err) {
      console.warn('[MeshEngine] LanDiscovery multicast join failed:', err.message)
    }
    try {
      this.socket.setBroadcast(true)
    } catch {}
    try {
      this.socket.setMulticastTTL(64)
    } catch {}
  }

  _interfaceAddress() {
    try {
      const ifaces = os.networkInterfaces()
      for (const name of Object.keys(ifaces)) {
        for (const net of ifaces[name] || []) {
          if (net.family === 'IPv4' && !net.internal) return net.address
        }
      }
    } catch {}
    return '0.0.0.0'
  }

  _announce() {
    if (!this.started || !this.socket) return
    const self = this.getSelfAnnouncement()
    if (!self || !self.key) return
    const buf = Buffer.from(JSON.stringify(self))
    for (const target of this.targets) {
      try {
        this.socket.send(buf, 0, buf.length, this.port, target)
      } catch (err) {
        console.warn('[MeshEngine] LanDiscovery send failed:', err.message)
      }
    }
  }

  _handleMessage(msg) {
    if (!this.started || msg.length > MAX_MESSAGE_SIZE) return
    let ann = null
    try {
      ann = JSON.parse(msg.toString())
    } catch {
      return
    }
    this.handleAnnouncement(ann)
  }

  // Handle a LAN announcement (a raw key string or a decoded object). Returns
  // true when the peer is new and was handed to joinPeer, false when ignored.
  handleAnnouncement(announcement) {
    if (!this.started) return false
    const key =
      typeof announcement === 'string'
        ? announcement
        : announcement && typeof announcement.key === 'string'
          ? announcement.key
          : null
    if (typeof key !== 'string' || key.length !== PEER_KEY_HEX_LEN) return false

    const selfKey = this.getPublicKey()
    if (selfKey && key === selfKey) return false

    const existing = this.known.get(key)
    if (existing) {
      existing.seenAt = this.now()
      return false
    }

    this.prune()
    this.known.set(key, {
      key,
      id: announcement && announcement.id,
      name: announcement && announcement.name,
      os: announcement && announcement.os,
      seenAt: this.now()
    })
    this.onPeerKey(key, {
      id: announcement && announcement.id,
      name: announcement && announcement.name
    })
    return true
  }

  // Drop stale entries so a churning network cannot grow this map unboundedly.
  prune() {
    const cutoff = this.now() - PRUNE_MS
    for (const [key, entry] of this.known.entries()) {
      if (entry.seenAt < cutoff) this.known.delete(key)
    }
    if (this.known.size > MAX_PEERS) {
      const sorted = Array.from(this.known.entries()).sort((a, b) => a[1].seenAt - b[1].seenAt)
      const excess = this.known.size - MAX_PEERS
      for (let i = 0; i < excess; i++) this.known.delete(sorted[i][0])
    }
  }

  // Whether a peer noise key was (recently) discovered on the local network.
  // Used by onConnection to auto-trust LAN-discovered peers when the
  // autoTrustLAN setting is enabled — an explicit signal that beats IP-based
  // heuristics (same-machine sockets, NATs, port suffixes, etc.).
  has(key) {
    return typeof key === 'string' && this.known.has(key)
  }

  knownPeers() {
    return Array.from(this.known.values())
  }

  stop() {
    this.started = false
    if (this.timer) {
      clearInterval(this.timer)
      this.timer = null
    }
    if (this.socket) {
      try {
        this.socket.close()
      } catch {}
      this.socket = null
    }
  }
}

module.exports = LanDiscovery
