'use strict'

// @mesh/core — zero-cloud P2P messenger + file-transfer engine.
//
// Single entrypoint: `MeshEngine`, an EventEmitter that owns the full P2P
// stack (Hyperswarm / HyperDHT / Noise XX, LAN discovery, trust + pairing,
// Corestore / Hyperbee replication, and the transfer engine) with no Electron
// and no IPC. Storage roots come from config — never from app.getPath().

const { EventEmitter, path, os, fs, fsp } = require('./compat.js')
const Hyperswarm = require('hyperswarm')

const { EVENTS, MESSAGES, pairingTopic } = require('./protocol.js')
const { generateDropCode, normalizeDropCode } = require('./crypto.js')
const { TrustManager } = require('./engine/TrustManager.js')
const { TransferEngine } = require('./engine/TransferEngine.js')
const { SyncEngine } = require('./engine/SyncEngine.js')
const MetricsCollector = require('./engine/MetricsCollector.js')
const TopicRegistry = require('./engine/TopicRegistry.js')
const NotificationStore = require('./engine/NotificationStore.js')
const ReplicationScope = require('./engine/ReplicationScope.js')
const LanDiscovery = require('./engine/LanDiscovery.js')
const { loadOrCreateNoiseKeypair } = require('./noiseKeypair.js')
const { createStorage } = require('./storage.js')
const { createConnections, getTransferMethod } = require('./connections/index.js')
const { RelayClient } = require('./connections/relayClient.js')

const PAIR_WAIT_TIMEOUT = 60 * 1000 // max time pairWithCode waits for verification
const EXPIRATION_INTERVAL_MS = 10 * 1000

// Expiration presets for one-time DROP shares. Single source of truth so the
// engine, Electron handlers, and the renderer agree on the accepted values.
function getDurationMs(preset) {
  const map = {
    '5m': 5 * 60 * 1000,
    '10m': 10 * 60 * 1000,
    '15m': 15 * 60 * 1000,
    '30m': 30 * 60 * 1000,
    '1h': 60 * 60 * 1000,
    '6h': 6 * 60 * 60 * 1000,
    '24h': 24 * 60 * 60 * 1000,
    '3d': 3 * 24 * 60 * 60 * 1000,
    '7d': 7 * 24 * 60 * 60 * 1000,
    never: 0
  }
  return map[preset] || 30 * 60 * 1000
}

// DHT relay fallback: on restrictive networks (symmetric NAT, TCP-only VPNs)
// direct UDP hole-punching fails. hyperswarm then reconnects the peer through
// a DHT relay node, which tunnels the noise stream over TCP. We pick a relay
// from the known-good bootstrap nodes first — they are the Holepunch-operated
// public DHT nodes (stable, publicly reachable, relay-capable) — and fall back
// to the longest-resident routing-table node only if the bootstrap nodes are
// not in our routing table yet.
//
// The Pear docs (docs.pears.com "Connect two peers by key with HyperDHT"):
// "HyperDHT's holepunching will fail if both the client peer and the server
// peer are on randomizing NATs, in which case the connection must be relayed
// through a third peer. HyperDHT does not do any relaying by default." Keet
// relays through other participants; our equivalent is the public bootstrap
// nodes, which are always reachable.
//
// NOTE: the first connection attempt must NOT be gated on `dht.randomized` —
// that property is never set to true in hyperdht 6.x (the holepuncher tracks
// randomized NAT on its own field and bumps dht._randomPunches, not
// dht.randomized), so gating on it silently disabled relaying and pairing
// across different networks timed out.
let lastRelayId = null
const BOOTSTRAP_HOSTS = new Set([
  '88.99.3.86',
  '142.93.90.113',
  '138.68.147.8'
])

// Pass 0: an online, trusted, desktop-class paired peer — our own relay.
// Only called when preferOwnRelay is on. The peer must be a desktop (os match)
// and its own connection must NOT already be relayed (a phone relaying through
// a phone is pointless). Returns the peer's noise public key (hex) — the DHT
// dials relays by key.
function pickOwnPeerRelay(engine) {
  try {
    if (!engine || !engine.preferOwnRelay) return null
    const peers = engine.peers
    if (!peers || peers.size === 0) return null
    for (const [peerId, peerObj] of peers.entries()) {
      const dev = peerObj && peerObj.device
      if (!dev || !dev.isOnline || !dev.isTrusted || !dev.publicKey) continue
      // Desktop-class only: a phone behind cellular CGNAT is a poor relay.
      const osName = String(dev.os || '').toLowerCase()
      const isDesktop = /win|mac|darwin|linux|ubuntu|debian|fedora/.test(osName)
      if (!isDesktop) continue
      // If this peer's own link to us is relayed, relaying back through it is
      // a loop — skip. Also skip the peer we last used as relay (rotation).
      if (dev.relayed) continue
      if (lastRelayId === peerId) continue
      // Remember which own-peer key we chose so onConnection can label
      // relayed links as "via your Desktop".
      if (engine) engine._lastOwnRelayKey = peerId
      lastRelayId = peerId
      return peerId // hex noise public key — exactly what dht.connect() wants
    }
  } catch {}
  return null
}

function pickRelayNode(dht) {
  try {
    const nodes = dht && dht.nodes
    if (!nodes || nodes.length === 0) return null

    // Pass 1: prefer a stable public bootstrap node from the routing table.
    // These are the Holepunch-operated relay-capable nodes; ephemeral peers
    // (phones, laptops behind NAT) make terrible relays.
    let best = null
    for (let node = nodes.latest; node; node = node.prev) {
      if (!node.id || !node.host || !node.port) continue
      if (node.id === lastRelayId) continue
      if (!BOOTSTRAP_HOSTS.has(node.host)) continue
      if (!best || node.added < best.added) best = node
    }
    if (best) {
      lastRelayId = best.id
      return best.id
    }

    // Pass 2: fall back to the longest-resident routing-table node. A DHT node
    // that has stayed in the table longest is the best uptime proxy we have
    // for ephemeral peers; bootstrap nodes are preferred above, so this only
    // catches the no-bootstrap-in-table case (e.g. custom bootstrap).
    best = null
    for (let node = nodes.latest; node; node = node.prev) {
      if (!node.id || !node.host || !node.port) continue
      if (node.id === lastRelayId) continue
      // Never relay through a private-range peer — it is behind NAT itself.
      if (/^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|127\.|169\.254\.)/.test(node.host)) continue
      if (!best || node.added < best.added) best = node
    }
    if (best) lastRelayId = best.id
    return best ? best.id : null
  } catch {}
  return null
}

class MeshEngine extends EventEmitter {
  /**
   * @param {object} [config]
   * @param {string} config.storageDir    Directory for engine state (corestores, keypair, bees). Required.
   * @param {string} [config.downloadsDir] Directory for received files. Defaults to <storageDir>/downloads.
   * @param {string} [config.deviceName]  Human-readable device name. Defaults to the hostname.
   * @param {boolean} [config.autoAcceptOffers] Auto-accept incoming file offers (headless-friendly). Default true.
   * @param {boolean} [config.autoTrustLAN] Auto-trust peers discovered on the LAN. Default false —
   *   devices must complete the pairing handshake (keys + code) to be trusted.
   * @param {boolean} [config.lanDiscovery] Enable UDP LAN discovery. Default true.
   */
  constructor(config = {}) {
    super()

    if (!config.storageDir || typeof config.storageDir !== 'string') {
      throw new Error('MeshEngine requires a storageDir in its config')
    }

    this.config = config
    this.storageDir = config.storageDir
    this.downloadsDir = config.downloadsDir || path.join(config.storageDir, 'downloads')
    this.deviceName = config.deviceName || os.hostname()
    this.autoAcceptOffers = config.autoAcceptOffers === true
    // Security default: NEVER auto-trust LAN peers. A device must complete the
    // pairing handshake (prove knowledge of the MD- code) to be trusted — LAN
    // discovery only helps the connection establish, it grants no trust.
    this.autoTrustLAN = config.autoTrustLAN === true
    this.lanDiscoveryEnabled = config.lanDiscovery !== false
    // Own-device relay: when true, this node prefers an online paired desktop
    // as the relay for connections that can't holepunch directly (instead of
    // going straight to the public bootstrap nodes). Only paired, trusted
    // devices ever see this node's key, so the relay is private to the mesh.
    this.preferOwnRelay = config.preferOwnRelay !== false

    this.started = false

    // Mutable engine state (mirrors the old worker's shared `ctx`)
    this.peers = new Map() // peerId (noise pubkey hex) -> { connection, device, signaling, transferMethod, pairing }
    this.activeClaims = new Set() // drop codes currently being claimed
    this.activeClaimOptions = new Map() // drop code -> options ({ interactive: true })
    this.interactiveClaims = config.interactiveClaims === true
    this.connectionCount = 0

    // Engines, wired in start()
    this.swarm = null
    this.storage = null
    this.trustManager = null
    this.transferEngine = null
    this.metricsCollector = null
    this.topicRegistry = null
    this.replicationScope = null
    this.lanDiscovery = null
    this.notificationStore = null
    this.connections = null
    this.relayMode = config.relayMode || 'auto' // 'auto' | 'relay-primary' | 'direct-only'
    this.customRelayUrl = config.customRelayUrl || config.relayUrl || ''
    this.relayClient = new RelayClient({
      relayUrl: this.customRelayUrl,
      mode: this.relayMode
    })
    this.expirationTimer = null
    // Set when refreshNetwork() finds the DHT unreachable (dead network, no
    // replacement yet). A self-scheduled retry re-runs the rebuild so the
    // engine heals when connectivity silently returns without an OS event.
    this._networkRetryTimer = null

    // EventEmitter throws on 'error' with no listener; the engine always has
    // a no-op handler so a stray error can never crash the process.
    this.on(EVENTS.ERROR, () => {})
  }

  get deviceIdentity() {
    return this.storage ? this.storage.getDeviceIdentity() : null
  }

  get peerId() {
    return this.swarm && this.swarm.keyPair
      ? Buffer.from(this.swarm.keyPair.publicKey).toString('hex')
      : ''
  }

  // Hyperbee factory for engine-internal stores (devices, transfers, settings...).
  getBee(name) {
    return this.storage.getBee(name)
  }

  // Build a fresh Hyperswarm around the persistent noise keypair. Used at
  // boot and again by refreshNetwork() — the relay picker must be re-created
  // with the swarm because it binds to the new DHT node at call time.
  _createSwarm() {
    return new Hyperswarm({
      keyPair: this.noiseKeyPair,
      // Relay is always available as the fallback: hyperswarm tries the direct
      // UDP holepunch first on every connection, and only uses the relay when
      // direct fails (HOLEPUNCH_ABORTED / HOLEPUNCH_DOUBLE_RANDOMIZED_NATS /
      // REMOTE_NOT_HOLEPUNCHABLE) — which is exactly when a relay is needed.
      // Gating this on `dht.randomized` broke relaying entirely (that property
      // is never set in hyperdht 6.x) and made cross-network pairing time out.
      // Pass 0 prefers an online paired desktop (own relay, private to the
      // mesh); Pass 1/2 fall back to public bootstrap + routing-table nodes.
      relayThrough: (force, s) => pickOwnPeerRelay(this) || pickRelayNode(s.dht)
    })
  }

  // Rebuild the swarm after a network/interface change (Wi-Fi → cellular,
  // router swap, VPN toggle). The old DHT node + UDP/TCP sockets are bound to
  // the previous interface: lookups and topic announcements silently die with
  // them, so paired devices can no longer find this node even though the app
  // has connectivity. A fresh swarm re-binds to the current interface and
  // re-announces every active topic (identity, paired peers, pairing codes,
  // drop shares) via TopicRegistry.reattach. Identity, trust, pairing codes,
  // and stores are untouched.
  async refreshNetwork() {
    if (!this.started) return this
    // Serialize refreshes: a rapid Wi-Fi → cellular → Wi-Fi toggle fires
    // several distinct network events while the first rebuild is still in
    // flight. Without this, a second refresh can destroy the swarm the first
    // one just created mid-initSwarm and the engine stays offline until the
    // next event.
    if (this._refreshing) {
      this._refreshQueued = true
      return this
    }
    this._refreshing = true
    let ok = false
    try {
      ok = await this._rebuildSwarm()
    } finally {
      this._refreshing = false
    }
    if (this._refreshQueued) {
      this._refreshQueued = false
      await this.refreshNetwork()
      return this
    }
    // If the DHT could not bootstrap (network dropped with no replacement yet),
    // schedule a retry instead of going silent. The OS layer fires no event
    // when connectivity silently returns, so the engine must probe on its own.
    // A later successful refresh clears the timer via the ok path below.
    if (!ok && !this._networkRetryTimer) {
      this._networkRetryTimer = setTimeout(async () => {
        this._networkRetryTimer = null
        if (this.started) await this.refreshNetwork()
      }, 15000)
      if (this._networkRetryTimer.unref) this._networkRetryTimer.unref()
    } else if (ok && this._networkRetryTimer) {
      clearTimeout(this._networkRetryTimer)
      this._networkRetryTimer = null
    }
    return this
  }

  async _rebuildSwarm() {
    console.log('[MeshEngine] Network change detected — rebuilding swarm')
    // Tear down established peer connections FIRST. swarm.destroy() kills the
    // DHT transport (UDP socket → NAT mappings, relay TCP) but does not iterate
    // the established noise streams, so without this a peer entry stays
    // 'online' on a dead transport until keepalive (~90s) evicts it, and
    // in-flight transfers keep writing into the dead socket. Destroying each
    // connection triggers the on('close') cleanup (peers map, replication
    // scope, claim streams, transfer park) immediately.
    for (const peerObj of this.peers.values()) {
      const conn = peerObj && peerObj.connection
      if (conn && typeof conn.destroy === 'function') {
        try {
          conn.destroy(new Error('network changed'))
        } catch {}
      }
    }
    try {
      // force: the graceful path (pending DHT flush waits, server close) can
      // hang for minutes on a vanished network (TCP half-open relays). A
      // network-switch rebuild must not block on the dead transport.
      await this.swarm.destroy({ force: true }).catch(() => {})
    } catch (err) {
      console.warn('[MeshEngine] swarm.destroy during refresh:', err.message)
    }
    this.swarm = this._createSwarm()
    if (this.metricsCollector && typeof this.metricsCollector.rebind === 'function') {
      this.metricsCollector.rebind(this.swarm)
    }
    // initSwarm() returns whether the DHT bootstrapped on the new network.
    // false means we are offline (or the DHT is unreachable) — refreshNetwork()
    // uses that to schedule a self-retry.
    const dhtOk = await this.connections.initSwarm()
    await this.connections.reconnectKnownPeers()
    console.log(
      `[MeshEngine] Swarm rebuilt on new network (dht=${dhtOk ? 'ready' : 'down'})`
    )
    return dhtOk
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  async start() {
    if (this.started) return
    try {
      fs.mkdirSync(this.storageDir, { recursive: true })
      fs.mkdirSync(this.downloadsDir, { recursive: true })
    } catch (err) {
      throw new Error(`MeshEngine cannot create storage dirs: ${err.message}`)
    }

    console.log('[MeshEngine] starting...')

    // The swarm noise keypair is this node's peer identity and MUST persist
    // across restarts: trust, device records, and direct reconnects all key
    // on it. A fresh keypair per boot orphans every previously paired device.
    // It is also reused verbatim when refreshNetwork() rebuilds the swarm
    // after a network/interface change — identity must never rotate there.
    this.noiseKeyPair = loadOrCreateNoiseKeypair(this.storageDir)
    console.log(
      `[MeshEngine] Noise key (stable): ${this.noiseKeyPair.publicKey.toString('hex').slice(0, 12)}...`
    )

    this.swarm = this._createSwarm()

    this.storage = createStorage({
      storageDir: this.storageDir,
      downloadsDir: this.downloadsDir,
      deviceName: this.deviceName
    })

    // Apply the configured device name / platform before identity init.
    const platformMap = { win32: 'Windows', darwin: 'macOS', linux: 'Linux' }
    this.storage.setDeviceInfo({
      name: this.deviceName,
      os: platformMap[os.platform()] || os.platform()
    })

    // Connection layer first: lanDiscovery wiring needs it (workers/main.js
    // creates connections before the engines for the same reason).
    this.connections = createConnections(this)

    this.metricsCollector = new MetricsCollector({ swarm: this.swarm })
    this.metricsCollector.start()

    this.topicRegistry = new TopicRegistry({
      computeTopicHash: this.storage.computeTopicHash,
      swarm: this.swarm
    })

    this.replicationScope = new ReplicationScope({
      exchangeStore: this.storage.exchangeStore,
      isPeerTrusted: (peerId) => this.peers.get(peerId)?.pairing?.trusted === true,
      onStream: (stream) => this.metricsCollector?.trackStream(stream)
    })

    this.trustManager = new TrustManager({
      getBee: this.storage.getBee,
      computeTopicHash: this.storage.computeTopicHash,
      swarm: this.swarm,
      topicRegistry: this.topicRegistry,
      relayClient: this.relayClient,
      getPeers: () => this.peers,
      sendHandshake: (peerId) => this.connections.sendHandshake(peerId),
      emit: (event, data) => this.emit(event, data),
      isRefreshing: () => this._refreshing === true,
      getDeviceIdentity: () => this.storage.getDeviceIdentity(),
      getPeerId: () => this.peerId,
      onTrustGranted: (peerId, code) => {
        const peerObj = this.peers.get(peerId)
        if (peerObj && peerObj.pairing) peerObj.pairing.code = code
        // A HANDSHAKE that arrived while the challenge was still outstanding is
        // applied now that trust is granted, so the connection always completes.
        this.connections.replicateExchange(peerId)
        this.connections.flushPendingHandshake(peerId)
        // If the handshake already completed earlier (LAN auto-trust / prior
        // pairing), re-broadcast completion so pairWithCode gets a fresh signal.
        this.connections.rebroadcastPeerCompletion(peerId, code)
      }
    })

    this.notificationStore = new NotificationStore({
      emit: (event, data) => this.emit(event, data)
    })

    // Surface transfer activity in the notification store (the renderer's bell
    // drawer shows them live via notification:received).
    this.on(EVENTS.TRANSFER_OFFER, (offer) => {
      if (this.notificationStore && offer && offer.source !== 'sync') {
        this.notificationStore.addNotification(
          'Incoming Transfer',
          `${offer.filename || 'A file'} from ${offer.senderIdentity?.name || 'a device'}`,
          'info'
        )
      }
    })
    this.on(EVENTS.TRANSFER_QUEUED, (t) => {
      if (this.notificationStore && t && t.direction === 'send' && t.source !== 'sync') {
        this.notificationStore.addNotification(
          'Sharing Started',
          `${t.filename || 'A file'} → ${t.peerName || 'a device'}`,
          'info'
        )
      }
    })
    this.on(EVENTS.TRANSFER_COMPLETED, (t) => {
      if (this.notificationStore && t && t.source !== 'sync') {
        this.notificationStore.addNotification(
          'Transfer Complete',
          `${t.filename || 'A file'} · ${t.direction === 'send' ? 'sent' : 'received'}`,
          'success'
        )
      }
    })
    this.on(EVENTS.SYNC_LIBRARY_ADDED, (lib) => {
      if (this.notificationStore && lib) {
        this.notificationStore.addNotification(
          'Folder Sync Active',
          `Folder "${lib.name || 'Sync'}" linked for direct P2P synchronization.`,
          'info'
        )
      }
    })
    let lastSyncNotificationTime = 0
    this.on(EVENTS.SYNC_COMPLETED, (d) => {
      if (this.notificationStore && d && (d.pushed > 0 || d.deleted > 0)) {
        const now = Date.now()
        if (now - lastSyncNotificationTime < 10000) return
        lastSyncNotificationTime = now
        const libName = d.name || (this.syncEngine?.libraries.get(d.id)?.name) || 'Sync folder'
        this.notificationStore.addNotification(
          'Folder Synced',
          `"${libName}": ${d.pushed || 0} file(s) synced, ${d.deleted || 0} deleted.`,
          'success'
        )
      }
    })
    this.on(EVENTS.SYNC_DELETED, (d) => {
      if (this.notificationStore && d) {
        this.notificationStore.addNotification(
          'File Moved to Trash',
          `Synced file "${d.rel || 'file'}" was deleted and archived safely.`,
          'info'
        )
      }
    })
    this.on(EVENTS.SYNC_INVITE_RECEIVED, (invite) => {
      if (this.notificationStore && invite) {
        this.notificationStore.addNotification(
          'Sync Request Received',
          `${invite.peerName || 'A device'} wants to sync folder "${invite.name || 'Sync'}".`,
          'info'
        )
      }
    })
    this.on(EVENTS.SYNC_ERROR, (err) => {
      if (this.notificationStore && err) {
        this.notificationStore.addNotification(
          'Sync Issue',
          err.message || 'An issue occurred during folder sync.',
          'error'
        )
      }
    })
    this.transferEngine = new TransferEngine({
      getBee: this.storage.getBee,
      exchangeStore: this.replicationScope,
      sendEvent: (event, data) => this.emit(event, data),
      getPeers: () => this.peers,
      getDeviceIdentity: () => this.storage.getDeviceIdentity(),
      getDownloadDirectory: () => this.getDownloadDirectory(),
      getTransferMethod,
      fsp,
      path,
      // Lets TransferEngine preserve a conflicting local copy (two-way sync)
      // instead of silently overwriting it. SyncEngine owns the mode.
      getSyncMode: (syncLibraryId) =>
        this.syncEngine && syncLibraryId
          ? this.syncEngine.getLibraryMode(syncLibraryId)
          : null,
      // Base dir for the .p2p-staging partial sweep at init.
      getStagingRoot: () => this.downloadsDir
    })

    this.syncEngine = new SyncEngine({
      getBee: this.storage.getBee,
      getPeers: () => this.peers,
      getPeerId: () => this.peerId,
      sendEvent: (event, data) => this.emit(event, data),
      transferEngine: this.transferEngine,
      downloadsDir: this.downloadsDir,
      fsp,
      path,
      fs,
      autoAcceptOffers: this.autoAcceptOffers
    })
    this.transferEngine.resolveSyncDest = (offer) => this.syncEngine.resolveSyncDest(offer)
    // Pause/delete authority: TransferEngine rejects incoming sync offers for
    // libraries that are paused or removed instead of routing them somewhere.
    this.transferEngine.syncAllowed = (offer) => this.syncEngine.isSyncAllowed(offer)
    // Strict-verify authority: the transfer-level re-sync skip only trusts a
    // size+mtime match when WE delivered that exact version.
    this.transferEngine.syncDelivered = (libId, rel, mtimeMs) =>
      this.syncEngine.isDeliveredVersion(libId, rel, mtimeMs)
    // A failed/interrupted sync push must retry: drop the optimistic remote
    // mark so the next round re-pushes the file instead of assuming the peer
    // has it (the streaming sender cannot mark delivery before 'done'). All
    // terminal states also advance the transferring-phase counter so the UI
    // transitions back to "synced" exactly when the round's payload is done.
    const onSyncTransferTerminal = (record) => {
      if (this.syncEngine && typeof this.syncEngine.handleTransferTerminal === 'function') {
        this.syncEngine.handleTransferTerminal(record)
      }
    }
    this.on(EVENTS.TRANSFER_FAILED, onSyncTransferTerminal)
    this.on(EVENTS.TRANSFER_CANCELLED, onSyncTransferTerminal)
    this.on(EVENTS.TRANSFER_COMPLETED, onSyncTransferTerminal)
    this.on(EVENTS.TRANSFER_STARTED, (record) => {
      if (this.syncEngine && typeof this.syncEngine.handleTransferStarted === 'function') {
        this.syncEngine.handleTransferStarted(record)
      }
    })
    // A trusted peer just came online: give pending sync libraries a nudge
    // instead of waiting for the next rescan tick (and re-announce invites),
    // and re-enqueue any sync sends parked as WAITING_PEER while it was offline.
    this.on(EVENTS.PEER_CONNECTED, (device) => {
      const peerId = device && (device.publicKey || device.peerId)
      if (peerId && this.syncEngine) {
        this.syncEngine.tick().catch(() => {})
      }
      if (this.transferEngine && typeof this.transferEngine.retryWaiting === 'function') {
        this.transferEngine.retryWaiting().catch(() => {})
      }
    })
    if (this.connections && this.connections.refs) {
      this.connections.refs.handleSyncIndex = (peerId, msg) => this.syncEngine.handleSyncIndex(peerId, msg)
      this.connections.refs.handleSyncDelete = (peerId, msg) => this.syncEngine.handleSyncDelete(peerId, msg)
      this.connections.refs.handleSyncInvite = (peerId, msg) => this.syncEngine.handleSyncInvite(peerId, msg)
      this.connections.refs.handleSyncInviteAccept = (peerId, msg) => this.syncEngine.handleSyncInviteAccept(peerId, msg)
      this.connections.refs.handleSyncInviteDecline = (peerId, msg) => this.syncEngine.handleSyncInviteDecline(peerId, msg)
      this.connections.refs.handleSyncRemove = (peerId, msg) => this.syncEngine.handleSyncRemove(peerId, msg)
      this.connections.refs.handleSyncVerify = (peerId, msg) => this.syncEngine.handleSyncVerify(peerId, msg)
      this.connections.refs.handleSyncVerifyResult = (peerId, msg) => this.syncEngine.handleSyncVerifyResult(peerId, msg)
    }

    this.lanDiscovery = this.lanDiscoveryEnabled
      ? new LanDiscovery({
          swarm: this.swarm,
          getDeviceIdentity: () => this.storage.getDeviceIdentity(),
          onPeerKey: (key) => {
            try {
              const peerKey = Buffer.from(key, 'hex')
              if (peerKey.length === 32) {
                console.log(`[MeshEngine] LAN discovery -> joinPeer(${key.slice(0, 12)}...)`)
                this.swarm.joinPeer(peerKey)
                // The connection may already exist (e.g. via the DHT identity
                // topic) before this announcement lands: promote it to direct
                // trust now so autoTrustLAN still bypasses the pairing handshake.
                this.connections.maybeAutoTrustLanPeer(key).catch(() => {})
              }
            } catch (err) {
              console.warn('[MeshEngine] LAN discovery joinPeer failed:', err.message)
            }
          }
        })
      : null

    this.metricsCollector = new MetricsCollector({ swarm: this.swarm })
    this.metricsCollector.start()

    await this.storage.storeReady()

    // Hydrate persisted settings so the LAN auto-trust / auto-accept toggles
    // survive restarts (the Settings UI writes the settings bee; the engine
    // must honor it on boot, not just in-session).
    try {
      const bee = await this.storage.getBee('settings')
      const entry = await bee.get('settings')
      const s = entry && entry.value
      if (s && typeof s.autoTrustLAN === 'boolean') this.autoTrustLAN = s.autoTrustLAN
      if (s && typeof s.autoAcceptOffers === 'boolean') this.autoAcceptOffers = s.autoAcceptOffers
      if (s && typeof s.preferOwnRelay === 'boolean') this.preferOwnRelay = s.preferOwnRelay
      if (s && typeof s.relayMode === 'string') {
        this.relayMode = s.relayMode
        if (this.relayClient) this.relayClient.setMode(s.relayMode)
      }
      if (s && typeof s.customRelayUrl === 'string') {
        this.customRelayUrl = s.customRelayUrl
        if (this.relayClient) this.relayClient.setRelayUrl(s.customRelayUrl)
      }
    } catch {}

    await this.replicationScope.init()
    await this.transferEngine.init()
    await this.syncEngine.init()

    const identity = await this.storage.initIdentity()
    console.log('[MeshEngine] Device identity:', identity.name, identity.id)

    await this.trustManager.loadRevokedKeys()
    await this.trustManager.loadTrustedPeerKeys()
    await this.connections.initSwarm()
    console.log('[MeshEngine] Swarm joined, listening for peers')

    await this.connections.reconnectKnownPeers()

    // Prime the host pairing code so getIdentity() can return it synchronously.
    await this.trustManager.getOrCreatePairingCode()

    // Restore active/unexpired drop shares so their DHT topics re-announce on restart
    await this.restorePendingShares()

    this.expirationTimer = setInterval(() => this.checkPendingExpirations(), EXPIRATION_INTERVAL_MS)
    if (this.expirationTimer.unref) this.expirationTimer.unref()

    this.started = true
    if (this.relayClient) {
      this.relayClient.setPeerId(this.peerId)
      this.relayClient.start()
    }
    console.log('[MeshEngine] ready')
    return this
  }

  async stop() {
    if (!this.started) return
    this.started = false
    console.log('[MeshEngine] stopping...')
    if (this.relayClient) this.relayClient.stop()
    // Stop the unref'd maintenance intervals (reconnectKnownPeers, sendPings)
    // so they never touch the stores while we tear down below.
    if (this.connections && typeof this.connections.teardown === 'function') {
      this.connections.teardown()
    }
    if (this.expirationTimer) {
      clearInterval(this.expirationTimer)
      this.expirationTimer = null
    }
    if (this._networkRetryTimer) {
      clearTimeout(this._networkRetryTimer)
      this._networkRetryTimer = null
    }
    if (this.metricsCollector) this.metricsCollector.stop()
    if (this.lanDiscovery) this.lanDiscovery.stop()
    if (this.syncEngine) await this.syncEngine.stop()
    if (this.transferEngine) await this.transferEngine.shutdown()
    if (this.replicationScope) this.replicationScope.closeAll()
    if (this.topicRegistry) this.topicRegistry.leaveAll()
    if (this.swarm) await this.swarm.destroy()
    if (this.storage) {
      await this.storage.store.close().catch(() => {})
      await this.storage.exchangeStore.close().catch(() => {})
    }
    console.log('[MeshEngine] stopped')
  }

  // ─── Identity & pairing ───────────────────────────────────────────────────

  /**
   * Local device identity.
   * @returns {{ deviceId: string, publicKey: string, pairingCode: string }}
   *   publicKey is the swarm (noise) peer key — the value other engines pass
   *   to offerFile(). pairingCode is the active host code (primed at start()).
   */
  getIdentity() {
    const identity = this.deviceIdentity || {}
    return {
      deviceId: identity.id || '',
      publicKey: this.peerId,
      pairingCode: this.trustManager ? this.trustManager.getActiveHostCode() || '' : ''
    }
  }

  /**
   * Pair with the device that owns `code` (challenge-response over the DHT
   * pairing topic). Resolves with the paired device record once trust is
   * mutually verified; rejects on timeout.
   * @param {string} code  e.g. "MD-ABCD-EFGH-JKLM-NPQR"
   * @param {{ timeoutMs?: number }} [opts]
   */
  pairWithCode(code, { timeoutMs = PAIR_WAIT_TIMEOUT } = {}) {
    if (!this.started) return Promise.reject(new Error('Engine not started'))
    const clean = this.trustManager.registerJoinerCode(code)
    if (!clean) {
      return Promise.reject(
        new Error('Invalid pairing code format. Expected MD-XXXX-XXXX-XXXX-XXXX')
      )
    }
    if (this.trustManager && clean === this.trustManager.getActiveHostCode()) {
      return Promise.reject(new Error('Cannot pair a device with its own pairing code.'))
    }
    console.log(`[MeshEngine] Pairing with code: ${clean}`)
    return new Promise((resolve, reject) => {
      let settled = false
      let gotPairingFailure = false
      const finish = (fn, value) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        this.removeListener(EVENTS.TRUST_PAIRED, onPaired)
        this.removeListener(EVENTS.PEER_DISCONNECTED, onDisconnected)
        this.removeListener(EVENTS.TRUST_REVOKED, onRevoked)
        this.removeListener(EVENTS.PAIRING_FAILED, onPairingFailed)
        // A settled pairing (success OR failure) drops the joiner secret and
        // leaves its topic. Without this a failed/abandoned pairing keeps the
        // code registered for its full 15-min TTL and the DHT topic announced.
        // The host code is unaffected — it is a different secret (role: 'host').
        try {
          this.trustManager.dropJoinerCode(clean)
        } catch {}
        fn(value)
      }
      const onPaired = ({ peer, code: pairedCode }) => {
        if (pairedCode !== clean) return // a different pairing completed
        finish(resolve, peer)
      }
      const onPairingFailed = ({ peerId }) => {
        // A MAC/nonce mismatch on OUR challenge: the host answered with the
        // wrong code (or a stale one after deletion). Record it so a
        // disconnect/timeout surfaces the real error instead of a generic hang.
        if (peerId) gotPairingFailure = true
      }
      const onDisconnected = ({ id, publicKey }) => {
        // The connection we were challenging died before verification — either
        // the host rejected our MAC (wrong code) or the transport dropped. If
        // a pairing failure was already recorded, surface that instead of a
        // generic timeout. Otherwise the engine's reconnect loop may re-attempt
        // (devices.reconnectKnownPeers / topic re-join), so only settle if the
        // failure is definitive.
        if (gotPairingFailure) {
          finish(
            reject,
            new Error(
              'Pairing rejected by the host — the code may be wrong or the host deleted this device.'
            )
          )
        }
        void id
        void publicKey
      }
      const onRevoked = ({ publicKey }) => {
        // We were explicitly removed by the host mid-pairing. Fail fast so the
        // UI shows a real error instead of a 30-60s hang.
        finish(reject, new Error('The host removed this device. Pair again with the current code.'))
        void publicKey
      }
      const timer = setTimeout(() => {
        finish(
          reject,
          new Error(
            gotPairingFailure
              ? 'Pairing failed — the code was rejected by the host.'
              : `Pairing timed out after ${timeoutMs}ms — is the host online and is the code correct?`
          )
        )
      }, timeoutMs)
      if (timer.unref) timer.unref()
      this.on(EVENTS.TRUST_PAIRED, onPaired)
      this.on(EVENTS.PEER_DISCONNECTED, onDisconnected)
      this.on(EVENTS.TRUST_REVOKED, onRevoked)
      this.on(EVENTS.PAIRING_FAILED, onPairingFailed)
    })
  }

  // ─── Files ────────────────────────────────────────────────────────────────

  /**
   * Offer a local file to a paired peer. Returns the created send-transfer
   * record; the peer receives a 'transfer:offer' event.
   * @param {string} peerId    Peer noise public key (hex) from the 'trust:paired' event.
   * @param {string} filePath  Absolute path of the file to send.
   */
  async offerFile(peerId, filePath) {
    if (!this.started) throw new Error('Engine not started')
    let targetPeerId = peerId
    if (typeof peerId === 'string' && !this.peers.has(peerId)) {
      for (const [noiseKey, pObj] of this.peers.entries()) {
        if (pObj.device?.id === peerId || pObj.device?.publicKey === peerId || noiseKey === peerId) {
          targetPeerId = noiseKey
          break
        }
      }
    }
    if (typeof targetPeerId !== 'string' || !this.peers.has(targetPeerId)) {
      throw new Error(`Peer not connected: ${peerId || '(empty)'}`)
    }
    const stats = await fsp.stat(filePath)
    if (!stats.isFile()) throw new Error(`Not a file: ${filePath}`)
    const peerObj = this.peers.get(targetPeerId)
    return this.transferEngine.startSend({
      peerId: targetPeerId,
      peerName: peerObj?.device?.name || 'Unknown',
      filePath,
      filename: path.basename(filePath),
      fileSize: stats.size
    })
  }

  /** Approve a pending incoming offer (use when autoAcceptOffers is false). */
  async acceptTransfer(transferId) {
    return this.transferEngine.approve(transferId)
  }

  // ─── Folder sync (SyncEngine) ───────────────────────────────────────────

  /**
   * Start syncing a local folder to a trusted paired device (two-way or push).
   * Fast stat indexing (<100ms) with zero local storage duplication.
   * @param {{ path: string, peerId: string, name?: string, mode?: string }} params
   */
  async addSyncLibrary(params) {
    if (!this.started) throw new Error('Engine not started')
    return this.syncEngine.addLibrary(params)
  }

  async removeSyncLibrary(libraryId) {
    return this.syncEngine.removeLibrary(libraryId)
  }

  listSyncLibraries() {
    return this.syncEngine ? this.syncEngine.listLibraries() : []
  }

  /** Rescan + sync a library now. */
  async syncLibrary(libraryId) {
    return this.syncEngine.syncLibrary(libraryId)
  }

  async pauseSyncLibrary(libraryId) {
    return this.syncEngine.pauseSync(libraryId)
  }

  async resumeSyncLibrary(libraryId) {
    return this.syncEngine.resumeSync(libraryId)
  }

  async acceptSyncInvite({ id, customPath }) {
    return this.syncEngine.acceptSyncInvite({ id, customPath })
  }

  async declineSyncInvite({ id }) {
    return this.syncEngine.declineSyncInvite({ id })
  }

  listPendingSyncInvites() {
    return this.syncEngine ? this.syncEngine.listPendingInvites() : []
  }

  /** Decline a pending incoming offer. */
  async declineTransfer(transferId) {
    return this.transferEngine.decline(transferId)
  }

  /** Pause a running transfer. */
  async pauseTransfer(transferId) {
    return this.transferEngine.pause(transferId)
  }

  /** Resume a paused or interrupted transfer (reuses the stored core). */
  async resumeTransfer(transferId) {
    return this.transferEngine.resume(transferId)
  }

  /** Cancel a transfer; the partial download stays resumable. */
  async cancelTransfer(transferId) {
    // A cancelled pending claim should stop advertising its code too.
    try {
      const bee = await this.storage.getBee('transfers')
      const entry = await bee.get(transferId)
      const record = entry && entry.value
      if (record && record.claimCode) {
        this.activeClaims.delete(record.claimCode)
        try {
          this.topicRegistry.leave(`p2p-file-${record.claimCode}`)
        } catch {}
      }
    } catch {}
    return this.transferEngine.cancel(transferId)
  }

  /** Re-queue a failed/interrupted transfer from scratch. */
  async retryTransfer(transferId) {
    return this.transferEngine.retry(transferId)
  }

  /** Delete an individual transfer record (cancelling if active). */
  async deleteTransfer(transferId) {
    return this.transferEngine.delete(transferId)
  }

  /** Remove transfer records (terminal by default, or all including pending when includePending=true). */
  async clearTransfers(options) {
    return this.transferEngine.clear(options)
  }

  /** Engine storage overview: record counts + on-disk size (for the UI). */
  async getStorageStats() {
    const counts = { transfers: 0, history: 0, shared: 0, syncLibraries: 0 }
    try {
      for (const name of ['transfers', 'history', 'shared']) {
        const bee = await this.storage.getBee(name)
        for await (const node of bee.createReadStream()) {
          if (typeof node.key === 'string' && node.key.startsWith('__')) continue
          counts[name]++
        }
      }
      const syncBee = await this.storage.getBee('sync')
      for await (const node of syncBee.createReadStream()) counts.syncLibraries++
    } catch {}
    let sizeBytes = 0
    try {
      const walk = async (dir) => {
        const entries = await fsp.readdir(dir, { withFileTypes: true }).catch(() => [])
        for (const ent of entries) {
          const abs = path.join(dir, ent.name)
          if (typeof ent.isDirectory === 'function' && ent.isDirectory()) {
            await walk(abs)
          } else {
            try {
              const st = await fsp.stat(abs)
              sizeBytes += st.size || 0
            } catch {}
          }
        }
      }
      await walk(this.storageDir)
    } catch {}
    return { ...counts, sizeBytes }
  }

  /** Clear the persisted transfer log and transfer history. */
  async clearTransferLog() {
    await this.transferEngine.clear().catch(() => {})
    try {
      const bee = await this.storage.getBee('history')
      const keys = []
      for await (const node of bee.createReadStream()) keys.push(node.key)
      for (const k of keys) await bee.del(k).catch(() => {})
    } catch {}
    return { success: true }
  }

  /**
   * Rebuild the metadata store to reclaim RocksDB blob garbage (repeated
   * rewrites of large sync-library records leave old blob versions behind).
   * The noise keypair and swarm identity are preserved; the derived device id
   * may change, so devices may need re-pairing afterwards.
   */
  async compactStorage() {
    if (this.storage && typeof this.storage.compactStore === 'function') {
      await this.storage.compactStore()
      // Refresh the in-memory identity against the rebuilt store.
      try {
        await this.storage.initIdentity()
      } catch {}
      return { success: true }
    }
    return { success: false }
  }

  /**
   * Start sending a file to a peer. `peerId` may be the peer's noise public
   * key (from trust:paired / getPeers) or its stable device id; fileSize is
   * stat'ed when omitted.
   * @param {{ peerId: string, filePath: string, filename?: string, fileSize?: number, priority?: string, transferMethod?: string }} params
   */
  async startTransfer(params = {}) {
    if (!this.started) throw new Error('Engine not started')
    const { peerId } = params
    let noiseKey = typeof peerId === 'string' ? peerId : ''
    if (noiseKey && !this.peers.has(noiseKey)) {
      // Accept the stable device id too (the pre-MeshEngine renderer sends
      // device.id): resolve it to the live noise key.
      for (const [key, peerObj] of this.peers.entries()) {
        if (peerObj.device && peerObj.device.id === noiseKey) {
          noiseKey = key
          break
        }
      }
    }
    if (!this.peers.has(noiseKey)) {
      throw new Error(`Peer not connected: ${peerId || '(empty)'}`)
    }
    const peerObj = this.peers.get(noiseKey)
    let fileSize = params.fileSize
    let filePath = params.filePath
    if (!filePath) throw new Error('Transfer requires a filePath')
    if (typeof fileSize !== 'number' || fileSize <= 0) {
      const stats = await fsp.stat(filePath)
      if (!stats.isFile()) throw new Error(`Not a file: ${filePath}`)
      fileSize = stats.size
    }
    return this.transferEngine.startSend({
      ...params,
      peerId: noiseKey,
      peerName: params.peerName || peerObj?.device?.name || 'Unknown',
      filePath,
      filename: params.filename || path.basename(filePath),
      fileSize
    })
  }

  /** List persisted transfers (newest first). */
  async listTransfers() {
    return this.transferEngine.list()
  }

  /** Connected (handshake-complete) peer device records. */
  getPeers() {
    const out = []
    for (const peerObj of this.peers.values()) {
      if (peerObj.pairing && peerObj.pairing.complete && peerObj.device) {
        out.push({ ...peerObj.device, peerId: peerObj.device.publicKey })
      }
    }
    return out
  }

  /**
   * All known devices: the persisted devices bee (written by the trusted-
   * handshake path) merged with live connection state. Rows are deduplicated
   * by the stable identity key; self and placeholder rows are excluded.
   */
  async listDevices() {
    const deviceMap = new Map()
    const canonicalKey = (dev) =>
      typeof dev.identityKey === 'string' && dev.identityKey
        ? dev.identityKey
        : typeof dev.id === 'string'
          ? dev.id
          : null
    const isSelf = (dev) =>
      this.deviceIdentity &&
      (dev.id === this.deviceIdentity.id || dev.publicKey === this.deviceIdentity.publicKey)

    try {
      const bee = await this.getBee('devices')
      for await (const node of bee.createReadStream()) {
        const dev = node.value
        if (!dev || !dev.id || isSelf(dev)) continue
        if (dev.name && dev.name.startsWith('Device-')) continue
        const key = canonicalKey(dev)
        if (!key) continue
        const existing = deviceMap.get(key)
        if (existing && (existing.lastSeen || '') > (dev.lastSeen || '')) continue
        deviceMap.set(key, { ...dev, isOnline: false })
      }
    } catch (err) {
      console.warn('[MeshEngine] listDevices (persisted) failed:', err.message)
    }

    for (const [, peerObj] of this.peers.entries()) {
      const dev = peerObj.device
      if (!dev || !dev.id || dev.name === 'Connecting...' || isSelf(dev)) continue
      const key = canonicalKey(dev)
      if (!key) continue
      const existing = deviceMap.get(key)
      const name = (existing && existing.name) ? existing.name : dev.name
      deviceMap.set(key, { ...dev, ...existing, name, isOnline: true })
    }

    return Array.from(deviceMap.values())
  }

  /** Rename a device in persistent store and live peer registry. */
  async renameDevice(id, name) {
    if (!id || typeof id !== 'string') throw new Error('Invalid device id')
    const cleanName = String(name || '').trim()
    if (!cleanName) throw new Error('Name cannot be empty')

    const bee = await this.getBee('devices')
    const entry = await bee.get(id)
    if (!entry || !entry.value) throw new Error('Device not found')

    const updated = { ...entry.value, name: cleanName }
    await bee.put(id, updated)

    // Update in-memory peerObj if peer is currently connected
    for (const [, peerObj] of this.peers.entries()) {
      if (peerObj.device && (peerObj.device.id === id || peerObj.device.publicKey === updated.publicKey)) {
        peerObj.device.name = cleanName
      }
    }

    this.emit(EVENTS.DEVICE_UPDATED, updated)
    return updated
  }

  /** Delete a paired device from persistent store, revoke its trust key, and disconnect. */
  async removeDevice(id) {
    if (!id || typeof id !== 'string') throw new Error('Invalid device id')
    const bee = await this.getBee('devices')
    const entry = await bee.get(id)
    const device = entry && entry.value
    await bee.del(id)

    if (device && device.publicKey) {
      this.trustManager.removeTrustedKey(device.publicKey)
      await this.trustManager.revokeKey(device.publicKey)
    }

    try {
      await this.trustManager.rotateHostPairingCode()
    } catch (err) {
      console.warn('[MeshEngine] Failed to rotate pairing code after device deletion:', err.message)
    }

    if (device) {
      if (device.publicKey) {
        this.topicRegistry.leave(`p2p-node-${device.publicKey}`)
      }
      for (const [pId, peerObj] of this.peers.entries()) {
        if (
          peerObj.device &&
          (peerObj.device.id === id || peerObj.device.publicKey === device.publicKey || pId === device.publicKey)
        ) {
          // Tell the deleted device it was removed BEFORE destroying the
          // connection so its UI can react immediately and its local trust in
          // us is revoked. Fire-and-forget: the message may not flush if the
          // transport dies first, in which case the revoked-key set on its
          // next reconnect still refuses auto-trust.
          try {
            if (peerObj.signaling) {
              peerObj.signaling.send({
                type: MESSAGES.DEVICE_REMOVED,
                deviceId: id
              })
            }
          } catch {}
          try {
            peerObj.connection.destroy()
          } catch {}
        }
      }
    }

    this.emit(EVENTS.PEER_DISCONNECTED, { id, publicKey: device?.publicKey })
    return { success: true, id }
  }

  /**
   * Persisted settings (merged with live engine values). Used by hosts to
   * restore engine toggles across restarts.
   */
  async getSettings() {
    try {
      const bee = await this.getBee('settings')
      const entry = await bee.get('settings')
      return {
        autoAcceptOffers: this.autoAcceptOffers,
        autoTrustLAN: this.autoTrustLAN,
        preferOwnRelay: this.preferOwnRelay,
        relayMode: this.relayMode,
        customRelayUrl: this.customRelayUrl,
        ...(entry?.value || {})
      }
    } catch {
      return {
        autoAcceptOffers: this.autoAcceptOffers,
        autoTrustLAN: this.autoTrustLAN,
        preferOwnRelay: this.preferOwnRelay,
        relayMode: this.relayMode,
        customRelayUrl: this.customRelayUrl
      }
    }
  }

  /** Toggle auto-accept of incoming transfer offers (persisted). */
  async setAutoAcceptOffers(value) {
    this.autoAcceptOffers = value !== false
    try {
      const bee = await this.getBee('settings')
      const entry = await bee.get('settings')
      await bee.put('settings', {
        ...(entry?.value || {}),
        autoAcceptOffers: this.autoAcceptOffers
      })
    } catch (err) {
      console.warn('[MeshEngine] setAutoAcceptOffers persist failed:', err.message)
    }
    return this.autoAcceptOffers
  }

  /** Toggle auto-trust of LAN-discovered peers (persisted). */
  async setAutoTrustLAN(value) {
    this.autoTrustLAN = value !== false
    try {
      const bee = await this.getBee('settings')
      const entry = await bee.get('settings')
      await bee.put('settings', { ...(entry?.value || {}), autoTrustLAN: this.autoTrustLAN })
    } catch (err) {
      console.warn('[MeshEngine] setAutoTrustLAN persist failed:', err.message)
    }
    return this.autoTrustLAN
  }

  /**
   * Toggle "prefer my own desktop as relay" (persisted). When on, this node
   * prefers an online paired desktop over the public bootstrap nodes for
   * connections that need a relay. The relay is private to the mesh: only
   * paired, trusted devices hold the desktop's key, so it can never be used
   * by other MeshDrop nodes.
   */
  async setPreferOwnRelay(value) {
    this.preferOwnRelay = value !== false
    try {
      const bee = await this.getBee('settings')
      const entry = await bee.get('settings')
      await bee.put('settings', { ...(entry?.value || {}), preferOwnRelay: this.preferOwnRelay })
    } catch (err) {
      console.warn('[MeshEngine] setPreferOwnRelay persist failed:', err.message)
    }
    return this.preferOwnRelay
  }

  /**
   * Set relay transport strategy: 'auto' | 'relay-primary' | 'direct-only' (persisted).
   */
  async setRelayMode(mode) {
    if (!['auto', 'relay-primary', 'direct-only'].includes(mode)) {
      mode = 'auto'
    }
    this.relayMode = mode
    if (this.relayClient) {
      this.relayClient.setMode(mode)
    }
    try {
      const bee = await this.getBee('settings')
      const entry = await bee.get('settings')
      await bee.put('settings', { ...(entry?.value || {}), relayMode: this.relayMode })
    } catch (err) {
      console.warn('[MeshEngine] setRelayMode persist failed:', err.message)
    }
    return this.relayMode
  }

  /**
   * Set custom Cloudflare / WSS relay worker URL (persisted).
   */
  async setCustomRelayUrl(url) {
    this.customRelayUrl = typeof url === 'string' ? url.trim() : ''
    if (this.relayClient) {
      this.relayClient.setRelayUrl(this.customRelayUrl)
    }
    try {
      const bee = await this.getBee('settings')
      const entry = await bee.get('settings')
      await bee.put('settings', { ...(entry?.value || {}), customRelayUrl: this.customRelayUrl })
    } catch (err) {
      console.warn('[MeshEngine] setCustomRelayUrl persist failed:', err.message)
    }
    return this.customRelayUrl
  }

  getStatus() {
    return this.connections.getConnectionStatus()
  }

  /** Live diagnostics snapshot (honest: null means "not measured"). */
  getDiagnostics() {
    if (!this.metricsCollector || !this.connections) {
      return {
        natType: null,
        relayStatus: 'Disabled',
        dhtNodes: null,
        avgLatencyMs: null,
        packetLossPercent: null,
        noiseProtocol: 'Noise_XX_25519_ChaChaPoly_BLAKE2b',
        bandwidthMbps: null,
        systemCpuUsage: null,
        systemRamUsage: null,
        connectedPeersCount: 0,
        connected: false,
        uptimeMs: 0,
        bytesReceived: 0,
        bytesSent: 0
      }
    }
    const count = this.connections.authenticatedPeerCount()
    // connected = mesh reachable (DHT up OR a trusted peer attached) — see
    // connections/getConnectionStatus(). Do NOT recompute from peer count alone
    // here: that made a healthy, zero-peer app report "Mesh Connecting…".
    const { connected } = this.connections.getConnectionStatus()
    return this.metricsCollector.snapshot({
      peerCount: count,
      connected,
      relayStatus: 'Enabled',
      avgLatencyMs: this.connections.getPeerLatency(),
      packetLossPercent: this.connections.getPacketLoss()
    })
  }

  // ─── Internals ────────────────────────────────────────────────────────────

  async getDownloadDirectory() {
    try {
      await fsp.mkdir(this.downloadsDir, { recursive: true })
    } catch {}
    return this.downloadsDir
  }

  async getAutoTrustLAN() {
    return this.autoTrustLAN
  }

  async cleanupPendingShare(id, newStatus = 'cancelled') {
    try {
      const bee = await this.storage.getBee('pendingShares')
      const entry = await bee.get(id)
      if (!entry) return null
      const share = { ...entry.value, status: newStatus }
      await bee.put(id, share)
      // A terminal share stops advertising: leave its code topic so no future
      // claimer can connect to a dead drop.
      if (share.code) {
        try {
          this.topicRegistry.leave(`p2p-file-${share.code}`)
        } catch {}
      }
      return share
    } catch (err) {
      console.warn('[MeshEngine] cleanupPendingShare failed:', err.message)
      return null
    }
  }

  // Hydrate unexpired active drop shares on boot and re-join their DHT topics
  async restorePendingShares() {
    try {
      const bee = await this.storage.getBee('pendingShares')
      const now = Date.now()
      let restoredCount = 0
      for await (const node of bee.createReadStream()) {
        const share = node.value
        if (share && share.isHost === true && share.code) {
          const isTerminal = share.status === 'expired' || share.status === 'cancelled'
          const isExpired = share.expiresAt > 0 && now >= share.expiresAt
          if (!isTerminal && !isExpired) {
            this.topicRegistry.join(`p2p-file-${share.code}`, { client: true, server: true })
            restoredCount++
          }
        }
      }
      if (restoredCount > 0) {
        console.log(`[MeshEngine] Restored ${restoredCount} active drop share topic(s) on boot`)
      }
    } catch (err) {
      console.warn('[MeshEngine] Failed to restore pending drop shares:', err.message)
    }
  }

  // ─── One-time DROP shares (WeTransfer-style; no pairing required) ───────

  // Recursively enumerate a folder's files (symlinks skipped). Relative paths
  // use '/' separators so display names are portable across OSes; the receive
  // side flattens them to safe filenames.
  async _enumerateFolder(folderPath, { maxFiles = 100 } = {}) {
    const out = []
    const walk = async (dir, rel) => {
      if (out.length >= maxFiles) return
      let entries = []
      try {
        entries = await fsp.readdir(dir, { withFileTypes: true })
      } catch {
        return
      }
      for (const e of entries) {
        if (out.length >= maxFiles) return
        const full = path.join(dir, e.name)
        const relPath = rel ? `${rel}/${e.name}` : e.name
        if (e.isDirectory()) await walk(full, relPath)
        else if (e.isFile()) {
          try {
            const stat = await fsp.stat(full)
            out.push({ filePath: full, filename: relPath, fileSize: stat.size })
          } catch {}
        }
      }
    }
    await walk(folderPath, '')
    return out
  }

  // Stage a file into a drop core, persist the pending-share record, and
  // advertise the code topic so claimers can find us. Accepts either a single
  // file ({ filePath, filename, fileSize }), a multi-file list ({ files: [...] }),
  // or a folder ({ folderPath }) — folders are enumerated recursively. One
  // drop core per file (multi-file shares serve every core on claim). Returns
  // the share record (files[] carries per-file keys + integrity data).
  async createDropShare({
    filePath,
    filename,
    fileSize,
    fileType,
    files,
    folderPath,
    expirationPreset = '30m',
    maxDownloads = 0 // 0 = unlimited downloads until expiry
  }) {
    if (!this.transferEngine || !this.topicRegistry) throw new Error('Engine not ready')

    // ── Normalize the input into a file list ──────────────────────────────
    let list = []
    if (Array.isArray(files) && files.length > 0) {
      list = files.map((f) => ({
        filePath: f.filePath,
        filename: f.filename || path.basename(f.filePath || 'file'),
        fileSize: f.fileSize
      }))
    } else if (folderPath) {
      list = await this._enumerateFolder(folderPath)
      if (list.length === 0) throw new Error('No shareable files found in that folder')
    } else if (filePath) {
      list = [{ filePath, filename: filename || path.basename(filePath), fileSize }]
    }
    if (list.length === 0) throw new Error('No files to share')
    if (list.length > 100) list = list.slice(0, 100)

    for (const f of list) {
      if (typeof f.fileSize !== 'number' || f.fileSize <= 0) {
        try {
          f.fileSize = (await fsp.stat(f.filePath)).size
        } catch {
          throw new Error(`Cannot stat file: ${f.filename}`)
        }
      }
    }
    // Empty files (0 bytes) are skipped: the claim receiver rejects them.
    const before = list.length
    list = list.filter((f) => f.fileSize > 0)
    if (list.length === 0) throw new Error('No shareable files (all files are empty)')
    if (list.length < before) {
      console.warn(`[MeshEngine] Skipped ${before - list.length} empty file(s) in drop share`)
    }

    const code = generateDropCode()
    const shareId = `drop-${Date.now().toString(36)}`
    const folderName = folderPath ? path.basename(folderPath) : null

    // ── Stage every file into its own drop core ───────────────────────────
    const staged = []
    const usedNames = new Set()
    for (let i = 0; i < list.length; i++) {
      const f = list[i]
      // Flatten relative paths to a single safe segment + dedupe collisions.
      let flat = String(f.filename || 'file')
        .replace(/[^a-zA-Z0-9._ -]/g, '_')
        .replace(/\s+/g, ' ')
        .trim() || 'file'
      let candidate = flat
      let n = 1
      while (usedNames.has(candidate.toLowerCase())) candidate = `${flat}-${++n}`
      usedNames.add(candidate.toLowerCase())

      // Stage a copy in engine temp storage ONLY for smaller files (<= 50MB) so the
      // share survives the source file being moved/deleted without wasting tens
      // of GBs of disk space for large files.
      let finalPath = f.filePath
      if (f.fileSize <= 50 * 1024 * 1024) {
        const tempDir = path.join(this.storageDir, 'p2p-temp', shareId)
        await fsp.mkdir(tempDir, { recursive: true }).catch(() => {})
        const stagedPath = path.join(tempDir, candidate)
        await fsp.copyFile(f.filePath, stagedPath).catch((err) => {
          console.warn(`[MeshEngine] Drop staging failed for ${f.filename}, using original path:`, err.message)
        })
        if (await fsp.stat(stagedPath).catch(() => null)) finalPath = stagedPath
      }

      const coreName = list.length === 1 ? `file-drop-${shareId}` : `file-drop-${shareId}-${i}`
      const s = await this.transferEngine.stageDrop({
        transferId: shareId,
        coreName,
        filePath: finalPath,
        filename: f.filename,
        fileSize: f.fileSize,
        fileType: fileType || this.transferEngine.getFileType(f.filename)
      })
      staged.push({
        filename: f.filename,
        fileSize: f.fileSize,
        fileType: fileType || this.transferEngine.getFileType(f.filename),
        filePath: finalPath,
        originalPath: f.filePath,
        coreName,
        coreKey: s.coreKey,
        manifestHash: s.manifestHash,
        checksum: s.checksum,
        blockSize: s.blockSize,
        blockCount: s.blockCount
      })
    }

    this.topicRegistry.join(`p2p-file-${code}`, { client: true, server: true })

    const totalSize = staged.reduce((a, f) => a + f.fileSize, 0)
    const duration = getDurationMs(expirationPreset)
    const createdAt = Date.now()
    const expiresAt = duration > 0 ? createdAt + duration : 0

    const pendingShare = {
      id: shareId,
      code,
      filename: folderName || staged[0].filename,
      fileSize: totalSize,
      fileType: staged[0].fileType,
      filePath: staged[0].filePath,
      originalPath: staged[0].originalPath,
      coreKey: staged[0].coreKey,
      manifestHash: staged[0].manifestHash,
      checksum: staged[0].checksum,
      blockSize: staged[0].blockSize,
      blockCount: staged[0].blockCount,
      folderName,
      files: staged,
      createdAt,
      expiresAt,
      expirationPreset,
      maxDownloads: typeof maxDownloads === 'number' && maxDownloads > 0 ? maxDownloads : 0,
      downloadCount: 0,
      status: 'waiting',
      isHost: true
    }

    const bee = await this.storage.getBee('pendingShares')
    await bee.put(shareId, pendingShare)
    console.log(
      `[MeshEngine] Drop share created: ${code} (${staged.length} file(s), ${totalSize} bytes, expires ${expirationPreset})`
    )
    return pendingShare
  }

  // Claim a DROP code: join the code's topic and request the share from any
  // host already advertising there. The download starts when the host answers
  // CLAIM_FILE_RES (which opens per-core replication, see connections.js).
  // MD- pairing codes are app-level and rejected here.
  async claimDropCode(rawCode, options = null) {
    if (!this.started || !this.topicRegistry) throw new Error('Engine not ready')
    const code = normalizeDropCode(rawCode)
    if (!code) throw new Error('Invalid DROP code — expected DROP-XXXX-XXXX')

    console.log(`[MeshEngine] Claiming one-time file code: ${code}`)
    this.activeClaims.add(code)
    if (options) {
      this.activeClaimOptions.set(code, options)
    }
    this.topicRegistry.join(`p2p-file-${code}`, { client: true, server: true })

    // Send the claim request to any peers already connected on the topic.
    for (const [, peerObj] of this.peers.entries()) {
      if (peerObj.signaling) {
        peerObj.signaling.send({ type: MESSAGES.CLAIM_FILE_REQ, code })
      }
    }
    // Honest pending state: until the host answers with an offer, the user
    // sees a "waiting for sender" row instead of an empty transfer list. It is
    // cleared the moment the claim resolves (see connections/claims.js).
    if (this.transferEngine) {
      await this.transferEngine.createWaitingClaim({ code }).catch((err) => {
        console.warn('[MeshEngine] createWaitingClaim failed:', err.message)
      })
    }
    return { success: true, code }
  }

  async confirmClaimDownload(params) {
    if (!this.connections || !this.connections.confirmClaimDownload) {
      throw new Error('Connections not ready')
    }
    return this.connections.confirmClaimDownload(params)
  }

  async cancelClaimDownload(params) {
    if (!this.connections || !this.connections.cancelClaimDownload) {
      throw new Error('Connections not ready')
    }
    return this.connections.cancelClaimDownload(params)
  }

  // Host-side listing: active one-time sends on THIS device (expiring stale
  // records as a side effect, like the old handler did).
  async listPendingShares() {
    const bee = await this.storage.getBee('pendingShares')
    const results = []
    const now = Date.now()
    for await (const node of bee.createReadStream()) {
      const share = node.value
      if (share && share.isHost === true) {
        if (share.status === 'waiting' && share.expiresAt > 0 && now >= share.expiresAt) {
          await this.cleanupPendingShare(share.id, 'expired')
          share.status = 'expired'
        }
        results.push(share)
      }
    }
    return results.sort((a, b) => b.createdAt - a.createdAt)
  }

  async extendPendingShare({ id, addMinutes = 30 }) {
    const bee = await this.storage.getBee('pendingShares')
    const entry = await bee.get(id)
    if (!entry) throw new Error('Pending share not found')
    const share = entry.value
    if (share.isHost !== true) {
      throw new Error('Permission denied: only the share host can extend expiration')
    }
    const now = Date.now()
    const baseTime = share.expiresAt > 0 && share.expiresAt > now ? share.expiresAt : now
    share.expiresAt = baseTime + addMinutes * 60 * 1000
    share.status = 'waiting'
    await bee.put(id, share)
    this.topicRegistry.ensure(`p2p-file-${share.code}`, { client: true, server: true })
    return share
  }

  async cancelPendingShare({ id }) {
    const share = await this.cleanupPendingShare(id, 'cancelled')
    if (!share) throw new Error('Pending share not found')
    return share
  }

  async deletePendingShare({ id }) {
    const share = await this.cleanupPendingShare(id, 'cancelled')
    const bee = await this.storage.getBee('pendingShares')
    await bee.del(id)
    return { deleted: id, share }
  }

  async checkPendingExpirations() {
    // Expire old pairing secrets so stale codes cannot be used indefinitely
    if (this.trustManager) this.trustManager.expireSecrets()
    try {
      const bee = await this.storage.getBee('pendingShares')
      const now = Date.now()
      for await (const node of bee.createReadStream()) {
        const share = node.value
        if (!share || share.status === 'expired' || share.status === 'cancelled') continue
        if (share.expiresAt > 0 && now >= share.expiresAt) {
          console.log(`[MeshEngine] Pending share ${share.code} (${share.id}) EXPIRED`)
          await this.cleanupPendingShare(share.id, 'expired')
        }
      }
    } catch (err) {
      console.warn('[MeshEngine] Expiration check failed:', err.message)
    }
  }
}

module.exports = { MeshEngine, EVENTS, pairingTopic, getDurationMs }
