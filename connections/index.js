'use strict'

// Connections layer — composition root.
//
// createConnections(engine) builds one shared context ({ engine, peers, swarm,
// activeClaims, stopped, refs }) and instantiates the focused sub-modules:
//   signaling.js  — the p2p-signal-v1 protomux channel + inbound router
//   keepalive.js  — PING/PONG latency probe + packet-loss proxy
//   claims.js     — one-time DROP claim flow (per-core replication)
//   devices.js    — handshake application, device persistence, LAN auto-trust
// Cross-module calls go through ctx.refs, populated once every module exists,
// so no module needs to know about the others' internals.
//
// This module keeps the connection lifecycle (onConnection), the swarm boot
// (initSwarm), exchange-store replication, and the public status surface.

const b4a = require('b4a')
const { EVENTS } = require('../protocol.js')
const { isRelayedConnection, getTransferMethod } = require('./util.js')
const { createSignaling } = require('./signaling.js')
const { createKeepAlive, PING_INTERVAL_MS } = require('./keepalive.js')
const { createClaims } = require('./claims.js')
const { createDeviceRegistry } = require('./devices.js')

function createConnections(engine) {
  const ctx = {
    engine,
    peers: engine.peers,
    swarm: engine.swarm,
    activeClaims: engine.activeClaims,
    // Set by teardown() (engine.stop): the unref'd maintenance intervals below
    // must stop touching the corestore once the engine is stopped, otherwise
    // they hammer a closed store with SESSION_CLOSED errors forever.
    stopped: false,
    // The public P2P network (DHT) is reachable once bootstrapped. "Online"
    // means THIS — independent of whether any paired peer is attached. Peer
    // attachment is reported separately via peerCount.
    dhtReady: false,
    refs: {}
  }

  const signaling = createSignaling(ctx)
  const keepalive = createKeepAlive(ctx)
  const claims = createClaims(ctx)
  const devices = createDeviceRegistry(ctx)

  // Open exchange-store replication for a peer on its connection. Only called
  // after the peer has been authenticated (trusted pairing, verified handshake,
  // or a valid one-time-share claim). The private metadata store is never exposed.
  function replicateExchange(peerId) {
    const peerObj = ctx.peers.get(peerId)
    if (!peerObj || peerObj.replStream) return
    try {
      peerObj.replStream = engine.replicationScope.replicate(peerId, peerObj.connection)
      if (!peerObj.replStream) return
      console.log(`[MeshEngine] Exchange replication opened with ${peerId.slice(0, 12)}...`)
    } catch (err) {
      console.warn(
        `[MeshEngine] Failed to replicate exchange store with ${peerId.slice(0, 12)}...:`,
        err.message
      )
    }
  }

  // Wire cross-module references AFTER every factory has run so modules can
  // call each other without import cycles.
  ctx.refs = {
    ...signaling,
    ...keepalive,
    ...claims,
    ...devices,
    replicateExchange
  }

  // Count only peers whose handshake completed: pairing.complete is set
  // exclusively by the verified challenge-response path.
  function authenticatedPeerCount() {
    let n = 0
    for (const p of ctx.peers.values()) {
      if (p.pairing && p.pairing.complete && p.device && p.device.isOnline) n++
    }
    return n
  }

  function getConnectionStatus() {
    let relayedPeerCount = 0
    for (const p of ctx.peers.values()) {
      if (p.pairing && p.pairing.complete && p.device && p.device.isOnline && p.device.relayed) {
        relayedPeerCount++
      }
    }
    const authenticated = authenticatedPeerCount()
    return {
      // Online = the mesh is reachable (DHT bootstrapped) OR a trusted peer is
      // attached. A freshly booted app with zero peers is still online — it can
      // receive claim connections and be found by paired devices.
      connected: ctx.dhtReady === true || authenticated > 0,
      peerCount: authenticated,
      relayedPeerCount,
      directPeerCount: authenticated - relayedPeerCount
    }
  }

  async function onConnection(connection, peerInfo) {
    engine.connectionCount++
    const peerId = (peerInfo?.publicKey ? b4a.toString(peerInfo.publicKey, 'hex') : null) || `peer-${engine.connectionCount}`

    // Register cleanup handlers first so a connection that closes while the
    // (async) settings read below is in flight can never leak from the map.
    connection.on('close', () => {
      engine.connectionCount--
      const peerObj = ctx.peers.get(peerId)
      const devId = peerObj?.device?.id || peerId
      // Per-core drop replication lives on the peer object, not the
      // ReplicationScope (claims are never store-trusted) — close it too, and
      // release any claim-bundle completion listeners.
      if (peerObj) {
        if (peerObj.dropStreams) {
          for (const s of peerObj.dropStreams) {
            try {
              s.destroy()
            } catch {}
          }
        }
        if (peerObj.claimCleanups) {
          for (const fn of peerObj.claimCleanups) {
            try {
              fn()
            } catch {}
          }
        }
      }
      ctx.peers.delete(peerId)
      engine.replicationScope.close(peerId)
      engine.emit(EVENTS.PEER_DISCONNECTED, { id: devId, peerId })
    })
    connection.on('error', () => {})

    // A deleted (revoked) device must not re-admit itself through the
    // auto-trust paths: its key is refused for direct trust and LAN auto-trust
    // below. The connection itself is NOT destroyed — it falls through to the
    // pairing challenge, and only a fresh pairing with the CURRENT code (the
    // old one was rotated away on deletion) can re-admit it.
    const revokedPeer = engine.trustManager.isRevoked(peerId)
    if (revokedPeer) {
      console.log(
        `[MeshEngine] Deleted peer ${peerId.slice(0, 12)}... reconnecting — refusing auto-trust (re-pairing required)`
      )
    }

    const remoteIp =
      peerInfo?.host ||
      connection.remoteAddress ||
      connection.rawStream?.remoteAddress ||
      connection._socket?.remoteAddress ||
      ''
    const transferMethod = getTransferMethod(remoteIp)
    const relayed = isRelayedConnection(peerInfo, connection, ctx.swarm && ctx.swarm.dht)

    // Trust is earned: a previously verified trusted key always wins. Otherwise
    // honor the autoTrustLAN preference: peers discovered on the local network
    // (explicit LAN-discovery signal, or a private-range remote address) are
    // trusted immediately without any challenge-response handshake.
    const lanDiscovered =
      engine.lanDiscovery &&
      typeof engine.lanDiscovery.has === 'function' &&
      engine.lanDiscovery.has(peerId)
    let directTrusted = engine.trustManager.isTrustedPublicKey(peerId)
    if (
      !revokedPeer &&
      !directTrusted &&
      (await engine.getAutoTrustLAN()) &&
      (transferMethod === 'lan' || lanDiscovered)
    ) {
      directTrusted = true
      console.log(
        `[MeshEngine] Auto-trusting LAN peer ${peerId.slice(0, 12)}... (autoTrustLAN enabled)`
      )
    }

    const signalingApi = signaling.setupPeerSignaling(connection, peerId, { directTrusted })
    // NOTE: the private metadata store is NEVER replicated here. The exchange
    // store (file cores only) is replicated once the peer is authenticated.

    const peer = {
      id: peerId,
      publicKey: peerId,
      name: 'Connecting...',
      os: 'Unknown',
      osVersion: '',
      avatar: '',
      isTrusted: directTrusted,
      isEncrypted: true,
      isOnline: true,
      lastSeen: new Date().toISOString(),
      ipAddress: remoteIp,
      transferMethod,
      relayed
    }

    // Trust is earned: only a known trusted noise public key (direct), a
    // successful pairing challenge (pairing), or the autoTrustLAN preference
    // for LAN peers ever sets isTrusted = true.
    // `timeout` is owned by TrustManager's watchdog: it is armed only when a
    // PAIRING_CHALLENGE is sent/received and cleared on verification — it
    // must NOT start at connection open, otherwise a slow code typist loses
    // the race to the timer.
    const pairing = {
      mode: directTrusted ? 'direct' : 'pairing',
      trusted: directTrusted,
      complete: false,
      outstanding: [], // { nonce, code, codeId }
      pendingChallenges: [], // { codeId, nonce } received but not yet answerable
      pendingHandshake: null, // HANDSHAKE received before our challenge verified
      timeout: null,
      code: null
    }

    ctx.peers.set(peerId, { connection, device: peer, signaling: signalingApi, transferMethod, pairing })

    if (directTrusted) {
      replicateExchange(peerId)
    }
  }

  async function initSwarm() {
    ctx.stopped = false
    // refreshNetwork() may have replaced engine.swarm — always rebind so the
    // connection handler and every module ref read the live swarm.
    ctx.swarm = engine.swarm
    ctx.dhtReady = false
    ctx.swarm.on('connection', onConnection)

    try {
      await ctx.swarm.dht.ready()
      ctx.dhtReady = true
    } catch (err) {
      console.error('DHT ready failed:', err.message)
    }

    if (!engine.deviceIdentity) await engine.initIdentity()
    if (engine.deviceIdentity && engine.deviceIdentity.publicKey) {
      console.log(
        `[MeshEngine] Listening on self identity DHT topic for VPN/relay discovery: ${engine.deviceIdentity.publicKey.slice(0, 12)}...`
      )
      engine.topicRegistry.ensure(`p2p-peer-${engine.deviceIdentity.publicKey}`, {
        client: true,
        server: true
      })
      // Re-join every other active topic (paired peers, pairing codes, drop
      // shares) on the possibly-new swarm — after a network-change rebuild
      // this re-announces them on the fresh DHT node. Harmless no-op for
      // labels already joined on the same swarm.
      engine.topicRegistry.reattach(ctx.swarm)

      await ctx.swarm.listen()
      if (engine.lanDiscovery) {
        // A network change leaves the old multicast socket bound to the dead
        // interface — stop it and rebind on the current one.
        engine.lanDiscovery.stop()
        engine.lanDiscovery.start()
      }
      ctx.swarm.flush().catch(() => {})
    }

    // Automatically reconnect to all stored paired peers on startup and interval
    await devices.reconnectKnownPeers()
    startIntervals()
  }

  // Maintenance intervals are started once per engine lifetime; initSwarm is
  // re-entered by refreshNetwork() and must not stack duplicate timers.
  function startIntervals() {
    if (ctx.intervalsStarted) return
    ctx.intervalsStarted = true
    setInterval(devices.reconnectKnownPeers, 60000).unref()
    // PING/PONG latency probe
    setInterval(keepalive.sendPings, PING_INTERVAL_MS).unref()
  }

  return {
    refs: ctx.refs,
    setupPeerSignaling: signaling.setupPeerSignaling,
    sendHandshake: signaling.sendHandshake,
    replicateExchange,
    sendPairingChallenges: signaling.sendPairingChallenges,
    handlePeerMessage: signaling.handlePeerMessage,
    onConnection,
    initSwarm,
    reconnectKnownPeers: devices.reconnectKnownPeers,
    authenticatedPeerCount,
    getConnectionStatus,
    getPeerLatency: keepalive.getPeerLatency,
    getPacketLoss: keepalive.getPacketLoss,
    flushPendingHandshake: devices.flushPendingHandshake,
    rebroadcastPeerCompletion: devices.rebroadcastPeerCompletion,
    maybeAutoTrustLanPeer: devices.maybeAutoTrustLanPeer,
    // Flip the maintenance intervals off; called from engine.stop().
    teardown: () => {
      ctx.stopped = true
    }
  }
}

module.exports = { createConnections, getTransferMethod }
