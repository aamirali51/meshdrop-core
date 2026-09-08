"use strict";

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

const b4a = require("b4a");
const { EVENTS } = require("../protocol.js");
const { isRelayedConnection, getTransferMethod } = require("./util.js");
const { createSignaling } = require("./signaling.js");
const { createKeepAlive, PING_INTERVAL_MS } = require("./keepalive.js");
const { createClaims } = require("./claims.js");
const { createDeviceRegistry } = require("./devices.js");

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
    refs: {},
  };

  const signaling = createSignaling(ctx);
  const keepalive = createKeepAlive(ctx);
  const claims = createClaims(ctx);
  const devices = createDeviceRegistry(ctx);

  // Per-connection topic attribution: map the connection's Hyperswarm topics
  // (raw 32-byte topic hashes on peerInfo) back to the labels we joined them
  // under, via the registry's topicHex -> label map, and record the labels on
  // engine.peerTopics for this peer. Hyperswarm only populates peerInfo.topics
  // on the side whose own lookups discovered the peer, and it appends to the
  // SAME peerInfo object in place as lookups land — an existing connection
  // gains a new topic whenever either side joins a shared one. So a one-shot
  // read at connection open would snapshot labels too early (a second code
  // joined minutes later would never be attributed); a lightweight sweep
  // re-reads every live peer's topics for the life of the connection. Labels
  // are deleted on connection close so a stale entry can never suppress
  // pairing forever.
  function attributePeerTopics(peerId, peerInfo) {
    if (!engine.peerTopics || !peerInfo || !peerInfo.topics || !peerInfo.topics.length) return
    if (!ctx.peers.has(peerId)) return // connection already closed — never resurrect labels
    const registry = engine.topicRegistry
    if (!registry || typeof registry.labelFor !== "function") return
    let set = engine.peerTopics.get(peerId)
    for (const topic of peerInfo.topics) {
      const label = registry.labelFor(topic) // hex-encodes Buffers internally — keys stay hex
      if (!label) continue
      if (!set) {
        set = new Set()
        engine.peerTopics.set(peerId, set)
      }
      set.add(label)
    }
  }
  const peerInfos = new Map(); // peerId -> live peerInfo object (topic source of truth)
  let attributionTimer = null;
  function sweepAttribution() {
    if (ctx.stopped) {
      if (attributionTimer) {
        clearInterval(attributionTimer)
        attributionTimer = null
      }
      return
    }
    for (const [peerId, info] of peerInfos.entries()) {
      if (!ctx.peers.has(peerId)) {
        peerInfos.delete(peerId)
        continue
      }
      attributePeerTopics(peerId, info)
    }
  }
  function ensureAttributionSweep() {
    if (attributionTimer) return
    attributionTimer = setInterval(sweepAttribution, 3000)
    if (attributionTimer.unref) attributionTimer.unref()
  }
  // Hooked on the engine so topic-driven code (the TunnelManager CLAIM drive)
  // can pull the freshest labels synchronously instead of waiting for a sweep tick.
  engine.refreshPeerTopicAttribution = sweepAttribution;

  // Open exchange-store replication for a peer on its connection. Only called
  // after the peer has been authenticated (trusted pairing, verified handshake,
  // or a valid one-time-share claim). The private metadata store is never exposed.
  function replicateExchange(peerId) {
    const peerObj = ctx.peers.get(peerId);
    if (!peerObj || peerObj.replStream) return;
    try {
      peerObj.replStream = engine.replicationScope.replicate(
        peerId,
        peerObj.connection,
      );
      if (!peerObj.replStream) return;
      console.log(
        `[MeshEngine] Exchange replication opened with ${peerId.slice(0, 12)}...`,
      );
    } catch (err) {
      console.warn(
        `[MeshEngine] Failed to replicate exchange store with ${peerId.slice(0, 12)}...:`,
        err.message,
      );
    }
  }

  // Wire cross-module references AFTER every factory has run so modules can
  // call each other without import cycles.
  ctx.refs = {
    ...signaling,
    ...keepalive,
    ...claims,
    ...devices,
    replicateExchange,
  };

  // Reconnect ONE peer that is currently on relay/internet onto a direct LAN
  // path. Called when LAN discovery hears a peer that is already connected:
  // destroying the connection triggers the normal cleanup (peers map,
  // replication scope, claim streams, transfer park) and then we re-join the
  // peer key so hyperswarm's direct-first punch lands on the LAN route.
  //
  // Guards: only switch peers that are actually relayed/internet (a direct LAN
  // peer needs nothing), never while a switch is already in flight, and never
  // more often than the cooldown so a failed direct attempt doesn't churn.
  const LAN_SWITCH_COOLDOWN_MS = 60 * 1000
  const _lanSwitchState = new Map() // peerId -> { lastAttemptAt, inflight }

  async function switchPeerToDirect(peerId) {
    const peerObj = ctx.peers.get(peerId)
    if (!peerObj || !peerObj.connection || !peerObj.signaling) return
    // Already on a direct LAN path — nothing to do.
    const isRelayed = !!(peerObj.device && peerObj.device.relayed) || !!peerObj.relayed
    if (!isRelayed && peerObj.transferMethod === 'lan') return
    const st = _lanSwitchState.get(peerId)
    if (st && st.inflight) return
    if (st && Date.now() - st.lastAttemptAt < LAN_SWITCH_COOLDOWN_MS) return
    if (!engine.autoLanSwitch) return

    _lanSwitchState.set(peerId, { lastAttemptAt: Date.now(), inflight: true })
    console.log(
      `[MeshEngine] LAN switch: ${peerId.slice(0, 12)}... heard on LAN (currently ${peerObj.transferMethod || 'internet'}${isRelayed ? ', relayed' : ''}) — reconnecting direct`
    )
    try {
      // Destroy this one connection. The on('close') handler removes the peer
      // from the map and emits PEER_DISCONNECTED; in-flight transfers park as
      // resumable and re-queue when the peer reconnects.
      peerObj.connection.destroy(new Error('switching to LAN'))
    } catch (err) {
      console.warn(`[MeshEngine] LAN switch destroy failed for ${peerId.slice(0, 12)}...:`, err.message)
    }
    try {
      // Re-join the peer key so hyperswarm re-establishes direct (LAN discovery
      // primed the direct path). The identity-topic rejoin lets it announce.
      // Fix 4: exclude this peerId itself from own-relay candidate.
      const peerKey = Buffer.from(peerId, 'hex')
      if (peerKey.length === 32) {
        const prevTarget = engine._pendingDialTarget
        engine._pendingDialTarget = peerId
        try {
          ctx.swarm.joinPeer(peerKey)
        } finally {
          engine._pendingDialTarget = prevTarget
        }
      }
      await devices.reconnectKnownPeers()
      ctx.swarm.flush().catch(() => {})
    } catch (err) {
      console.warn(`[MeshEngine] LAN switch rejoin failed for ${peerId.slice(0, 12)}...:`, err.message)
    } finally {
      const s = _lanSwitchState.get(peerId)
      if (s) s.inflight = false
    }
  }

  // Count only peers whose handshake completed: pairing.complete is set
  // exclusively by the verified challenge-response path.
  function authenticatedPeerCount() {
    let n = 0;
    for (const p of ctx.peers.values()) {
      if (p.pairing && p.pairing.complete && p.device && p.device.isOnline) n++;
    }
    return n;
  }

  function getConnectionStatus() {
    let relayedPeerCount = 0;
    let relayedViaOwnPeerCount = 0;
    for (const p of ctx.peers.values()) {
      if (
        p.pairing &&
        p.pairing.complete &&
        p.device &&
        p.device.isOnline &&
        p.device.relayed
      ) {
        relayedPeerCount++;
        if (p.device.relayedViaOwnPeer) relayedViaOwnPeerCount++;
      }
    }
    const authenticated = authenticatedPeerCount();
    return {
      // Online = the mesh is reachable (DHT bootstrapped) OR a trusted peer is
      // attached. A freshly booted app with zero peers is still online — it can
      // receive claim connections and be found by paired devices.
      connected: ctx.dhtReady === true || authenticated > 0,
      peerCount: authenticated,
      relayedPeerCount,
      relayedViaOwnPeerCount,
      directPeerCount: authenticated - relayedPeerCount,
    };
  }

  async function onConnection(connection, peerInfo) {
    engine.connectionCount++;
    const peerId =
      (peerInfo?.publicKey ? b4a.toString(peerInfo.publicKey, "hex") : null) ||
      `peer-${engine.connectionCount}`;

    // Attribute whatever topics this connection already carries (the dialing
    // side has them at open), then keep re-reading the live peerInfo via the
    // attribution sweep — Hyperswarm fills/appends topics as its lookups land.
    peerInfos.set(peerId, peerInfo);
    attributePeerTopics(peerId, peerInfo);
    ensureAttributionSweep();

    // Register cleanup handlers first so a connection that closes while the
    // (async) settings read below is in flight can never leak from the map.
    connection.on("close", () => {
      engine.connectionCount--;
      const peerObj = ctx.peers.get(peerId);
      const devId = peerObj?.device?.id || peerId;
      // Per-core drop replication lives on the peer object, not the
      // ReplicationScope (claims are never store-trusted) — close it too, and
      // release any claim-bundle completion listeners.
      if (peerObj) {
        if (peerObj.dropStreams) {
          for (const s of peerObj.dropStreams) {
            try {
              s.destroy();
            } catch {}
          }
        }
        if (peerObj.claimCleanups) {
          for (const fn of peerObj.claimCleanups) {
            try {
              fn();
            } catch {}
          }
        }
      }
      ctx.peers.delete(peerId);
      // Per-connection topic attribution dies with the connection — a stale
      // label must never suppress pairing (or gate CLAIM sends) for a later,
      // unrelated connection.
      peerInfos.delete(peerId);
      if (engine.peerTopics) engine.peerTopics.delete(peerId);
      engine.replicationScope.close(peerId);
      engine.emit(EVENTS.PEER_DISCONNECTED, { id: devId, peerId });
    });
    connection.on("error", () => {});

    // A deleted (revoked) device must not re-admit itself through the
    // auto-trust paths: its key is refused for direct trust and LAN auto-trust
    // below. The connection itself is NOT destroyed — it falls through to the
    // pairing challenge, and only a fresh pairing with the CURRENT code (the
    // old one was rotated away on deletion) can re-admit it.
    const revokedPeer = engine.trustManager.isRevoked(peerId);
    if (revokedPeer) {
      console.log(
        `[MeshEngine] Deleted peer ${peerId.slice(0, 12)}... reconnecting — refusing auto-trust (re-pairing required)`,
      );
    }

    const remoteIp =
      peerInfo?.host ||
      connection.remoteAddress ||
      connection.rawStream?.remoteAddress ||
      connection._socket?.remoteAddress ||
      "";
    const transferMethod = getTransferMethod(remoteIp);
    const relayed = isRelayedConnection(
      peerInfo,
      connection,
      ctx.swarm && ctx.swarm.dht,
    );
    // Honest label (C.2): per-peer attempt tracking — the relay key for THIS
    // peerId is stored on its pending entry by the relayThrough picker.
    // Label "relayed via <device>" ONLY when that key is a paired peer. Never guess.
    let relayedViaOwnPeer = false;
    let relayedViaPeerKey = null;
    try {
      if (relayed) {
        const pending = engine._pendingRelayByPeer && engine._pendingRelayByPeer.get(peerId)
        const k = pending ? pending.key : (engine._pendingRelayKey || null)
        const isOwn = pending ? !!pending.isOwn : (engine._pendingRelayIsOwn === true)
        if (k && isOwn && engine.peers && engine.peers.has(k)) {
          relayedViaOwnPeer = true
          relayedViaPeerKey = k
        } else {
          relayedViaOwnPeer = false
          relayedViaPeerKey = null
        }
        if (pending && engine._pendingRelayByPeer) engine._pendingRelayByPeer.delete(peerId)
      }
    } catch {}

    // Trust is earned: a previously verified trusted key always wins. With the
    // two-tier trust model, autoTrustLAN no longer grants trust: a peer heard
    // on the local network (explicit LAN-discovery signal, or a private-range
    // remote address) is only recognized at the "lan" level — identity
    // exchange + display. Full pairing requires the user's explicit
    // confirmation via the device-detected-on-lan prompt (confirmLanPeer).
    const lanDiscovered =
      engine.lanDiscovery &&
      typeof engine.lanDiscovery.has === "function" &&
      engine.lanDiscovery.has(peerId);
    const directTrusted = engine.trustManager.isTrustedPublicKey(peerId);
    const lanRecognized =
      !directTrusted &&
      !revokedPeer &&
      (await engine.getAutoTrustLAN()) &&
      (transferMethod === "lan" || lanDiscovered);

    const signalingApi = signaling.setupPeerSignaling(connection, peerId);
    // NOTE: the private metadata store is NEVER replicated here. The exchange
    // store (file cores only) is replicated once the peer is authenticated.

    const peer = {
      id: peerId,
      publicKey: peerId,
      name: "Connecting...",
      os: "Unknown",
      osVersion: "",
      avatar: "",
      isTrusted: directTrusted,
      lanLevel: !directTrusted && lanRecognized,
      isEncrypted: true,
      isOnline: true,
      lastSeen: new Date().toISOString(),
      ipAddress: remoteIp,
      transferMethod,
      relayed,
      relayedViaOwnPeer,
      relayedViaPeerKey,
    };

    // Trust is earned: only a known trusted noise public key (direct) or a
    // successful pairing challenge (pairing) ever sets isTrusted = true. The
    // autoTrustLAN preference only yields the "lan" recognition level.
    // `timeout` is owned by TrustManager's watchdog: it is armed only when a
    // PAIRING_CHALLENGE is sent/received and cleared on verification — it
    // must NOT start at connection open, otherwise a slow code typist loses
    // the race to the timer.
    const pairing = {
      mode: directTrusted ? "direct" : lanRecognized ? "lan" : "pairing",
      trusted: directTrusted,
      complete: false,
      outstanding: [], // { nonce, code, codeId }
      pendingChallenges: [], // { codeId, nonce } received but not yet answerable
      pendingHandshake: null, // HANDSHAKE received before our challenge verified
      timeout: null,
      code: null,
    };

    ctx.peers.set(peerId, {
      connection,
      device: peer,
      signaling: signalingApi,
      transferMethod,
      pairing,
    });
    // Let TunnelManager re-send pending TUNNEL_CLAIM to this newly found peer (code-host rendezvous)
    try { if (engine.tunnelManager && typeof engine.tunnelManager._onPeerSignalingReady === 'function') engine.tunnelManager._onPeerSignalingReady(peerId) } catch {}

    if (directTrusted) {
      replicateExchange(peerId);
    } else if (lanRecognized) {
      // Two-tier trust: recognized on the LAN, not paired. Ask the user to
      // confirm; the handshake exchange fills in the device name.
      console.log(
        `[MeshEngine] LAN peer ${peerId.slice(0, 12)}... recognized at 'lan' level (autoTrustLAN enabled) — pairing requires user confirmation`,
      );
      engine.emit(EVENTS.DEVICE_DETECTED_LAN, {
        peerId,
        publicKey: peerId,
        id: null,
        name: null,
      });
    }
  }

  async function initSwarm() {
    ctx.stopped = false;
    // refreshNetwork() may have replaced engine.swarm — always rebind so the
    // connection handler and every module ref read the live swarm.
    ctx.swarm = engine.swarm;
    ctx.dhtReady = false;
    ctx.swarm.on("connection", onConnection);
    // hyperswarm surfaces transport-level failures on 'ban' (peer rejected,
    // e.g. firewalled) and 'update' (peers added/removed/connected). There is
    // no 'connection-error' event in hyperswarm 4.x — the per-connection
    // 'error' is swallowed at onConnection. The ban guard below lets a failing
    // DHT node self-heal instead of silently degrading.
    ctx.swarm.on("ban", (peerInfo, err) => {
      if (err && ctx.dhtReady && !ctx.stopped) {
        console.warn(
          `[MeshEngine] DHT banned peer (${err.message}) — scheduling swarm refresh`,
        );
        scheduleRefresh();
      }
    });
    // 'update' fires when the routing table changes. It is informational for
    // the UI (exposed as network:status) but also a cheap trigger to re-run
    // topic re-announce when the DHT recovers from a ban/rejection.
    ctx.swarm.on("update", () => {
      if (ctx.dhtReady && !ctx.stopped) {
        engine.emit("network:update", {
          // Plain counts only: the event may cross a JSON IPC boundary (mobile
          // bridge serializes every event), so live Connection objects or a
          // Set are out. hyperswarm 4.x has no swarm.knownPeers — the routing
          // table length (TimeOrderedSet) is the meaningful "known DHT nodes"
          // figure.
          known:
            ctx.swarm.dht && ctx.swarm.dht.nodes
              ? ctx.swarm.dht.nodes.length
              : 0,
          connecting: ctx.swarm.connecting || 0,
          connected: ctx.swarm.connections ? ctx.swarm.connections.size : 0,
        });
      }
    });

    try {
      await ctx.swarm.dht.ready();
      ctx.dhtReady = true;
      // DHT bootstrapped — cancel any pending retry and reset the backoff.
      if (ctx._dhtRetryTimer) {
        clearTimeout(ctx._dhtRetryTimer);
        ctx._dhtRetryTimer = null;
      }
      ctx._dhtRetry = 0;
    } catch (err) {
      console.error("DHT ready failed:", err.message);
      // DHT bootstrap is a hard prerequisite for announcing/lookup; retry with
      // backoff so a transient dead network (Wi-Fi dropped, no cell fallback
      // yet) heals without an OS event. 15s * 8 backoff ≈ 2min of attempts.
      if (ctx._dhtRetryTimer) clearTimeout(ctx._dhtRetryTimer);
      ctx._dhtRetry = ctx._dhtRetry ? Math.min(ctx._dhtRetry * 2, 8) : 1;
      const delay = 15000 * ctx._dhtRetry;
      ctx._dhtRetryTimer = setTimeout(() => {
        ctx._dhtRetryTimer = null;
        if (ctx.stopped) return;
        initSwarm();
      }, delay);
      if (ctx._dhtRetryTimer.unref) ctx._dhtRetryTimer.unref();
    }

    if (!engine.deviceIdentity) await engine.initIdentity();
    if (engine.deviceIdentity && engine.deviceIdentity.publicKey) {
      console.log(
        `[MeshEngine] Listening on self identity DHT topic for VPN/relay discovery: ${engine.deviceIdentity.publicKey.slice(0, 12)}...`,
      );
      engine.topicRegistry.ensure(
        `p2p-peer-${engine.deviceIdentity.publicKey}`,
        {
          client: true,
          server: true,
        },
      );
      // Re-join every other active topic (paired peers, pairing codes, drop
      // shares) on the possibly-new swarm — after a network-change rebuild
      // this re-announces them on the fresh DHT node. Harmless no-op for
      // labels already joined on the same swarm.
      engine.topicRegistry.reattach(ctx.swarm);

      await ctx.swarm.listen();
      if (engine.lanDiscovery) {
        // A network change leaves the old multicast socket bound to the dead
        // interface — stop it and rebind on the current one.
        engine.lanDiscovery.stop();
        engine.lanDiscovery.start();
      }
      ctx.swarm.flush().catch(() => {});
    }

    // Automatically reconnect to all stored paired peers on startup and interval
    await devices.reconnectKnownPeers();
    startIntervals();
    // Tells refreshNetwork()/start() whether the DHT actually bootstrapped.
    // false means the engine is offline — the caller schedules a self-retry.
    return ctx.dhtReady;
  }

  // Debounced swarm-level self-heal. refreshNetwork() is serialized internally
  // (_refreshing flag), so a raw call is safe — but only schedule from
  // swarm-level events when the engine is healthy.
  let banTimer = null;
  function scheduleRefresh() {
    if (banTimer || ctx.stopped || !engine.started) return;
    banTimer = setTimeout(() => {
      banTimer = null;
      if (!ctx.stopped && engine.started) engine.refreshNetwork();
    }, 5000);
    if (banTimer.unref) banTimer.unref();
  }

  // Maintenance intervals are started once per engine lifetime; initSwarm is
  // re-entered by refreshNetwork() and must not stack duplicate timers.
  function startIntervals() {
    if (ctx.intervalsStarted) return;
    ctx.intervalsStarted = true;
    setInterval(devices.reconnectKnownPeers, 60000).unref();
    // PING/PONG latency probe
    setInterval(keepalive.sendPings, PING_INTERVAL_MS).unref();
  }

  return {
    refs: ctx.refs,
    setupPeerSignaling: signaling.setupPeerSignaling,
    sendHandshake: signaling.sendHandshake,
    replicateExchange,
    confirmLanPeer: devices.confirmLanPeer,
    sendPairingChallenges: signaling.sendPairingChallenges,
    handlePeerMessage: signaling.handlePeerMessage,
    // First-chance inbound interceptor, consulted by signaling.js before the
    // generic router (set by SiteManager for scoped allowlist challenges so
    // they never reach the device-trust grant path).
    setBeforePeerMessage(fn) {
      ctx.beforePeerMessage = fn
    },
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
    switchPeerToDirect,
    confirmClaimDownload: claims.confirmClaimDownload,
    cancelClaimDownload: claims.cancelClaimDownload,
    // Flip the maintenance intervals off; called from engine.stop().
    teardown: () => {
      ctx.stopped = true;
    },
  };
}

module.exports = { createConnections, getTransferMethod };
