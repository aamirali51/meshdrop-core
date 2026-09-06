'use strict'

// Device registry: applying verified identity handshakes, persisting paired
// devices, LAN auto-trust promotion, completion re-broadcast, and reconnecting
// known peers on startup. Handshake/challenge I/O lives in signaling.js;
// trust verification lives in TrustManager.

const { EVENTS, PROTOCOL_VERSION } = require('../protocol.js')
const { deriveDeviceId } = require('../crypto.js')

function createDeviceRegistry(ctx) {
  const { engine, peers } = ctx

  // Apply a verified peer's identity handshake: fill in device metadata, mark
  // the pairing complete, persist the device, and emit paired/connected
  // events. Only ever called once the peer is at least LAN-recognized
  // (identity exchange + display) or fully trusted.
  async function applyHandshake(peerId, msg) {
    const peerObj = peers.get(peerId)
    if (!peerObj || !peerObj.pairing) return
    // Two-tier trust: a 'lan' peer may exchange identity (display purposes
    // only) but is not paired. Everything below the matrix grants lan peers a
    // display row with isTrusted=false and no paired capabilities.
    const lanOnly = !peerObj.pairing.trusted && peerObj.pairing.mode === 'lan'
    if (!peerObj.pairing.trusted && !lanOnly) return
    // A revoked key must never complete a handshake, even on a connection that
    // predates the revocation (delete raced an in-flight handshake).
    if (engine.trustManager.isRevoked(peerId)) {
      console.warn(
        `[MeshEngine] Refusing direct handshake from revoked peer ${peerId.slice(0, 12)}... (deleted device; re-pairing required)`
      )
      peerObj.pairing.pendingHandshake = msg
      return
    }
    if (!msg.identity) return

    // Protocol compatibility: sync semantics changed across versions (v2 =
    // streaming sync). A peer running a different wire version is marked
    // incompatible — sync is gated so the apps never silently mis-sync.
    // One-shot transfers keep working (their wire format is unchanged).
    const peerVersion = msg.protocolVersion
    if (peerVersion !== PROTOCOL_VERSION) {
      peerObj.incompatible = true
      console.warn(
        `[MeshEngine] Peer ${peerId.slice(0, 12)}... runs protocol v${peerVersion || '?'}, this build is v${PROTOCOL_VERSION}. Folder sync is disabled for this peer — update both apps.`
      )
    } else {
      peerObj.incompatible = false
    }
    // Do not list self as a remote peer
    if (
      engine.deviceIdentity &&
      (msg.identity.id === engine.deviceIdentity.id ||
        msg.identity.publicKey === engine.deviceIdentity.publicKey)
    ) {
      console.log(`[MeshEngine] Ignoring self-handshake from ${msg.identity.name}`)
      return
    }

    // Device identity comes from the peer's stable identity (persisted across
    // restarts), NOT the ephemeral noise key. `publicKey` stays the current
    // noise key (joinPeer + trust), `identityKey` the stable identity core key
    // (topic discovery).
    const deviceId =
      msg.identity.id || deriveDeviceId(msg.identity.publicKey) || deriveDeviceId(peerId)
    peerObj.device.id = deviceId
    peerObj.device.publicKey = peerId // noise public key: used for joinPeer + trust
    peerObj.device.identityKey = msg.identity.publicKey || '' // identity core key: used for topic discovery
    peerObj.device.name = msg.identity.name || peerObj.device.name
    peerObj.device.os = msg.identity.os || peerObj.device.os
    peerObj.device.isTrusted = !lanOnly
    peerObj.device.lanLevel = lanOnly
    peerObj.device.isOnline = true
    // Capability discovery: peer's relay server running (propagated via handshake canRelay)
    if (msg.identity && typeof msg.identity.canRelay === 'boolean') {
      peerObj.device.canRelay = !!msg.identity.canRelay
    }
    if (!lanOnly) {
      peerObj.device.trustedAt = peerObj.device.trustedAt || new Date().toISOString()
      peerObj.pairing.complete = true
    }
    peerObj.device.lastSeen = new Date().toISOString()
    // Belt-and-suspenders: TrustManager clears its watchdog on challenge
    // verification; also drop any stale timer now that the handshake has
    // completed so the connection can never be killed by a leftover one.
    if (peerObj.pairing.timeout) {
      clearTimeout(peerObj.pairing.timeout)
      peerObj.pairing.timeout = null
    }
    peers.set(peerId, peerObj)

    // Send reciprocal handshake if not sent yet. sendHandshake sets the
    // handshakeSent flag itself; setting it here first made the call a no-op
    // and left the peer that verified FIRST without our identity.
    if (!peerObj.handshakeSent && peerObj.signaling) {
      ctx.refs.sendHandshake(peerId)
    }

    // Persist / update online device in Hyperbee, keyed by the peer's stable
    // identity id so restarts update the same record instead of duplicating it.
    try {
      const bee = await engine.getBee('devices')
      const { relayed: _relayed, ...deviceToPersist } = peerObj.device
      // The user's rename is authoritative: a device's own reported name must
      // never clobber a custom name on reconnect (the "renamed phone resets to
      // 'MeshDrop Mobile' after an app update" bug). Keep the stored custom
      // name, remember what the peer reports now, and carry the trust timeline.
      const existing = await bee.get(deviceId).catch(() => null)
      const prev = (existing && existing.value) || null
      const persisted = { ...deviceToPersist }
      if (prev) {
        if (prev.customName) {
          persisted.name = prev.customName
        }
        persisted.customName = prev.customName || ''
        persisted.lastReportedName = deviceToPersist.name || prev.lastReportedName || ''
        persisted.trustedAt = prev.trustedAt || persisted.trustedAt
        persisted.pairedAt = prev.pairedAt || persisted.pairedAt
        persisted.isFavorite = prev.isFavorite || false
        persisted.avatar = prev.avatar || persisted.avatar
      } else {
        persisted.customName = ''
        persisted.lastReportedName = persisted.name || ''
      }
      await bee.put(deviceId, persisted)
    } catch (err) {
      console.error('[MeshEngine] Failed to save device to bee:', err)
    }
    if (!lanOnly) engine.trustManager.addTrustedKey(peerId)

    if (lanOnly) {
      // LAN-level identity is now known: (re)fire the detection prompt with
      // the real device name. No paired/connected events — a lan peer is
      // displayed, but nothing downstream may treat it as paired.
      engine.emit(EVENTS.DEVICE_DETECTED_LAN, {
        peerId,
        publicKey: peerId,
        id: peerObj.device.id,
        name: peerObj.device.name,
      })
      return
    }

    engine.emit(EVENTS.TRUST_PAIRED, { peer: peerObj.device, code: peerObj.pairing.code })
    engine.emit(EVENTS.PEER_CONNECTED, peerObj.device)
  }

  // Process a HANDSHAKE that arrived before our side of the challenge-response
  // verified. Runs from onTrustGranted, i.e. strictly after pairing.trusted
  // became true, so the buffered handshake is never applied to an untrusted
  // peer.
  function flushPendingHandshake(peerId) {
    const peerObj = peers.get(peerId)
    if (!peerObj || !peerObj.pairing) return
    const p = peerObj.pairing
    // Applies once the peer is paired OR merely LAN-recognized (identity
    // exchange is granted at the lan level).
    if (!p.trusted && p.mode !== 'lan') return
    if (!p.pendingHandshake) return
    const msg = p.pendingHandshake
    p.pendingHandshake = null
    applyHandshake(peerId, msg)
  }

  // Re-broadcast completion events for a peer whose handshake already finished
  // earlier (e.g. the code's host was auto-trusted before the code was
  // entered). TrustManager re-confirms such peers when they prove knowledge of
  // a freshly registered code.
  function rebroadcastPeerCompletion(peerId, code) {
    const peerObj = peers.get(peerId)
    if (!peerObj || !peerObj.pairing || !peerObj.pairing.complete) return
    if (!peerObj.device || !peerObj.device.name || peerObj.device.name === 'Connecting...') return
    engine.emit(EVENTS.TRUST_PAIRED, { peer: peerObj.device, code })
    engine.emit(EVENTS.PEER_CONNECTED, peerObj.device)
  }

  // A LAN announcement can land AFTER the connection formed (discovery is
  // asynchronous and the connection may have come via the DHT identity topic).
  // onConnection can therefore not be the only place autoTrustLAN is honored:
  // recognize a still-pairing peer at the LAN level the moment it is
  // discovered on the local network. LAN-level grants identity exchange +
  // display ONLY — replication, transfer offers, pairing-code answering and
  // relay eligibility stay locked until the user explicitly confirms pairing
  // (confirmLanPeer). Revoked peers are never recognized at any level.
  async function maybeAutoTrustLanPeer(peerId) {
    const peerObj = peers.get(peerId)
    if (!peerObj || !peerObj.pairing || peerObj.pairing.trusted) return
    if (engine.trustManager.isRevoked(peerId)) return // deleted devices never auto-trust
    if (peerObj.pairing.mode !== 'pairing' && peerObj.pairing.mode !== 'lan') return
    if (!(await engine.getAutoTrustLAN())) return
    const wasPairing = peerObj.pairing.mode === 'pairing'
    if (wasPairing) {
      peerObj.pairing.mode = 'lan'
      // Drop any watchdog the challenge phase armed; a lan peer gets none.
      if (peerObj.pairing.timeout) {
        clearTimeout(peerObj.pairing.timeout)
        peerObj.pairing.timeout = null
      }
      peers.set(peerId, peerObj)
      console.log(
        `[MeshEngine] LAN peer ${peerId.slice(0, 12)}... recognized at 'lan' level (autoTrustLAN enabled) — pairing requires user confirmation`
      )
    }
    // Identity exchange is allowed at lan level; replication is NOT.
    ctx.refs.sendHandshake(peerId)
    // The peer may already have sent its HANDSHAKE while we were still pairing.
    flushPendingHandshake(peerId)
    // Surface the one-tap confirm. Emitted again from applyHandshake once the
    // handshake supplies the real device name; the renderer keys on publicKey.
    engine.emit(EVENTS.DEVICE_DETECTED_LAN, {
      peerId,
      publicKey: peerId,
      id: peerObj.device && peerObj.device.id,
      name: peerObj.device && peerObj.device.name,
    })
  }

  // Explicit user confirmation (the "Device detected on your network — pair?"
  // prompt): promote a lan-level peer to full pairing. This is the ONLY path
  // from lan to paired — no timeout, no re-announcement, no code answer can
  // get here without this call.
  async function confirmLanPeer(peerKey) {
    const peerId = typeof peerKey === 'string' ? peerKey : ''
    const peerObj = peers.get(peerId)
    if (!peerObj || !peerObj.pairing) {
      throw new Error('Device is not connected right now.')
    }
    if (engine.trustManager.isRevoked(peerId)) {
      throw new Error('This device was removed and cannot be re-trusted from the prompt.')
    }
    if (peerObj.pairing.trusted) return peerObj.device // already paired
    peerObj.pairing.mode = 'direct'
    peerObj.pairing.trusted = true
    peerObj.device.isTrusted = true
    peerObj.device.lanLevel = false
    peerObj.device.trustedAt = peerObj.device.trustedAt || new Date().toISOString()
    if (peerObj.pairing.timeout) {
      clearTimeout(peerObj.pairing.timeout)
      peerObj.pairing.timeout = null
    }
    peers.set(peerId, peerObj)
    console.log(
      `[MeshEngine] LAN peer ${peerId.slice(0, 12)}... confirmed by user — promoted to paired`
    )
    engine.trustManager.addTrustedKey(peerId)
    ctx.refs.sendHandshake(peerId)
    ctx.refs.replicateExchange(peerId)
    flushPendingHandshake(peerId)
    // Persist the paired state (applyHandshake persisted a lan-only row).
    try {
      const bee = await engine.getBee('devices')
      const existing = await bee.get(peerObj.device.id).catch(() => null)
      const prev = (existing && existing.value) || null
      if (prev) {
        await bee.put(peerObj.device.id, {
          ...prev,
          isTrusted: true,
          trustedAt: peerObj.device.trustedAt,
          lanLevel: false,
        })
      }
    } catch (err) {
      console.warn('[MeshEngine] Failed to persist LAN pairing confirmation:', err.message)
    }
    engine.emit(EVENTS.TRUST_PAIRED, { peer: peerObj.device, code: peerObj.pairing.code })
    engine.emit(EVENTS.PEER_CONNECTED, peerObj.device)
    return peerObj.device
  }

  async function reconnectKnownPeers() {
    if (ctx.stopped) return
    try {
      const bee = await engine.getBee('devices')
      for await (const node of bee.createReadStream()) {
        const dev = node.value
        // Only reconnect peers whose trust was established under the new scheme
        // (trustedAt is set exclusively by the challenge/verified-handshake path).
        if (
          dev &&
          dev.isTrusted === true &&
          dev.trustedAt &&
          dev.publicKey &&
          dev.publicKey.length === 64
        ) {
          try {
            const peerKey = Buffer.from(dev.publicKey, 'hex')
            // The identity core key (identityKey) is what the peer listens on in
            // initSwarm; the noise public key is what joinPeer needs to connect.
            const peerTopicLabel = `p2p-peer-${dev.identityKey || dev.publicKey}`
            // Join the peer DHT topic AND attempt direct connection to the peer key
            engine.topicRegistry.ensure(peerTopicLabel, { client: true, server: true })
            // Fix 4: never pick the dial target itself as its own relay.
            // Scope _pendingDialTarget narrowly around joinPeer so pickOwnPeerRelay sees it.
            const prevTarget = engine._pendingDialTarget
            engine._pendingDialTarget = dev.publicKey
            try {
              ctx.swarm.joinPeer(peerKey)
            } finally {
              engine._pendingDialTarget = prevTarget
            }
            // Never leave the flush unhandled: refreshNetwork() destroys the
            // old swarm while reconnectKnownPeers may be mid-flight, and the
            // flush rejects with ERR_SWARM_DESTROYED. An unhandled rejection
            // in the Bare worklet is treated as fatal (unhandledRejection
            // handler in src/engine/index.js), killing the engine.
            ctx.swarm.flush().catch((err) => {
              if (err && !/destroyed/i.test(String(err.message))) {
                console.warn('[MeshEngine] flush during reconnect failed:', err.message)
              }
            })
          } catch (err) {
            console.error(`[MeshEngine] Failed to reconnect to peer ${dev.id}:`, err.message)
          }
        }
      }
    } catch (err) {
      console.error('[MeshEngine] reconnectKnownPeers failed:', err.message)
    }
  }

  return {
    applyHandshake,
    flushPendingHandshake,
    rebroadcastPeerCompletion,
    maybeAutoTrustLanPeer,
    confirmLanPeer,
    reconnectKnownPeers
  }
}

module.exports = { createDeviceRegistry }
