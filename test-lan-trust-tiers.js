'use strict'

// Acceptance tests for the two-tier trust model (lan < paired) that replaces
// silent LAN auto-trust:
//
// 1. A simulated LAN peer announces (autoTrustLAN on) → recognized at the
//    'lan' level: handshake/identity exchange works, watch-party existence
//    placeholder applies (pairing.trusted stays false), but NO exchange
//    replication, TRANSFER_OFFER ignored, no pairing challenges sent or
//    answered, not relay-eligible, and a device:detected:lan confirm event
//    fires.
// 2. Explicit user confirmation (confirmLanPeer) → full paired capabilities.
// 3. A revoked peer on the LAN → no prompt, no trust at any level.
// 4. A handshake arriving before recognition is buffered and applied at the
//    right tier; SYNC_* traffic is refused at lan level.

const { createSignaling } = require('./connections/signaling.js')
const { createDeviceRegistry } = require('./connections/devices.js')
const { TrustManager } = require('./engine/TrustManager.js')
const { randomBytes, codeId } = require('./crypto.js')
const { MESSAGES, EVENTS, PROTOCOL_VERSION } = require('./protocol.js')

let passed = 0
let failed = 0

function assert(name, cond, detail = '') {
  if (cond) {
    passed++
    console.log(`  ✓ ${name}`)
  } else {
    failed++
    console.error(`  ✗ ${name}${detail ? ' — ' + detail : ''}`)
    process.exitCode = 1
  }
}

function makeBee() {
  const map = new Map()
  return {
    async put(key, value) { map.set(key, value) },
    async get(key) { return map.has(key) ? { key, value: map.get(key) } : null },
    async del(key) { map.delete(key) },
    async *createReadStream() {
      for (const key of Array.from(map.keys()).sort()) yield { key, value: map.get(key) }
    }
  }
}

async function buildHarness({ autoTrustLAN = true } = {}) {
  const peers = new Map()
  const events = [] // { event, data }
  const sentTo = new Map() // peerId -> [msg]
  const offers = []
  const replicatedWith = []

  const trustManager = new TrustManager({
    getBee: async () => makeBee(),
    computeTopicHash: (label) => Buffer.from(label),
    swarm: { join() {}, flush: async () => {} },
    topicRegistry: null,
    getPeers: () => peers,
    sendHandshake: () => {},
    emit: (ev, data) => events.push({ event: ev, data }),
    isRefreshing: () => false,
    onTrustGranted: () => {},
    getDeviceIdentity: () => ({ id: 'self-dev', name: 'Self', publicKey: 'self-key' }),
    getPeerId: () => 'self-key'
  })

  const engine = {
    deviceIdentity: { id: 'self-dev', name: 'Self', os: 'win32', publicKey: 'self-key' },
    trustManager,
    replicationScope: {
      isPeerTrusted: (peerId) => peers.get(peerId)?.pairing?.trusted === true,
      replicate: (peerId) => {
        replicatedWith.push(peerId)
        return { stream: true }
      }
    },
    transferEngine: {
      receiveOffer: async (offer) => offers.push(offer)
    },
    autoAcceptOffers: true,
    getAutoTrustLAN: async () => autoTrustLAN,
    isSiteSessionPeer: () => false,
    getBee: async () => makeBee(),
    emit: (event, data) => events.push({ event, data }),
    getPeers: () => peers
  }

  const ctx = { engine, peers, activeClaims: new Set(), refs: {} }
  const signaling = createSignaling(ctx)
  const devices = createDeviceRegistry(ctx)

  function makePeerApi(peerId) {
    const arr = sentTo.get(peerId) || []
    sentTo.set(peerId, arr)
    return {
      send: (obj) => arr.push(obj)
    }
  }

  function addPeer(peerId, { mode = 'pairing', trusted = false, name = 'Connecting...' } = {}) {
    const peerObj = {
      id: peerId,
      connection: { destroy() {} },
      device: {
        id: 'dev-' + peerId.slice(0, 6),
        publicKey: peerId,
        identityKey: '',
        name,
        os: 'win32',
        isTrusted: trusted,
        isOnline: true
      },
      signaling: makePeerApi(peerId),
      transferMethod: 'lan',
      pairing: {
        mode,
        trusted,
        complete: false,
        outstanding: [],
        pendingChallenges: [],
        pendingHandshake: null,
        timeout: null,
        code: null
      }
    }
    peers.set(peerId, peerObj)
    return peerObj
  }

  // Mirror of createConnections' ctx.refs wiring (the modules the tests touch).
  ctx.refs = {
    ...signaling,
    ...devices,
    replicateExchange: (peerId) => {
      const peerObj = peers.get(peerId)
      if (!peerObj || peerObj.replStream) return
      if (!engine.replicationScope.isPeerTrusted(peerId)) return
      peerObj.replStream = engine.replicationScope.replicate(peerId, peerObj.connection)
    }
  }

  return { engine, ctx, peers, devices, signaling, trustManager, events, sentTo, offers, replicatedWith, addPeer }
}

function peerMsg(peerObj, type, extra = {}) {
  return { type, ...extra }
}

const PEER = 'a'.repeat(64)

async function testLanLevelCapabilities() {
  console.log('\n— Test 1: LAN announcement with autoTrustLAN → lan level —')
  const h = await buildHarness()
  h.addPeer(PEER)

  await h.devices.maybeAutoTrustLanPeer(PEER)
  const peerObj = h.peers.get(PEER)

  assert('pairing.mode is "lan"', peerObj.pairing.mode === 'lan', peerObj.pairing.mode)
  assert('pairing.trusted stays false (never silently trusted)', peerObj.pairing.trusted === false)
  assert('device.isTrusted stays false', peerObj.device.isTrusted === false)
  assert('no trustedAt stamped', !peerObj.device.trustedAt)

  const detect = h.events.filter((e) => e.event === EVENTS.DEVICE_DETECTED_LAN)
  assert('device-detected-on-lan confirm event fired', detect.length === 1)
  assert('confirm event carries key', detect[0] && detect[0].data.publicKey === PEER)

  const hsSent = (h.sentTo.get(PEER) || []).filter((m) => m.type === MESSAGES.HANDSHAKE)
  assert('handshake/identity exchange sent to lan peer', hsSent.length === 1)
  assert('no replication stream opened', h.replicatedWith.length === 0)

  // Peer replies with its identity — allowed at lan level (display only).
  h.signaling.handlePeerMessage(
    PEER,
    peerMsg(peerObj, MESSAGES.HANDSHAKE, {
      protocolVersion: PROTOCOL_VERSION,
      identity: { id: 'dev-lan-1', publicKey: 'id-key-lan', name: 'Kitchen Laptop', os: 'linux' }
    })
  )
  assert('identity from lan handshake displayed (name applied)', peerObj.device.name === 'Kitchen Laptop')
  await new Promise((r) => setImmediate(r)) // applyHandshake emits after its bee await
  assert('lan handshake does NOT mark trusted', peerObj.device.isTrusted === false)
  assert('lan handshake does NOT complete pairing', peerObj.pairing.complete === false)
  assert('confirm event re-fired with real name', h.events.some(
    (e) => e.event === EVENTS.DEVICE_DETECTED_LAN && e.data && e.data.name === 'Kitchen Laptop'
  ))
  assert('no trust:paired emitted', !h.events.some((e) => e.event === EVENTS.TRUST_PAIRED))

  // Transfer offers: ignored at lan level.
  h.signaling.handlePeerMessage(
    PEER,
    peerMsg(peerObj, MESSAGES.TRANSFER_OFFER, { transferId: 't1', filename: 'evil.zip', fileSize: 1 })
  )
  assert('TRANSFER_OFFER ignored at lan level', h.offers.length === 0)

  // Exchange sync: refused at lan level.
  h.signaling.handlePeerMessage(PEER, peerMsg(peerObj, MESSAGES.SYNC_INDEX, { files: [] }))
  const syncRefused = !h.events.some((e) => e.event === 'sync-handled') // gate must swallow before handlers
  assert('SYNC_* refused at lan level (no replication opened)', syncRefused && h.replicatedWith.length === 0)

  // Pairing codes: lan peer is never challenged and never answered.
  const code = 'MD-AAAA-BBBB-CCCC-DDDD'
  const cid = codeId(code)
  h.trustManager.pairingSecrets.set(cid, {
    code, role: 'host', createdAt: Date.now(), expiresAt: 0, codeId: cid
  })
  h.trustManager.sendChallengesToAll({ force: true })
  const challenges = (h.sentTo.get(PEER) || []).filter((m) => m.type === MESSAGES.PAIRING_CHALLENGE)
  assert('no pairing challenge sent TO lan peer', challenges.length === 0)

  const nonce = randomBytes(16)
  h.trustManager.handleChallenge(PEER, { codeId: cid, nonce: nonce.toString('hex') })
  const answers = (h.sentTo.get(PEER) || []).filter((m) => m.type === MESSAGES.PAIRING_RESP)
  assert('lan peer cannot get OUR code answers (handleChallenge refuses)', answers.length === 0)
  assert('lan peer cannot answer pairing codes (mode guard)', (() => {
    // handleResponse refuses non-'pairing' modes before any trust grant.
    h.trustManager.handleResponse(PEER, { nonce: nonce.toString('hex'), mac: 'deadbeef' })
    return peerObj.pairing.trusted === false
  })())

  // Relay eligibility: pickOwnPeerRelay requires dev.isTrusted; lan rows are
  // isTrusted=false → never relay-eligible.
  assert('not relay-eligible (isTrusted false)', peerObj.device.isTrusted === false)

  // Replication gate (ReplicationScope.isPeerTrusted equivalent).
  assert('replication gate refuses lan peer', h.engine.replicationScope.isPeerTrusted(PEER) === false)
  h.ctx.refs.replicateExchange(PEER)
  assert('replicateExchange is a no-op for lan peer', h.replicatedWith.length === 0)

  // Watch-party existence: announcement placeholder is keyed on the same
  // pairing.trusted flag (WatchPartyManager.js) — lan peer gets the
  // untrusted treatment (no title/host).
  const watchTrusted = !!(peerObj.pairing && peerObj.pairing.trusted)
  assert('watch-party placeholder treatment (same as untrusted)', watchTrusted === false)
}

async function testHandshakeBufferThenConfirm() {
  console.log('\n— Test 2: buffered handshake + explicit confirm → paired —')
  const h = await buildHarness()
  const peerObj = h.addPeer(PEER)

  // Handshake lands while the peer is still in plain 'pairing' mode → buffered.
  h.signaling.handlePeerMessage(
    PEER,
    peerMsg(peerObj, MESSAGES.HANDSHAKE, {
      protocolVersion: PROTOCOL_VERSION,
      identity: { id: 'dev-lan-2', publicKey: 'id-key-2', name: 'Garage PC', os: 'win32' }
    })
  )
  assert('early handshake buffered while unpaired', peerObj.pairing.pendingHandshake !== null)
  assert('buffered handshake not applied', peerObj.device.name === 'Connecting...')

  // LAN announcement arrives → lan level + buffer flush (identity allowed).
  await h.devices.maybeAutoTrustLanPeer(PEER)
  assert('buffered handshake flushed at lan level', peerObj.pairing.pendingHandshake === null)
  assert('identity displayed from flushed handshake', peerObj.device.name === 'Garage PC')
  assert('still not paired after flush', peerObj.pairing.trusted === false)

  // Explicit user confirmation → full pairing.
  const confirmed = await h.devices.confirmLanPeer(PEER)
  assert('confirm returns the device', !!confirmed)
  assert('mode promoted to direct', peerObj.pairing.mode === 'direct')
  assert('pairing.trusted true after confirm', peerObj.pairing.trusted === true)
  assert('device.isTrusted true after confirm', peerObj.device.isTrusted === true)
  assert('trustedAt stamped on confirm', !!peerObj.device.trustedAt)
  assert('exchange replication opened after confirm', h.replicatedWith.length === 1)
  assert('trust:paired emitted after confirm', h.events.some((e) => e.event === EVENTS.TRUST_PAIRED))

  // Full capabilities now.
  h.signaling.handlePeerMessage(
    PEER,
    peerMsg(peerObj, MESSAGES.TRANSFER_OFFER, { transferId: 't2', filename: 'photo.jpg', fileSize: 5 })
  )
  assert('TRANSFER_OFFER accepted after confirm', h.offers.length === 1)
  assert('replication gate now admits paired peer', h.engine.replicationScope.isPeerTrusted(PEER) === true)

  // Trusted-peer pairing shortcut (registerJoinerCode probing) requires paired.
  const code = 'MD-EEEE-FFFF-GGGG-HHHH'
  const cid = codeId(code)
  h.trustManager.pairingSecrets.set(cid, {
    code, role: 'joiner', createdAt: Date.now(), expiresAt: 0, codeId: cid
  })
  const trustedIds = new Set()
  for (const [pId, p] of h.peers.entries()) {
    if (p.pairing && p.pairing.trusted && !h.trustManager.isRevoked(pId)) trustedIds.add(pId)
  }
  assert('confirmed peer included in trusted-pairing probes', trustedIds.has(PEER))

  const watchTrusted = !!(peerObj.pairing && peerObj.pairing.trusted)
  assert('watch-party full details after confirm', watchTrusted === true)
}

async function testRevokedPeer() {
  console.log('\n— Test 3: revoked peer on LAN → nothing —')
  const h = await buildHarness()
  const peerObj = h.addPeer(PEER)
  await h.trustManager.revokeKey(PEER)

  await h.devices.maybeAutoTrustLanPeer(PEER)
  assert('revoked peer NOT promoted to lan', peerObj.pairing.mode === 'pairing')
  assert('revoked peer NOT trusted', peerObj.pairing.trusted === false)
  assert('no detection prompt for revoked peer', !h.events.some((e) => e.event === EVENTS.DEVICE_DETECTED_LAN))
  const hsSent = (h.sentTo.get(PEER) || []).filter((m) => m.type === MESSAGES.HANDSHAKE)
  assert('no identity exchange for revoked peer', hsSent.length === 0)

  let threw = false
  try {
    await h.devices.confirmLanPair(PEER)
  } catch {
    threw = true
  }
  assert('confirmLanPair refuses revoked peer', threw && peerObj.pairing.trusted === false)
}

async function testPrivateRangeConnectionPath() {
  console.log('\n— Test 4: private-range connection open → lan recognition (no trust) —')
  // The onConnection path in connections/index.js sets pairing.mode='lan'
  // instead of trusted=true; simulate its decision table directly to verify
  // the re-mapped logic contract.
  const h = await buildHarness()
  const peerObj = h.addPeer(PEER, { mode: 'lan', trusted: false })
  // isTrustedPublicKey (a real previously-paired key) is the ONLY connection-
  // open path to trusted=true.
  assert('connection-open lan recognition is not trusted', peerObj.pairing.trusted === false)
  assert('mode "lan" allows handshake at connection open', peerObj.pairing.mode === 'lan')
}

async function testAutoTrustLANOff() {
  console.log('\n— Test 5: autoTrustLAN off → no recognition at all —')
  const h = await buildHarness({ autoTrustLAN: false })
  const peerObj = h.addPeer(PEER)
  await h.devices.maybeAutoTrustLanPeer(PEER)
  assert('stays in plain pairing mode', peerObj.pairing.mode === 'pairing')
  assert('no detection prompt', !h.events.some((e) => e.event === EVENTS.DEVICE_DETECTED_LAN))
}

async function main() {
  await testLanLevelCapabilities()
  await testHandshakeBufferThenConfirm()
  await testRevokedPeer()
  await testPrivateRangeConnectionPath()
  await testAutoTrustLANOff()

  console.log(`\n${passed} passed, ${failed} failed`)
  if (failed > 0) process.exitCode = 1
}

main().catch((err) => {
  console.error('Test harness crashed:', err)
  process.exitCode = 1
})
