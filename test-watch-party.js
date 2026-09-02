'use strict'

// Watch Party End-to-End Test Suite
// Verifies the guest media data plane that pairs with the control plane:
// 1. Host stages party media under the deterministic shareId at room creation
// 2. Room join delivers the media descriptor ONLY to the joining participant
// 3. Guest auto-accepts the transfer under the deterministic id and receives
//    the full file (byte-identical) — desktop<->desktop and android<->desktop
//    share this exact core path, so two generic peers cover both directions
// 4. Play / pause / seek state sync reaches the guests with media in flight
// 5. Late joiner receives the staged media after the first guest finished
// 6. Rejoining guest with completed media gets an immediate source-ready
// 7. leaveRoom tears the media streams down

const fs = require('fs')
const os = require('os')
const path = require('path')
const { EventEmitter } = require('events')
const Corestore = require('corestore')

const { WatchPartyManager, PARTY_EVENTS } = require('./engine/WatchPartyManager.js')
const { TransferEngine } = require('./engine/TransferEngine.js')
const { fsp, path: p } = require('./compat.js')
const { EVENTS } = require('./protocol.js')

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
      const keys = Array.from(map.keys()).sort()
      for (const key of keys) yield { key, value: map.get(key) }
    },
    size() { return map.size }
  }
}

// One "device": a real TransferEngine + WatchPartyManager over a real
// Corestore, wired like the MeshEngine does it (peers map, storage.exchangeStore,
// WATCH_* signaling routed to the party manager — the auth gate the production
// router applies before WATCH messages are delivered is emulated by the pipe).
function makeSide(peerKey, name, tmpRoot) {
  const store = new Corestore(path.join(tmpRoot, 'store-' + peerKey))
  // Per-name bees — the real engine namespaces storage.getBee(name) this way,
  // and two bee namespaces share keys (e.g. 'shared' mirrors transfer ids).
  const bees = new Map()
  const getBee = async (beeName) => {
    if (!bees.has(beeName)) bees.set(beeName, makeBee())
    return bees.get(beeName)
  }
  const peers = new Map()
  const engine = new EventEmitter()
  engine.peerKey = peerKey
  engine.storage = {
    exchangeStore: store,
    getDeviceIdentity: () => ({ id: 'dev-' + peerKey, name })
  }
  engine.peers = peers
  engine.topicRegistry = null
  engine.getBee = getBee

  engine.transferEngine = new TransferEngine({
    getBee,
    exchangeStore: store,
    sendEvent: (event, data) => engine.emit(event, data),
    getPeers: () => peers,
    getDeviceIdentity: engine.storage.getDeviceIdentity,
    getDownloadDirectory: async () => path.join(tmpRoot, 'dl-' + peerKey),
    getTransferMethod: () => 'lan',
    fsp,
    path: p,
    getSyncMode: () => null,
    getStagingRoot: async () => path.join(tmpRoot, 'staging-' + peerKey)
  })

  engine.watchParty = new WatchPartyManager({ engine })
  return engine
}

// WATCH_* pipe between two sides (mirrors the signaling router branch).
// Connections are NoiseSecretStreams over a streamx duplex pair — the same
// shape hyperswarm hands to onConnection in production, so per-core
// replication (claims/party) works identically.
function connectPeers(sideA, sideB) {
  const streamx = require('streamx')
  const NoiseSecretStream = require('@hyperswarm/secret-stream')

  const wirePair = () => {
    const a = new streamx.Duplex({ write(data, cb) { b.push(data); cb() } })
    const b = new streamx.Duplex({ write(data, cb) { a.push(data); cb() } })
    return [a, b]
  }
  const [rawA, rawB] = wirePair()
  const connA = new NoiseSecretStream(true, rawA)
  const connB = new NoiseSecretStream(false, rawB)

  const pipe = (from, to) => ({
    send: (msg) => {
      if (!msg || typeof msg.type !== 'string' || !msg.type.startsWith('WATCH_')) return
      Promise.resolve().then(() => {
        to.watchParty.handleMessage(from.peerKey, msg)
      }).catch(() => {})
    }
  })

  sideA.peers.set(sideB.peerKey, {
    connection: connA,
    signaling: pipe(sideA, sideB),
    device: { id: 'dev-' + sideB.peerKey, publicKey: sideB.peerKey, name: sideB.peerKey }
  })
  sideB.peers.set(sideA.peerKey, {
    connection: connB,
    signaling: pipe(sideB, sideA),
    device: { id: 'dev-' + sideA.peerKey, publicKey: sideA.peerKey, name: sideA.peerKey }
  })
}

function waitFor(fn, timeoutMs = 20000, label = 'condition') {
  const start = Date.now()
  return new Promise((resolve, reject) => {
    const tick = () => {
      let v
      try {
        v = fn()
      } catch (err) {
        return reject(err)
      }
      if (v) return resolve(v)
      if (Date.now() - start > timeoutMs) return reject(new Error('timeout waiting for ' + label))
      setTimeout(tick, 100)
    }
    tick()
  })
}

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout: ' + label)), ms))
  ])
}

async function waitForCompletedTransfer(engine, transferId, label) {
  const evt = new Promise((resolve) => {
    const onDone = (t) => {
      if (t && t.id === transferId && t.status === 'completed') {
        engine.removeListener(EVENTS.TRANSFER_COMPLETED, onDone)
        resolve(t)
      }
    }
    engine.on(EVENTS.TRANSFER_COMPLETED, onDone)
  })
  return withTimeout(evt, 30000, 'transfer completed (' + label + ')')
}

async function runAllTests() {
  console.log('\n======================================================')
  console.log('         MESHDROP WATCH PARTY E2E TESTS              ')
  console.log('======================================================\n')

  const tmpRoot = path.join(os.tmpdir(), `meshdrop-watch-party-${Date.now()}`)
  await fsp.mkdir(tmpRoot, { recursive: true })

  try {
    // ── Test 1: Guest receives the party media over the deterministic id ────
    console.log('Test 1: Host -> Guest media transfer on room join (desktop<->desktop / android<->desktop path)')
    const host = makeSide('host-key', 'Linux Desktop', tmpRoot)
    const guestA = makeSide('guest-a-key', 'Galaxy Phone', tmpRoot)
    connectPeers(host, guestA)

    const mediaBytes = Buffer.alloc(200 * 1024 + 123)
    for (let i = 0; i < mediaBytes.length; i++) mediaBytes[i] = i % 251
    const mediaPath = path.join(tmpRoot, 'movie.mp4')
    await fsp.writeFile(mediaPath, mediaBytes)

    const room = await host.watchParty.createRoom({ title: 'Movie Night', filePath: mediaPath })
    assert('Room created with deterministic shareId', room && typeof room.shareId === 'string' && room.shareId.startsWith('watch-party-'), JSON.stringify(room))
    assert('Room staged real media core (coreKey present)', typeof host.watchParty.activeRoom.coreKey === 'string' && host.watchParty.activeRoom.coreKey.length === 64)
    assert('Room carries manifest hash', typeof host.watchParty.activeRoom.manifestHash === 'string' && host.watchParty.activeRoom.manifestHash.length > 0)

    // The staged core holds block 0 (manifest) + data blocks.
    const stagedCore = host.storage.exchangeStore.get({ name: room.shareId })
    await stagedCore.ready()
    assert(
      'Staged core holds manifest + data blocks',
      stagedCore.length === 1 + Math.ceil(mediaBytes.length / 65536),
      `core.length=${stagedCore.length}, expected=${1 + Math.ceil(mediaBytes.length / 65536)}`
    )

    const mediaOfferEvents = []
    const stateSyncEvents = []
    guestA.watchParty.on(PARTY_EVENTS.MEDIA_OFFER, (e) => mediaOfferEvents.push(e))
    guestA.watchParty.on(PARTY_EVENTS.STATE_SYNC, (e) => stateSyncEvents.push(e))

    const joined = await guestA.watchParty.joinRoom({ roomCode: room.roomCode })
    assert('Guest room record carries the deterministic shareId', joined && joined.shareId === room.shareId)

    const completedA = await waitForCompletedTransfer(guestA, room.shareId, 'guest A')
    assert('Guest auto-accepted the party media offer', mediaOfferEvents.length === 1)
    assert('Guest transfer id equals the deterministic shareId', completedA.id === room.shareId)
    assert('Guest media ready to play', fs.existsSync(completedA.destPath), completedA.destPath)

    const received = await fsp.readFile(completedA.destPath)
    assert('Received media is byte-identical', received.length === mediaBytes.length && received.equals(mediaBytes))

    // Playback sync flows alongside the media transfer.
    host.watchParty.broadcastPlaybackState({ action: 'play', positionSec: 42.5 })
    host.watchParty.broadcastPlaybackState({ action: 'pause', positionSec: 42.5 })
    host.watchParty.broadcastPlaybackState({ action: 'seek', positionSec: 100 })
    await waitFor(() => stateSyncEvents.length >= 3, 5000, 'state sync events')
    assert('Play sync reached guest', stateSyncEvents.some((e) => e.action === 'play' && e.positionSec === 42.5))
    assert('Pause sync reached guest', stateSyncEvents.some((e) => e.action === 'pause' && e.positionSec === 42.5))
    assert('Seek sync reached guest', stateSyncEvents.some((e) => e.action === 'seek' && e.positionSec === 100))

    // ── Test 2: Late joiner still gets the staged media ─────────────────────
    console.log('\nTest 2: Late joiner receives the staged media')
    const guestB = makeSide('guest-b-key', 'Second Desktop', tmpRoot)
    connectPeers(host, guestB)

    await guestB.watchParty.joinRoom({ roomCode: room.roomCode })
    const completedB = await waitForCompletedTransfer(guestB, room.shareId, 'guest B')
    const receivedB = await fsp.readFile(completedB.destPath)
    assert('Late joiner received byte-identical media', receivedB.equals(mediaBytes))

    // ── Test 3: Rejoin with completed media reports source-ready instantly ──
    console.log('\nTest 3: Rejoin with completed media')
    await guestA.watchParty.leaveRoom()
    const readyEvents = []
    guestA.watchParty.on(PARTY_EVENTS.MEDIA_READY, (e) => readyEvents.push(e))
    await guestA.watchParty.joinRoom({ roomCode: room.roomCode })
    await withTimeout(
      waitFor(() => readyEvents.length > 0, 5000, 'media ready'),
      6000,
      'media ready event'
    )
    assert(
      'Rejoining guest got source-ready with its local file',
      readyEvents.length === 1 && readyEvents[0].destPath === completedA.destPath,
      JSON.stringify(readyEvents)
    )

    // ── Test 4: Teardown ─────────────────────────────────────────────────────
    console.log('\nTest 4: Room teardown')
    await guestB.watchParty.leaveRoom()
    await host.watchParty.leaveRoom()
    assert('Host room closed cleanly', host.watchParty.activeRoom === null)

    for (const side of [host, guestA, guestB]) {
      for (const peerObj of side.peers.values()) {
        if (peerObj.partyMediaStream && !peerObj.partyMediaStream.destroyed) {
          throw new Error('party media stream leaked for ' + side.peerKey)
        }
      }
    }
    assert('No party media streams leaked', true)

    await host.storage.exchangeStore.close().catch(() => {})
    await guestA.storage.exchangeStore.close().catch(() => {})
    await guestB.storage.exchangeStore.close().catch(() => {})
  } finally {
    try {
      await fsp.rm(tmpRoot, { recursive: true, force: true }).catch(() => {})
    } catch {}
  }

  console.log('\n======================================================')
  console.log(`  WATCH PARTY RESULTS: ${passed} PASSED, ${failed} FAILED`)
  console.log('======================================================\n')

  if (failed > 0) process.exit(1)
  process.exit(0)
}

runAllTests().catch((err) => {
  console.error('Test runner fatal error:', err)
  process.exit(1)
})
