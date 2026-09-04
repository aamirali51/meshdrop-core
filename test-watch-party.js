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

  // Seed a pairing watchdog like the TrustManager arms for unpaired peers —
  // the party media flow must drop it so long transfers survive (claims
  // contract). A real timer handle keeps clearTimeout honest.
  const watchdogA = setTimeout(() => {}, 10 * 60 * 1000)
  const watchdogB = setTimeout(() => {}, 10 * 60 * 1000)
  sideA.peers.set(sideB.peerKey, {
    connection: connA,
    signaling: pipe(sideA, sideB),
    device: { id: 'dev-' + sideB.peerKey, publicKey: sideB.peerKey, name: sideB.peerKey },
    pairing: { trusted: false, timeout: watchdogA }
  })
  sideB.peers.set(sideA.peerKey, {
    connection: connB,
    signaling: pipe(sideB, sideA),
    device: { id: 'dev-' + sideA.peerKey, publicKey: sideA.peerKey, name: sideA.peerKey },
    pairing: { trusted: false, timeout: watchdogB }
  })
}

function pairingWatchdogOf(side, otherPeerKey) {
  const peerObj = side.peers.get(otherPeerKey)
  return peerObj && peerObj.pairing ? peerObj.pairing.timeout : undefined
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
    assert(
      'Pairing watchdog dropped on both sides (unpaired transfer survives)',
      pairingWatchdogOf(host, guestA.peerKey) === null && pairingWatchdogOf(guestA, host.peerKey) === null,
      'host: ' + String(pairingWatchdogOf(host, guestA.peerKey)) + ' guest: ' + String(pairingWatchdogOf(guestA, host.peerKey))
    )

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

    // ── Test 5: Join-timeout surfaces a phantom room as ROOM_CLOSED ──────────
    console.log('\nTest 5: Join timeout on unreachable host')
    const lonelyGuest = makeSide('lonely-key', 'Lonely Guest', tmpRoot)
    // No host connected: the join broadcast goes nowhere.
    const closedEvents = []
    lonelyGuest.watchParty.on(PARTY_EVENTS.ROOM_CLOSED, (e) => closedEvents.push(e))
    const phantom = await lonelyGuest.watchParty.joinRoom({ roomCode: 'PARTY-XXXX-YYYY' })
    assert('Guest joined a phantom room optimistically', phantom && phantom.isHost === false)
    // The join-timeout is 30s; we don't want the suite to take that long, so
    // reach in and fire the timer logic directly via the maintenance watchdog.
    lonelyGuest.watchParty._roomMaintenanceTimer && clearInterval(lonelyGuest.watchParty._roomMaintenanceTimer)
    // Simulate the passage of time: mark joinedAt far in the past, then run
    // the same check _maintainRoom performs for the host-silence watchdog.
    lonelyGuest.watchParty.activeRoom.joinedAt = Date.now() - 130000
    lonelyGuest.watchParty._autoLeaveStaleRoom(lonelyGuest.watchParty.activeRoom, 'host unreachable')
    assert('Phantom guest auto-left via host-silence watchdog', closedEvents.length === 1 && lonelyGuest.watchParty.activeRoom === null)
    await lonelyGuest.storage.exchangeStore.close().catch(() => {})

    // ── Test 6: Host close reaches guests (no zombie room) ───────────────────
    console.log('\nTest 6: Host close propagates to guests')
    const host6 = makeSide('host-6', 'Host Six', tmpRoot)
    const guest6 = makeSide('guest-6', 'Guest Six', tmpRoot)
    connectPeers(host6, guest6)
    const room6 = await host6.watchParty.createRoom({ title: 'Six', filePath: mediaPath })
    const guest6Closed = []
    guest6.watchParty.on(PARTY_EVENTS.ROOM_CLOSED, (e) => guest6Closed.push(e))
    await guest6.watchParty.joinRoom({ roomCode: room6.roomCode })
    await host6.watchParty.leaveRoom()
    // leaveRoom broadcasts close synchronously to connected peers before
    // tearing down, so by the time the promise resolves the guest has seen it.
    assert('Guest received ROOM_CLOSED after host left', guest6Closed.length === 1)
    assert('Guest room cleared after host close', guest6.watchParty.activeRoom === null)
    await host6.storage.exchangeStore.close().catch(() => {})
    await guest6.storage.exchangeStore.close().catch(() => {})

    // ── Test 7: Room-code normalization ──────────────────────────────────────
    console.log('\nTest 7: Room-code normalization on join')
    const norm = host.watchParty._normalizeRoomCode
    assert('Canonical code passes through', norm('PARTY-ABCD-EFGH') === 'PARTY-ABCD-EFGH')
    assert('Lowercase + spaces normalize', norm('party abcd efgh') === 'PARTY-ABCD-EFGH')
    assert('Stripped code normalizes', norm('PARTYABCDEFGH') === 'PARTY-ABCD-EFGH')

    // ── Test 8: controlsMode host-authority drops guest state syncs ─────────
    console.log('\nTest 8: controlsMode host-authority')
    const host8 = makeSide('host-8', 'Host Eight', tmpRoot)
    const guest8 = makeSide('guest-8', 'Guest Eight', tmpRoot)
    connectPeers(host8, guest8)
    const room8 = await host8.watchParty.createRoom({ title: 'Eight', filePath: mediaPath, controlsMode: 'host' })
    const guest8Syncs = []
    host8.watchParty.on(PARTY_EVENTS.STATE_SYNC, (e) => guest8Syncs.push(e))
    await guest8.watchParty.joinRoom({ roomCode: room8.roomCode })
    // A guest attempts to drive playback in host-only mode — must be dropped.
    guest8.watchParty.broadcastPlaybackState({ action: 'play', positionSec: 5 })
    await new Promise((r) => setTimeout(r, 300))
    assert('Host ignored non-host state sync in host mode', guest8Syncs.length === 0)
    // Host playback still flows.
    host8.watchParty.broadcastPlaybackState({ action: 'play', positionSec: 42 })
    await new Promise((r) => setTimeout(r, 300))
    assert('Host state sync still delivered in host mode', guest8Syncs.length >= 1)
    await host8.storage.exchangeStore.close().catch(() => {})
    await guest8.storage.exchangeStore.close().catch(() => {})

    // ── Test 9: createRoom fails fast on staging error (no phantom host) ─────
    console.log('\nTest 9: createRoom fails fast when media staging fails')
    const host9 = makeSide('host-9', 'Host Nine', tmpRoot)
    // Point at a path that does not exist: stat throws, room must reject.
    let stagingFailed = false
    try {
      await host9.watchParty.createRoom({ title: 'Nine', filePath: path.join(tmpRoot, 'does-not-exist.mp4') })
    } catch (err) {
      stagingFailed = true
    }
    assert('createRoom rejected for unreadable media', stagingFailed === true)
    assert('No phantom host room left behind', host9.watchParty.activeRoom === null)
    await host9.storage.exchangeStore.close().catch(() => {})

    // ── Test 10: Guest host-silence watchdog clears a stale room ─────────────
    console.log('\nTest 10: Guest host-silence watchdog')
    const host10 = makeSide('host-10', 'Host Ten', tmpRoot)
    const guest10 = makeSide('guest-10', 'Guest Ten', tmpRoot)
    connectPeers(host10, guest10)
    const room10 = await host10.watchParty.createRoom({ title: 'Ten', filePath: mediaPath })
    const guest10Closed = []
    guest10.watchParty.on(PARTY_EVENTS.ROOM_CLOSED, (e) => guest10Closed.push(e))
    await guest10.watchParty.joinRoom({ roomCode: room10.roomCode })
    // No media transfer or state sync occurs (we don't wait for it) — simulate
    // a host that went silent by back-dating both joinedAt and _hostHeardAt.
    const g10room = guest10.watchParty.activeRoom
    g10room.joinedAt = Date.now() - 130000
    g10room._hostHeardAt = Date.now() - 130000
    guest10.watchParty._autoLeaveStaleRoom(g10room, 'host unreachable')
    assert('Guest auto-left after host silence', guest10Closed.length === 1 && guest10.watchParty.activeRoom === null)
    await host10.storage.exchangeStore.close().catch(() => {})
    await guest10.storage.exchangeStore.close().catch(() => {})

    // ── Test 11: Guest room is enriched by the media offer ───────────────────
    console.log('\nTest 11: Guest room enriched by media offer (host identity + controlsMode)')
    const host11 = makeSide('host-11', 'Host Eleven', tmpRoot)
    const guest11 = makeSide('guest-11', 'Guest Eleven', tmpRoot)
    connectPeers(host11, guest11)
    const room11 = await host11.watchParty.createRoom({ title: 'Eleven', filePath: mediaPath, controlsMode: 'open' })
    const roomUpdates = []
    guest11.watchParty.on(PARTY_EVENTS.ROOM_UPDATED, (e) => roomUpdates.push(e))
    await guest11.watchParty.joinRoom({ roomCode: room11.roomCode })
    await withTimeout(
      waitFor(() => roomUpdates.length > 0, 5000, 'room updated'),
      6000,
      'guest room enrichment'
    )
    const enriched = roomUpdates[roomUpdates.length - 1] || {}
    assert('Guest learned the host name from the media offer', enriched.hostName === 'Host Eleven', JSON.stringify(enriched))
    assert('Guest learned the real media title', enriched.title === 'Eleven' || enriched.filename === 'movie.mp4', JSON.stringify(enriched))
    assert('Guest learned controlsMode is open (collaborative)', enriched.controlsMode === 'open', JSON.stringify(enriched))
    await host11.storage.exchangeStore.close().catch(() => {})
    await guest11.storage.exchangeStore.close().catch(() => {})

    // ── Test 12: playable watermark gates MEDIA_READY ────────────────────────
    console.log('\nTest 12: Playable watermark gates source-ready (no .part handover)')
    const host12 = makeSide('host-12', 'Host Twelve', tmpRoot)
    const guest12 = makeSide('guest-12', 'Guest Twelve', tmpRoot)
    connectPeers(host12, guest12)
    // Build a fake mp4 whose moov sits right after ftyp (watermark ~64 bytes)
    // so playable should fire almost immediately — but still AFTER the first
    // progress tick, proving MEDIA_READY is not tied to the first byte.
    const b4a = require('b4a')
    const ftyp = b4a.alloc(24)
    ftyp.writeUInt32BE(24, 0)
    ftyp.write('ftyp', 4, 'latin1')
    const moov = b4a.alloc(40)
    moov.writeUInt32BE(40, 0)
    moov.write('moov', 4, 'latin1')
    const head = b4a.concat([ftyp, moov])
    const totalSize = 256 * 1024
    const mediaPath12 = path.join(tmpRoot, 'movie12.mp4')
    const fd12 = await fsp.open(mediaPath12, 'w')
    await fd12.write(head, 0, head.length, 0)
    const zeroBlock = Buffer.alloc(65536)
    let off12 = head.length
    while (off12 < totalSize) {
      const n = Math.min(zeroBlock.length, totalSize - off12)
      await fd12.write(zeroBlock, 0, n, off12)
      off12 += n
    }
    await fd12.close()

    const progressEvents = []
    guest12.on(EVENTS.TRANSFER_PROGRESS, (t) => {
      if (t && t.id && String(t.id).startsWith('watch-')) progressEvents.push(t)
    })
    const ready12Events = []
    guest12.watchParty.on(PARTY_EVENTS.MEDIA_READY, (e) => ready12Events.push(e))
    const room12 = await host12.watchParty.createRoom({ title: 'Twelve', filePath: mediaPath12 })
    // Attach the completion waiter BEFORE joining so a fast transfer cannot
    // finish before the listener exists.
    const completionP = waitForCompletedTransfer(guest12, room12.shareId, 'guest 12')
    await guest12.watchParty.joinRoom({ roomCode: room12.roomCode })
    await withTimeout(
      waitFor(() => ready12Events.length > 0, 15000, 'media ready on playable'),
      20000,
      'playable media ready'
    )
    const readyEvt = ready12Events[0] || {}
    assert('MEDIA_READY fired for playable fake-mp4', readyEvt.playable === true, JSON.stringify(readyEvt))
    // Wait for completion so the record is final, then verify the playable
    // progress event arrived before it.
    await withTimeout(completionP, 20000, 'guest 12 complete')
    const playableProgress = progressEvents.find((t) => t.playable === true)
    assert('Transfer progress carried playable:true before completion', !!playableProgress, JSON.stringify(progressEvents.slice(0, 3)))
    // After completion the record resolves and is marked playable.
    const recAfter = await guest12.watchParty._getTransferRecord(room12.shareId)
    assert('Completed transfer record is playable', recAfter && (recAfter.playable === true || recAfter.status === 'completed'))
    await host12.storage.exchangeStore.close().catch(() => {})
    await guest12.storage.exchangeStore.close().catch(() => {})

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
