'use strict'

// Fast in-memory unit tests for the simplified single-owner sync model and the
// transfer-log retention — no DHT, no network, runs in a few seconds.
//
//   node test-sync-unit.js

const fs = require('fs')
const os = require('os')
const path = require('path')

const { SyncEngine } = require('./engine/SyncEngine.js')
const { TransferEngine } = require('./engine/TransferEngine.js')
const { fsp, path: p } = require('./compat.js')
const { MESSAGES } = require('./protocol.js')

let passed = 0
function ok(name, cond, detail) {
  if (cond) {
    passed++
    console.log(`PASS  ${name}`)
  } else {
    console.error(`FAIL  ${name}${detail !== undefined ? ' — ' + detail : ''}`)
    process.exitCode = 1
  }
}

const harnesses = [] // SyncEngine instances to stop (closes fs.watch handles)

// Minimal in-memory Hyperbee substitute (put/get/del/createReadStream + batch).
function makeBee() {
  const map = new Map()
  return {
    async put(key, value) { map.set(key, value) },
    async get(key) { return map.has(key) ? { key, value: map.get(key) } : null },
    async del(key) { map.delete(key) },
    async *createReadStream(range) {
      const keys = Array.from(map.keys()).sort()
      for (const key of keys) {
        if (range) {
          if (range.gte && key < range.gte) continue
          if (range.lte && key > range.lte) continue
        }
        yield { key, value: map.get(key) }
      }
    },
    batch() {
      const ops = []
      return {
        async put(key, value) { ops.push(['put', key, value]) },
        async del(key) { ops.push(['del', key]) },
        async flush() {
          for (const op of ops) {
            if (op[0] === 'put') map.set(op[1], op[2])
            else map.delete(op[1])
          }
        },
        async close() {}
      }
    },
    size() { return map.size }
  }
}

function makeSyncHarness(tmpRoot) {
  const sends = []
  const sentMessages = []
  const events = []
  const peers = new Map([
    ['peer1', {
      connection: {},
      signaling: { send: (m) => sentMessages.push(m) },
      device: { id: 'dev1', publicKey: 'peer1', name: 'Peer 1' },
      pairing: { trusted: true }
    }]
  ])
  const sync = new SyncEngine({
    getBee: async () => makeBee(),
    getPeers: () => peers,
    getPeerId: () => 'local-key',
    sendEvent: (e, d) => events.push([e, d]),
    transferEngine: { startSend: async (params) => { sends.push(params) } },
    downloadsDir: path.join(tmpRoot, 'downloads'),
    fsp,
    path: p,
    fs,
    autoAcceptOffers: true,
    verifyTimeoutMs: 20 // fast fallback for the batch pre-verification in tests
  })
  harnesses.push(sync)
  return { sync, peers, sends, sentMessages, events }
}

// Same harness, but all getBee calls share one bee — persistence round-trips
// (restart simulation) actually read back what was written.
function makePersistentHarness(tmpRoot, sharedBee, opts = {}) {
  const sends = []
  const sentMessages = []
  const events = []
  const peers = new Map([
    ['peer1', {
      connection: {},
      signaling: { send: (m) => sentMessages.push(m) },
      device: { id: 'dev1', publicKey: 'peer1', name: 'Peer 1' },
      pairing: { trusted: true }
    }]
  ])
  const sync = new SyncEngine({
    getBee: async () => sharedBee,
    getPeers: () => peers,
    getPeerId: () => 'local-key',
    sendEvent: (e, d) => events.push([e, d]),
    transferEngine: { startSend: async (params) => { sends.push(params) } },
    downloadsDir: path.join(tmpRoot, 'downloads'),
    fsp,
    path: p,
    fs,
    autoAcceptOffers: true,
    verifyTimeoutMs: opts.verifyTimeoutMs || 20,
    ...opts
  })
  harnesses.push(sync)
  return { sync, peers, sends, sentMessages, events }
}

// Wire two harnesses together so SYNC_VERIFY / SYNC_VERIFY_RESULT actually
// round-trip through the real handlers (no network, full message flow). The
// owner's startSend also SIMULATES real delivery: the file is copied into the
// receiver's library folder, so later verify rounds see it as present (the
// self-healing re-check then behaves like the real system).
function linkHarnesses(owner, receiver) {
  const ownerPeer = owner.peers.get('peer1')
  const receiverPeer = receiver.peers.get('peer1')
  owner.sync.transferEngine.startSend = async (params) => {
    owner.sends.push(params)
    const recvLib = receiver.sync.libraries.get(params.syncLibraryId)
    if (recvLib && params.syncRelPath && params.filePath) {
      const dest = path.join(recvLib.localPath, ...params.syncRelPath.split('/'))
      await fsp.mkdir(path.dirname(dest), { recursive: true })
      await fsp.copyFile(params.filePath, dest).catch(() => {})
    }
  }
  ownerPeer.signaling.send = async (msg) => {
    owner.sentMessages.push(msg)
    if (msg && msg.type === MESSAGES.SYNC_VERIFY) {
      await receiver.sync.handleSyncVerify('peer1', msg)
    }
  }
  receiverPeer.signaling.send = async (msg) => {
    receiver.sentMessages.push(msg)
    if (msg && msg.type === MESSAGES.SYNC_VERIFY_RESULT) {
      await owner.sync.handleSyncVerifyResult('peer1', msg)
    }
  }
}

async function main() {
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'sync-unit-'))
  try {
    // ── 1. One-way push: owner diffs against its own sentIndex ───────────────
    {
      const { sync, sends, sentMessages } = makeSyncHarness(tmpRoot)
      const srcDir = path.join(tmpRoot, 'push-src')
      fs.mkdirSync(path.join(srcDir, 'sub'), { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa')
      fs.writeFileSync(path.join(srcDir, 'sub', 'b.bin'), 'bbb')

      const lib = await sync.addLibrary({ path: srcDir, peerId: 'peer1', name: 'PushLib', mode: 'send-only' })
      ok('push library created with push mode', lib.mode === 'push', lib.mode)
      await sync.handleSyncInviteAccept('peer1', { libraryId: lib.id })
      await sync.syncLibrary(lib.id)
      const rels = sends.map((s) => s.syncRelPath).sort()
      ok('push sends every new file once', JSON.stringify(rels) === JSON.stringify(['a.txt', 'sub/b.bin']), JSON.stringify(rels))
      ok('push sends streaming params', sends.every((s) => s.source === 'sync' && s.syncLibraryId === lib.id && s.syncMtimeMs > 0))
      ok('push does NOT announce SYNC_INDEX (owner tracks its own sentIndex)', !sentMessages.some((m) => m.type === MESSAGES.SYNC_INDEX))

      // Change one file → only it is re-pushed.
      sends.length = 0
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa-changed')
      await sync.syncLibrary(lib.id)
      ok('push re-sends ONLY the changed file', sends.length === 1 && sends[0].syncRelPath === 'a.txt', JSON.stringify(sends.map((s) => s.syncRelPath)))

      // Delete a file → pure backup: NO delete propagation, no re-push.
      sends.length = 0
      sentMessages.length = 0
      fs.unlinkSync(path.join(srcDir, 'sub', 'b.bin'))
      await sync.syncLibrary(lib.id)
      ok('push does NOT propagate deletes (true backup)', sends.length === 0 && !sentMessages.some((m) => m.type === MESSAGES.SYNC_DELETE), `sends=${sends.length}`)

      // Failed push clears the sentIndex mark → next round re-pushes.
      sends.length = 0
      fs.writeFileSync(path.join(srcDir, 'c.txt'), 'ccc')
      await sync.syncLibrary(lib.id)
      ok('new file pushed', sends.length === 1 && sends[0].syncRelPath === 'c.txt')
      sync.handleTransferTerminal({ source: 'sync', syncLibraryId: lib.id, syncRelPath: 'c.txt', status: 'failed', direction: 'send' })
      sends.length = 0
      await sync.syncLibrary(lib.id)
      ok('failed push is re-pushed on the next round', sends.length === 1 && sends[0].syncRelPath === 'c.txt', `sends=${sends.length}`)
    }

    // ── 2. Receive-only: a pure sink ─────────────────────────────────────────
    {
      const { sync, sends, sentMessages } = makeSyncHarness(tmpRoot)
      const recvDir = path.join(tmpRoot, 'recv-lib')
      await sync.handleSyncInvite('peer1', { libraryId: 'lib-recv', name: 'RecvLib', mode: 'push' })
      const accepted = await sync.acceptSyncInvite({ id: 'lib-recv', customPath: recvDir })
      const lib = sync.libraries.get('lib-recv')
      ok('receiver library is receive_only', lib && lib.mode === 'receive_only', lib && lib.mode)
      ok('receiver accepted into custom path', accepted.path === recvDir, accepted.path)
      sends.length = 0
      sentMessages.length = 0
      await sync.syncLibrary('lib-recv')
      ok('receive_only sink never pushes', sends.length === 0)
      ok('receive_only sink never announces an index', !sentMessages.some((m) => m.type === MESSAGES.SYNC_INDEX))
    }

    // ── 3. Two-way (desktop↔desktop) keeps index exchange + deletes ─────────
    {
      const { sync, sends, sentMessages } = makeSyncHarness(tmpRoot)
      const srcDir = path.join(tmpRoot, 'tw-src')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'd.txt'), 'ddd')
      const lib = await sync.addLibrary({ path: srcDir, peerId: 'peer1', name: 'TwoWay', mode: 'two-way' })
      ok('two-way library keeps two-way mode', lib.mode === 'two-way', lib.mode)
      await sync.handleSyncInviteAccept('peer1', { libraryId: lib.id })
      await sync.syncLibrary(lib.id)
      ok('two-way announces its index', sentMessages.some((m) => m.type === MESSAGES.SYNC_INDEX))
      ok('two-way pushes its files', sends.length === 1 && sends[0].syncRelPath === 'd.txt')
    }

    // ── 4. Incompatible peers are gated from sync ───────────────────────────
    {
      const { sync, peers, sends } = makeSyncHarness(tmpRoot)
      const srcDir = path.join(tmpRoot, 'inc-src')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'e.txt'), 'eee')
      const lib = await sync.addLibrary({ path: srcDir, peerId: 'peer1', name: 'IncLib', mode: 'send-only' })
      // Mark the peer incompatible BEFORE the accept handler pushes.
      peers.get('peer1').incompatible = true
      await sync.handleSyncInviteAccept('peer1', { libraryId: lib.id })
      await sync.syncLibrary(lib.id)
      ok('incompatible peer is not pushed to', sends.length === 0, `sends=${sends.length}`)
    }

    // ── 5. Transfer-log retention ────────────────────────────────────────────
    {
      const bee = makeBee()
      const te = new TransferEngine({
        getBee: async () => bee,
        exchangeStore: { get: () => ({ ready: async () => {}, close: async () => {}, append: async () => {}, length: 0, key: Buffer.alloc(32) }) },
        sendEvent: () => {},
        getPeers: () => new Map(),
        getDeviceIdentity: () => ({ id: 'dev', name: 'Test' }),
        getDownloadDirectory: async () => tmpRoot,
        getTransferMethod: () => 'dht',
        fsp,
        path: p
      })
      for (let i = 0; i < 250; i++) {
        await bee.put(`t-${i}`, { id: `t-${i}`, status: 'completed', completedAt: new Date(2026, 0, 1, 0, 0, i).toISOString() })
      }
      await bee.put('__meta__', { schema: 'transfer.2' })
      await te._pruneTerminalTransfers()
      let remaining = 0
      for await (const node of bee.createReadStream()) {
        if (node.key !== '__meta__') remaining++
      }
      ok('terminal transfer records pruned to 200', remaining === 200, `remaining=${remaining}`)
    }

    // ── 6. A transfer cancelled while queued must never start (pause bug) ────
    {
      const bee = makeBee()
      const runs = new Map()
      const te = new TransferEngine({
        getBee: async () => bee,
        exchangeStore: { get: () => ({ ready: async () => {}, close: async () => {}, append: async () => {}, length: 0, key: Buffer.alloc(32) }) },
        sendEvent: () => {},
        getPeers: () => new Map(),
        getDeviceIdentity: () => ({ id: 'dev', name: 'Test' }),
        getDownloadDirectory: async () => tmpRoot,
        getTransferMethod: () => 'dht',
        fsp,
        path: p
      })
      te.runs = runs
      const transfer = {
        id: 'transfer-pause-test',
        direction: 'send',
        source: 'sync',
        isSync: true,
        syncLibraryId: 'lib-1',
        filename: 'a.jpg',
        fileSize: 100,
        filePath: path.join(tmpRoot, 'a.jpg'),
        peerId: 'peer-x',
        status: 'queued',
        priority: 'bulk'
      }
      await bee.put(transfer.id, transfer)
      // Simulate pause: cancel all sync transfers for the library.
      await te.cancelSyncTransfers('lib-1')
      // The queue pops it afterwards — it must NOT start a run.
      await te._startTransfer({ ...transfer })
      ok('cancelled-while-queued transfer does not start', runs.size === 0, `runs=${runs.size}`)
      const after = await bee.get(transfer.id)
      ok('cancelled transfer record stays cancelled', after && after.value && (after.value.status === 'cancelled' || after.value.status === 'interrupted'), after && after.value && after.value.status)
    }

    // ── 7. Deleting a library removes the peer's side too ────────────────────
    {
      const { sync, sentMessages } = makeSyncHarness(tmpRoot)
      const srcDir = path.join(tmpRoot, 'del-src')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'f.txt'), 'fff')
      const lib = await sync.addLibrary({ path: srcDir, peerId: 'peer1', name: 'DelLib', mode: 'send-only' })
      await sync.handleSyncInviteAccept('peer1', { libraryId: lib.id })
      // Owner deletes → the peer is notified.
      await sync.removeLibrary(lib.id)
      ok('owner removal notifies the peer (SYNC_REMOVE)', sentMessages.some((m) => m.type === MESSAGES.SYNC_REMOVE && m.libraryId === lib.id))
      ok('owner library removed locally', !sync.libraries.has(lib.id))

      // The peer mirrors the removal: library is gone locally, files untouched.
      const { sync: peer } = makeSyncHarness(tmpRoot)
      await peer.handleSyncInvite('peer1', { libraryId: 'lib-peer', name: 'PeerLib', mode: 'push' })
      await peer.acceptSyncInvite({ id: 'lib-peer', customPath: path.join(tmpRoot, 'peer-lib') })
      ok('peer had the receive library', peer.libraries.has('lib-peer'))
      await peer.handleSyncRemove('peer1', { libraryId: 'lib-peer' })
      ok('peer library removed on SYNC_REMOVE', !peer.libraries.has('lib-peer'))
      // A later re-announce of the same id must NOT recreate it.
      await peer.handleSyncInvite('peer1', { libraryId: 'lib-peer', name: 'PeerLib', mode: 'push' })
      ok('removed library is not re-created by a re-announce', !peer.libraries.has('lib-peer') && !peer.pendingInvites.has('lib-peer'))
    }

    // ── 13. accepted survives restart; re-invites for live links auto-accept ──
    {
      const bee = makeBee()
      const { sync } = makePersistentHarness(tmpRoot, bee)
      const srcDir = path.join(tmpRoot, 'persist-src')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa')

      const lib = await sync.addLibrary({ path: srcDir, peerId: 'peer1', name: 'PersistLib', mode: 'send-only' })
      ok('fresh peer-bound library starts unaccepted', lib.status === 'waiting_peer' && sync.libraries.get(lib.id).accepted === false)
      // The peer accepts (which also pushes the initial file set).
      await sync.handleSyncInviteAccept('peer1', { libraryId: lib.id })
      ok('accept marks the library accepted', sync.libraries.get(lib.id).accepted === true)

      // "Restart": a new engine over the same bee must restore the accept state.
      const { sync: sync2, sends: sends2, sentMessages: sm2, events: ev2 } = makePersistentHarness(tmpRoot, bee)
      await sync2.loadLibraries()
      const restored = sync2.libraries.get(lib.id)
      ok('accepted survives restart', !!restored && restored.accepted === true, restored && `accepted=${restored.accepted}`)
      // A file lands while the app was down → resume pushes it directly.
      fs.writeFileSync(path.join(srcDir, 'new.txt'), 'new')
      const before = sm2.length
      await sync2.syncLibrary(lib.id)
      ok('restored library pushes without re-inviting', !sm2.slice(before).some((m) => m.type === MESSAGES.SYNC_INVITE))
      ok('restored library pushes new files only', JSON.stringify(sends2.map((s) => s.syncRelPath).sort()) === JSON.stringify(['new.txt']), JSON.stringify(sends2.map((s) => s.syncRelPath)))

      // A re-announced invite for the existing library is re-confirmed silently.
      const before2 = sm2.length
      const invitesBefore = ev2.filter(([e]) => e === 'sync:invite:received').length
      await sync2.handleSyncInvite('peer1', { libraryId: lib.id, name: 'PersistLib', mode: 'push' })
      ok('re-invite for a live link auto-accepts', sm2.slice(before2).some((m) => m.type === MESSAGES.SYNC_INVITE_ACCEPT && m.libraryId === lib.id))
      ok('re-invite does not raise a modal event', ev2.filter(([e]) => e === 'sync:invite:received').length === invitesBefore)
      ok('re-invite does not re-add to pending invites', !sync2.pendingInvites.has(lib.id))
    }

    // ── 14. legacy records (no accepted key) restore as accepted; explicit
    //        accepted:false stays waiting ─────────────────────────────────────
    {
      const bee = makeBee()
      await bee.put('sync-legacy', {
        id: 'sync-legacy',
        name: 'Legacy',
        localPath: path.join(tmpRoot, 'legacy-src'),
        peerId: 'peer1',
        mode: 'push',
        index: {}, remoteIndex: {}, sentIndex: {}, lastScanAt: 0, lastSyncAt: 0
      })
      await bee.put('sync-never', {
        id: 'sync-never',
        name: 'Never',
        localPath: path.join(tmpRoot, 'never-src'),
        peerId: 'peer1',
        mode: 'push',
        accepted: false,
        index: {}, remoteIndex: {}, sentIndex: {}, lastScanAt: 0, lastSyncAt: 0
      })
      const { sync } = makePersistentHarness(tmpRoot, bee)
      await sync.loadLibraries()
      ok('legacy record restores as accepted', sync.libraries.get('sync-legacy').accepted === true)
      ok('explicit accepted:false stays waiting', sync.libraries.get('sync-never').accepted === false)
    }

    // ── 15. batch pre-verification: existing files never become transfers ────
    {
      const bee = makeBee()
      const ownerH = makePersistentHarness(tmpRoot, bee, { verifyTimeoutMs: 1000 })
      const receiverH = makePersistentHarness(tmpRoot, bee, { verifyTimeoutMs: 1000 })
      const { sync: owner, sends: ownerSends } = ownerH
      const { sync: receiver } = receiverH
      linkHarnesses(ownerH, receiverH)

      const srcDir = path.join(tmpRoot, 'verify-src')
      const dstDir = path.join(tmpRoot, 'verify-dst')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.mkdirSync(dstDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa')
      fs.writeFileSync(path.join(srcDir, 'b.txt'), 'bbb')
      fs.writeFileSync(path.join(srcDir, 'c.txt'), 'ccc')
      // The receiver already holds a.txt and b.txt byte-identical.
      fs.copyFileSync(path.join(srcDir, 'a.txt'), path.join(dstDir, 'a.txt'))
      fs.copyFileSync(path.join(srcDir, 'b.txt'), path.join(dstDir, 'b.txt'))

      const lib = await owner.addLibrary({ path: srcDir, peerId: 'peer1', name: 'VerifyLib', mode: 'send-only' })
      await receiver.handleSyncInvite('peer1', { libraryId: lib.id, name: 'VerifyLib', mode: 'push' })
      await receiver.acceptSyncInvite({ id: lib.id, customPath: dstDir })
      // Accepting on the owner triggers the diff + verify + push round.
      await owner.handleSyncInviteAccept('peer1', { libraryId: lib.id })

      ok('verify: only missing files become transfers', JSON.stringify(ownerSends.map((s) => s.syncRelPath).sort()) === JSON.stringify(['c.txt']), JSON.stringify(ownerSends.map((s) => s.syncRelPath)))
      ok('verify: SYNC_VERIFY was sent', ownerH.sentMessages.some((m) => m.type === MESSAGES.SYNC_VERIFY))
      ok('verify: verified files marked in sentIndex', !!(owner.libraries.get(lib.id).sentIndex['a.txt'] && owner.libraries.get(lib.id).sentIndex['b.txt']))
      // 2 verified marks + the optimistic mark for the real transfer (c.txt).
      ok('verify: verified marks persisted as rows', (await owner._iterRows('sent', lib.id)).length === 3)

      // Second round: everything is known — nothing pushed, no verify needed.
      ownerSends.length = 0
      await owner.syncLibrary(lib.id)
      ok('verify: second round pushes nothing', ownerSends.length === 0, `sends=${ownerSends.length}`)
    }

    // ── 16. verify fallback: an unresponsive peer still gets every file ──────
    {
      const h = makeSyncHarness(tmpRoot)
      const { sync, sends } = h
      const srcDir = path.join(tmpRoot, 'fallback-src')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa')
      const lib = await sync.addLibrary({ path: srcDir, peerId: 'peer1', name: 'FbLib', mode: 'send-only' })
      await sync.handleSyncInviteAccept('peer1', { libraryId: lib.id })
      await sync.syncLibrary(lib.id)
      ok('verify fallback: unresponsive peer → per-file flow still sends', sends.length === 1 && sends[0].syncRelPath === 'a.txt', `sends=${sends.length}`)
      ok('verify fallback: SYNC_VERIFY was attempted', h.sentMessages.some((m) => m.type === MESSAGES.SYNC_VERIFY))
    }

    // ── 17. legacy config rows migrate to per-file rows on load ──────────────
    {
      const bee = makeBee()
      await bee.put('sync-mig', {
        id: 'sync-mig',
        name: 'Mig',
        localPath: path.join(tmpRoot, 'mig-src'),
        peerId: 'peer1',
        mode: 'push',
        index: { 'a.txt': { size: 3, mtimeMs: 111, sig: '3-111', authorKey: 'k', deleted: false } },
        sentIndex: { 'a.txt': { sig: '3-111' } },
        remoteIndex: {},
        lastScanAt: 0,
        lastSyncAt: 0
      })
      const { sync } = makePersistentHarness(tmpRoot, bee)
      await sync.loadLibraries()
      const lib = sync.libraries.get('sync-mig')
      ok('migration: index loaded', !!(lib.index['a.txt'] && lib.index['a.txt'].sig === '3-111'))
      ok('migration: sentIndex loaded', !!lib.sentIndex['a.txt'])
      const stored = await bee.get('sync-mig')
      ok('migration: config row no longer embeds the maps', !stored.value.index && !stored.value.sentIndex)
      ok('migration: per-file rows written', (await sync._iterRows('index', 'sync-mig')).length === 1 && (await sync._iterRows('sent', 'sync-mig')).length === 1)
    }

    // ── 18. lean per-file manifest: a change writes only its own row ─────────
    {
      const bee = makeBee()
      const { sync } = makePersistentHarness(tmpRoot, bee)
      const srcDir = path.join(tmpRoot, 'rows-src')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa')
      const lib = await sync.addLibrary({ path: srcDir, peerId: 'peer1', name: 'RowsLib', mode: 'send-only' })
      const cfg = (await bee.get(lib.id)).value
      ok('rows: config row has no embedded maps', !cfg.index && !cfg.sentIndex && !cfg.remoteIndex)
      ok('rows: initial index row written', (await sync._iterRows('index', lib.id)).length === 1)

      // A new file + a changed file → exactly two row writes (no full rewrite).
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa-changed')
      fs.writeFileSync(path.join(srcDir, 'b.txt'), 'bbb')
      await sync.syncLibrary(lib.id)
      const rows = await sync._iterRows('index', lib.id)
      const aRow = rows.find(([r]) => r === 'a.txt')
      ok('rows: new file added as its own row', rows.length === 2 && rows.some(([r]) => r === 'b.txt'))
      ok('rows: changed file row updated in place', aRow && aRow[1].size === 11, aRow && `size=${aRow[1].size}`)
    }

    // ── 19. run phases: analyzing → transferring → synced ────────────────────
    {
      const { sync, events } = makeSyncHarness(tmpRoot)
      const srcDir = path.join(tmpRoot, 'phase-src')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa')
      const lib = await sync.addLibrary({ path: srcDir, peerId: 'peer1', name: 'PhaseLib', mode: 'send-only' })
      await sync.handleSyncInviteAccept('peer1', { libraryId: lib.id })
      await sync.syncLibrary(lib.id)
      const phases = events.filter(([e]) => e === 'sync:phase').map(([, d]) => d.phase)
      ok('phases: analyzing emitted', phases.includes('analyzing'), JSON.stringify(phases))
      ok('phases: transferring emitted for real payload', phases.includes('transferring'))
      ok('phases: not synced while payload is pending', !phases.includes('synced'))
      // The last transfer finishes → the round completes.
      sync.handleTransferTerminal({ source: 'sync', syncLibraryId: lib.id, syncRelPath: 'a.txt', status: 'completed', direction: 'send' })
      const phases2 = events.filter(([e]) => e === 'sync:phase').map(([, d]) => d.phase)
      ok('phases: round completes to synced after last transfer', phases2[phases2.length - 1] === 'synced')
    }

    // ── 20. interrupted transfers never advance the completion counter ───────
    {
      const { sync, events } = makeSyncHarness(tmpRoot)
      const srcDir = path.join(tmpRoot, 'counter-src')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa')
      fs.writeFileSync(path.join(srcDir, 'b.txt'), 'bbb')
      const lib = await sync.addLibrary({ path: srcDir, peerId: 'peer1', name: 'CntLib', mode: 'send-only' })
      await sync.handleSyncInviteAccept('peer1', { libraryId: lib.id })
      await sync.syncLibrary(lib.id)
      // a.txt dies mid-flight (cancelled), b.txt genuinely completes.
      sync.handleTransferTerminal({ source: 'sync', syncLibraryId: lib.id, syncRelPath: 'a.txt', status: 'cancelled', direction: 'send' })
      sync.handleTransferTerminal({ source: 'sync', syncLibraryId: lib.id, syncRelPath: 'b.txt', status: 'completed', direction: 'send' })
      const phases = events.filter(([e]) => e === 'sync:phase').map(([, d]) => d)
      const lastTransferring = phases.filter((d) => d.phase === 'transferring').pop()
      ok('counter: interrupted transfer does not count as done', !!lastTransferring && lastTransferring.done === 1, JSON.stringify(lastTransferring))
      ok('counter: round finishes when in-flight hits zero', phases[phases.length - 1].phase === 'synced')
    }

    // ── 21. a round that ends without transfers still resolves the phase ─────
    {
      const h = makeSyncHarness(tmpRoot)
      const { sync, events } = h
      const srcDir = path.join(tmpRoot, 'offline-src')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa')
      const lib = await sync.addLibrary({ path: srcDir, peerId: 'peer1', name: 'OffLib', mode: 'send-only' })
      await sync.handleSyncInviteAccept('peer1', { libraryId: lib.id })
      // Peer disappears mid-session → the round must still end on a resolved
      // phase, never leave a stale "Analyzing N/N" on screen.
      h.peers.clear()
      await sync.syncLibrary(lib.id)
      const phases = events.filter(([e]) => e === 'sync:phase').map(([, d]) => d.phase)
      ok('offline round resolves to synced (never stuck analyzing)', phases[phases.length - 1] === 'synced', JSON.stringify(phases))
      ok('offline round never claims Synchronized (waiting_peer)', sync.libraries.get(lib.id).status === 'waiting_peer', sync.libraries.get(lib.id).status)
    }

    // ── 22. self-healing backup: files lost on the receiver are re-pushed ────
    {
      const bee = makeBee()
      const ownerH = makePersistentHarness(tmpRoot, bee, { verifyTimeoutMs: 1000 })
      const receiverH = makePersistentHarness(tmpRoot, bee, { verifyTimeoutMs: 1000 })
      const { sync: owner, sends: ownerSends } = ownerH
      const { sync: receiver } = receiverH
      linkHarnesses(ownerH, receiverH)

      const srcDir = path.join(tmpRoot, 'heal-src')
      const dstDir = path.join(tmpRoot, 'heal-dst')
      fs.mkdirSync(srcDir, { recursive: true })
      fs.mkdirSync(dstDir, { recursive: true })
      fs.writeFileSync(path.join(srcDir, 'a.txt'), 'aaa')
      fs.writeFileSync(path.join(srcDir, 'b.txt'), 'bbb')

      const lib = await owner.addLibrary({ path: srcDir, peerId: 'peer1', name: 'HealLib', mode: 'send-only' })
      await receiver.handleSyncInvite('peer1', { libraryId: lib.id, name: 'HealLib', mode: 'push' })
      await receiver.acceptSyncInvite({ id: lib.id, customPath: dstDir })
      await owner.handleSyncInviteAccept('peer1', { libraryId: lib.id })
      ok('heal: initial push delivered both files', ownerSends.length === 2, `sends=${ownerSends.length}`)

      // The receiver's copy of a.txt vanishes (user deleted it on the desktop).
      fs.rmSync(path.join(dstDir, 'a.txt'))
      ownerSends.length = 0
      await owner.syncLibrary(lib.id)

      // The verify round must detect the loss and re-push a.txt.
      ok('heal: lost receiver file is re-pushed', JSON.stringify(ownerSends.map((s) => s.syncRelPath).sort()) === JSON.stringify(['a.txt']), JSON.stringify(ownerSends.map((s) => s.syncRelPath)))
      ok('heal: surviving files are not re-pushed', !ownerSends.some((s) => s.syncRelPath === 'b.txt'))

      // Next round: everything is present again — nothing to do.
      ownerSends.length = 0
      await owner.syncLibrary(lib.id)
      ok('heal: round after restore pushes nothing', ownerSends.length === 0, `sends=${ownerSends.length}`)
    }

    console.log(`\n${passed} unit checks passed`)
    // Close watcher handles so the process can exit.
    for (const sync of harnesses) {
      await sync.stop().catch(() => {})
    }
  } finally {
    fs.rmSync(tmpRoot, { recursive: true, force: true })
  }
}

main().catch((err) => {
  console.error('UNIT TEST CRASH:', err)
  process.exit(1)
})
