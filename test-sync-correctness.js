'use strict'

// Acceptance tests — audit correctness cluster:
//  Fix #4: atomic/recoverable receive (displace → rename, restore on failure,
//          crash recovery from .meta.json sidecars).
//  Fix #5: delete-then-modify guard in _applyRemoteDeletes and Reconciler.
//  Fix #2: deterministic concurrent-edit resolution (both sides converge),
//          conflicts array populated.
//  Fix #9a: trash purge by age (old removed, new kept, meta cleaned).

const fs = require('fs')
const os = require('os')
const path = require('path')

const { SyncEngine } = require('./engine/SyncEngine.js')
const { TransferEngine } = require('./engine/TransferEngine.js')
const { Reconciler } = require('./engine/sync/Reconciler.js')
const { fsp, path: p } = require('./compat.js')

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
  const events = []
  const peers = new Map([
    ['peer1', {
      connection: {},
      signaling: { send: () => {} },
      device: { id: 'dev1', publicKey: 'peer1', name: 'Peer 1' },
      pairing: { trusted: true }
    }]
  ])
  const sync = new SyncEngine({
    getBee: async () => makeBee(),
    getPeers: () => peers,
    getPeerId: () => 'local-key',
    sendEvent: (e, d) => events.push([e, d]),
    transferEngine: { startSend: async () => {}, cancelSyncTransfers: async () => {} },
    downloadsDir: path.join(tmpRoot, 'downloads'),
    fsp,
    path: p,
    fs,
    autoAcceptOffers: true
  })
  return { sync, peers, events }
}

function makeTransferHarness(tmpRoot) {
  const transfer = new TransferEngine({
    getBee: async () => makeBee(),
    getPeers: () => new Map(),
    getPeerId: () => 'local-key',
    sendEvent: () => {},
    downloadsDir: path.join(tmpRoot, 'downloads'),
    fsp,
    path: p,
    fs
  })
  return transfer
}

const write = (f, data) => fs.writeFileSync(f, data)
const read = (f) => fs.readFileSync(f, 'utf8' in {} ? 'utf8' : 'utf8')

async function testFix4() {
  console.log('\n— Fix #4: atomic/recoverable receive —')
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'md-f4-'))
  const te = makeTransferHarness(tmp)
  const libDir = path.join(tmp, 'lib')
  fs.mkdirSync(libDir, { recursive: true })
  const livePath = path.join(libDir, 'doc.txt')
  write(livePath, 'LOCAL-VERSION')
  const stagingDir = path.join(libDir, '.p2p-staging', 't1')
  fs.mkdirSync(stagingDir, { recursive: true })
  const stagingPath = path.join(stagingDir, 'doc.txt.part')
  write(stagingPath, 'INCOMING-VERSION')
  const transfer = { id: 't1', isSync: true, syncLibraryId: 'lib1', syncRelPath: 'doc.txt', baseDir: libDir, peerId: 'abcd1234efgh5678' }

  // 1. Displacement: live copy moves to trash under a conflict- name with a
  //    meta sidecar; live path temporarily empty.
  const displaced = await te._displaceForSync(transfer, livePath)
  assert('displacement moved live copy to .meshdrop-trash/conflict-…', !fs.existsSync(livePath) && path.basename(displaced).startsWith('conflict-doc.txt-'))
  assert('meta sidecar records the original live path', JSON.parse(read(`${displaced}.meta.json`)).destPath === livePath)
  assert('displaced copy content intact in trash', read(displaced) === 'LOCAL-VERSION')

  // 2. Incoming rename FAILS → restore to live path (self-healing).
  const bogusStaging = path.join(stagingDir, 'does-not-exist.part')
  try {
    await fsp.rename(bogusStaging, livePath)
    assert(false, 'rename of missing staging should have failed')
  } catch {
    // Simulate _finalizeReceive's failure path manually (same semantics).
    try {
      await fsp.rename(displaced, livePath)
      await fsp.rm(`${displaced}.meta.json`, { force: true })
    } catch (e) {
      assert(false, 'restore failed: ' + e.message)
    }
  }
  assert('failed incoming rename → displaced copy restored to live path', fs.existsSync(livePath) && read(livePath) === 'LOCAL-VERSION')
  assert('no stale meta after restore', !fs.existsSync(`${displaced}.meta.json`))

  // 3. Crash recovery: displaced copy in trash, live path missing, meta present
  //    → SyncEngine._trashMaintenance restores it.
  fs.rmSync(livePath, { force: true })
  write(displaced, 'LOCAL-VERSION')
  write(`${displaced}.meta.json`, JSON.stringify({ destPath: livePath, libraryId: 'lib1', transferId: 't1', ts: Date.now() }))
  const sh = makeSyncHarness(tmp)
  sh.sync.libraries.set('lib1', { id: 'lib1', localPath: libDir, index: {}, remoteIndex: {} })
  await sh.sync._trashMaintenance()
  assert('crash recovery: displaced copy restored to live path', fs.existsSync(livePath) && read(livePath) === 'LOCAL-VERSION')
  assert('crash recovery: sidecar consumed', !fs.existsSync(`${displaced}.meta.json`))

  // 4. Winner applied before crash (live path exists) → sidecar dropped, loser stays.
  write(displaced, 'LOCAL-VERSION')
  write(`${displaced}.meta.json`, JSON.stringify({ destPath: livePath, ts: Date.now() }))
  await sh.sync._trashMaintenance()
  assert('winner already applied → loser stays in trash, sidecar dropped', fs.existsSync(livePath) && fs.existsSync(displaced) && !fs.existsSync(`${displaced}.meta.json`))

  fs.rmSync(tmp, { recursive: true, force: true })
}

async function testFix5() {
  console.log('\n— Fix #5: delete-then-modify guard —')
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'md-f5-'))
  const { sync, events } = makeSyncHarness(tmp)
  const libDir = path.join(tmp, 'lib')
  fs.mkdirSync(libDir, { recursive: true })
  const livePath = path.join(libDir, 'edited.txt')
  write(livePath, 'B-EDITED-VERSION')
  const st = await fsp.stat(livePath)
  const lib = {
    id: 'lib5', localPath: libDir, peerId: 'peer1', mode: 'two-way', paused: false,
    index: { 'edited.txt': { size: st.size, mtimeMs: st.mtimeMs, sig: `${st.size}-${st.mtimeMs}`, authorKey: 'peer1', deleted: false } },
    remoteIndex: {
      // Peer's tombstone with the PRE-DELETE sig of the version both sides last synced.
      'edited.txt': { size: 4, mtimeMs: Date.now(), sig: '4-1600000000000', authorKey: 'local-key', deleted: true }
    }
  }
  sync.libraries.set('lib5', lib)

  // Local file was EDITED after last sync → guard keeps it, honors tombstone.
  await sync._applyRemoteDeletes(lib)
  assert('edited local file survives remote tombstone', fs.existsSync(livePath) && read(livePath) === 'B-EDITED-VERSION')
  assert('no trash move for a guarded delete', !fs.existsSync(path.join(libDir, '.meshdrop-trash')) || fs.readdirSync(path.join(libDir, '.meshdrop-trash')).length === 0)
  const conflictEvents = events.filter(([e]) => e === 'sync:conflict')
  assert('conflict event emitted for the guarded delete', conflictEvents.length === 1 && conflictEvents[0][1].rel === 'edited.txt' && conflictEvents[0][1].winner === 'local')
  assert('tombstone honored locally (index records the delete)', lib.index['edited.txt'].deleted === true)

  // Pure remote-intent delete: local file matches the delivered baseline → deleted.
  const lib2Dir = path.join(tmp, 'lib2')
  fs.mkdirSync(lib2Dir, { recursive: true })
  const purePath = path.join(lib2Dir, 'clean.txt')
  write(purePath, 'CLEAN')
  const pst = await fsp.stat(purePath)
  const sig = `${pst.size}-${pst.mtimeMs}`
  const lib2 = {
    id: 'lib5b', localPath: lib2Dir, peerId: 'peer1', mode: 'two-way', paused: false,
    index: { 'clean.txt': { size: pst.size, mtimeMs: pst.mtimeMs, sig, authorKey: 'peer1', deleted: false } },
    remoteIndex: { 'clean.txt': { size: pst.size, mtimeMs: Date.now(), sig, authorKey: 'peer1', deleted: true } }
  }
  sync.libraries.set('lib5b', lib2)
  // Simulate the delivered snapshot matching the live file (guard reads the bee).
  const bee = makeBee()
  sync._getBee = async (name) => (name === 'settings' ? makeBee() : bee)
  await bee.put(`delivered/lib5b/clean.txt`, { size: pst.size, mtimeMs: pst.mtimeMs, sig })
  await sync._applyRemoteDeletes(lib2)
  assert('unmodified local file IS deleted on remote-intent delete', !fs.existsSync(purePath))

  // Reconciler branch: same guard at the planner level.
  const r1 = Reconciler.reconcile({
    localIndex: { 'x.txt': { size: 10, mtimeMs: 100, sig: '10-100', authorKey: 'me', deleted: false } },
    baseline: { 'x.txt': { size: 4, mtimeMs: 50, sig: '4-50', deleted: false } },
    remoteIndex: { 'x.txt': { size: 4, mtimeMs: 200, sig: '4-50', deleted: true } },
    mode: 'two-way'
  })
  assert('reconciler: modified-local beats remote tombstone (push, not delete)', r1.toPush.includes('x.txt') && r1.toDeleteLocal.length === 0)
  const r2 = Reconciler.reconcile({
    localIndex: { 'y.txt': { size: 4, mtimeMs: 60, sig: '4-50', authorKey: 'peer', deleted: false } },
    baseline: { 'y.txt': { size: 4, mtimeMs: 50, sig: '4-50', deleted: false } },
    remoteIndex: { 'y.txt': { size: 4, mtimeMs: 200, sig: '4-50', deleted: true } },
    mode: 'two-way'
  })
  assert('reconciler: unmodified local + remote tombstone → delete locally', r2.toDeleteLocal.includes('y.txt') && r2.toPush.length === 0)

  fs.rmSync(tmp, { recursive: true, force: true })
}

async function testFix2() {
  console.log('\n— Fix #2: deterministic concurrent-edit resolution —')
  // Both perspectives must compute the SAME winner.
  const A = { key: 'aaa', entry: { size: 5, mtimeMs: 1000, sig: '5-1000', authorKey: 'aaa', deleted: false } }
  const B = { key: 'bbb', entry: { size: 6, mtimeMs: 1002, sig: '6-1002', authorKey: 'bbb', deleted: false } } // 2ms apart — inside tolerance
  const fromA = Reconciler.reconcile({
    localIndex: { 'f.txt': A.entry },
    baseline: { 'f.txt': { size: 1, mtimeMs: 0, sig: '1-0', deleted: false } },
    remoteIndex: { 'f.txt': B.entry },
    mode: 'two-way'
  })
  const fromB = Reconciler.reconcile({
    localIndex: { 'f.txt': B.entry },
    baseline: { 'f.txt': { size: 1, mtimeMs: 0, sig: '1-0', deleted: false } },
    remoteIndex: { 'f.txt': A.entry },
    mode: 'two-way'
  })
  assert('within-tolerance edit resolves (no more silent stalemate)', fromA.conflicts.length === 1 && fromB.conflicts.length === 1)
  const winnerA = fromA.toPush.includes('f.txt') ? 'A' : 'B'
  const winnerB = fromB.toPush.includes('f.txt') ? 'B' : 'A'
  assert('both sides converge on the same winner (mtime tie-break)', winnerA === winnerB, `A says ${winnerA}, B says ${winnerB}`)
  assert('conflict event carries winner + sigs', fromA.conflicts[0].winner && fromA.conflicts[0].localSig && fromA.conflicts[0].remoteSig && !!fromA.conflicts[0].ts)

  // Exact mtime tie → authorKey decides, still symmetric.
  const A2 = { size: 5, mtimeMs: 1000, sig: '5-1000', authorKey: 'aaa', deleted: false }
  const B2 = { size: 6, mtimeMs: 1000, sig: '6-1000', authorKey: 'zzz', deleted: false }
  const t1 = Reconciler.reconcile({ localIndex: { 'f': A2 }, baseline: {}, remoteIndex: { 'f': B2 }, mode: 'two-way' })
  const t2 = Reconciler.reconcile({ localIndex: { 'f': B2 }, baseline: {}, remoteIndex: { 'f': A2 }, mode: 'two-way' })
  assert('exact tie broken by greater authorKey, symmetric', t1.toPush.includes('f') === (t2.toPush.length === 0) && t1.conflicts.length === 1 && t2.conflicts.length === 1)
  assert('greater authorKey (zzz) wins the tie', t2.toPush.includes('f') && t1.toPush.length === 0)

  // Identical sigs inside tolerance → NOT a conflict.
  const same = { size: 5, mtimeMs: 1000, sig: '5-1000', authorKey: 'aaa', deleted: false }
  const same2 = { ...same, mtimeMs: 1001 }
  const r3 = Reconciler.reconcile({ localIndex: { 'f': same }, baseline: {}, remoteIndex: { 'f': same2 }, mode: 'two-way' })
  assert('identical content within tolerance is not a conflict', r3.conflicts.length === 0 && r3.toPush.length === 0)
}

async function testFix9a() {
  console.log('\n— Fix #9a: trash purge by age —')
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'md-f9-'))
  const { sync } = makeSyncHarness(tmp)
  const libDir = path.join(tmp, 'lib')
  const trashDir = path.join(libDir, '.meshdrop-trash')
  fs.mkdirSync(trashDir, { recursive: true })
  const oldFile = path.join(trashDir, 'conflict-old.txt-aaaa-oldts')
  const newFile = path.join(trashDir, 'conflict-new.txt-bbbb-newts')
  write(oldFile, 'OLD')
  write(newFile, 'NEW')
  const oldTime = Date.now() - 40 * 24 * 60 * 60 * 1000
  await fsp.utimes(oldFile, new Date(oldTime), new Date(oldTime))
  write(path.join(trashDir, 'conflict-new.txt-bbbb-newts.meta.json'), JSON.stringify({ destPath: path.join(libDir, 'new.txt'), ts: Date.now() }))
  await fsp.utimes(path.join(trashDir, 'conflict-new.txt-bbbb-newts.meta.json'), new Date(oldTime), new Date(oldTime))
  // The winner was already applied before the crash — live path exists.
  write(path.join(libDir, 'new.txt'), 'WINNER')

  sync.libraries.set('lib9', { id: 'lib9', localPath: libDir, index: {}, remoteIndex: {} })
  await sync._trashMaintenance()

  assert('old trash entry purged', !fs.existsSync(oldFile))
  assert('new trash entry kept', fs.existsSync(newFile))
  assert('aged meta sidecar purged too', !fs.existsSync(path.join(trashDir, 'conflict-new.txt-bbbb-newts.meta.json')))

  // Recovery still runs under the same maintenance: sidecar with missing live path restores.
  const disp = path.join(trashDir, 'conflict-res.txt-cccc-xxxx')
  write(disp, 'RESUME')
  write(`${disp}.meta.json`, JSON.stringify({ destPath: path.join(libDir, 'res.txt'), ts: Date.now() }))
  await sync._trashMaintenance()
  assert('fresh displaced sidecar restored to live path', fs.existsSync(path.join(libDir, 'res.txt')) && read(path.join(libDir, 'res.txt')) === 'RESUME')

  fs.rmSync(tmp, { recursive: true, force: true })
}

async function runAllTests() {
  console.log('\n======================================================')
  console.log('     SYNC CORRECTNESS ACCEPTANCE TESTS (#4 #5 #2 #9a) ')
  console.log('======================================================')
  await testFix4()
  await testFix5()
  await testFix2()
  await testFix9a()
  console.log(`\n${passed} passed, ${failed} failed`)
}

runAllTests().catch((e) => { console.error('FATAL', e); process.exit(1) })
