'use strict'

// REAL hypercore coverage truth (Prompt-3 follow-up).
//
// Pins the byte↔block mapping used by TransferEngine.coveredThrough /
// waitForRange to the INSTALLED hypercore — not a fake bitfield:
//   1. LOUD log of `typeof core.has(k)` for the installed hypercore. If has()
//      returns a Promise (hypercore ≥10 async semantics), any sync-style read
//      of the result in production would treat EVERY byte as covered.
//   2. core.clear() punches a real bitfield hole in the middle of ~20
//      patterned 64 KiB blocks; coverage mapping is asserted against it:
//      block 0 of the core is the manifest slot, manifest block m lives at
//      core index m+1 and covers bytes [m*blockSize, (m+1)*blockSize).
//
//   node test-real-core-coverage.js
//
// No DHT/network. Uses the installed hypercore + the real TransferEngine
// coverage primitives (no scheduler, no webdav server).

const fs = require('fs')
const path = require('path')
const os = require('os')

const Hypercore = require('hypercore')
const { TransferEngine } = require('./engine/TransferEngine.js')

let passed = 0
let failed = 0
function ok(name, cond, detail) {
  if (cond) {
    passed++
    console.log(`PASS  ${name}`)
  } else {
    failed++
    console.error(`FAIL  ${name}${detail !== undefined ? ' — ' + detail : ''}`)
    process.exitCode = 1
  }
}

const BLOCK = 64 * 1024 // CHUNK_SIZE
const MANIFEST_BLOCKS = 20 // manifest blocks m = 0..19 (core indices 1..20)
const FILE = MANIFEST_BLOCKS * BLOCK
const ID = 'real-core-coverage-transfer'

// Deterministic, non-zero per-block pattern (manifest block m).
function blockPattern(m) {
  return (m * 7 + 13) & 0xff
}

// Hole: manifest blocks 8..11 (core indices 9..12) cleared.
const HOLE_START_M = 8
const HOLE_END_M = 11 // inclusive

async function main() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'meshdrop-real-core-coverage-'))
  const core = new Hypercore(path.join(dir, 'core'))
  await core.ready()

  // ── 1. THE POINT OF THIS TEST: has() sync-vs-async on the installed build ─
  const hasResult = core.has(1)
  console.log('')
  console.log('══════════════════════════════════════════════════════════════')
  console.log(`>>> INSTALLED hypercore ${require('hypercore/package.json').version}`)
  console.log(`>>> typeof core.has(1) === '${typeof hasResult}'`)
  console.log(`>>> core.has(1) instanceof Promise === ${hasResult instanceof Promise}`)
  console.log('>>> If that is a Promise, every sync-style read of has() in')
  console.log('>>> production treats EVERY byte as covered. coveredThrough/')
  console.log('>>> waitForRange MUST await it (they do — asserted below).')
  console.log('══════════════════════════════════════════════════════════════')
  console.log('')

  // ── 2. Real bitfield: manifest slot + patterned blocks, then punch a hole ─
  const blocks = [Buffer.alloc(BLOCK, 0)] // core block 0 = manifest slot
  for (let m = 0; m < MANIFEST_BLOCKS; m++) blocks.push(Buffer.alloc(BLOCK, blockPattern(m)))
  await core.append(blocks)

  ok('appended manifest slot + 20 patterned blocks (21 core blocks)',
    core.length === MANIFEST_BLOCKS + 1, core.length)

  await core.clear(1 + HOLE_START_M, 1 + HOLE_END_M + 1) // core indices 9..12

  ok('cleared core index reports has() === false (awaited)',
    (await core.has(1 + HOLE_START_M)) === false)
  ok('surviving prefix block still has() === true (awaited)',
    (await core.has(1 + HOLE_START_M - 1)) === true)
  ok('block after the hole still has() === true (awaited)',
    (await core.has(1 + HOLE_END_M + 1)) === true)

  // ── 3. Prompt-3 byte↔block mapping against the real bitfield ─────────────
  const engine = Object.create(TransferEngine.prototype)
  engine.runs = new Map()
  const record = { id: ID, fileSize: FILE, stagingPath: path.join(dir, 'file.bin'), playable: true, status: 'downloading' }
  engine.runs.set(ID, {
    direction: 'receive',
    fd: null,
    core,
    flags: { paused: false, cancelled: false },
    scheduler: null,
    record,
    blockSize: BLOCK,
    blockCount: MANIFEST_BLOCKS,
    receivedBlocks: 0
  })
  engine.getBee = async () => ({ get: async (id) => (id === ID ? { value: record } : null) })

  // coveredThrough itself is async — assert the call shape while we are here.
  const callShape = engine.coveredThrough(ID, 0)
  ok('engine.coveredThrough() returns a Promise (async gate)', callShape instanceof Promise)

  // Range fully inside the hole → must detect uncovered.
  const inHole = await engine.coveredThrough(ID, (HOLE_START_M + 1) * BLOCK + 5)
  ok('real core: byteStart inside the hole → null (uncovered detected)',
    inHole === null, inHole)

  // Hole start byte itself is uncovered (block 8 not held).
  const holeStart = await engine.coveredThrough(ID, HOLE_START_M * BLOCK)
  ok('real core: byteStart at the hole boundary → null', holeStart === null, holeStart)

  // Covered prefix from byte 0 → clamped to the last byte before the hole.
  const prefix = await engine.coveredThrough(ID, 0)
  ok('real core: covered prefix clamps at the hole edge (HOLE_START_M*BLOCK - 1)',
    prefix === HOLE_START_M * BLOCK - 1, prefix)

  // Prefix with an explicit byteEnd caps the scan correctly.
  const prefixCapped = await engine.coveredThrough(ID, 0, 5 * BLOCK)
  ok('real core: covered prefix with byteEnd cap → byteEnd',
    prefixCapped === 5 * BLOCK, prefixCapped)

  // Range starting after the hole runs through EOF (clamped to fileSize - 1).
  const tail = await engine.coveredThrough(ID, (HOLE_END_M + 1) * BLOCK)
  ok('real core: post-hole range covered through EOF',
    tail === FILE - 1, tail)

  // Degenerate inputs behave as on the fake path.
  ok('real core: byteStart beyond fileSize → null',
    (await engine.coveredThrough(ID, FILE)) === null)
  ok('real core: unknown transfer → null',
    (await engine.coveredThrough('nope', 0)) === null)

  // waitForRange: covered prefix resolves immediately; hole never resolves.
  const t0 = Date.now()
  const waitCovered = await engine.waitForRange(ID, 0, 3000)
  const elapsed = Date.now() - t0
  ok('real core: waitForRange on covered prefix resolves immediately',
    waitCovered === HOLE_START_M * BLOCK - 1 && elapsed < 1000,
    `through=${waitCovered} elapsed=${elapsed}ms`)
  const waitHole = await engine.waitForRange(ID, HOLE_START_M * BLOCK + 1, 300)
  ok('real core: waitForRange inside the hole → null after its timeout',
    waitHole === null, waitHole)

  await core.close()
  fs.rmSync(dir, { recursive: true, force: true })

  console.log('')
  console.log('══════════════════════════════════════════════════════════════')
  console.log(`  REAL-CORE COVERAGE: ${passed} passed, ${failed} failed`)
  console.log('══════════════════════════════════════════════════════════════')
  process.exit(failed > 0 ? 1 : 0)
}

main().catch((err) => {
  console.error('Test runner fatal error:', err)
  process.exit(1)
})
