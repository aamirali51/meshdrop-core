'use strict'

// Acceptance tests for the sparse-file range race fix: the webdav 206 range
// handler must gate reads on the hypercore bitfield (coverage truth), never
// serve zero-filled .part holes, clamp 206s to the covered prefix, use the
// manifest fileSize as the Content-Range total, and cap held/waiting
// responses per transfer.
//
//   node test-range-coverage.js
//
// No DHT/network: a fake hypercore exposes a controllable bitfield via
// has(); the real TransferEngine coverage primitives, the real
// ChunkScheduler priority queue, and the real webdav HTTP server are used.

const http = require('http')
const fs = require('fs')
const path = require('path')
const os = require('os')

const { TransferEngine } = require('./engine/TransferEngine.js')
const { ChunkScheduler } = require('./engine/transfer/scheduler.js')
const webdav = require('../meshdrop-app/electron/webdav.js')

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

const BLOCK = 64 * 1024 // CHUNK_SIZE
const FILE = 8 * 1024 * 1024 // 128 blocks
const ID = 'range-test-transfer'

// Deterministic, non-zero per-block pattern (manifest block m).
function blockPattern(m) {
  return (m * 7 + 13) & 0xff
}

function makeFakeCore() {
  const covered = new Set()
  return {
    covered,
    // Mirrors the real hypercore: has() is ASYNC (returns a Promise —
    // hypercore/index.js:834 in 11.35.0). Keeping the fake promise-based
    // locks the await-semantics into this suite: a sync-style regression
    // (truthy Promise read as "covered") fails these tests.
    has(i, length) {
      let present = true
      if (length === undefined) present = covered.has(i)
      else for (let j = i; j < i + length; j++) if (!covered.has(j)) { present = false; break }
      return Promise.resolve(present)
    },
    async get(i, opts = {}) {
      if (covered.has(i)) return Buffer.alloc(BLOCK, blockPattern(i - 1))
      if (opts.wait) return new Promise(() => {})
      return null
    },
    async update() {},
    async ready() {}
  }
}

function makeEngine() {
  const engine = Object.create(TransferEngine.prototype)
  engine.runs = new Map()
  return engine
}

function makeRun({ core, scheduler, record }) {
  return {
    direction: 'receive',
    fd: null,
    core,
    flags: { paused: false, cancelled: false },
    scheduler,
    record,
    blockSize: BLOCK,
    blockCount: FILE / BLOCK,
    receivedBlocks: 0
  }
}

// ── Unit checks: byte↔block mapping against bitfield truth ─────────────────
async function unitTests() {
  const core = makeFakeCore()
  const engine = makeEngine()
  const record = { id: ID, fileSize: 1000000 } // non-multiple of BLOCK
  engine.runs.set(ID, makeRun({ core, scheduler: null, record }))
  core.covered.add(0) // manifest
  for (let m = 0; m < 16; m++) core.covered.add(m + 1) // blocks 0..15

  ok('unit: fully covered non-aligned file → last byte',
    (await engine.coveredThrough(ID, 0)) === 999999, await engine.coveredThrough(ID, 0))
  ok('unit: mid-file start inside covered region',
    (await engine.coveredThrough(ID, 999000)) === 999999, await engine.coveredThrough(ID, 999000))
  core.covered.delete(10)
  ok('unit: hole inside region caps the answer at the hole',
    (await engine.coveredThrough(ID, 0)) === 9 * BLOCK - 1, await engine.coveredThrough(ID, 0))
  ok('unit: start inside the hole → null',
    (await engine.coveredThrough(ID, 9 * BLOCK + 5)) === null, await engine.coveredThrough(ID, 9 * BLOCK + 5))
  ok('unit: byteEnd caps the scan',
    (await engine.coveredThrough(ID, 0, 5 * BLOCK)) === 5 * BLOCK, await engine.coveredThrough(ID, 0, 5 * BLOCK))
  ok('unit: unknown transfer → null', (await engine.coveredThrough('nope', 0)) === null)
  ok('unit: byteStart beyond fileSize → null', (await engine.coveredThrough(ID, 2000000)) === null)
  core.covered.delete(0)
  core.covered.delete(1)
  ok('unit: first block uncovered → null', (await engine.coveredThrough(ID, 0)) === null)
}

// ── HTTP harness ────────────────────────────────────────────────────────────
function request(port, range) {
  return new Promise((resolve, reject) => {
    const req = http.get({
      host: '127.0.0.1',
      port,
      path: `/stream/transfer?id=${ID}&path=${encodeURIComponent(stagedPath)}`,
      headers: range ? { Range: range } : {}
    }, (res) => {
      const chunks = []
      res.on('data', (c) => chunks.push(c))
      res.on('end', () => resolve({
        status: res.statusCode,
        headers: res.headers,
        body: Buffer.concat(chunks),
        elapsed: Date.now() - started
      }))
    })
    req.on('error', reject)
    var started = Date.now()
  })
}

function hasZeroByte(buf) {
  return buf.includes(0)
}

function checkPattern(body, startByte) {
  for (let i = 0; i < body.length; i++) {
    const abs = startByte + i
    const m = Math.floor(abs / BLOCK)
    if (body[i] !== blockPattern(m)) return `byte ${abs}: got ${body[i]}, want ${blockPattern(m)}`
  }
  return null
}

// ── Main ────────────────────────────────────────────────────────────────────
let stagedPath = ''
let tmpDir = ''

async function main() {
  await unitTests()

  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'meshdrop-range-'))
  stagedPath = path.join(tmpDir, 'movie.part')

  // The .part: covered blocks carry the real pattern, un-downloaded regions
  // are zero-filled holes (exactly the on-disk state the bug manifests in).
  fs.writeFileSync(stagedPath, Buffer.alloc(FILE, 0))
  const core = makeFakeCore()
  const record = { id: ID, fileSize: FILE, stagingPath: stagedPath, playable: true, status: 'downloading' }
  const scheduler = new ChunkScheduler({
    core,
    firstDataBlock: 1,
    lastDataBlock: FILE / BLOCK,
    blocks: [],
    blockSize: BLOCK,
    onBlock: async () => {},
    transferId: ID
  })

  const engine = makeEngine()
  engine.runs.set(ID, makeRun({ core, scheduler, record }))
  engine.getBee = async () => ({ get: async (id) => (id === ID ? { value: record } : null) })

  const fillBlock = (m) => fs.writeSync(fs.openSync(stagedPath, 'r+'), Buffer.alloc(BLOCK, blockPattern(m)), 0, BLOCK, m * BLOCK)
  const coverBlock = (m) => { fillBlock(m); core.covered.add(m + 1) }

  // Stage ~20%: head + early blocks.
  for (let m = 0; m < 26; m++) coverBlock(m)

  webdav.setWebDAVEngine(engine)
  const port = await webdav.startWebDAVServer({ port: 41987 })
  console.log(`# webdav server on :${port}`)

  // ── 1. Seek into un-downloaded region: 416 (after wait), NEVER zero bytes,
  //      and the held-wait cap kicks in for excess concurrent requests. ──────
  scheduler.paused = true // scheduler paused: no blocks can land
  const seekRange = `bytes=${Math.floor(FILE * 0.8)}-${Math.floor(FILE * 0.8) + 65535}`
  const six = await Promise.all(Array.from({ length: 6 }, () => request(port, seekRange)))
  const immediate = six.filter((r) => r.elapsed < 2000)
  const held = six.filter((r) => r.elapsed >= 9000)
  ok('seek into hole: every response is 416 (no 206, no zero bytes)',
    six.every((r) => r.status === 416 && !hasZeroByte(r.body)),
    six.map((r) => `${r.status}@${r.elapsed}ms`).join(','))
  ok('seek into hole: at most 4 requests held, excess rejected immediately',
    immediate.length >= 2 && held.length <= 4,
    `immediate=${immediate.length} held=${held.length}`)
  ok('seek into hole: held requests waited the full 10s budget',
    held.every((r) => r.elapsed < 11500),
    held.map((r) => r.elapsed).join(','))
  ok('seek into hole: prioritizeRange queued the missing blocks on the scheduler',
    scheduler.priority.length > 0, `priority=${scheduler.priority.length}`)
  ok('seek into hole: 416 carries Content-Range bytes */total',
    six.every((r) => r.headers['content-range'] === `bytes */${FILE}`),
    six[0].headers['content-range'])
  scheduler.paused = false

  // ── 2. Range spanning a hole: 206 clipped to the covered prefix. ──────────
  // Trim coverage to exactly 0..1MiB so the numbers are clean.
  for (let m = 16; m < 26; m++) core.covered.delete(m + 1)
  const holeSpan = await request(port, 'bytes=0-3145727') // 0..3MiB
  const wantEnd = 16 * BLOCK - 1
  ok('hole span: 206 with covered-prefix Content-Range and manifest total',
    holeSpan.status === 206 &&
    holeSpan.headers['content-range'] === `bytes 0-${wantEnd}/${FILE}`,
    `${holeSpan.status} ${holeSpan.headers['content-range']}`)
  ok('hole span: body length matches the clipped range',
    holeSpan.body.length === wantEnd + 1, holeSpan.body.length)
  ok('hole span: no zero bytes in the 206 body',
    !hasZeroByte(holeSpan.body))
  ok('hole span: body bytes match the on-disk pattern',
    checkPattern(holeSpan.body, 0) === null, checkPattern(holeSpan.body, 0))

  // ── 3. Resume: cold engine (Set/LRU gone), bitfield truth serves instantly.
  const engine2 = makeEngine()
  engine2.runs.set(ID, makeRun({ core, scheduler: null, record })) // LRU cold, no scheduler
  webdav.setWebDAVEngine(engine2)
  const resumed = await request(port, 'bytes=0-65535')
  ok('resume: served immediately from bitfield truth (no wait)',
    resumed.status === 206 && resumed.elapsed < 1000,
    `${resumed.status} elapsed=${resumed.elapsed}ms`)
  ok('resume: body correct',
    checkPattern(resumed.body, 0) === null && !hasZeroByte(resumed.body))
  // A range the bitfield knows is a hole still 416s with no scheduler at all.
  const coldHole = await request(port, seekRange)
  ok('resume: cold-engine hole still 416s (LRU eviction ≠ coverage)',
    coldHole.status === 416 && coldHole.elapsed < 11500,
    `${coldHole.status} elapsed=${coldHole.elapsed}ms`)
  webdav.setWebDAVEngine(engine)

  // ── 4. Active-transfer seek: response lands once the blocks download. ─────
  // Simulated downloader: drains the scheduler priority queue like the real
  // sweep, writes blocks positionally, and marks the bitfield.
  const downloader = setInterval(() => {
    while (scheduler.priority.length > 0) {
      const coreIndex = scheduler.priority.shift()
      scheduler._prioritySet.delete(coreIndex)
      coverBlock(coreIndex - 1)
      scheduler.completed.add(coreIndex)
    }
  }, 25)
  const activeSeek = await request(port, `bytes=${4 * BLOCK}-${6 * BLOCK - 1}`) // blocks 4..5 (hole)
  clearInterval(downloader)
  ok('active seek: 206 arrives after blocks download, within the wait budget',
    activeSeek.status === 206 && activeSeek.elapsed < 10000,
    `${activeSeek.status} elapsed=${activeSeek.elapsed}ms`)
  ok('active seek: full requested range served (blocks landed)',
    activeSeek.body.length === 2 * BLOCK &&
    activeSeek.headers['content-range'] === `bytes ${4 * BLOCK}-${6 * BLOCK - 1}/${FILE}`,
    `${activeSeek.body.length} ${activeSeek.headers['content-range']}`)
  ok('active seek: body bytes correct',
    checkPattern(activeSeek.body, 4 * BLOCK) === null && !hasZeroByte(activeSeek.body))

  // ── 5. Fail-safe: engine without coverage primitives (cold init race) must
  //      416, never fall back to the legacy raw stream over holes. ───────────
  webdav.setWebDAVEngine({ getBee: engine.getBee })
  const failSafe = await request(port, 'bytes=0-65535')
  ok('fail-safe: coverage-less engine → immediate 416, no raw stream',
    failSafe.status === 416 && failSafe.elapsed < 2000 && !hasZeroByte(failSafe.body),
    `${failSafe.status} elapsed=${failSafe.elapsed}ms`)
  webdav.setWebDAVEngine(engine)

  // ── 6. REAL hypercore regression: async has() semantics + core.clear hole ─
  // The fake above is promise-based to mirror this, but this section pins the
  // behavior to the actual installed hypercore permanently.
  const Hypercore = require('../meshdrop-app/node_modules/hypercore')
  const realDir = fs.mkdtempSync(path.join(os.tmpdir(), 'meshdrop-realcore-'))
  const realCore = new Hypercore(realDir)
  await realCore.ready()
  const realBlocks = [Buffer.alloc(BLOCK, 0)] // core block 0: manifest slot
  for (let m = 0; m < FILE / BLOCK; m++) realBlocks.push(Buffer.alloc(BLOCK, blockPattern(m)))
  await realCore.append(realBlocks)
  const hasPromise = realCore.has(1)
  ok('real core: has() returns a Promise (the divergence sync code would hide)',
    hasPromise instanceof Promise)
  await hasPromise
  // Punch a hole: clear core indices 70..73 ⇒ manifest blocks 69..72
  // (bytes [69*BLOCK, 73*BLOCK) become un-downloaded).
  await realCore.clear(70, 74)
  ok('real core: cleared block reports has() === false',
    (await realCore.has(71)) === false, await realCore.has(71))

  const realRecord = { id: ID, fileSize: FILE, stagingPath: stagedPath, playable: true, status: 'downloading' }
  const realEngine = makeEngine()
  realEngine.runs.set(ID, makeRun({ core: realCore, scheduler: null, record: realRecord }))
  realEngine.getBee = async () => ({ get: async (id) => (id === ID ? { value: realRecord } : null) })
  webdav.setWebDAVEngine(realEngine)

  // Re-stage the .part to match the real bitfield: pattern everywhere except
  // the zero-filled hole.
  fs.writeFileSync(stagedPath, Buffer.alloc(FILE, 0))
  for (let m = 0; m < FILE / BLOCK; m++) if (m < 69 || m > 72) fillBlock(m)

  ok('real core: coveredThrough ends exactly at the cleared hole',
    (await realEngine.coveredThrough(ID, 0)) === 69 * BLOCK - 1,
    await realEngine.coveredThrough(ID, 0))
  ok('real core: byte start inside the cleared hole → null',
    (await realEngine.coveredThrough(ID, 70 * BLOCK + 3)) === null,
    await realEngine.coveredThrough(ID, 70 * BLOCK + 3))
  ok('real core: region after the hole is covered through EOF',
    (await realEngine.coveredThrough(ID, 73 * BLOCK)) === FILE - 1,
    await realEngine.coveredThrough(ID, 73 * BLOCK))
  ok('real core: prioritizeRange with no scheduler queues nothing (harmless)',
    (await realEngine.prioritizeRange(ID, 70 * BLOCK, 73 * BLOCK - 1)) === 0)

  const realHole = await request(port, `bytes=${70 * BLOCK}-${71 * BLOCK - 1}`)
  ok('real core: range inside the hole → 416 after the wait, never 206 with zeros',
    realHole.status === 416 && !hasZeroByte(realHole.body) &&
    realHole.elapsed >= 9000 && realHole.elapsed < 11500,
    `${realHole.status} elapsed=${realHole.elapsed}ms ${realHole.headers['content-range']}`)
  // Request past the hole edge so the 206 must clamp to the covered prefix.
  const realCovered = await request(port, `bytes=0-${69 * BLOCK + 100}`)
  ok('real core: covered prefix serves a clean 206 clamped at the hole edge',
    realCovered.status === 206 && !hasZeroByte(realCovered.body) &&
    checkPattern(realCovered.body, 0) === null &&
    realCovered.headers['content-range'] === `bytes 0-${69 * BLOCK - 1}/${FILE}`,
    `${realCovered.status} ${realCovered.headers['content-range']}`)

  await realCore.close()
  fs.rmSync(realDir, { recursive: true, force: true })

  webdav.stopWebDAVServer()
  fs.rmSync(tmpDir, { recursive: true, force: true })
  console.log(`\n${passed} assertions passed`)
}

main().catch((err) => {
  console.error('FATAL', err)
  process.exitCode = 1
})
