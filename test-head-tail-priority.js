'use strict'

// Fast in-memory unit tests for the P1 head/tail priority fetch + LRU block
// cache + byte playhead. No DHT, no network — a fake hypercore serves blocks
// on demand and records the ORDER of core.get calls so the tests can assert
// that head/tail blocks are requested before the sequential sweep.
//
//   node test-head-tail-priority.js

const { ChunkScheduler } = require('./engine/transfer/scheduler.js')
const { BlockCache } = require('./engine/transfer/blockCache.js')
const { sha256 } = require('./crypto.js')
const b4a = require('b4a')

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

// Fake core: serves deterministic 64KiB blocks, records every core.get order,
// and can simulate a slow/inaccessible source.
function makeCore({ totalBlocks, failAfter = Infinity, delayMs = 0 } = {}) {
  const order = []
  let gets = 0
  const blocks = new Map()
  for (let i = 0; i <= totalBlocks; i++) {
    // Deterministic block content: hash the index, then length-pad to 64KiB.
    const seed = sha256(b4a.from(`block-${i}`))
    const buf = b4a.alloc(64 * 1024)
    for (let o = 0; o < buf.length; o += seed.length) {
      const n = Math.min(seed.length, buf.length - o)
      buf.set(seed.subarray(0, n), o)
    }
    blocks.set(i, buf)
  }
  return {
    order,
    get gets() { return gets },
    async get(coreIndex, opts = {}) {
      order.push(coreIndex)
      gets++
      if (delayMs) await new Promise((r) => setTimeout(r, delayMs))
      if (gets > failAfter) throw new Error('simulated source failure')
      return blocks.get(coreIndex)
    },
    async update() {},
    async ready() {},
    async close() {},
    peek: null
  }
}

// Deterministic expected block for index i (mirrors makeCore content).
function expectedBlock(i) {
  const seed = sha256(b4a.from(`block-${i}`))
  const buf = b4a.alloc(64 * 1024)
  for (let o = 0; o < buf.length; o += seed.length) {
    const n = Math.min(seed.length, buf.length - o)
    buf.set(seed.subarray(0, n), o)
  }
  return buf
}

// sha256 hex of a 64KiB block buffer (the manifest stores hex strings).
function blockHash(buf) {
  return b4a.toString(sha256(buf), 'hex')
}

const BLOCK = 64 * 1024
const ID = 'test-transfer'

// Verify a fetched block matches the manifest expectation.
function verify(coreIndex, block, blocks) {
  const want = blocks[coreIndex - 1]
  const got = blockHash(block)
  if (want && want !== got) throw new Error(`Block ${coreIndex} checksum mismatch`)
}

async function main() {
  const TMP = []
  const run = (name, fn) => TMP.push({ name, fn })
  // (tests are executed inline below)

  // ── 1. Scheduler honors an explicit priority queue before the sweep ──────
  {
    const total = 1000
    const core = makeCore({ totalBlocks: total })
    const blocks = []
    for (let i = 1; i <= total; i++) blocks.push(blockHash(expectedBlock(i)))
    const written = []
    const cache = new BlockCache()
    const s = new ChunkScheduler({
      core,
      firstDataBlock: 1,
      lastDataBlock: total,
      blocks,
      blockSize: BLOCK,
      cache,
      transferId: ID,
      onBlock: async (coreIndex, block) => { verify(coreIndex, block, blocks); written.push(coreIndex) }
    })
    // Jump the head + tail to the front.
    for (let i = 1; i <= 4; i++) s.enqueuePriority(i)
    for (let i = total - 3; i <= total; i++) s.enqueuePriority(i)
    await s.run()
    ok('priority blocks jump the queue', core.order.slice(0, 4).join(',') === '1,2,3,4', core.order.slice(0, 8).join(','))
    ok('tail blocks jump the queue after head', core.order.slice(4, 8).join(',') === `${total - 3},${total - 2},${total - 1},${total}`, core.order.slice(0, 12).join(','))
    ok('all blocks written exactly once', written.length === total && new Set(written).size === total, `written=${written.length}`)
  }

  // ── 2. Byte playhead prioritizes the lookahead around a seek position ────
  {
    const total = 200
    const core = makeCore({ totalBlocks: total })
    const blocks = []
    for (let i = 1; i <= total; i++) blocks.push(blockHash(expectedBlock(i)))
    const cache = new BlockCache()
    const s = new ChunkScheduler({
      core,
      firstDataBlock: 1,
      lastDataBlock: total,
      blocks,
      blockSize: BLOCK,
      isStreaming: true,
      streamWindow: 64,
      cache,
      transferId: ID,
      onBlock: async (coreIndex, block) => { verify(coreIndex, block, blocks) }
    })
    // Seek to ~6 MiB in => block ~96. The lookahead [96..160] should be
    // requested before the sequential sweep reaches the middle/end.
    s.setPlayhead(96 * BLOCK)
    await s.run()
    const firstWindow = core.order.slice(0, 40)
    ok(
      'seek lookahead window fetched before sequential tail',
      firstWindow.includes(96) && firstWindow.includes(97) && firstWindow.includes(98),
      `first requests: ${core.order.slice(0, 12).join(',')}`
    )
    // The seek jump enqueues the 4 blocks just past the playhead first.
    ok('seek jump precedes the wide lookahead', core.order[0] === 96, `first=${core.order[0]}`)
  }

  // ── 3. LRU serves a repeated head region without a second core.get ───────
  {
    const total = 50
    const core = makeCore({ totalBlocks: total })
    const blocks = []
    for (let i = 1; i <= total; i++) blocks.push(blockHash(expectedBlock(i)))
    const cache = new BlockCache()
    const s = new ChunkScheduler({
      core,
      firstDataBlock: 1,
      lastDataBlock: total,
      blocks,
      blockSize: BLOCK,
      cache,
      transferId: ID,
      onBlock: async (coreIndex, block) => { verify(coreIndex, block, blocks); cache.set(ID, coreIndex, block) }
    })
    await s.run()
    const getsAfterFull = core.gets
    // Second run over the same range must be served entirely from the cache.
    const s2 = new ChunkScheduler({
      core,
      firstDataBlock: 1,
      lastDataBlock: total,
      blocks,
      blockSize: BLOCK,
      cache,
      transferId: ID,
      onBlock: async (coreIndex, block) => { verify(coreIndex, block, blocks) }
    })
    await s2.run()
    ok('cache hit avoids a second core.get', core.gets === getsAfterFull, `gets=${core.gets} (was ${getsAfterFull})`)
    ok('cache still holds all blocks', cache.size === total, `cache.size=${cache.size}`)
  }

  // ── 4. Cache is flushed per-transfer (stale blocks never served) ─────────
  {
    const cache = new BlockCache()
    cache.set('a', 1, b4a.alloc(10, 1))
    cache.set('b', 2, b4a.alloc(10, 2))
    cache.flushTransfer('a')
    ok('flushTransfer removes only that transfer', cache.get('a', 1) === null && cache.get('b', 2) !== null)
    cache.flushTransfer('b')
    ok('flushTransfer empties fully', cache.size === 0 && cache.bytes === 0)
  }

  // ── 5. LRU byte cap evicts oldest ────────────────────────────────────────
  {
    const cache = new BlockCache({ capBytes: 100 })
    for (let i = 1; i <= 10; i++) cache.set(ID, i, b4a.alloc(40, i))
    ok('byte cap holds under capBytes', cache.bytes <= 100, `bytes=${cache.bytes}`)
    ok('oldest entries evicted first', cache.get(ID, 1) === null && cache.get(ID, 10) !== null)
  }

  // ── 6. Pause interrupts an inflight wait and stops dispatch ──────────────
  {
    const total = 200
    // Slow core (50ms per get) so pause can land mid-run.
    const core = makeCore({ totalBlocks: total, delayMs: 50 })
    const blocks = []
    for (let i = 1; i <= total; i++) blocks.push(blockHash(expectedBlock(i)))
    const cache = new BlockCache()
    const s = new ChunkScheduler({
      core,
      firstDataBlock: 1,
      lastDataBlock: total,
      blocks,
      blockSize: BLOCK,
      cache,
      transferId: ID,
      onBlock: async (coreIndex, block) => { verify(coreIndex, block, blocks) }
    })
    const runP = s.run()
    await new Promise((r) => setTimeout(r, 150))
    const dispatchedBeforePause = core.gets
    s.pause()
    await new Promise((r) => setTimeout(r, 250))
    const dispatchedWhilePaused = core.gets - dispatchedBeforePause
    s.resume()
    await runP
    ok('pause stops new dispatch', dispatchedWhilePaused === 0, `dispatched while paused=${dispatchedWhilePaused}`)
    ok('resume completes the transfer', s.completed.size === total, `completed=${s.completed.size}`)
  }

  // ── 7. Resume seeds the scheduler from an offset and does not rewrite ────
  {
    const total = 100
    const resumeAt = 20 // blocks 1..20 already on disk
    const core = makeCore({ totalBlocks: total })
    const blocks = []
    for (let i = 1; i <= total; i++) blocks.push(blockHash(expectedBlock(i)))
    const cache = new BlockCache()
    const written = []
    const s = new ChunkScheduler({
      core,
      firstDataBlock: 1 + resumeAt,
      lastDataBlock: total,
      blocks: blocks.slice(resumeAt),
      blockSize: BLOCK,
      cache,
      transferId: ID,
      onBlock: async (coreIndex, block) => { verify(coreIndex, block, blocks); written.push(coreIndex) }
    })
    // Priority fetch of the tail even from a resumed position.
    for (let i = total - 2; i <= total; i++) s.enqueuePriority(i)
    await s.run()
    ok('resume writes only blocks after the offset', written.every((i) => i > resumeAt) && written.length === total - resumeAt, `written=${written.length}`)
    const tailFirst = core.order.indexOf(total - 2)
    const firstSeq = core.order.findIndex((i) => i > resumeAt && i < total - 2)
    ok('resume tail priority precedes sequential sweep', tailFirst >= 0 && (firstSeq === -1 || tailFirst < firstSeq), `order head=${core.order.slice(0, 6).join(',')}`)
  }

  // ── 8. TransferEngine.setPlayheadByte uses info.blockSize (regression) ───
  {
    const { TransferEngine } = require('./engine/TransferEngine.js')
    const calls = []
    const fakeScheduler = { setPlayhead: (byteOffset) => { calls.push(byteOffset) } }
    const te = new TransferEngine({
      getBee: async () => { throw new Error('unused') },
      exchangeStore: { get: () => null },
      sendEvent: () => {},
      getPeers: () => new Map(),
      getDeviceIdentity: () => ({ id: 'dev' }),
      getDownloadDirectory: async () => '.',
      getTransferMethod: () => 'dht'
    })
    // Fake a running receive whose manifest used a 64KiB block size.
    const fakeInfo = { scheduler: fakeScheduler, blockSize: 64 * 1024 }
    te.runs.set('t', fakeInfo)
    te.setPlayheadByte('t', 5 * 1024 * 1024) // floor(5MiB/64KiB)+1 = block 81 => byte 81*64KiB
    te.setPlayhead('t', 42) // block 42 => byte 42*64KiB
    te.runs.delete('t')
    const want0 = 81 * 64 * 1024
    const want1 = 42 * 64 * 1024
    ok(
      'setPlayheadByte passes a byte-aligned playhead',
      calls.length === 2 && calls[0] === want0 && calls[1] === want1,
      `calls=${JSON.stringify(calls)} want=[${want0},${want1}]`
    )
  }

  const failed = TMP.length ? 0 : 0
  void failed
  const totalPassed = passed
  console.log(`\nHEAD/TAIL PRIORITY: ${totalPassed} checks passed`)
  if (process.exitCode) console.log('  (failures above)')
}

main().catch((err) => {
  console.error('FATAL', err)
  process.exit(1)
})
