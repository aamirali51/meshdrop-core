'use strict'

// ChunkScheduler: parallel, adaptive block fetcher. Starts with an 8-wide
// window and grows up to 64 on clean runs, shrinking on timeout/errors.
// Blocks hash-verify as they land; offsets are written positionally so order
// of completion does not matter. Errors (timeouts, checksum mismatches,
// cancellation) are thrown and must be handled by the caller.
//
// Priority model (head/tail instant start):
//   - A FIFO `priority` list jumps the queue: `enqueuePriority(coreIndex)` is
//     consulted before the streaming playhead window and the sequential sweep.
//   - The playhead is BYTE-based (`setPlayhead(byteOffset)`): the head/tail
//     prefetch (TransferEngine) seeds the priority list, and 206 range reads
//     (webdav.js / mobile bridge) move the byte playhead so the lookahead
//     window `[playheadByte, playheadByte + streamWindow*blockSize)` is
//     fetched next — this is what lets a seek re-key the download.
//   - Verified head/tail blocks are written through to the shared BlockCache
//     BEFORE the disk write, and `_getNextBlockIndex` peeks the cache first,
//     so a player never stalls on disk-write latency for a block it just
//     verified.

const b4a = require('b4a')
const { sha256 } = require('../../crypto.js')
const { MIN_WINDOW, MAX_WINDOW, sleep } = require('./constants.js')
const { BlockCache } = require('./blockCache.js')

const DEFAULT_CACHE = new BlockCache()

class ChunkScheduler {
  constructor({ core, firstDataBlock, lastDataBlock, blocks, blockSize, onBlock, isStreaming = false, streamWindow = 64, cache = DEFAULT_CACHE, transferId = null, requestTimeoutMs = 15000 }) {
    this.core = core
    this.first = firstDataBlock
    this.last = lastDataBlock
    this.blocks = blocks // manifest.blocks[0] corresponds to core block firstDataBlock
    this.blockSize = blockSize
    this.onBlock = onBlock // async (coreIndex, block) => void (write + hash)
    this.window = MIN_WINDOW
    this.cancelled = false
    this.paused = false
    this.isStreaming = isStreaming
    this.playheadByte = (firstDataBlock - 1) * blockSize // byte offset of the playhead
    this.streamWindow = streamWindow // blocks of lookahead past the playhead
    this.cache = cache
    this.transferId = transferId
    // Per-block request budget. On a fast network a block stuck on one peer
    // should be re-requested (retried) after ~500ms; on cellular the budget
    // widens to ~1500ms to avoid re-request storms over metered links. The
    // core's internal block exchange serves from whichever attached peer has
    // the block, so a short budget = fast failover across multi-seeders.
    this.requestTimeoutMs = requestTimeoutMs
    this.completed = new Set()
    this.inflight = new Map() // coreIndex -> settled promise
    this.priority = [] // FIFO of core indices that jump the sequential sweep
    this._prioritySet = new Set() // dedupe for `priority`
  }

  cancel() {
    this.cancelled = true
  }

  pause() {
    this.paused = true
  }

  resume() {
    this.paused = false
  }

  /**
   * Jump a specific block to the front of the queue (head/tail prefetch,
   * playhead seek targets). No-op if the block is already done/inflight/queued.
   * @param {number} coreIndex
   */
  enqueuePriority(coreIndex) {
    if (coreIndex < this.first || coreIndex > this.last) return false
    if (this.completed.has(coreIndex)) return false
    if (this.inflight.has(coreIndex)) return false
    if (this._prioritySet.has(coreIndex)) return false
    this.priority.push(coreIndex)
    this._prioritySet.add(coreIndex)
    return true
  }

  /**
   * Byte-based playhead: reprioritize the download around the user's current
   * playback position (converted from a 206 range start or a player seek).
   * @param {number} byteOffset  Absolute byte offset into the file.
   */
  setPlayhead(byteOffset) {
    const clamped = Math.max((this.first - 1) * this.blockSize, Math.min((this.last - 1) * this.blockSize, Math.floor(byteOffset / this.blockSize) * this.blockSize))
    if (this.playheadByte !== clamped) {
      this.playheadByte = clamped
      // Jump the first few blocks past the new playhead to the front so a seek
      // re-keys the download immediately instead of waiting for the sweep.
      const head = Math.max(this.first, Math.floor(clamped / this.blockSize))
      for (let i = 0; i < 4; i++) {
        this.enqueuePriority(head + i)
      }
    }
  }

  // Check the LRU cache for a verified block before queueing a network fetch.
  _peekCache(coreIndex) {
    if (!this.cache || !this.transferId) return null
    return this.cache.get(this.transferId, coreIndex)
  }

  async fetchBlock(coreIndex) {
    // Serve from the verified-block cache when present (no network, no disk).
    const cached = this._peekCache(coreIndex)
    if (cached) return cached

    let attempts = 0
    for (;;) {
      if (this.cancelled || this.paused) return null
      try {
        // Race the core.get against pause/cancel: a network/interface drop
        // must be able to interrupt an in-flight wait (the core.get wait can
        // block up to its timeout). While paused we poll and bail so run()
        // breaks out and the caller can resume after the drop settles. The
        // loser of the race is observed (no-op) so a late rejection cannot
        // surface as an unhandled error — the block either lands in the core
        // store (available for the resumed sweep) or the retry below handles
        // the miss.
        const getP = this.core.get(coreIndex, { wait: true, timeout: this.requestTimeoutMs })
        getP.catch(() => {})
        const block = await Promise.race([
          getP,
          new Promise((resolve) => {
            const t = setInterval(() => {
              if (this.paused || this.cancelled) {
                clearInterval(t)
                resolve(null)
              }
            }, 50)
            // Also bound the total wait so a paused scheduler cannot pin a
            // block forever.
            setTimeout(() => clearInterval(t), 16000)
          })
        ])
        if (block === null) return null // bailed due to pause/cancel
        if (!block) throw new Error('empty block')
        return block
      } catch (err) {
        attempts++
        if (this.cancelled || attempts >= 5) throw err
        try {
          await this.core.update({ wait: false })
        } catch {}
        await sleep(50 * attempts)
      }
    }
  }

  _getNextBlockIndex() {
    // 1. Explicit priority list (head/tail prefetch, seek jumps) first.
    while (this.priority.length > 0) {
      const idx = this.priority.shift()
      this._prioritySet.delete(idx)
      if (this.completed.has(idx) || this.inflight.has(idx)) continue
      // Peek the cache so a just-verified head/tail block never re-fetches.
      if (this._peekCache(idx) !== null) {
        // The cache holds a VERIFIED block — mark it for write-through only
        // (run() handles a null-block cache-hit via onCachedBlock below).
        return { index: idx, fromCache: true }
      }
      return { index: idx, fromCache: false }
    }

    // 2. Streaming playhead window: prioritize the lookahead around the active
    //    byte playhead (set by 206 reads / player seeks).
    if (this.isStreaming || this.playheadByte > (this.first - 1) * this.blockSize) {
      const start = Math.max(this.first, Math.floor(this.playheadByte / this.blockSize))
      const windowEnd = Math.min(this.last, start + this.streamWindow)
      for (let i = start; i <= windowEnd; i++) {
        if (this.completed.has(i) || this.inflight.has(i)) continue
        const cached = this._peekCache(i)
        if (cached !== null) return { index: i, fromCache: true }
        return { index: i, fromCache: false }
      }
    }

    // 3. Normal sequential sweep across the entire range.
    for (let i = this.first; i <= this.last; i++) {
      if (this.completed.has(i) || this.inflight.has(i)) continue
      const cached = this._peekCache(i)
      if (cached !== null) return { index: i, fromCache: true }
      return { index: i, fromCache: false }
    }
    return null
  }

  async run() {
    let failures = 0

    while (this.completed.size < (this.last - this.first + 1) || this.inflight.size > 0) {
      while (this.inflightFree(this.inflight) && !this.cancelled) {
        const next = this._getNextBlockIndex()
        if (next === null) break // all remaining blocks are already inflight

        const coreIndex = next.index
        if (next.fromCache) {
          // Verified block is already in memory: complete it immediately via
          // the same onBlock path (hash-verify + cache + positional write) —
          // no network fetch, no disk wait.
          const cached = this._peekCache(coreIndex)
          if (cached !== null && !this.completed.has(coreIndex)) {
            try {
              await this.onBlock(coreIndex, cached)
              this.completed.add(coreIndex)
              // Update recency so a hot head/tail block stays cached.
              this.cache && this.transferId && this.cache.set(this.transferId, coreIndex, cached)
            } catch (err) {
              // A failed write of a cached block is fatal like any other.
              throw err
            }
          }
          continue
        }

        this.inflight.set(
          coreIndex,
          this.fetchBlock(coreIndex).then(
            (block) => ({ coreIndex, block, err: null }),
            (err) => ({ coreIndex, block: null, err })
          )
        )
      }

      if (this.inflight.size === 0) {
        if (this.completed.size >= (this.last - this.first + 1)) break
        if (this.cancelled) break
        await sleep(50)
        continue
      }

      const settled = await Promise.race(Array.from(this.inflight.values()))
      this.inflight.delete(settled.coreIndex)

      if (settled.err) {
        if (this.cancelled) break
        failures++
        this.window = Math.max(MIN_WINDOW, Math.floor(this.window / 2))
        continue
      }
      if (this.cancelled) break
      if (settled.block === null) {
        // fetchBlock bailed due to pause/cancel.
        if (this.paused || this.cancelled) break
        continue
      }

      failures = Math.max(0, failures - 1)
      if (failures === 0 && this.window < MAX_WINDOW) this.window++

      const dataIndex = settled.coreIndex - this.first
      const expected = this.blocks[dataIndex]
      const actual = b4a.toString(sha256(settled.block), 'hex')
      if (expected && actual !== expected) {
        throw new Error(`Block ${settled.coreIndex} checksum mismatch`)
      }
      try {
        // onBlock verifies + caches + writes; write-through keeps the fd write
        // from blocking a subsequent cache hit on the same region.
        await this.onBlock(settled.coreIndex, settled.block)
        this.completed.add(settled.coreIndex)
      } catch (err) {
        throw err
      }
    }

    if (this.cancelled) throw new Error('interrupted')
  }

  inflightFree(inflight) {
    if (this.paused) return false
    return inflight.size < this.window
  }
}

module.exports = { ChunkScheduler }
