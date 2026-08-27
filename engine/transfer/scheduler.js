'use strict'

// ChunkScheduler: parallel, adaptive block fetcher. Starts with an 8-wide
// window and grows up to 64 on clean runs, shrinking on timeout/errors.
// Blocks hash-verify as they land; offsets are written positionally so order
// of completion does not matter. Errors (timeouts, checksum mismatches,
// cancellation) are thrown and must be handled by the caller.

const b4a = require('b4a')
const { sha256 } = require('../../crypto.js')
const { MIN_WINDOW, MAX_WINDOW, sleep } = require('./constants.js')

class ChunkScheduler {
  constructor({ core, firstDataBlock, lastDataBlock, blocks, blockSize, onBlock, isStreaming = false }) {
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
    this.playhead = firstDataBlock
    this.streamWindow = 64 // ~4MB lookahead at 64KB chunks
    this.completed = new Set()
    this.inflight = new Map() // coreIndex -> settled promise
    this._queueDirty = false
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
   * Dynamically reprioritize chunk download to focus on the user's current playback position.
   * @param {number} coreIndex  The target block index to jump to
   */
  setPlayhead(coreIndex) {
    const clamped = Math.max(this.first, Math.min(this.last, coreIndex))
    if (this.playhead !== clamped) {
      this.playhead = clamped
      this._queueDirty = true
    }
  }

  async fetchBlock(coreIndex) {
    let attempts = 0
    for (;;) {
      if (this.cancelled) return null
      try {
        const block = await this.core.get(coreIndex, { wait: true, timeout: 15000 })
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
    // 1. If in streaming mode or playhead is active, check priority stream window first
    if (this.isStreaming || this.playhead > this.first) {
      const windowEnd = Math.min(this.last, this.playhead + this.streamWindow)
      for (let i = this.playhead; i <= windowEnd; i++) {
        if (!this.completed.has(i) && !this.inflight.has(i)) {
          return i
        }
      }
    }

    // 2. Normal sequential sweep across the entire range
    for (let i = this.first; i <= this.last; i++) {
      if (!this.completed.has(i) && !this.inflight.has(i)) {
        return i
      }
    }
    return null
  }

  async run() {
    let failures = 0

    while (this.completed.size < (this.last - this.first + 1) || this.inflight.size > 0) {
      while (this.inflightFree(this.inflight) && !this.cancelled) {
        const coreIndex = this._getNextBlockIndex()
        if (coreIndex === null) break // all remaining blocks are already inflight

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

      failures = Math.max(0, failures - 1)
      if (failures === 0 && this.window < MAX_WINDOW) this.window++

      const dataIndex = settled.coreIndex - this.first
      const expected = this.blocks[dataIndex]
      const actual = b4a.toString(sha256(settled.block), 'hex')
      if (expected && actual !== expected) {
        throw new Error(`Block ${settled.coreIndex} checksum mismatch`)
      }
      try {
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
