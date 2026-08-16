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
  constructor({ core, firstDataBlock, lastDataBlock, blocks, blockSize, onBlock }) {
    this.core = core
    this.first = firstDataBlock
    this.last = lastDataBlock
    this.blocks = blocks // manifest.blocks[0] corresponds to core block firstDataBlock
    this.blockSize = blockSize
    this.onBlock = onBlock // async (coreIndex, block) => void (write + hash)
    this.window = MIN_WINDOW
    this.cancelled = false
    this.paused = false
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

  async run() {
    let next = this.first
    let failures = 0
    const inflight = new Map() // coreIndex -> settled promise

    while (next <= this.last || inflight.size > 0) {
      while (this.inflightFree(inflight) && next <= this.last) {
        if (this.cancelled) break
        const coreIndex = next++
        inflight.set(
          coreIndex,
          this.fetchBlock(coreIndex).then(
            (block) => ({ coreIndex, block, err: null }),
            (err) => ({ coreIndex, block: null, err })
          )
        )
      }

      const settled = await Promise.race(Array.from(inflight.values()))
      inflight.delete(settled.coreIndex)

      if (settled.err) {
        if (this.cancelled) break
        failures++
        this.window = Math.max(MIN_WINDOW, Math.floor(this.window / 2))
        if (next > settled.coreIndex) next = settled.coreIndex // re-queue this block
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
