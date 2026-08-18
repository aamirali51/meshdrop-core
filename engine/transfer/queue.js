'use strict'

// TransferQueue: persistent, priority-ordered scheduler. Tracks running
// counts per direction (global) and per peer so a flood of queued transfers
// can never starve other peers or directions.

const { MAX_CONCURRENT, MAX_PER_PEER, DEFAULT_PRIORITY } = require('./constants.js')

class TransferQueue {
  constructor({ maxConcurrent = MAX_CONCURRENT, maxPerPeer = MAX_PER_PEER } = {}) {
    this.maxConcurrent = maxConcurrent
    this.maxPerPeer = maxPerPeer
    this.queued = { interactive: [], bulk: [], background: [] } // priority tier -> [transfer]
    this.active = { send: 0, receive: 0 } // running count per direction
    this.activeByPeer = new Map() // peerId -> { send: 0, receive: 0 }
  }

  enqueue(transfer) {
    const tier = this.queued[transfer.priority] || this.queued[DEFAULT_PRIORITY]
    transfer.queuedAt = transfer.queuedAt || Date.now()
    tier.push(transfer)
  }

  // Highest-priority queued transfer whose slots are free, or null.
  popNext(direction) {
    for (const tier of [this.queued.interactive, this.queued.bulk, this.queued.background]) {
      for (let i = 0; i < tier.length; i++) {
        const t = tier[i]
        if (t.direction !== direction) continue
        if (this._hasSlot(t)) {
          tier.splice(i, 1)
          return t
        }
      }
    }
    return null
  }

  size() {
    return this.queued.interactive.length + this.queued.bulk.length + this.queued.background.length
  }

  claim(transfer) {
    const dir = transfer.direction
    this.active[dir]++
    const key = transfer.peerId || 'anon'
    const entry = this.activeByPeer.get(key) || { send: 0, receive: 0 }
    entry[dir]++
    this.activeByPeer.set(key, entry)
  }

  release(transfer) {
    const dir = transfer.direction
    this.active[dir] = Math.max(0, this.active[dir] - 1)
    const key = transfer.peerId || 'anon'
    const entry = this.activeByPeer.get(key)
    if (entry) {
      entry[dir] = Math.max(0, entry[dir] - 1)
      if (entry.send === 0 && entry.receive === 0) this.activeByPeer.delete(key)
    }
  }

  _hasSlot(transfer) {
    const dir = transfer.direction
    // Sync transfers of small files are the many-files case (a photo library
    // round pushes hundreds of 1-10KB files). Serializing them per-peer (
    // maxPerPeer = 1) costs a full ready->manifest->blocks->done handshake
    // round-trip per file, which on a phone adds seconds per file. The sync
    // stream is ACK-windowed (bounded memory: 32 blocks ~ 2MB in flight), so
    // a modest per-peer + global sync concurrency pipelines small files
    // without disk thrash. Large regular transfers stay serialized.
    const isSync = transfer.source === 'sync' || transfer.isSync
    const maxGlobal = isSync ? Math.max(this.maxConcurrent, 4) : this.maxConcurrent
    if (this.active[dir] >= maxGlobal) return false
    const maxForPeer = isSync ? Math.max(this.maxPerPeer, 4) : this.maxPerPeer
    const key = transfer.peerId || 'anon'
    const entry = this.activeByPeer.get(key)
    return !entry || entry[dir] < maxForPeer
  }
}

module.exports = { TransferQueue }
