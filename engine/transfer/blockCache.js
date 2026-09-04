'use strict'

// BlockCache: small in-memory LRU for VERIFIED head/tail/manifest blocks.
//
// Why: video startup must not stall on disk-write latency. The receive path
// hash-verifies a block, then writes it to the .part file; a player mounting
// that same region (head metadata, tail cues) would otherwise wait on the fd.
// Head/tail priority blocks are cached here after verification and BEFORE the
// fd write so a cache hit can be served (or re-served) without a second
// network fetch OR a disk wait. Entries are keyed (transferId, coreIndex) and
// flushed on interrupt/cancel/resume so a stale block is never served to a new
// transfer reusing the same id. Caps are profile-bounded (the LRU is only
// meaningful on mobile if it stays small on cellular).
//
// This module is deliberately dependency-free and uses a plain Map (insertion
// order = recency) instead of a linked list — the core runs in the single-
// threaded Bare worklet on mobile, so no lock is needed.

class BlockCache {
  constructor({ capBytes = 64 * 1024 * 1024, ttlMs = 60 * 1000 } = {}) {
    this.capBytes = capBytes
    this.ttlMs = ttlMs
    this._map = new Map() // key -> { bytes, length, at }
    this._bytes = 0
  }

  _key(transferId, coreIndex) {
    return `${transferId}:${coreIndex}`
  }

  setCapBytes(capBytes) {
    this.capBytes = capBytes
    this._evict()
  }

  // Store a verified block. `bytes` is stored by reference (the caller's
  // verified buffer — it is immutable once verified, and copying 64 KiB per
  // block would defeat the purpose).
  set(transferId, coreIndex, bytes) {
    const key = this._key(transferId, coreIndex)
    const length = bytes ? bytes.length : 0
    if (!bytes || length === 0) return
    const prev = this._map.get(key)
    if (prev) {
      // Refresh recency + TTL without re-accounting bytes.
      this._map.delete(key)
      this._map.set(key, { bytes, length, at: Date.now() })
      return
    }
    this._map.set(key, { bytes, length, at: Date.now() })
    this._bytes += length
    this._evict()
  }

  get(transferId, coreIndex) {
    const key = this._key(transferId, coreIndex)
    const entry = this._map.get(key)
    if (!entry) return null
    if (this.ttlMs > 0 && Date.now() - entry.at > this.ttlMs) {
      this._map.delete(key)
      this._bytes -= entry.length
      return null
    }
    // Refresh recency (Map re-insert).
    this._map.delete(key)
    this._map.set(key, entry)
    return entry.bytes
  }

  // Drop every block belonging to a transfer (interrupt/cancel/resume — a
  // .part survives an interrupt and must never mix with stale cached blocks).
  flushTransfer(transferId) {
    const prefix = `${transferId}:`
    let removed = 0
    for (const key of this._map.keys()) {
      if (key.startsWith(prefix)) {
        removed += this._map.get(key).length
        this._map.delete(key)
      }
    }
    this._bytes = Math.max(0, this._bytes - removed)
  }

  clear() {
    this._map.clear()
    this._bytes = 0
  }

  get size() {
    return this._map.size
  }

  get bytes() {
    return this._bytes
  }

  _evict() {
    // Evict oldest-first (Map iteration = insertion order) until under cap.
    while (this._bytes > this.capBytes && this._map.size > 0) {
      const oldestKey = this._map.keys().next().value
      const oldest = this._map.get(oldestKey)
      this._map.delete(oldestKey)
      this._bytes = Math.max(0, this._bytes - oldest.length)
    }
  }
}

module.exports = { BlockCache }
