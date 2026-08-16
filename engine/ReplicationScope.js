'use strict'

// The private Corestore contains identity and application metadata. This
// boundary exposes only the exchange Corestore to authenticated peers.

const SCHEMA_VERSION = 'exchange.1'
const META_NAME = 'exchange-metadata'

class ReplicationScope {
  constructor({ exchangeStore, isPeerTrusted, onStream }) {
    this.exchangeStore = exchangeStore
    this.isPeerTrusted = isPeerTrusted || (() => true)
    this.onStream = onStream || (() => {})
    this.streams = new Map()
  }

  async init() {
    const meta = this.exchangeStore.get({ name: META_NAME })
    await meta.ready()
    if (meta.length === 0) {
      await meta.append(Buffer.from(JSON.stringify({ schema: SCHEMA_VERSION })))
    }
    await meta.close().catch(() => {})
  }

  get(key) {
    return this.exchangeStore.get(key)
  }

  replicate(peerId, connection) {
    if (!peerId || !connection || !this.isPeerTrusted(peerId)) return null
    if (this.streams.has(peerId)) return this.streams.get(peerId)

    const stream = this.exchangeStore.replicate(connection)
    this.streams.set(peerId, stream)
    stream.on('close', () => this.streams.delete(peerId))
    stream.on('error', () => {})
    this.onStream(stream, peerId)
    return stream
  }

  close(peerId) {
    const stream = this.streams.get(peerId)
    if (!stream) return
    this.streams.delete(peerId)
    try {
      stream.destroy()
    } catch {}
  }

  closeAll() {
    for (const peerId of this.streams.keys()) this.close(peerId)
  }

  activePeers() {
    return Array.from(this.streams.keys())
  }
}

module.exports = ReplicationScope
