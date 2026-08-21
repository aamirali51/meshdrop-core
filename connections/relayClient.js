'use strict'

// Global Port 443 WebSocket Relay Client for @mesh/core.
//
// Connects to the Cloudflare WSS relay (or custom HTTPS relay) to enable
// instant pairing and signaling fallback across restrictive NATs (symmetric NAT,
// pfSense, cellular CGNAT, university Wi-Fi, and firewalled networks) where
// direct UDP holepunching fails.

const DEFAULT_RELAY_URL = 'wss://meshdrop-relay.aamirabdullah33.workers.dev'

class RelayClient {
  /**
   * @param {object} [opts]
   * @param {string} [opts.relayUrl]
   * @param {string} [opts.localPeerId]  Hex noise public key of this device
   * @param {function} [opts.onMessage]   (topic, msg, fromPeerId) => void
   */
  constructor(opts = {}) {
    this.relayUrl = opts.relayUrl || DEFAULT_RELAY_URL
    this.localPeerId = opts.localPeerId || ''
    this.onMessage = opts.onMessage || (() => {})
    this.topics = new Set() // Set of active topic strings (e.g. 'p2p-pair-MD-XXXX...')
    this.sockets = new Map() // topic -> WebSocket instance
    this.started = false
    this.reconnectTimers = new Map()
  }

  setPeerId(peerId) {
    this.localPeerId = peerId
  }

  start() {
    this.started = true
    for (const topic of this.topics) {
      this._connectTopic(topic)
    }
  }

  stop() {
    this.started = false
    for (const [, timer] of this.reconnectTimers.entries()) {
      clearTimeout(timer)
    }
    this.reconnectTimers.clear()
    for (const [, ws] of this.sockets.entries()) {
      try {
        ws.close()
      } catch {}
    }
    this.sockets.clear()
  }

  join(topic) {
    if (!topic) return
    this.topics.add(topic)
    if (this.started) {
      this._connectTopic(topic)
    }
  }

  leave(topic) {
    if (!topic) return
    this.topics.delete(topic)
    if (this.reconnectTimers.has(topic)) {
      clearTimeout(this.reconnectTimers.get(topic))
      this.reconnectTimers.delete(topic)
    }
    const ws = this.sockets.get(topic)
    if (ws) {
      this.sockets.delete(topic)
      try {
        ws.close()
      } catch {}
    }
  }

  send(topic, payload) {
    if (!topic || !payload) return false
    const ws = this.sockets.get(topic)
    if (ws && ws.readyState === 1 /* OPEN */) {
      try {
        const msg = {
          topic,
          fromPeerId: this.localPeerId,
          data: payload,
          ts: Date.now()
        }
        ws.send(JSON.stringify(msg))
        return true
      } catch (err) {
        console.warn('[RelayClient] Failed to send over WSS:', err.message)
      }
    }
    return false
  }

  _connectTopic(topic) {
    if (!this.started || !this.topics.has(topic)) return
    if (this.sockets.has(topic)) {
      const existing = this.sockets.get(topic)
      if (existing && (existing.readyState === 0 || existing.readyState === 1)) {
        return
      }
    }

    const WS = typeof globalThis.WebSocket !== 'undefined'
      ? globalThis.WebSocket
      : null

    if (!WS) {
      console.warn('[RelayClient] globalThis.WebSocket unavailable in this environment')
      return
    }

    try {
      const url = `${this.relayUrl}?topic=${encodeURIComponent(topic)}`
      const ws = new WS(url)
      this.sockets.set(topic, ws)

      ws.onopen = () => {
        console.log(`[RelayClient] Connected to Cloudflare WSS relay for topic: ${topic}`)
      }

      ws.onmessage = (event) => {
        try {
          const raw = typeof event.data === 'string' ? event.data : event.data.toString('utf8')
          const parsed = JSON.parse(raw)
          if (!parsed || (parsed.fromPeerId && parsed.fromPeerId === this.localPeerId)) {
            return
          }
          this.onMessage(topic, parsed.data, parsed.fromPeerId)
        } catch (err) {
          console.warn('[RelayClient] Error parsing incoming WSS message:', err.message)
        }
      }

      ws.onclose = () => {
        this.sockets.delete(topic)
        if (this.started && this.topics.has(topic)) {
          this._scheduleReconnect(topic)
        }
      }

      ws.onerror = (err) => {
        try {
          ws.close()
        } catch {}
      }
    } catch (err) {
      console.warn('[RelayClient] WebSocket init error:', err.message)
      this._scheduleReconnect(topic)
    }
  }

  _scheduleReconnect(topic) {
    if (!this.started || !this.topics.has(topic)) return
    if (this.reconnectTimers.has(topic)) return
    const timer = setTimeout(() => {
      this.reconnectTimers.delete(topic)
      this._connectTopic(topic)
    }, 3000)
    if (timer.unref) timer.unref()
    this.reconnectTimers.set(topic, timer)
  }
}

module.exports = { RelayClient, DEFAULT_RELAY_URL }
