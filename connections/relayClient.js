'use strict'

// Global Port 443 WebSocket + KV Relay Client for @mesh/core.
//
// Connects to Cloudflare edge relay to enable instant pairing and signaling
// fallback across restrictive NATs (symmetric NAT, pfSense, cellular CGNAT,
// university Wi-Fi, and firewalled networks) where direct UDP holepunching fails.

const DEFAULT_RELAY_URL = 'https://meshdrop-relay.aamirabdullah33.workers.dev'

class RelayClient {
  /**
   * @param {object} [opts]
   * @param {string} [opts.relayUrl]
   * @param {string} [opts.mode]         'auto' | 'relay-primary' | 'direct-only'
   * @param {string} [opts.localPeerId]  Hex noise public key of this device
   * @param {function} [opts.onMessage]   (topic, msg, fromPeerId) => void
   */
  constructor(opts = {}) {
    this.baseUrl = (opts.relayUrl || DEFAULT_RELAY_URL).replace(/\/+$/, '')
    this.mode = opts.mode || 'auto' // 'auto' | 'relay-primary' | 'direct-only'
    this.localPeerId = opts.localPeerId || ''
    this.onMessage = opts.onMessage || (() => {})
    this.topics = new Set() // Set of active topic strings (e.g. 'p2p-pair-MD-XXXX...')
    this.sockets = new Map() // topic -> WebSocket instance
    this.pollTimers = new Map() // topic -> interval timer
    this.seenMessageIds = new Set()
    this.started = false
    this.reconnectTimers = new Map()
    // topic -> [{ msg, ts }]: sends that fired while the WebSocket was still
    // CONNECTING (lazy start: a pairing challenge fires right after start()).
    // Flushed on open; dropped after RELAY_SEND_TTL.
    this.pendingSends = new Map()
  }

  setPeerId(peerId) {
    this.localPeerId = peerId
  }

  setMode(mode) {
    if (!mode || this.mode === mode) return
    this.mode = mode
    if (this.mode === 'direct-only') {
      this.stop()
    } else if (this.started) {
      this.start()
    }
  }

  setRelayUrl(url) {
    if (!url || typeof url !== 'string') {
      this.baseUrl = DEFAULT_RELAY_URL
    } else {
      this.baseUrl = url.trim().replace(/\/+$/, '')
    }
    // Reconnect existing topics with new URL if active
    if (this.started && this.mode !== 'direct-only') {
      this.stop()
      this.start()
    }
  }

  start() {
    if (this.mode === 'direct-only') return
    this.started = true
    for (const topic of this.topics) {
      this._connectTopic(topic)
      this._startPolling(topic)
    }
  }

  stop() {
    this.started = false
    for (const [, timer] of this.reconnectTimers.entries()) {
      clearTimeout(timer)
    }
    this.reconnectTimers.clear()
    for (const [, timer] of this.pollTimers.entries()) {
      clearInterval(timer)
    }
    this.pollTimers.clear()
    this.pendingSends.clear()
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
    if (this.started && this.mode !== 'direct-only') {
      this._connectTopic(topic)
      this._startPolling(topic)
    }
  }

  leave(topic) {
    if (!topic) return
    this.topics.delete(topic)
    this.pendingSends.delete(topic)
    if (this.reconnectTimers.has(topic)) {
      clearTimeout(this.reconnectTimers.get(topic))
      this.reconnectTimers.delete(topic)
    }
    if (this.pollTimers.has(topic)) {
      clearInterval(this.pollTimers.get(topic))
      this.pollTimers.delete(topic)
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
    if (!topic || !payload || this.mode === 'direct-only') return false
    const msgId = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
    const msg = {
      id: msgId,
      topic,
      fromPeerId: this.localPeerId,
      data: payload,
      ts: Date.now()
    }

    this.seenMessageIds.add(msgId)

    // 1. Send via WebSocket if open. While the socket is still CONNECTING
    //    (lazy start), buffer instead — a pairing challenge fires immediately
    //    after start() and must not be lost to the handshake window.
    const ws = this.sockets.get(topic)
    if (ws && ws.readyState === 1 /* OPEN */) {
      try {
        ws.send(JSON.stringify(msg))
      } catch (err) {
        console.warn('[RelayClient] WSS send error:', err.message)
      }
    } else if (ws && ws.readyState === 0 /* CONNECTING */) {
      const q = this.pendingSends.get(topic) || []
      q.push({ msg, ts: Date.now() })
      if (q.length > 32) q.shift()
      this.pendingSends.set(topic, q)
    }

    // 2. Publish to Cloudflare Global KV via HTTP POST
    const httpUrl = `${this.baseUrl}/poll?topic=${encodeURIComponent(topic)}`
    if (typeof fetch === 'function') {
      fetch(httpUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(msg)
      }).catch(() => {})
    }

    return true
  }

  _startPolling(topic) {
    if (this.pollTimers.has(topic)) return
    const poll = async () => {
      if (!this.started || !this.topics.has(topic)) return
      const httpUrl = `${this.baseUrl}/poll?topic=${encodeURIComponent(topic)}`
      try {
        if (typeof fetch !== 'function') return
        const res = await fetch(httpUrl)
        if (!res.ok) return
        const body = await res.json()
        if (body && Array.isArray(body.messages)) {
          for (const m of body.messages) {
            if (!m || !m.data) continue
            if (m.fromPeerId && m.fromPeerId === this.localPeerId) continue
            const mid = m.id || `${m.fromPeerId}_${m.ts}_${JSON.stringify(m.data).slice(0, 32)}`
            if (this.seenMessageIds.has(mid)) continue
            this.seenMessageIds.add(mid)
            if (this.seenMessageIds.size > 500) {
              const arr = Array.from(this.seenMessageIds)
              this.seenMessageIds = new Set(arr.slice(-250))
            }
            this.onMessage(topic, m.data, m.fromPeerId)
          }
        }
      } catch {}
    }

    // Run immediate poll, then every 1200ms
    poll()
    const timer = setInterval(poll, 1200)
    if (timer.unref) timer.unref()
    this.pollTimers.set(topic, timer)
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

    if (!WS) return

    try {
      const wssUrl = this.baseUrl.replace(/^http/, 'ws') + `/?topic=${encodeURIComponent(topic)}`
      const ws = new WS(wssUrl)
      this.sockets.set(topic, ws)

      ws.onopen = () => {
        console.log(`[RelayClient] Connected to Cloudflare WSS relay for topic: ${topic}`)
        // Flush sends buffered while the socket was connecting (lazy start).
        const q = this.pendingSends.get(topic)
        if (q && q.length) {
          this.pendingSends.set(topic, [])
          for (const { msg, ts } of q) {
            if (Date.now() - ts > 30000) continue // stale — past the nonce challenge window
            try {
              ws.send(JSON.stringify(msg))
            } catch {}
          }
        }
      }

      ws.onmessage = (event) => {
        try {
          const raw = typeof event.data === 'string' ? event.data : event.data.toString('utf8')
          const parsed = JSON.parse(raw)
          if (!parsed || (parsed.fromPeerId && parsed.fromPeerId === this.localPeerId)) {
            return
          }
          const mid = parsed.id || `${parsed.fromPeerId}_${parsed.ts}`
          if (this.seenMessageIds.has(mid)) return
          this.seenMessageIds.add(mid)
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

      ws.onerror = () => {
        try {
          ws.close()
        } catch {}
      }
    } catch {
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
