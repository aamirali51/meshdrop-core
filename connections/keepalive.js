'use strict'

// PING/PONG latency probe over the p2p-signal-v1 channel. A light measurement
// protocol: every PING_INTERVAL_MS we send a timestamped PING to each
// authenticated peer; the peer echoes it back as a PONG. RTT feeds a rolling
// per-peer average (avgLatencyMs), and pings without a PONG feed a rolling
// success rate (packet-loss proxy).

const { MESSAGES } = require('../protocol.js')

const PING_INTERVAL_MS = 30000 // how often to ping each authenticated peer (30s: keeps the link alive without heating the radio)
const PING_TIMEOUT = 3000 // a ping without a PONG inside this window counts as lost
const PING_WINDOW = 20 // rolling outcomes / RTT samples kept per peer

function createKeepAlive(ctx) {
  const { engine, peers } = ctx

  function ensurePingState(peerObj) {
    if (peerObj.pings) return
    peerObj.pings = {
      nextId: 0,
      outstanding: new Map(), // id -> { sentAt, timer }
      rttSamples: [], // rolling RTT values (ms)
      window: [] // rolling ping outcomes: { ok: boolean }
    }
  }

  function pushPingResult(peerObj, ok, rtt) {
    const p = peerObj.pings
    if (ok && rtt >= 0) {
      p.rttSamples.push(rtt)
      if (p.rttSamples.length > PING_WINDOW) p.rttSamples.shift()
    }
    p.window.push({ ok })
    if (p.window.length > PING_WINDOW) p.window.shift()
  }

  function sendPings() {
    if (ctx.stopped) return
    for (const [peerId, peerObj] of peers.entries()) {
      if (
        !peerObj.pairing ||
        !peerObj.pairing.complete ||
        !peerObj.device ||
        !peerObj.device.isOnline ||
        !peerObj.signaling
      ) {
        continue
      }
      ensurePingState(peerObj)
      // Never stack more than one outstanding ping per peer at a time.
      if (peerObj.pings.outstanding.size > 0) continue
      const id = ++peerObj.pings.nextId
      const sentAt = Date.now()
      const timer = setTimeout(() => {
        peerObj.pings.outstanding.delete(id)
        pushPingResult(peerObj, false, null)
      }, PING_TIMEOUT)
      if (timer.unref) timer.unref()
      peerObj.pings.outstanding.set(id, { sentAt, timer })
      try {
        peerObj.signaling.send({ type: MESSAGES.PING, id, sentAt })
      } catch (err) {
        const entry = peerObj.pings.outstanding.get(id)
        if (entry) {
          clearTimeout(entry.timer)
          peerObj.pings.outstanding.delete(id)
        }
      }
    }
  }

  function handlePing(peerId, msg) {
    const peerObj = peers.get(peerId)
    if (!peerObj || !peerObj.signaling) return
    if (!msg || typeof msg.id !== 'number' || typeof msg.sentAt !== 'number') return
    try {
      peerObj.signaling.send({ type: MESSAGES.PONG, id: msg.id, sentAt: msg.sentAt })
    } catch {}
  }

  function handlePong(peerId, msg) {
    const peerObj = peers.get(peerId)
    if (!peerObj || !peerObj.pings) return
    if (!msg || typeof msg.id !== 'number' || typeof msg.sentAt !== 'number') return
    const entry = peerObj.pings.outstanding.get(msg.id)
    if (!entry) return
    peerObj.pings.outstanding.delete(msg.id)
    clearTimeout(entry.timer)
    pushPingResult(peerObj, true, Date.now() - msg.sentAt)
  }

  // Rolling-average RTT across authenticated peers; null until measured.
  function getPeerLatency() {
    let total = 0
    let count = 0
    for (const p of peers.values()) {
      if (!p.pairing || !p.pairing.complete || !p.device || !p.device.isOnline) continue
      const samples = p.pings && p.pings.rttSamples
      if (!samples || samples.length === 0) continue
      total += samples.reduce((a, b) => a + b, 0) / samples.length
      count++
    }
    return count > 0 ? Math.round(total / count) : null
  }

  // Packet-loss proxy from ping outcomes; null until enough samples exist.
  function getPacketLoss() {
    let failed = 0
    let total = 0
    for (const p of peers.values()) {
      if (!p.pairing || !p.pairing.complete || !p.device || !p.device.isOnline) continue
      const window = p.pings && p.pings.window
      if (!window) continue
      for (const r of window) {
        total++
        if (!r.ok) failed++
      }
    }
    return total >= 5 ? Math.round((failed / total) * 100) : null
  }

  return { sendPings, handlePing, handlePong, getPeerLatency, getPacketLoss }
}

module.exports = { createKeepAlive, PING_INTERVAL_MS }
