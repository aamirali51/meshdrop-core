'use strict'

// MetricsCollector reports only values that are actually measured. Nothing is
// fabricated: throughput comes from replication stream byte counters, peer
// counts from the connection registry, DHT node count from the routing table,
// and CPU/RAM from node:os.

const { os } = require('../compat.js')

class MetricsCollector {
  constructor({ intervalMs = 5000, swarm = null } = {}) {
    this.startedAt = Date.now()
    this.intervalMs = intervalMs
    // The swarm's dht is a dht-rpc instance whose routing table (nodes) is a
    // time-ordered set with a live .length — see hyperdht/lib/nat.js which
    // reads this.dht.nodes.length the same way.
    this.dht = swarm && swarm.dht ? swarm.dht : null
    this.streams = new Set() // replication streams to sample
    this.lastBytesIn = 0
    this.lastBytesOut = 0
    this.bytesIn = 0
    this.bytesOut = 0
    this.throughputInBps = 0
    this.throughputOutBps = 0
    this.timer = null
    this.samples = []

    // System metrics, refreshed each tick. null until first measured.
    this.cpuUsagePercent = null
    this.ramUsagePercent = null
    this._lastCpuTotal = null
    this._lastCpuIdle = null
  }

  start() {
    if (this.timer) return
    this.timer = setInterval(() => this._sample(), this.intervalMs)
    if (this.timer.unref) this.timer.unref()
  }

  // Point the sampler at a replacement swarm (network-change rebuild).
  rebind(swarm) {
    this.dht = swarm && swarm.dht ? swarm.dht : null
    this.samples = []
  }

  stop() {
    if (this.timer) clearInterval(this.timer)
    this.timer = null
  }

  trackStream(stream) {
    this.streams.add(stream)
    stream.on('close', () => this.streams.delete(stream))
  }

  _readCounters() {
    let inBytes = 0
    let outBytes = 0
    for (const s of this.streams) {
      if (typeof s.bytesRead === 'number') inBytes += s.bytesRead
      if (typeof s.bytesWritten === 'number') outBytes += s.bytesWritten
    }
    return { inBytes, outBytes }
  }

  _sample() {
    const { inBytes, outBytes } = this._readCounters()
    const deltaIn = Math.max(0, inBytes - this.lastBytesIn)
    const deltaOut = Math.max(0, outBytes - this.lastBytesOut)
    this.lastBytesIn = inBytes
    this.lastBytesOut = outBytes
    this.throughputInBps = Math.round(deltaIn / (this.intervalMs / 1000))
    this.throughputOutBps = Math.round(deltaOut / (this.intervalMs / 1000))
    this.bytesIn = inBytes
    this.bytesOut = outBytes
    this.samples.push({ t: Date.now(), inBps: this.throughputInBps, outBps: this.throughputOutBps })
    if (this.samples.length > 120) this.samples.shift()

    this._sampleSystemMetrics()
  }

  // CPU and RAM from node:os. CPU is system-wide: the idle/total delta over
  // the sampling interval is unit-agnostic, so it works regardless of platform
  // tick units (jiffies, 100ns units, etc.).
  _sampleSystemMetrics() {
    try {
      const total = os.totalmem()
      const free = os.freemem()
      if (total > 0) {
        this.ramUsagePercent = Math.min(
          100,
          Math.max(0, Math.round(((total - free) / total) * 100))
        )
      }
    } catch {}

    try {
      const cpus = os.cpus()
      if (!Array.isArray(cpus) || cpus.length === 0) return
      let total = 0
      let idle = 0
      for (const core of cpus) {
        const t = core && core.times
        if (!t) continue
        total += (t.user || 0) + (t.nice || 0) + (t.sys || 0) + (t.idle || 0) + (t.irq || 0)
        idle += t.idle || 0
      }
      if (total <= 0) return
      if (this._lastCpuTotal !== null) {
        const dTotal = total - this._lastCpuTotal
        const dIdle = idle - this._lastCpuIdle
        // Negative delta means the OS reset the counters; skip this tick.
        if (dTotal > 0) {
          this.cpuUsagePercent = Math.min(100, Math.max(0, Math.round((1 - dIdle / dTotal) * 100)))
        }
      }
      this._lastCpuTotal = total
      this._lastCpuIdle = idle
    } catch {}
  }

  // { peerCount, connected } come from the live connection registry; relayStatus
  // reflects whether the DHT relay fallback is configured for this node.
  // avgLatencyMs / packetLossPercent come from the PING/PONG probe in
  // connections.js (latency is a rolling average; loss is a ping success-rate
  // proxy). All pass-through values stay null until measured.
  snapshot({
    peerCount = 0,
    connected = false,
    relayStatus = 'Disabled',
    avgLatencyMs = null,
    packetLossPercent = null
  } = {}) {
    const maxThroughput = Math.max(this.throughputInBps, this.throughputOutBps)
    return {
      natType: null,
      relayStatus,
      dhtNodes: this.dht && this.dht.nodes ? this.dht.nodes.length : null,
      avgLatencyMs,
      packetLossPercent,
      noiseProtocol: 'Noise_XX_25519_ChaChaPoly_BLAKE2b',
      bandwidthMbps: maxThroughput > 0 ? Number((maxThroughput / (1024 * 1024)).toFixed(2)) : null,
      systemCpuUsage: this.cpuUsagePercent,
      systemRamUsage: this.ramUsagePercent,
      connectedPeersCount: peerCount,
      connected,
      uptimeMs: Date.now() - this.startedAt,
      bytesReceived: this.bytesIn,
      bytesSent: this.bytesOut
    }
  }
}

module.exports = MetricsCollector
