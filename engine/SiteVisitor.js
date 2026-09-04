'use strict'

// SiteVisitor — MeshDrop Sites VISITOR side of the live-read path (slice 3).
// Mirrors SiteServer: a visitor enters a SITE- code, joins the code-derived
// discovery topic, learns the real siteId from the host, then proves it is on
// the host's allowlist via SITE_HELLO, lists the drive and reads files (with
// byte ranges) as a stream of SITE_READ_DATA chunks that are reassembled +
// SHA-256 verified against the host's SITE_READ_END.
//
// The visitor is never paired to the host: the allowlist is the only gate.
//
// A visitor may hold SEVERAL simultaneous visits (folder shares from multiple
// hosts). Each visit is an object keyed by siteId (while a site is still being
// discovered the siteId is unknown, so pending visits are keyed by code until
// SITE_DISCOVER_RES reveals the real siteId). All outbound requests and every
// inbound message carry the siteId, so per-visit state never cross-talks.

const { createHash } = require('crypto')
const { EVENTS, siteTopic } = require('../protocol.js')
const { normalizeSiteCode } = require('../sites-codes.js')

const SHA256 = (buf) => createHash('sha256').update(buf).digest('hex')
// Discovery topic a visitor joins with just the SITE- code; the host announces
// on it while the site is published and answers with the real siteId.
const siteCodeTopic = (code) => `p2p-site-code-${code}`

const HELLO_TIMEOUT_MS = 30000
const READ_TIMEOUT_MS = 120000
const MAX_READ_SIZE = 1024 * 1024 * 1024 // 1 GiB safety cap

class SiteVisitor {
  constructor({ engine }) {
    this.engine = engine
    // Active visits. A visit is created in visitSite() keyed by its code
    // (pending — the siteId is not known yet) and re-keyed to its real siteId
    // once SITE_DISCOVER_RES answers.
    this._visits = new Map() // siteId|pendingCode -> visit object
    // Peers that verified this device for one of their sites (we answered a
    // SITE_VERIFY_CHALLENGE for them). These are site relationships, NOT
    // pairings — the automatic pairing loop must leave them alone.
    this._verifierHosts = new Set() // peerId -> true
    this._serialize = Promise.resolve()
    this._pendingReads = new Map() // reqId -> { siteId, resolve, reject, chunks: [], size, timer }
    this._pendingWrites = new Map() // reqId -> { siteId, resolve, reject, timer }
    this._reqCounter = 0
    this._listeners = []
    this._onPeerDisconnected = (info) => {
      const peerId = (info && (info.peerId || info.id)) || ''
      if (!peerId) return
      // Fail every visit whose host was that peer.
      for (const [key, site] of Array.from(this._visits.entries())) {
        if (site.hostPeerId && site.hostPeerId === peerId) {
          this._failVisit(key, 'host disconnected')
        }
      }
    }
    this.engine.on(EVENTS.PEER_DISCONNECTED, this._onPeerDisconnected)
    this._listeners.push([EVENTS.PEER_DISCONNECTED, this._onPeerDisconnected])
  }

  _chain(fn) {
    const run = this._serialize.then(fn, fn)
    this._serialize = run.catch(() => {})
    return run
  }

  // ─── Visit lifecycle ───────────────────────────────────────────────────────

  // Enter a SITE- code: normalize it, join the code-discovery topic, learn the
  // real siteId from the host, then complete SITE_HELLO. Resolves once the
  // host confirms we're allowlisted. Multiple visits may run concurrently.
  async visitSite(code) {
    return this._chain(async () => {
      const clean = normalizeSiteCode(code)
      if (!clean) throw new Error('Invalid SITE code — expected SITE-XXXX-XXXX')
      if (this._findByCode(clean)) {
        throw new Error('Already visiting that folder — close it first')
      }

      const site = {
        code: clean,
        siteId: null, // learned from the host
        name: clean,
        hostPeerId: null,
        _stage: 'discover'
      }
      this._visits.set(clean, site)

      // Join the discovery topic. The host announces on it while published and
      // answers SITE_DISCOVER with the real siteId.
      this.engine.topicRegistry.join(siteCodeTopic(clean), { client: true, server: true })

      // Re-broadcast SITE_DISCOVER on an interval while discovering: the host
      // may connect AFTER our first broadcast (topic join races the DHT
      // connection), and PEER_CONNECTED only fires for trusted/pairing peers —
      // not for an unpaired site host. A 2s re-broadcast until the host
      // answers is the robust equivalent of the pairing drive loop.
      if (site._discoverTimer) clearInterval(site._discoverTimer)
      site._discoverTimer = setInterval(() => {
        const cur = this._visits.get(clean)
        if (!cur || cur !== site || cur._stage !== 'discover') {
          clearInterval(site._discoverTimer)
          site._discoverTimer = null
          return
        }
        this._broadcastOnTopic({ type: 'SITE_DISCOVER', code: site.code })
      }, 2000)
      if (site._discoverTimer.unref) site._discoverTimer.unref()
      this._broadcastOnTopic({ type: 'SITE_DISCOVER', code: clean })

      return new Promise((resolve, reject) => {
        site._visitResolver = { resolve, reject }
        site._visitTimer = setTimeout(() => {
          this._failVisit(clean, 'No host responded — check the site code and try again')
          reject(new Error('No host responded — check the site code and try again'))
        }, HELLO_TIMEOUT_MS)
        if (site._visitTimer.unref) site._visitTimer.unref()
      })
    })
  }

  // The visit-map key for a site object (code while pending, siteId after).
  _keyFor(site) {
    for (const [key, s] of this._visits.entries()) {
      if (s === site) return key
    }
    return null
  }

  _findByCode(code) {
    for (const [, site] of this._visits.entries()) {
      if (site.code === code) return site
    }
    return null
  }

  // Resolve a siteId OR a pending code to the visit-map key.
  _resolveKey(siteIdOrCode) {
    if (!siteIdOrCode) return null
    if (this._visits.has(siteIdOrCode)) return siteIdOrCode
    const byCode = this._findByCode(siteIdOrCode)
    return byCode ? byCode.code : null
  }

  _requireSite(siteId) {
    const key = this._resolveKey(siteId)
    const site = key ? this._visits.get(key) : null
    if (!site) throw new Error('Not visiting that folder')
    return site
  }

  // Fail ONE visit: leave its topics, reject only its pending requests, clear
  // only its timers, and emit site:visit:stopped for that site.
  _failVisit(key, reason) {
    const prev = this._visits.get(key)
    if (!prev) return
    this._visits.delete(key)
    try { if (prev.code) this.engine.topicRegistry.leave(siteCodeTopic(prev.code)) } catch {}
    try { if (prev.siteId) this.engine.topicRegistry.leave(siteTopic(prev.siteId)) } catch {}
    const belongsToThisVisit = (p) => p && p.siteId && prev.siteId && p.siteId === prev.siteId
    for (const [reqId, p] of Array.from(this._pendingReads.entries())) {
      if (!belongsToThisVisit(p)) continue
      if (p.timer) clearTimeout(p.timer)
      this._pendingReads.delete(reqId)
      p.reject(new Error('Site read interrupted: ' + reason))
    }
    for (const [reqId, p] of Array.from(this._pendingWrites.entries())) {
      if (!belongsToThisVisit(p)) continue
      if (p.timer) clearTimeout(p.timer)
      this._pendingWrites.delete(reqId)
      p.reject(new Error('Site write interrupted: ' + reason))
    }
    if (prev._requestWaiters) {
      for (const w of prev._requestWaiters) { if (w.timer) clearTimeout(w.timer); w.reject(new Error('Site request interrupted: ' + reason)) }
      prev._requestWaiters = []
    }
    if (prev._visitResolver) prev._visitResolver = null
    if (prev._visitTimer) clearTimeout(prev._visitTimer)
    prev._visitTimer = null
    if (prev._discoverTimer) {
      clearInterval(prev._discoverTimer)
      prev._discoverTimer = null
    }
    this.engine.emit('site:visit:stopped', { siteId: prev.siteId, reason })
  }

  // Leave a specific visit by siteId (or, when the siteId is not yet known, by
  // the pending code). Leaving with no argument fails every active visit.
  async leaveSite(siteIdOrCode) {
    return this._chain(async () => {
      if (siteIdOrCode) {
        const key = this._resolveKey(siteIdOrCode)
        const prev = key ? this._visits.get(key) : null
        if (key && prev) this._failVisit(key, 'left')
        return { success: true, siteId: prev ? prev.siteId : null }
      }
      const keys = Array.from(this._visits.keys())
      for (const k of keys) this._failVisit(k, 'left')
      return { success: true }
    })
  }

  // Public summary of every active (siteId-known) visit.
  listActiveVisits() {
    const out = []
    for (const [, site] of this._visits.entries()) {
      out.push({
        siteId: site.siteId, // null while still discovering
        code: site.code,
        name: site.name,
        hostPeerId: site.hostPeerId // null while still discovering
      })
    }
    return out
  }

  // Broadcast a message to every peer currently connected (the discovery
  // re-broadcast goes wide on purpose: the code-matching host answers).
  _broadcastOnTopic(msg) {
    let sent = 0
    for (const [, peerObj] of this.engine.peers.entries()) {
      if (peerObj && peerObj.signaling && typeof peerObj.signaling.send === 'function') {
        try {
          peerObj.signaling.send(msg)
          sent++
        } catch {}
      }
    }
    return sent
  }

  // ─── Read API ──────────────────────────────────────────────────────────────

  async list(siteId, path = '/') {
    const site = this._requireSite(siteId)
    const res = await this._request(
      { type: 'SITE_LIST', siteId: site.siteId, path },
      site
    )
    if (!res.ok) throw new Error(res.error || 'list failed')
    return res.entries || []
  }

  // Ask the host for recursive folder stats (file count, total size, newest
  // mtime). Used by the folder-card grid to show "24 files · 1.8 GB" without
  // walking the whole tree over the wire on the visitor side.
  async stats(siteId) {
    const site = this._requireSite(siteId)
    const res = await this._request(
      { type: 'SITE_STATS', siteId: site.siteId },
      site
    )
    if (!res.ok) throw new Error(res.error || 'stats failed')
    return res.stats || { fileCount: 0, dirCount: 0, totalBytes: 0, newestMtimeMs: 0, partial: false }
  }

  // Read a file (or byte range). Returns { status, size, start, end, hash,
  // body: Buffer } with the body SHA-256-verified against the host's hash.
  async read(siteId, path, { range, ifNoneMatch, progressive } = {}) {
    const site = this._requireSite(siteId)
    const reqId = `site-${++this._reqCounter}-${Date.now().toString(36)}`
    const msg = { type: 'SITE_READ', siteId: site.siteId, reqId, path }
    if (range) msg.range = range
    if (ifNoneMatch) msg.ifNoneMatch = ifNoneMatch
    if (progressive) msg.progressive = true

    return new Promise((resolve, reject) => {
      const entry = { siteId: site.siteId, chunks: [], bytes: 0, resolve, reject }
      entry.timer = setTimeout(() => {
        this._pendingReads.delete(reqId)
        reject(new Error('Site read timed out'))
      }, READ_TIMEOUT_MS)
      if (entry.timer.unref) entry.timer.unref()
      this._pendingReads.set(reqId, entry)

      const peerObj = this.engine.peers.get(site.hostPeerId)
      if (!peerObj || !peerObj.signaling) {
        this._pendingReads.delete(reqId)
        clearTimeout(entry.timer)
        reject(new Error('Host not connected'))
        return
      }
      try {
        peerObj.signaling.send(msg)
      } catch (err) {
        this._pendingReads.delete(reqId)
        clearTimeout(entry.timer)
        reject(err)
      }
    })
  }

  // ─── Inbound (from the host) ───────────────────────────────────────────────

  handleMessage(peerId, msg) {
    if (!msg || typeof msg.type !== 'string') return
    if (msg.type === 'SITE_DISCOVER_RES') this._handleDiscoverRes(peerId, msg)
    else if (msg.type === 'SITE_HELLO_ACK') this._handleHelloAck(peerId, msg)
    else if (msg.type === 'SITE_LIST_RES' || msg.type === 'SITE_MKDIR_RES' || msg.type === 'SITE_DELETE_RES' || msg.type === 'SITE_STATS_RES') this._resolveRequest(peerId, msg)
    else if (msg.type === 'SITE_WRITE_END') {
      const p = this._pendingWrites.get(msg.reqId)
      if (p) { this._pendingWrites.delete(msg.reqId); clearTimeout(p.timer); if (msg.ok) p.resolve(msg); else p.reject(new Error(msg.error || 'write failed')) }
    }
    else if (msg.type === 'SITE_READ_DATA') this._handleReadData(peerId, msg)
    else if (msg.type === 'SITE_READ_END') this._handleReadEnd(peerId, msg)
    else if (msg.type === 'SITE_CLOSED') {
      // The host closed one of its sites: fail the matching visit.
      if (msg.siteId && this._visits.has(msg.siteId)) {
        this._failVisit(msg.siteId, msg.reason || 'host closed')
      }
    }
  }

  // The host answered our SITE_DISCOVER: learn the real siteId, leave the
  // discovery topic, join the real site topic, and send SITE_HELLO.
  _handleDiscoverRes(peerId, msg) {
    const code = typeof msg.code === 'string' ? msg.code : null
    const site = code ? this._findByCode(code) : null
    if (!site || site._stage !== 'discover') return
    if (!msg.ok || typeof msg.siteId !== 'string') {
      site._stage = 'failed'
      const key = this._keyFor(site)
      this._failVisit(key || code, msg.error || 'site not found')
      return
    }
    // Move the pending (code-keyed) visit to its real siteId.
    const oldKey = this._keyFor(site)
    if (oldKey) this._visits.delete(oldKey)
    site.siteId = msg.siteId
    site.hostPeerId = peerId
    site._stage = 'hello'
    this._visits.set(msg.siteId, site)
    try {
      this.engine.topicRegistry.leave(siteCodeTopic(site.code))
    } catch {}
    this.engine.topicRegistry.join(siteTopic(msg.siteId), { client: true, server: true })
    const peerObj = this.engine.peers.get(peerId)
    if (peerObj && peerObj.signaling) {
      peerObj.signaling.send({ type: 'SITE_HELLO', siteId: msg.siteId })
    }
  }

  _handleHelloAck(peerId, msg) {
    const site = this._visits.get(msg.siteId)
    if (!site || site.siteId !== msg.siteId) return
    site.hostPeerId = peerId
    site.name = (msg.site && msg.site.name) || site.name
    site._stage = 'ready'
    if (site._discoverTimer) { clearInterval(site._discoverTimer); site._discoverTimer = null }
    if (site._visitTimer) {
      clearTimeout(site._visitTimer)
      site._visitTimer = null
    }
    if (site._helloTimer) { clearTimeout(site._helloTimer); site._helloTimer = null }
    if (msg.ok === false) {
      const err = new Error(msg.error || 'not allowed')
      if (site._visitResolver) {
        const r = site._visitResolver
        site._visitResolver = null
        r.reject(err)
      }
      this._failVisit(msg.siteId, 'not-allowed')
      return
    }
    if (site._visitResolver) {
      const r = site._visitResolver
      site._visitResolver = null
      r.resolve({ siteId: site.siteId, name: site.name, hostPeerId: peerId })
    }
    this.engine.emit('site:visit:started', { siteId: site.siteId, name: site.name, hostPeerId: peerId })
  }

  _handleReadData(peerId, msg) {
    const p = this._pendingReads.get(msg.reqId)
    if (!p || !p.siteId || !msg.siteId || msg.siteId !== p.siteId) return
    if (typeof msg.data !== 'string') return
    p.chunks.push(Buffer.from(msg.data, 'base64'))
    p.bytes += msg.data.length
    if (p.bytes > MAX_READ_SIZE) {
      this._pendingReads.delete(msg.reqId)
      clearTimeout(p.timer)
      p.reject(new Error('Site read exceeds safety cap'))
    }
  }

  _handleReadEnd(peerId, msg) {
    const p = this._pendingReads.get(msg.reqId)
    if (!p) return
    this._pendingReads.delete(msg.reqId)
    if (p.timer) clearTimeout(p.timer)
    if (!msg.ok) {
      p.reject(new Error(msg.error || 'read failed'))
      return
    }
    if (msg.notModified) {
      p.resolve({ status: 'not-modified', etag: msg.etag, mime: msg.mime, size: msg.size })
      return
    }
    const body = Buffer.concat(p.chunks)
    if (msg.hash && SHA256(body).toString('hex') !== msg.hash) {
      p.reject(new Error('Site read checksum mismatch')); return
    }
    p.resolve({ status: 'ok', size: msg.size, start: msg.start, end: msg.end, hash: msg.hash, body, mime: msg.mime, etag: msg.etag, headers: msg.headers })
  }

  async writeFile(siteId, path, data) {
    const site = this._requireSite(siteId)
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(String(data || ''), 'utf8')
    const reqId = `site-w-${++this._reqCounter}-${Date.now().toString(36)}`
    const CHUNK = 64 * 1024
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this._pendingWrites.delete(reqId); reject(new Error('Site write timed out')) }, READ_TIMEOUT_MS)
      if (timer.unref) timer.unref()
      this._pendingWrites.set(reqId, { siteId: site.siteId, resolve, reject, timer })
      const host = this.engine.peers.get(site.hostPeerId)
      if (!host || !host.signaling) { clearTimeout(timer); this._pendingWrites.delete(reqId); reject(new Error('Host not connected')); return }
      const send = (m) => host.signaling.send(m)
      try {
        if (buf.length <= CHUNK) {
          send({ type: 'SITE_WRITE', siteId: site.siteId, reqId, path, data: buf.toString('base64'), final: true })
        } else {
          send({ type: 'SITE_WRITE', siteId: site.siteId, reqId, path })
          for (let off = 0; off < buf.length; off += CHUNK) {
            const slice = buf.subarray(off, Math.min(off + CHUNK, buf.length))
            send({ type: 'SITE_WRITE_DATA', siteId: site.siteId, reqId, data: slice.toString('base64') })
          }
          send({ type: 'SITE_WRITE_END', siteId: site.siteId, reqId })
        }
      } catch (e) { clearTimeout(timer); this._pendingWrites.delete(reqId); reject(e) }
    })
  }

  async mkdir(siteId, path) {
    const site = this._requireSite(siteId)
    const res = await this._request(
      { type: 'SITE_MKDIR', siteId: site.siteId, path },
      site
    )
    if (!res.ok) throw new Error(res.error || 'mkdir failed')
    return res
  }

  async remove(siteId, path) {
    const site = this._requireSite(siteId)
    const res = await this._request(
      { type: 'SITE_DELETE', siteId: site.siteId, path },
      site
    )
    if (!res.ok) throw new Error(res.error || 'delete failed')
    return res
  }

  // Resolve a simple request (list/mkdir/delete) that comes back as one
  // message. Responses are addressed by siteId and the host answers one
  // in-flight request per site at a time, so a per-visit FIFO is safe; the
  // siteId lookup keeps concurrent requests to different visits isolated.
  _resolveRequest(peerId, msg) {
    if (!msg || !msg.siteId) return
    const site = this._visits.get(msg.siteId)
    if (!site || !site._requestWaiters || !site._requestWaiters.length) return
    const w = site._requestWaiters.shift()
    if (w.timer) clearTimeout(w.timer)
    w.resolve(msg)
  }

  async _request(msg, site) {
    return new Promise((resolve, reject) => {
      if (!site._requestWaiters) site._requestWaiters = []
      const timer = setTimeout(() => {
        const idx = site._requestWaiters.findIndex((w) => w.resolve === resolve)
        if (idx >= 0) site._requestWaiters.splice(idx, 1)
        reject(new Error('Site request timed out'))
      }, 15000)
      if (timer.unref) timer.unref()
      const waiter = { timer, resolve }
      site._requestWaiters.push(waiter)
      const peerObj = this.engine.peers.get(site.hostPeerId)
      const drop = () => {
        const idx = site._requestWaiters.findIndex((w) => w.resolve === resolve)
        if (idx >= 0) site._requestWaiters.splice(idx, 1)
        clearTimeout(timer)
      }
      if (!peerObj || !peerObj.signaling) {
        drop()
        reject(new Error('Host not connected'))
        return
      }
      try {
        peerObj.signaling.send(msg)
      } catch (err) {
        drop()
        reject(err)
      }
    })
  }

  async stop() {
    const keys = Array.from(this._visits.keys())
    for (const k of keys) this._failVisit(k, 'stopped')
    for (const [evt, fn] of this._listeners) this.engine.removeListener(evt, fn)
    this._listeners = []
  }
}

module.exports = { SiteVisitor }
