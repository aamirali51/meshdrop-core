'use strict'

const { path, fsp } = require('../compat.js')
const { EVENTS, siteTopic } = require('../protocol.js')
const { resolveSitePath, parseRange, listDir, getSiteMimeType, resolveSiteFile, loadSiteManifest, resolveSiteHeaders } = require('../siteProtocol.js')
const { normalizeSiteCode } = require('../sites-codes.js')

const CHUNK = 64 * 1024
const MAX_LIST_ENTRIES = 2000
const siteCodeTopic = (code) => `p2p-site-code-${code}`
const SHA256 = (buf) => require('crypto').createHash('sha256').update(buf).digest('hex')

class SiteServer {
  constructor({ engine }) {
    this.engine = engine
    this._sites = new Map() // siteId -> { ...site, root, manifest }
    this._visitors = new Map() // peerId -> Map<siteId, { siteId, helloAt, lastSeenAt }>
    this._serialize = Promise.resolve()
    this._maintenance = null
    this._listeners = []
    this._onPeerDisconnected = (info) => {
      const peerId = (info && (info.peerId || info.id)) || ''
      if (peerId) this._handlePeerGone(peerId)
    }
    this.engine.on(EVENTS.PEER_DISCONNECTED, this._onPeerDisconnected)
    this._listeners.push([EVENTS.PEER_DISCONNECTED, this._onPeerDisconnected])
  }

  _chain(fn) {
    const run = this._serialize.then(fn, fn)
    this._serialize = run.catch(() => {})
    return run
  }

  async publish(site) {
    return this._chain(async () => {
      const root = path.resolve(site.folderPath)
      const st = await fsp.stat(root).catch(() => null)
      if (!st || !st.isDirectory()) {
        throw new Error('Site folder is not a readable directory: ' + site.folderPath)
      }
      const manifest = await loadSiteManifest(fsp, root).catch(() => null)
      const current = this._sites.get(site.siteId)
      if (!current) {
        this.engine.topicRegistry.join(siteCodeTopic(site.code), { client: true, server: true })
        this.engine.topicRegistry.join(siteTopic(site.siteId), { client: true, server: true })
      }
      this._sites.set(site.siteId, { ...site, root, manifest: manifest || null })
      this._startMaintenance()
      console.log(`[MeshEngine] Site published: ${site.name} (${site.siteId}) on ${siteTopic(site.siteId)}`)
      return { siteId: site.siteId, code: site.code }
    })
  }

  async unpublish(siteId) {
    return this._chain(async () => {
      if (siteId) {
        const s = this._sites.get(siteId)
        if (!s) return
        try { this.engine.topicRegistry.leave(siteTopic(siteId)) } catch {}
        try { if (s.code) this.engine.topicRegistry.leave(siteCodeTopic(s.code)) } catch {}
        this._sites.delete(siteId)
        for (const [peerId, map] of this._visitors.entries()) {
          if (map.has(siteId)) {
            this._sendTo(peerId, { type: 'SITE_CLOSED', siteId, reason: 'host-unpublished' })
            map.delete(siteId)
            if (map.size === 0) this._visitors.delete(peerId)
          }
        }
        if (this._sites.size === 0) this._stopMaintenance()
      } else {
        for (const [sid, s] of this._sites.entries()) {
          try { this.engine.topicRegistry.leave(siteTopic(sid)) } catch {}
          try { if (s.code) this.engine.topicRegistry.leave(siteCodeTopic(s.code)) } catch {}
        }
        this._stopMaintenance()
        for (const peerId of Array.from(this._visitors.keys())) {
          this._sendTo(peerId, { type: 'SITE_CLOSED', siteId, reason: 'host-unpublished' })
        }
        this._visitors.clear()
        this._sites.clear()
      }
    })
  }

  _startMaintenance() {
    this._stopMaintenance()
    this._maintenance = setInterval(() => {
      const now = Date.now()
      for (const [peerId, map] of this._visitors.entries()) {
        for (const [sid, v] of map.entries()) {
          if (now - v.lastSeenAt > 300000) map.delete(sid)
        }
        if (map.size === 0) this._visitors.delete(peerId)
      }
    }, 60000)
    if (this._maintenance.unref) this._maintenance.unref()
  }

  _stopMaintenance() {
    if (this._maintenance) {
      clearInterval(this._maintenance)
      this._maintenance = null
    }
  }

  async _getFileSize(site, reqPath) {
    try {
      const { resolveSitePath } = require('../siteProtocol.js')
      const { fsp } = require('../compat.js')
      const abs = resolveSitePath(site.root, reqPath)
      if (!abs) return 1024*1024
      const st = await fsp.stat(abs)
      return st.size || 0
    } catch { return 1024*1024 }
  }

  getActiveSite() {
    const first = this._sites.values().next().value || null
    if (!first) return null
    return {
      siteId: first.siteId,
      id: first.siteId,
      name: first.name,
      code: first.code,
      folderPath: first.folderPath,
      createdAt: first.createdAt || 0,
      expiresAt: first.expiresAt || 0,
      expirationPreset: first.expirationPreset || 'never',
      allowlist: Array.isArray(first.allowlist) ? first.allowlist.slice() : [],
      writeMode: first.writeMode || 'read-only',
      spa: !!first.spa,
      visitorCount: this._visitors.size
    }
  }

  listActiveSites() {
    const out = []
    for (const s of this._sites.values()) {
      let count = 0
      for (const map of this._visitors.values()) if (map.has(s.siteId)) count++
      out.push({ siteId: s.siteId, id: s.siteId, name: s.name, code: s.code, folderPath: s.folderPath, createdAt: s.createdAt || 0, expiresAt: s.expiresAt || 0, expirationPreset: s.expirationPreset || 'never', allowlist: Array.isArray(s.allowlist) ? s.allowlist.slice() : [], writeMode: s.writeMode || 'read-only', spa: !!s.spa, visitorCount: count })
    }
    return out
  }

  _isExpired(site) { return site && site.expiresAt > 0 && Date.now() >= site.expiresAt }
  async _isAllowlisted(siteId, peerId) {
    const site = this._sites.get(siteId)
    if (site) {
      if (this._isExpired(site)) return false
      try { return await this.engine.siteManager.store.isAllowed(siteId, peerId) } catch { return site.allowlist ? site.allowlist.some((e) => typeof e === 'string' ? e === peerId : e.key === peerId) : false }
    }
    try { return await this.engine.siteManager.store.isAllowed(siteId, peerId) } catch { return false }
  }

  async _canWrite(siteId, peerId) {
    try { return await this.engine.siteManager.store.canWrite(siteId, peerId) } catch { return false }
  }

  handleMessage(peerId, msg) {
    if (!msg || typeof msg.type !== 'string') return
    if (msg.type === 'SITE_DISCOVER') this._handleDiscover(peerId, msg)
    else if (msg.type === 'SITE_HELLO') this._handleHello(peerId, msg).catch((err) => console.warn('[SiteServer] hello failed:', err.message))
    else if (msg.type === 'SITE_LIST') this._handleList(peerId, msg).catch((err) => console.warn('[SiteServer] list failed:', err.message))
    else if (msg.type === 'SITE_READ') this._handleRead(peerId, msg).catch((err) => console.warn('[SiteServer] read failed:', err.message))
    else if (msg.type === 'SITE_WRITE' || msg.type === 'SITE_WRITE_DATA' || msg.type === 'SITE_WRITE_END') this._handleWrite(peerId, msg).catch((err) => console.warn('[SiteServer] write failed:', err.message))
    else if (msg.type === 'SITE_MKDIR') this._handleMkdir(peerId, msg).catch((err) => console.warn('[SiteServer] mkdir failed:', err.message))
    else if (msg.type === 'SITE_DELETE') this._handleDelete(peerId, msg).catch((err) => console.warn('[SiteServer] delete failed:', err.message))
    else if (msg.type === 'SITE_STATS') this._handleStats(peerId, msg).catch((err) => console.warn('[SiteServer] stats failed:', err.message))
  }

  // Recursively compute { fileCount, dirCount, totalBytes, newestMtimeMs } for
  // a site root. Bounded so a pathological tree can't stall the host: stop
  // after MAX_STATS_ENTRIES and report partial=true so the UI can degrade.
  async _computeStats(root, { budget = 20000 } = {}) {
    let fileCount = 0
    let dirCount = 0
    let totalBytes = 0
    let newestMtimeMs = 0
    let visited = 0
    let partial = false
    const walk = async (abs) => {
      if (visited >= budget) { partial = true; return }
      let entries = []
      try {
        entries = await fsp.readdir(abs, { withFileTypes: true })
      } catch { return }
      for (const ent of entries) {
        if (visited >= budget) { partial = true; return }
        if (ent.name.startsWith('.')) continue
        visited++
        const childAbs = require('path').join(abs, ent.name)
        if (ent.isDirectory()) {
          dirCount++
          await walk(childAbs)
        } else if (ent.isFile()) {
          fileCount++
          try {
            const st = await fsp.stat(childAbs)
            totalBytes += st.size || 0
            if (st.mtimeMs && st.mtimeMs > newestMtimeMs) newestMtimeMs = st.mtimeMs
          } catch {}
        }
      }
    }
    await walk(root)
    return { fileCount, dirCount, totalBytes, newestMtimeMs, partial }
  }

  async _handleStats(peerId, msg) {
    const ctx = this._sessionFor(peerId, msg)
    if (!ctx) { this._sendTo(peerId, { type: 'SITE_STATS_RES', siteId: msg && msg.siteId, ok: false, error: 'no-session' }); return }
    const { site } = ctx
    if (!(await this._isAllowlisted(site.siteId, peerId))) { const m = this._visitors.get(peerId); if (m) { m.delete(site.siteId); if (m.size === 0) this._visitors.delete(peerId) } return }
    const stats = await this._computeStats(site.root)
    this._sendTo(peerId, { type: 'SITE_STATS_RES', siteId: site.siteId, ok: true, stats })
  }

  _handleDiscover(peerId, msg) {
    const clean = normalizeSiteCode(msg && msg.code)
    if (!clean) return
    let site = null
    for (const s of this._sites.values()) if (s.code === clean) { site = s; break }
    if (!site || this._isExpired(site)) return
    const peerObj = this.engine.peers && this.engine.peers.get(peerId)
    if (!peerObj || !peerObj.signaling) return
    peerObj.signaling.send({ type: 'SITE_DISCOVER_RES', code: clean, ok: true, siteId: site.siteId, site: { name: site.name, siteId: site.siteId } })
  }

  _sessionFor(peerId, msg) {
    const map = this._visitors.get(peerId)
    if (!map || !msg || !msg.siteId) return null
    const v = map.get(msg.siteId)
    if (!v) return null
    const site = this._sites.get(msg.siteId)
    if (!site) return null
    if (this._isExpired(site)) { map.delete(msg.siteId); if (map.size === 0) this._visitors.delete(peerId); return null }
    v.lastSeenAt = Date.now()
    return { site, session: v }
  }

  async _handleHello(peerId, msg) {
    const site = this._sites.get(msg && msg.siteId)
    if (!site || !msg || msg.siteId !== site.siteId) return
    if (this._isExpired(site)) { this._sendTo(peerId, { type: 'SITE_HELLO_ACK', siteId: site.siteId, ok: false, error: 'expired' }); return }
    if (!(await this._isAllowlisted(site.siteId, peerId))) {
      this._sendTo(peerId, { type: 'SITE_HELLO_ACK', siteId: site.siteId, ok: false, error: 'not-allowed' })
      return
    }
    let map = this._visitors.get(peerId)
    if (!map) { map = new Map(); this._visitors.set(peerId, map) }
    map.set(site.siteId, { siteId: site.siteId, helloAt: Date.now(), lastSeenAt: Date.now() })
    const peerObj = this.engine.peers && this.engine.peers.get(peerId)
    if (peerObj && peerObj.pairing && peerObj.pairing.timeout) { clearTimeout(peerObj.pairing.timeout); peerObj.pairing.timeout = null }
    this._sendTo(peerId, { type: 'SITE_HELLO_ACK', siteId: site.siteId, ok: true, site: { name: site.name, siteId: site.siteId } })
    console.log(`[SiteServer] visitor ${peerId.slice(0, 12)}... joined site ${site.siteId}`)
  }

  async _handleList(peerId, msg) {
    const ctx = this._sessionFor(peerId, msg)
    if (!ctx) { this._sendTo(peerId, { type: 'SITE_LIST_RES', siteId: msg && msg.siteId, ok: false, error: 'no-session' }); return }
    const { site } = ctx
    if (!(await this._isAllowlisted(site.siteId, peerId))) { const m = this._visitors.get(peerId); if (m) { m.delete(site.siteId); if (m.size === 0) this._visitors.delete(peerId) } return }
    let target = site.root
    if (msg.path && msg.path !== '/') {
      const resolved = resolveSitePath(site.root, msg.path)
      if (!resolved) { this._sendTo(peerId, { type: 'SITE_LIST_RES', siteId: site.siteId, ok: false, error: 'invalid-path' }); return }
      const st = await fsp.stat(resolved).catch(() => null)
      if (!st || !st.isDirectory()) { this._sendTo(peerId, { type: 'SITE_LIST_RES', siteId: site.siteId, ok: false, error: 'not-found' }); return }
      target = resolved
    }
    const entries = await listDir(fsp, target, site.root)
    if (entries.length > MAX_LIST_ENTRIES) entries.length = MAX_LIST_ENTRIES
    this._sendTo(peerId, { type: 'SITE_LIST_RES', siteId: site.siteId, ok: true, path: msg.path || '/', entries })
  }

  async _handleRead(peerId, msg) {
    const ctx = this._sessionFor(peerId, msg)
    const reqId = msg && typeof msg.reqId === 'string' ? msg.reqId : ''
    if (!ctx) { this._sendTo(peerId, { type: 'SITE_READ_END', siteId: msg && msg.siteId, reqId, ok: false, error: 'no-session' }); return }
    const { site } = ctx
    if (msg.thumb && !msg.range) msg.range = `bytes=0-${Math.min(128*1024-1, (await this._getFileSize(site, msg.path))-1)}`
    if (!(await this._isAllowlisted(site.siteId, peerId))) { const m = this._visitors.get(peerId); if (m) { m.delete(site.siteId); if (m.size === 0) this._visitors.delete(peerId) } return }
    // ETag / If-None-Match short-circuit
    const ifNoneMatch = msg.ifNoneMatch ? String(msg.ifNoneMatch).replace(/"/g, '') : null
    // Progressive hint: if this is a video, the visitor's <video> will Range-request — host should prioritize start chunks
    const resolvedInfo = await resolveSiteFile(fsp, site.root, msg.path, { spa: !!site.spa, progressive: !!msg.progressive })
    if (resolvedInfo.kind !== 'file') {
      this._sendTo(peerId, { type: 'SITE_READ_END', siteId: site.siteId, reqId, ok: false, error: resolvedInfo.kind === 'invalid' ? 'invalid-path' : 'not-found' })
      return
    }
    const resolved = resolvedInfo.abs
    const sizeForHeaders = resolvedInfo.stat.size
    const etag = `"${resolvedInfo.stat.mtimeMs.toString(36)}-${sizeForHeaders.toString(36)}"`
    if (ifNoneMatch && ifNoneMatch === etag.replace(/"/g, '')) {
      this._sendTo(peerId, { type: 'SITE_READ_END', siteId: site.siteId, reqId, ok: true, path: msg.path, size: sizeForHeaders, start: 0, end: -1, hash: '', mime: getSiteMimeType(resolved), etag, notModified: true })
      return
    }
    let fd = null
    try {
      fd = await fsp.open(resolved, 'r')
      const st = await fd.stat()
      if (!st.isFile()) throw Object.assign(new Error('not a file'), { code: 'ENOTFILE' })
      const size = st.size
      const range = parseRange(msg.range, size)
      const start = range ? range.start : 0
      const end = range ? range.end : size - 1
      const mime = getSiteMimeType(resolved)
      const extraHeaders = resolveSiteHeaders(site.manifest, msg.path)
      if (size === 0 || start > end) {
        const hash = SHA256(Buffer.alloc(0))
        this._sendTo(peerId, { type: 'SITE_READ_END', siteId: site.siteId, reqId, ok: true, path: msg.path, size: 0, start: 0, end: -1, hash, mime, etag, headers: extraHeaders })
        await fd.close(); fd = null; return
      }
      const hash = require('crypto').createHash('sha256')
      let remaining = end - start + 1
      let pos = start
      let seq = 0
      let sentBytes = 0
      // Progressive streaming: send first 256K eagerly so <video> can start, then throttle
      const firstChunk = Math.min(256 * 1024, remaining)
      while (remaining > 0) {
        const want = remaining > firstChunk && seq === 0 ? firstChunk : Math.min(CHUNK, remaining)
        const buf = Buffer.alloc(want)
        const { bytesRead } = await fd.read(buf, 0, buf.length, pos)
        if (bytesRead <= 0) break
        const slice = bytesRead === buf.length ? buf : buf.subarray(0, bytesRead)
        hash.update(slice)
        this._sendTo(peerId, { type: 'SITE_READ_DATA', siteId: site.siteId, reqId, seq, data: slice.toString('base64') })
        sentBytes += bytesRead; pos += bytesRead; remaining -= bytesRead; seq++
        // Yield to event loop so other SITE_READs don't starve
        if (seq % 16 === 0) await new Promise(r => setImmediate(r))
      }
      await fd.close(); fd = null
      this._sendTo(peerId, { type: 'SITE_READ_END', siteId: site.siteId, reqId, ok: true, path: msg.path, size, start, end: start + sentBytes - 1, chunks: seq, hash: hash.digest('hex'), mime, etag, headers: extraHeaders })
    } catch (err) {
      if (fd) { try { await fd.close() } catch {} }
      this._sendTo(peerId, { type: 'SITE_READ_END', siteId: site.siteId, reqId, ok: false, error: err && err.code === 'ENOENT' ? 'not-found' : err && err.code === 'ENOTFILE' ? 'not-a-file' : 'read-failed' })
    }
  }

  _writeSessions = new Map() // reqId -> { siteId, peerId, relPath, abs, fd, hash, bytes }

  async _handleWrite(peerId, msg) {
    const type = msg.type
    if (type === 'SITE_WRITE') {
      const ctx = this._sessionFor(peerId, msg)
      const reqId = msg.reqId || `w-${Date.now().toString(36)}`
      if (!ctx) { this._sendTo(peerId, { type: 'SITE_WRITE_END', siteId: msg && msg.siteId, reqId, ok: false, error: 'no-session' }); return }
      if (!(await this._canWrite(ctx.site.siteId, peerId))) { this._sendTo(peerId, { type: 'SITE_WRITE_END', siteId: ctx.site.siteId, reqId, ok: false, error: 'read-only' }); return }
      const abs = resolveSitePath(ctx.site.root, msg.path)
      if (!abs) { this._sendTo(peerId, { type: 'SITE_WRITE_END', siteId: ctx.site.siteId, reqId, ok: false, error: 'invalid-path' }); return }
      await fsp.mkdir(path.dirname(abs), { recursive: true }).catch(() => {})
      const fd = await fsp.open(abs, 'w').catch((e) => { throw e })
      const hash = require('crypto').createHash('sha256')
      this._writeSessions.set(reqId, { siteId: ctx.site.siteId, peerId, abs, fd, hash, bytes: 0, reqId })
      // If body is inline (small writes)
      if (typeof msg.data === 'string' && msg.data) {
        const buf = Buffer.from(msg.data, 'base64')
        hash.update(buf); await fd.write(buf, 0, buf.length, 0)
        this._writeSessions.get(reqId).bytes += buf.length
      }
      if (msg.final) {
        const sess = this._writeSessions.get(reqId); this._writeSessions.delete(reqId)
        const digest = sess.hash.digest('hex'); await sess.fd.close()
        this._sendTo(peerId, { type: 'SITE_WRITE_END', siteId: ctx.site.siteId, reqId, ok: true, hash: digest, bytes: sess.bytes })
      } else if (!msg.data) {
        // streaming — wait for DATA/END
      } else {
        // inline already handled; if not final, keep open — client will send more DATA
        if (!msg.final) { /* keep */ } else { /* already closed */ }
      }
    } else if (type === 'SITE_WRITE_DATA') {
      const sess = this._writeSessions.get(msg.reqId)
      if (!sess || sess.peerId !== peerId) return
      const buf = Buffer.from(String(msg.data || ''), 'base64')
      sess.hash.update(buf); await sess.fd.write(buf, 0, buf.length, sess.bytes)
      sess.bytes += buf.length
    } else if (type === 'SITE_WRITE_END') {
      // This is the finalizer for streaming writes (client signals done) — avoid collision with READ_END
      // For writes the finalizer is SITE_WRITE + final:true; this branch handles explicit end
      const sess = this._writeSessions.get(msg.reqId)
      if (!sess || sess.peerId !== peerId) { this._sendTo(peerId, { type: 'SITE_WRITE_END', siteId: msg.siteId, reqId: msg.reqId, ok: false, error: 'no-session' }); return }
      // If this SITE_WRITE_END is actually the write finalizer (has hash expectation), close
      // Otherwise it's a duplicate — ignore
      if (sess.fd) {
        const digest = sess.hash.digest('hex'); await sess.fd.close(); sess.fd = null
        this._writeSessions.delete(msg.reqId)
        this._sendTo(peerId, { type: 'SITE_WRITE_END', siteId: sess.siteId, reqId: msg.reqId, ok: true, hash: digest, bytes: sess.bytes })
      }
    }
  }

  async _handleMkdir(peerId, msg) {
    const ctx = this._sessionFor(peerId, msg)
    if (!ctx) { this._sendTo(peerId, { type: 'SITE_MKDIR_RES', siteId: msg && msg.siteId, ok: false, error: 'no-session' }); return }
    if (!(await this._canWrite(ctx.site.siteId, peerId))) { this._sendTo(peerId, { type: 'SITE_MKDIR_RES', siteId: ctx.site.siteId, ok: false, error: 'read-only' }); return }
    const abs = resolveSitePath(ctx.site.root, msg.path)
    if (!abs) { this._sendTo(peerId, { type: 'SITE_MKDIR_RES', siteId: ctx.site.siteId, ok: false, error: 'invalid-path' }); return }
    try { await fsp.mkdir(abs, { recursive: true }); this._sendTo(peerId, { type: 'SITE_MKDIR_RES', siteId: ctx.site.siteId, ok: true, path: msg.path }) } catch (e) { this._sendTo(peerId, { type: 'SITE_MKDIR_RES', siteId: ctx.site.siteId, ok: false, error: 'mkdir-failed' }) }
  }

  async _handleDelete(peerId, msg) {
    const ctx = this._sessionFor(peerId, msg)
    if (!ctx) { this._sendTo(peerId, { type: 'SITE_DELETE_RES', siteId: msg && msg.siteId, ok: false, error: 'no-session' }); return }
    if (!(await this._canWrite(ctx.site.siteId, peerId))) { this._sendTo(peerId, { type: 'SITE_DELETE_RES', siteId: ctx.site.siteId, ok: false, error: 'read-only' }); return }
    const abs = resolveSitePath(ctx.site.root, msg.path)
    if (!abs) { this._sendTo(peerId, { type: 'SITE_DELETE_RES', siteId: ctx.site.siteId, ok: false, error: 'invalid-path' }); return }
    if (abs === ctx.site.root) { this._sendTo(peerId, { type: 'SITE_DELETE_RES', siteId: ctx.site.siteId, ok: false, error: 'invalid-path' }); return }
    try { await fsp.rm(abs, { recursive: true, force: true }); this._sendTo(peerId, { type: 'SITE_DELETE_RES', siteId: ctx.site.siteId, ok: true, path: msg.path }) } catch (e) { this._sendTo(peerId, { type: 'SITE_DELETE_RES', siteId: ctx.site.siteId, ok: false, error: 'delete-failed' }) }
  }

  _sendTo(peerId, msg) {
    const peerObj = this.engine.peers && this.engine.peers.get(peerId)
    if (peerObj && peerObj.signaling && typeof peerObj.signaling.send === 'function') {
      try { peerObj.signaling.send(msg); return true } catch (err) { console.warn('[SiteServer] send failed:', err.message) }
    }
    return false
  }

  _handlePeerGone(peerId) {
    this._visitors.delete(peerId)
    for (const [reqId, sess] of this._writeSessions.entries()) { if (sess.peerId === peerId) { try { sess.fd && sess.fd.close().catch(() => {}) } catch {}; this._writeSessions.delete(reqId) } }
  }

  async stop() {
    this._stopMaintenance()
    for (const [evt, fn] of this._listeners) this.engine.removeListener(evt, fn)
    this._listeners = []
    for (const s of this._sites.values()) { try { this.engine.topicRegistry.leave(siteTopic(s.siteId)) } catch {}; try { if (s.code) this.engine.topicRegistry.leave(siteCodeTopic(s.code)) } catch {} }
    this._sites.clear(); this._visitors.clear()
    for (const sess of this._writeSessions.values()) { try { sess.fd && sess.fd.close().catch(() => {}) } catch {} }
    this._writeSessions.clear()
  }
}

module.exports = { SiteServer }
