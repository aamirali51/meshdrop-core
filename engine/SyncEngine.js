'use strict'

// SyncEngine — fast, zero-duplication, memory-safe folder synchronization
// for @mesh/core (Desktop & Mobile).
//
// Key Design:
// 1. Instant Setup: Directory walk uses fast `fsp.stat` metadata (`size + mtimeMs`)
//    without reading full file payloads or computing heavy SHA-256 upfront.
// 2. Zero-Storage Duplication: Outgoing sync files stream directly from source
//    paths (`localPath/relPath`) without cloning into internal database storage.
// 3. System File Exclusion: Automatically skips Android/system artifacts (.thumbnails,
//    .nomedia, .trashed-*, .pending-*, .DS_Store, Thumbs.db, .git).
// 4. Peer-Gated: Engine stays idle (0% CPU/Battery) when the target peer is offline.
// 5. Memory-Safe: Chunks stream with backpressure, preventing OOM on 4K videos.

const { EVENTS, MESSAGES } = require('../protocol.js')
const { sha256 } = require('../crypto.js')

// Safety-net rescan interval. The watcher covers live changes; this bounds
// the worst case when the watcher misses an event (Windows recursive fs.watch
// is not 100% reliable) or the app was closed. 60s bounds two-way sync
// latency to a minute instead of five when the watcher misses.
const DEFAULT_SCAN_INTERVAL_MS = 60 * 1000
const MAX_LIBRARY_FILES = 50000
const CONFLICT_TOLERANCE_MS = 3000
// How close a receiver's on-disk mtime must be to the sender's recorded mtime
// for a file to count as "already have this exact version" (same tolerance as
// the TransferEngine per-file skip).
const VERIFY_TOLERANCE_MS = 1500
// How long the owner waits for the receiver's batch verify answer before
// falling back to the per-file skip handshake (old builds never answer).
const DEFAULT_VERIFY_TIMEOUT_MS = 3000
// Self-healing re-check: how many already-sent files the verify message may
// carry in one round. Larger sent sets are rotated across rounds so every
// file is re-checked eventually without shipping a multi-MB message.
const MAX_VERIFY_FILES = 4000

const IGNORED_NAMES = new Set([
  '.p2p-staging',
  '.meshdrop-trash',
  '.thumbnails',
  '.nomedia',
  '.DS_Store',
  'Thumbs.db',
  '.git',
  '.',
  '..'
])

function isIgnored(name) {
  if (!name || typeof name !== 'string') return true
  if (IGNORED_NAMES.has(name)) return true
  if (name.startsWith('.trashed-') || name.startsWith('.pending-')) return true
  // App-generated debris from the legacy conflict-naming scheme: these are
  // byte-identical duplicates the app itself created, never user content.
  // Excluding them keeps a polluted source folder from being backed up.
  if (name.includes('(conflicted copy)')) return true
  return false
}

// Characters Windows (NTFS) and FAT32/exFAT (SD cards, USB sticks) reject in
// file names. Linux allows all of them, so a sender can offer "Artist: Album"
// and the receiver's mkdir fails with EINVAL. Normalizing at the rel-key
// boundary keeps sender index, receiver index, delivered rows and the on-disk
// name identical on every platform.
const INVALID_FS_CHARS = /[<>:"/\\|?*\u0000-\u001F]/g
// FAT32/exFAT cap component names at 255 chars; keep headroom for the " (2)"
// dedupe suffix _uniqueFinalPath appends at write time.
const MAX_COMPONENT_LEN = 240

// Normalize a relative path from the wire: reject path traversal, and scrub
// every component to a name any target filesystem can store.
function safeRelPath(relPath) {
  if (typeof relPath !== 'string' || relPath.length === 0 || relPath.length > 1024) return ''
  const segments = relPath.split(/[\\/]+/).filter((s) => s !== '' && s !== '.' && s !== '..')
  if (segments.length === 0) return ''
  return segments
    .map((seg) => {
      // FAT also forbids trailing dots/spaces in names.
      let s = seg.replace(INVALID_FS_CHARS, '_').replace(/[. ]+$/, '')
      if (s.length > MAX_COMPONENT_LEN) {
        // Keep the extension when truncating: a long title cut to 240 chars
        // without its suffix stops being playable music on the receiver.
        const dot = s.lastIndexOf('.')
        const ext = dot > 0 ? s.slice(dot) : ''
        if (ext && ext.length < MAX_COMPONENT_LEN) {
          s = s.slice(0, MAX_COMPONENT_LEN - ext.length) + ext
        } else {
          s = s.slice(0, MAX_COMPONENT_LEN)
        }
      }
      return s === '' ? '_' : s
    })
    .join('/')
}

// Fast stat-only directory walker with bounded concurrency.
async function scanFolder(fsp, dir, baseDir, out, limit = MAX_LIBRARY_FILES) {
  const { path: p } = require('../compat.js')
  const WALKERS = 4
  const queue = [dir]

  async function worker() {
    while (queue.length > 0) {
      if (out.size >= limit) return
      const currentDir = queue.shift()
      if (!currentDir) continue

      let entries
      try {
        entries = await fsp.readdir(currentDir, { withFileTypes: true })
      } catch (err) {
        try {
          entries = await fsp.readdir(currentDir)
        } catch {
          continue
        }
      }
      if (!Array.isArray(entries)) continue

      const pendingStats = []
      for (const ent of entries) {
        if (out.size >= limit) return
        const name = typeof ent === 'string' ? ent : (ent && ent.name) || String(ent)
        if (isIgnored(name)) continue

        const abs = p.join(currentDir, name)
        if (typeof ent === 'object' && ent !== null) {
          if (typeof ent.isDirectory === 'function' && ent.isDirectory()) {
            if (out.size < limit) queue.push(abs)
            continue
          } else if (typeof ent.isFile === 'function' && ent.isFile()) {
            pendingStats.push(abs)
            continue
          }
        }
        pendingStats.push(abs)
      }

      const stats = await Promise.all(
        pendingStats.map((abs) =>
          fsp
            .stat(abs)
            .then((st) => ({ abs, st }))
            .catch(() => null)
        )
      )

      for (const item of stats) {
        if (item && item.st && out.size < limit) {
          const isDir = typeof item.st.isDirectory === 'function' ? item.st.isDirectory() : false
          if (isDir) {
            queue.push(item.abs)
          } else {
            // Normalize the key exactly like wire paths: the on-disk name may
            // contain chars the receiving filesystem can't store (e.g. ":" on
            // an SD card), and both peers must agree on the rel string.
            const rel = safeRelPath(p.relative(baseDir, item.abs).split(p.sep).join('/'))
            if (!rel) continue
            const rawMtime = item.st.mtimeMs || (item.st.mtime && typeof item.st.mtime.getTime === 'function' ? item.st.mtime.getTime() : item.st.mtime)
            const mtimeMs = Number(rawMtime) || Date.now()
            const size = Number(item.st.size) || 0
            const sig = `${size}-${mtimeMs}`
            out.set(rel, { size, mtimeMs, sig })
          }
        }
      }
    }
  }

  await Promise.all(Array.from({ length: WALKERS }, () => worker()))
}

// True when rel is exactly a dirty path, or lives under a dirty directory.
// Used by the incremental-scan tombstone loop: only dirty paths may be
// tombstoned (a dirty dir means everything under it was touched).
function isDirtyOrUnder(dirtySet, rel) {
  if (!dirtySet) return true
  if (dirtySet.has(rel)) return true
  const parts = rel.split('/')
  for (let i = 1; i < parts.length; i++) {
    if (dirtySet.has(parts.slice(0, i).join('/'))) return true
  }
  return false
}

// Incremental scan: stat only the dirty rel paths (and walk any that are
// directories), producing the same {rel -> {size,mtimeMs,sig,hash}} shape as
// scanFolder. Paths that no longer exist are returned with deleted:true so the
// caller can tombstone them. Used when the watcher told us exactly which files
// changed — avoids a full folder walk on every single-file edit.
async function statDirtyPaths(fsp, baseDir, dirtyPaths) {
  const out = new Map()
  let sawDir = false
  const { path: p } = require('../compat.js')
  for (const dirty of dirtyPaths) {
    if (!dirty || out.size >= MAX_LIBRARY_FILES) continue
    const abs = p.join(baseDir, ...dirty.split('/'))
    let st = null
    try {
      st = await fsp.stat(abs)
    } catch {
      // Gone — caller tombstones it via deleted:true
      out.set(dirty, { size: 0, mtimeMs: Date.now(), sig: '', hash: '', deleted: true })
      continue
    }
    const isDir = typeof st.isDirectory === 'function' ? st.isDirectory() : false
    if (isDir) {
      // A dirty directory (new subfolder, rename target) is a subtree — the
      // files inside can be mid-write during a transfer. Signal the caller to
      // fall back to a full scan rather than racing the transfer.
      sawDir = true
      continue
    }
    const rel = safeRelPath(dirty)
    if (!rel) continue
    const rawMtime = st.mtimeMs || (st.mtime && typeof st.mtime.getTime === 'function' ? st.mtime.getTime() : st.mtime)
    const mtimeMs = Number(rawMtime) || Date.now()
    const size = Number(st.size) || 0
    const sig = `${size}-${mtimeMs}`
    out.set(rel, { size, mtimeMs, sig, hash: '' })
  }
  out._sawDir = sawDir
  return out
}

// Convert index map to wire array
function indexToArray(index) {
  const out = []
  if (!index) return out
  for (const [rel, e] of Object.entries(index)) {
    if (!e) continue
    out.push({
      rel,
      size: e.size || 0,
      mtimeMs: e.mtimeMs || 0,
      sig: e.sig || `${e.size || 0}-${e.mtimeMs || 0}`,
      authorKey: e.authorKey || '',
      deleted: !!e.deleted
    })
  }
  return out
}

// Convert wire array to index object
function indexFromArray(entries) {
  const out = {}
  if (!Array.isArray(entries)) return out
  for (const e of entries) {
    if (!e || typeof e.rel !== 'string') continue
    const rel = safeRelPath(e.rel)
    if (!rel) continue
    out[rel] = {
      size: e.size || 0,
      mtimeMs: e.mtimeMs || 0,
      sig: typeof e.sig === 'string' && e.sig ? e.sig : `${e.size || 0}-${e.mtimeMs || 0}`,
      authorKey: typeof e.authorKey === 'string' ? e.authorKey : '',
      deleted: !!e.deleted
    }
  }
  return out
}

class SyncEngine {
  constructor({
    getBee,
    getPeers,
    getPeerId,
    sendEvent,
    transferEngine,
    downloadsDir,
    fsp,
    path,
    fs,
    scanIntervalMs,
    autoAcceptOffers,
    verifyTimeoutMs
  }) {
    this.getBee = getBee
    this.getPeers = getPeers
    this.getPeerId = getPeerId
    this.sendEvent = sendEvent
    this.transferEngine = transferEngine
    this.downloadsDir = downloadsDir
    this.fsp = fsp
    this.path = path
    this.fs = fs || null
    this.scanIntervalMs = scanIntervalMs || DEFAULT_SCAN_INTERVAL_MS
    this.autoAcceptOffers = autoAcceptOffers === true
    this.verifyTimeoutMs = verifyTimeoutMs || DEFAULT_VERIFY_TIMEOUT_MS
    this.libraries = new Map() // id -> library record
    this.pendingInvites = new Map() // id -> invite record
    this.removedLibraries = new Set() // ids the user removed — re-invites for these are ignored
    this.timer = null
    this._syncingSet = new Set() // library ids currently syncing
  }

  async init() {
    await this.loadLibraries()
    this.timer = setInterval(() => this.tick().catch(() => {}), this.scanIntervalMs)
    if (this.timer && this.timer.unref) this.timer.unref()
    console.log(`[SyncEngine] Initialized with ${this.libraries.size} sync library/libraries`)
  }

  async stop() {
    if (this.timer) {
      clearInterval(this.timer)
      this.timer = null
    }
    for (const lib of this.libraries.values()) {
      this._stopWatching(lib.id)
    }
    this._syncingSet.clear()
  }

  async loadLibraries() {
    try {
      const bee = await this.getBee('sync')
      // Config rows are keyed <libId> ('sync-...'); manifest rows use the
      // 'index/'|'sent/'|'remote/' prefixes, which sort before 'sync-' and
      // are excluded by this range.
      for await (const node of bee.createReadStream({ gte: 'sync-', lte: 'sync-\uffff' })) {
        const cfg = node.value
        if (!cfg || !cfg.id || !cfg.localPath) continue

        // Legacy records (pre lean-manifest) embedded the full index/sent/
        // remote maps in the config row. Migrate them out to per-file rows
        // once, then strip the maps so the config row stays tiny forever.
        const legacy = {
          index: cfg.index && typeof cfg.index === 'object' && Object.keys(cfg.index).length ? cfg.index : null,
          sentIndex: cfg.sentIndex && typeof cfg.sentIndex === 'object' && Object.keys(cfg.sentIndex).length ? cfg.sentIndex : null,
          remoteIndex: cfg.remoteIndex && typeof cfg.remoteIndex === 'object' && Object.keys(cfg.remoteIndex).length ? cfg.remoteIndex : null
        }

        const lib = {
          id: cfg.id,
          name: cfg.name,
          localPath: cfg.localPath,
          peerId: cfg.peerId || '',
          mode: cfg.mode || 'two-way',
          paused: !!cfg.paused,
          accepted: cfg.accepted,
          lastScanAt: cfg.lastScanAt || 0,
          lastSyncAt: cfg.lastSyncAt || 0,
          index: {},
          sentIndex: {},
          remoteIndex: {}
        }
        lib.status = lib.paused ? 'paused' : 'idle'
        // Records saved before `accepted` was persisted have no key at all.
        // Those links were live (the peer's isSyncAllowed gate still rejects
        // stray offers for unknown/paused links), so restore them as accepted
        // instead of re-announcing an invite for an already-active folder.
        // Only an explicit accepted:false (never-accepted link) keeps waiting.
        if (typeof lib.accepted !== 'boolean') lib.accepted = true

        // Load the per-file manifest rows.
        for (const [rel, entry] of await this._iterRows('index', lib.id)) lib.index[rel] = entry
        for (const [rel, entry] of await this._iterRows('sent', lib.id)) lib.sentIndex[rel] = entry
        for (const [rel, entry] of await this._iterRows('remote', lib.id)) lib.remoteIndex[rel] = entry

        // One-time migration of legacy embedded maps (rows take precedence).
        if (legacy.index) {
          const entries = []
          for (const [rel, e] of Object.entries(legacy.index)) {
            if (!lib.index[rel]) {
              lib.index[rel] = e
              entries.push([rel, e])
            }
          }
          if (entries.length) await this._writeRows('index', lib.id, entries)
        }
        if (legacy.sentIndex) {
          const entries = []
          for (const [rel, e] of Object.entries(legacy.sentIndex)) {
            if (!lib.sentIndex[rel]) {
              lib.sentIndex[rel] = e
              entries.push([rel, e])
            }
          }
          if (entries.length) await this._writeRows('sent', lib.id, entries)
        }
        if (legacy.remoteIndex) {
          const entries = []
          for (const [rel, e] of Object.entries(legacy.remoteIndex)) {
            if (!lib.remoteIndex[rel]) {
              lib.remoteIndex[rel] = e
              entries.push([rel, e])
            }
          }
          if (entries.length) await this._writeRows('remote', lib.id, entries)
        }
        if (legacy.index || legacy.sentIndex || legacy.remoteIndex) {
          // Strip the embedded maps from the config row (writes are tiny now).
          await this._persist(lib)
        }

        this.libraries.set(lib.id, lib)
        this._watchLibrary(lib)
      }
    } catch (err) {
      console.warn('[SyncEngine] loadLibraries warning:', err.message)
    }
  }

  // Persist the library CONFIG row only (small, rarely written). The file
  // manifest lives in per-file rows (_writeRows) so a single changed file
  // never rewrites the whole index — the full-record JSON rewrites are what
  // made RocksDB accumulate GBs of blob versions.
  async _persist(lib) {
    try {
      const bee = await this.getBee('sync')
      await bee.put(lib.id, {
        id: lib.id,
        name: lib.name,
        localPath: lib.localPath,
        peerId: lib.peerId,
        mode: lib.mode,
        paused: !!lib.paused,
        accepted: !!lib.accepted,
        lastScanAt: lib.lastScanAt || 0,
        lastSyncAt: lib.lastSyncAt || 0
      })
    } catch (err) {
      console.warn('[SyncEngine] _persist error:', err.message)
    }
  }

  // ─── Lean incremental manifest rows ───────────────────────────────────────
  // Key layout in the 'sync' bee:
  //   <libId>                 config (small JSON record, see _persist)
  //   index/<libId>/<rel>     one row per tracked file  ({size,mtimeMs,sig,authorKey,deleted})
  //   sent/<libId>/<rel>      one-way owner: "already pushed" markers
  //   remote/<libId>/<rel>    two-way: peer's copy of the index

  static _rowRange(kind, libId) {
    const prefix = `${kind}/${libId}/`
    return { gte: prefix, lte: `${prefix}\uffff` }
  }

  async _iterRows(kind, libId) {
    const bee = await this.getBee('sync')
    const out = []
    for await (const node of bee.createReadStream(SyncEngine._rowRange(kind, libId))) {
      out.push([node.key.slice(kind.length + libId.length + 2), node.value])
    }
    return out
  }

  async _writeRows(kind, libId, entries) {
    if (!entries || entries.length === 0) return
    try {
      const bee = await this.getBee('sync')
      const batch = bee.batch()
      try {
        for (const [rel, value] of entries) {
          await batch.put(`${kind}/${libId}/${rel}`, value)
        }
        await batch.flush()
      } catch (err) {
        try { await batch.close() } catch {}
        throw err
      }
    } catch (err) {
      console.warn(`[SyncEngine] _writeRows(${kind}) error:`, err.message)
    }
  }

  async _delRows(kind, libId, rels) {
    if (!rels || rels.length === 0) return
    try {
      const bee = await this.getBee('sync')
      const batch = bee.batch()
      try {
        for (const rel of rels) {
          await batch.del(`${kind}/${libId}/${rel}`)
        }
        await batch.flush()
      } catch (err) {
        try { await batch.close() } catch {}
        throw err
      }
    } catch (err) {
      console.warn(`[SyncEngine] _delRows(${kind}) error:`, err.message)
    }
  }

  async _remove(id) {
    try {
      const bee = await this.getBee('sync')
      const batch = bee.batch()
      try {
        await batch.del(id)
        for (const kind of ['index', 'sent', 'remote']) {
          for await (const node of bee.createReadStream(SyncEngine._rowRange(kind, id))) {
            await batch.del(node.key)
          }
        }
        await batch.flush()
      } catch (err) {
        try { await batch.close() } catch {}
        throw err
      }
    } catch (err) {
      console.warn('[SyncEngine] _remove error:', err.message)
    }
  }

  _findPeer(peerId) {
    if (!peerId || typeof peerId !== 'string') return null
    const peersMap = this.getPeers()
    let peerObj = peersMap.get(peerId)
    if (!peerObj) {
      for (const [, p] of peersMap.entries()) {
        if (
          p.device?.id === peerId ||
          p.device?.publicKey === peerId ||
          p.device?.identityKey === peerId
        ) {
          peerObj = p
          break
        }
      }
    }
    return peerObj
  }

  _sendToPeer(peerId, msg) {
    if (!peerId) return
    const peerObj = this._findPeer(peerId)
    if (peerObj?.signaling) {
      try {
        peerObj.signaling.send(msg)
      } catch (err) {
        console.warn('[SyncEngine] Failed to send sync message:', err.message)
      }
    }
  }

  // ─── File Watcher ──────────────────────────────────────────────────────────

  _watchLibrary(lib) {
    if (lib.watcher || !this.fs || typeof this.fs.watch !== 'function' || !lib.localPath) return
    try {
      // Capture the filename when the platform provides it (fs.watch passes
      // (eventType, filename)). Dirty-path tracking then lets syncLibrary do
      // an INCREMENTAL stat of just the changed paths instead of a full
      // folder walk — the "mark dirty, scan only that" model. On platforms
      // where filename is null (some recursive watchers), the dirty set is
      // empty and we fall back to a full scan.
      const watcher = this.fs.watch(lib.localPath, { recursive: true }, (eventType, filename) => {
        this._onFolderChange(lib.id, filename)
      })
      watcher.on('error', () => {
        // A watcher error (unmounted drive, ENOENT, fd exhaustion) must not
        // permanently disable change detection. Close and re-arm with
        // exponential backoff; the tick covers the gap meanwhile.
        this._stopWatching(lib.id)
        this._scheduleWatchRetry(lib.id)
      })
      lib.watcher = watcher
    } catch {
      // In environments where recursive watch is unsupported, timer covers it
      this._scheduleWatchRetry(lib.id)
    }
  }

  // Re-arm a watcher after an error with exponential backoff (5s → 10s → 20s
  // → ... capped at 5min). The 5-min tick is peer-gated, so without this an
  // unmounted-then-remounted drive silently misses all changes.
  _scheduleWatchRetry(id) {
    const lib = this.libraries.get(id)
    if (!lib || lib.watcher || lib._watchRetryTimer) return
    const delay = Math.min((lib._watchRetryCount || 0) * 5000 + 5000, 5 * 60 * 1000)
    lib._watchRetryCount = (lib._watchRetryCount || 0) + 1
    lib._watchRetryTimer = setTimeout(() => {
      lib._watchRetryTimer = null
      const l = this.libraries.get(id)
      if (!l || l.watcher || l.paused) return
      // Reset the backoff on a successful re-arm (the watcher constructor
      // either throws synchronously or the error handler drives retries).
      this._watchLibrary(id)
      if (l.watcher) l._watchRetryCount = 0
    }, delay)
    if (lib._watchRetryTimer.unref) lib._watchRetryTimer.unref()
  }

  _stopWatching(id) {
    const lib = this.libraries.get(id)
    if (!lib) return
    if (lib.flushTimer) {
      clearTimeout(lib.flushTimer)
      lib.flushTimer = null
    }
    if (lib._watchRetryTimer) {
      clearTimeout(lib._watchRetryTimer)
      lib._watchRetryTimer = null
    }
    if (lib.watcher) {
      try { lib.watcher.close() } catch {}
      lib.watcher = null
    }
  }

  _onFolderChange(id, filename) {
    const lib = this.libraries.get(id)
    if (!lib || lib.paused) return
    // Record the dirty path (relative to the library root). A null filename
    // (platform didn't report it) signals a full rescan is needed. The dirty
    // set is capped: beyond the cap a full scan is cheaper.
    if (typeof filename === 'string' && filename.length > 0) {
      if (!lib._dirtyPaths) lib._dirtyPaths = new Set()
      // Normalize: fs.watch filenames use the OS separator; convert to the
      // same forward-slash rel keys the scan uses. Strip a leading separator.
      let rel = filename.split(/[\\/]/).join('/').replace(/^\/+/, '')
      if (rel && !isIgnored(rel.split('/').pop() || rel)) {
        lib._dirtyPaths.add(rel)
        if (lib._dirtyPaths.size > 200) lib._dirtyPaths = null // full scan
      }
    } else {
      lib._dirtyPaths = null
    }
    clearTimeout(lib.flushTimer)
    lib.flushTimer = setTimeout(() => {
      // Watcher-triggered scan: incremental (uses the dirty set). Explicit
      // syncLibrary calls pass { full: true } and never trust the dirty set.
      this.syncLibrary(id, { full: false }).catch(() => {})
    }, 1000)
    if (lib.flushTimer.unref) lib.flushTimer.unref()
  }

  // ─── Destination Resolution for Receiver ───────────────────────────────────

  resolveSyncDest(offer) {
    if (!offer || offer.source !== 'sync') return null
    const libId = offer.syncLibraryId
    const lib = libId ? this.libraries.get(libId) : null
    const rel = safeRelPath(offer.syncRelPath || offer.filename)
    if (!rel) return null

    let baseDir
    if (lib && lib.localPath) {
      baseDir = lib.localPath
    } else {
      const libName = (lib && lib.name) || offer.syncLibraryName || 'Sync'
      baseDir = this.path.join(this.downloadsDir || '/storage/emulated/0/Download', 'Sync', libName)
    }

    return {
      baseDir,
      path: this.path.join(baseDir, ...rel.split('/'))
    }
  }

  // ─── Library Management API ───────────────────────────────────────────────

  async addLibrary({ path: localPath, peerId, name, mode }) {
    if (!localPath || typeof localPath !== 'string') {
      throw new Error('Local folder path is required')
    }

    let resolvedLocalPath = localPath
    const isAbs = this.path.isAbsolute
      ? this.path.isAbsolute(resolvedLocalPath)
      : resolvedLocalPath.startsWith('/') || resolvedLocalPath.startsWith('\\') || resolvedLocalPath.includes(':')
    if (!isAbs) {
      const androidRoot = '/storage/emulated/0'
      if (this.fs && typeof this.fs.existsSync === 'function' && this.fs.existsSync(androidRoot)) {
        resolvedLocalPath = this.path.join(androidRoot, resolvedLocalPath)
      } else if (this.downloadsDir) {
        resolvedLocalPath = this.path.join(this.path.dirname(this.downloadsDir), resolvedLocalPath)
      } else {
        resolvedLocalPath = this.path.resolve(resolvedLocalPath)
      }
    }
    localPath = resolvedLocalPath

    // Ensure folder exists
    try {
      await this.fsp.mkdir(localPath, { recursive: true })
    } catch {}

    const id = `sync-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`
    const libName = name || this.path.basename(localPath) || 'Sync-Folder'
    let normMode = mode
    if (normMode === 'send-only') normMode = 'push'
    if (normMode === 'receive-only') normMode = 'receive_only'
    if (normMode === 'two-way') normMode = 'two-way'
    if (!normMode) {
      // Mobile-first default: the device that owns the folder is the master.
      // Mobile folder → one-way push (mobile is the source of truth, desktop is
      // a read-only mirror). Desktop ↔ desktop → two-way mirror.
      const isLocalMobile = this.isMobile || (typeof process !== 'undefined' && process.platform === 'android')
      const peerObj = this._findPeer(peerId)
      const isPeerMobile = !!(peerObj && (peerObj.device?.type === 'mobile' || peerObj.device?.platform === 'android' || peerObj.device?.platform === 'ios'))
      if (isLocalMobile) normMode = 'push'
      else if (isPeerMobile) normMode = 'push'
      else normMode = 'two-way'
    }

    const lib = {
      id,
      name: libName,
      localPath,
      peerId: peerId || '',
      mode: normMode,
      status: 'idle',
      paused: false,
      accepted: !peerId, // a peer-bound library must be accepted by the peer before any file is pushed
      index: {},
      remoteIndex: {}, // two-way (desktop↔desktop) only
      sentIndex: {}, // one-way owner: what we have already pushed
      lastScanAt: Date.now(),
      lastSyncAt: 0
    }

    // Fast metadata stat pass on creation (<100ms)
    const scanOut = new Map()
    try {
      await scanFolder(this.fsp, localPath, localPath, scanOut)
      for (const [rel, meta] of scanOut.entries()) {
        lib.index[rel] = {
          size: meta.size,
          mtimeMs: meta.mtimeMs,
          sig: meta.sig,
          authorKey: this.getPeerId ? this.getPeerId() : '',
          deleted: false
        }
      }
    } catch (err) {
      console.warn('[SyncEngine] Initial fast stat pass error:', err.message)
    }

    // Write the initial manifest as per-file rows (the config row stays tiny).
    const initialIndex = Object.entries(lib.index)
    if (initialIndex.length) await this._writeRows('index', id, initialIndex)

    this.libraries.set(id, lib)
    await this._persist(lib)
    this._watchLibrary(lib)

    this.sendEvent(EVENTS.SYNC_LIBRARY_ADDED, {
      id: lib.id,
      name: lib.name,
      localPath: lib.localPath,
      mode: lib.mode,
      status: lib.status,
      fileCount: Object.keys(lib.index).length
    })

    // Announce library invite to peer if connected
    if (lib.peerId) {
      lib.status = 'waiting_peer'
      lib.accepted = false
      await this._persist(lib)
      this._sendToPeer(lib.peerId, {
        type: MESSAGES.SYNC_INVITE,
        libraryId: lib.id,
        name: lib.name,
        mode: lib.mode,
        fileCount: Object.keys(lib.index).length,
        totalSize: Object.values(lib.index).reduce((acc, x) => acc + (x?.size || 0), 0)
      })
    }

    return {
      id: lib.id,
      name: lib.name,
      localPath: lib.localPath,
      peerId: lib.peerId,
      mode: lib.mode,
      status: lib.status,
      fileCount: Object.keys(lib.index).length
    }
  }

  async removeLibrary(id) {
    const lib = this.libraries.get(id)
    if (!lib) return null
    const peerId = lib.peerId
    await this._removeLibraryLocal(id)
    // Tell the peer to remove its side of the mapping too (its received files
    // stay on disk — true backup — only the sync link is removed).
    if (peerId) {
      this._sendToPeer(peerId, { type: MESSAGES.SYNC_REMOVE, libraryId: id })
    }
    return { id }
  }

  // Remove a library locally: stop watching, cancel its transfers, drop it
  // from the map + bee, and remember the id so a peer re-announce can never
  // silently re-create the link.
  async _removeLibraryLocal(id) {
    const lib = this.libraries.get(id)
    if (!lib) return
    this._stopWatching(id)
    this.libraries.delete(id)
    this.removedLibraries.add(id)
    if (this.transferEngine?.cancelSyncTransfers) {
      await this.transferEngine.cancelSyncTransfers(id).catch(() => {})
    }
    await this._remove(id)
    this.sendEvent(EVENTS.SYNC_LIBRARY_REMOVED, { id })
  }

  // The peer removed its side of the library: mirror the removal locally
  // (files on disk are kept — the receiver never deletes a backup).
  async handleSyncRemove(peerId, msg) {
    if (!msg || !msg.libraryId) return
    await this._removeLibraryLocal(msg.libraryId)
  }

  listLibraries() {
    return Array.from(this.libraries.values()).map((lib) => {
      let totalSize = 0
      let liveFiles = 0
      if (lib.index) {
        for (const entry of Object.values(lib.index)) {
          if (entry && !entry.deleted) {
            liveFiles++
            totalSize += entry.size || 0
          }
        }
      }
      return {
        id: lib.id,
        name: lib.name,
        localPath: lib.localPath,
        peerId: lib.peerId,
        mode: lib.mode || 'two-way',
        status: lib.paused ? 'paused' : (this._syncingSet.has(lib.id) ? 'syncing' : lib.status || 'idle'),
        // Ephemeral run phase: 'analyzing' | 'transferring' | 'synced' — lets
        // the UI render the comparison phase separately from the payload bar.
        phase: (lib._phase && lib._phase.phase) || 'synced',
        phaseTotal: (lib._phase && lib._phase.total) || 0,
        phaseDone: (lib._phase && lib._phase.done) || 0,
        paused: !!lib.paused,
        fileCount: liveFiles,
        totalSize,
        size: totalSize,
        lastScanAt: lib.lastScanAt || 0,
        lastSyncAt: lib.lastSyncAt || 0
      }
    })
  }

  async pauseSync(id) {
    const lib = this.libraries.get(id)
    if (!lib) return null
    lib.paused = true
    lib.status = 'paused'
    this._stopWatching(id)
    if (this.transferEngine?.cancelSyncTransfers) {
      await this.transferEngine.cancelSyncTransfers(id).catch(() => {})
    }
    // Pausing ends the round: clear any analyzing/transferring phase so the
    // card shows only the Paused pill, never a stale counter.
    lib._phasePending = 0
    lib._phaseDone = 0
    this._setPhase(lib, 'synced')
    await this._persist(lib)
    return lib
  }

  async resumeSync(id) {
    const lib = this.libraries.get(id)
    if (!lib) return null
    lib.paused = false
    lib.status = 'idle'
    this._watchLibrary(lib)
    await this._persist(lib)
    this.syncLibrary(id, { full: true }).catch(() => {})
    return lib
  }

  // Return a library's sync mode ('push' | 'two-way' | 'receive_only') for the
  // given library id, or null if it doesn't exist. Used by TransferEngine to
  // decide overwrite vs conflict-preserve on sync receive.
  getLibraryMode(id) {
    const lib = this.libraries.get(id)
    return lib ? lib.mode || null : null
  }

  // Streaming content hash of a file, used ONLY for ambiguous same-size/
  // same-mtime files in a verify round — never in the hot scan path. The hash
  // is the SHA-256 of the concatenated 256 KiB chunk hashes (deterministic and
  // comparable across peers; bounded memory regardless of file size).
  async _hashFileFast(abs) {
    try {
      const fd = await this.fsp.open(abs, 'r')
      try {
        const chunkHashes = []
        const buf = Buffer.alloc(256 * 1024)
        let pos = 0
        for (;;) {
          const { bytesRead } = await fd.read(buf, 0, buf.length, pos)
          if (bytesRead <= 0) break
          chunkHashes.push(sha256(buf.subarray(0, bytesRead)))
          pos += bytesRead
        }
        return Buffer.isBuffer(chunkHashes[0])
          ? Buffer.concat(chunkHashes.map((h) => (Buffer.isBuffer(h) ? h : Buffer.from(h)))).toString('hex')
          : ''
      } finally {
        await fd.close().catch(() => {})
      }
    } catch {
      return ''
    }
  }

  // ─── Sync Execution & Diffing ─────────────────────────────────────────────

  // Single entry point for a sync round. Concurrent calls (watcher debounce +
  // explicit UI/tick/PEER_CONNECTED triggers) are serialized per library via a
  // promise chain, so nothing is ever dropped: a call that arrives while a
  // scan is in flight awaits it, then runs its own scan. This is the
  // deterministic "compare on demand" model — no _pendingRescan loss.
  async syncLibrary(id, opts = {}) {
    const lib = this.libraries.get(id)
    if (!lib || lib.paused) return null
    const prev = lib._scanChain || Promise.resolve()
    const run = prev.then(() => this._runScan(lib, id, opts))
    lib._scanChain = run.catch(() => {})
    return run
  }

  async _runScan(lib, id, opts = {}) {
    this._syncingSet.add(id)
    lib.status = 'syncing'
    this.sendEvent(EVENTS.SYNC_SCAN, { id, status: 'syncing' })
    this._setPhase(lib, 'analyzing', { total: 0, done: 0 })

    try {
      // 1. Fast stat pass to update local index. Incremental when the watcher
      // told us exactly which paths changed (dirty-flag model): stat only
      // those instead of walking the whole folder. Falls back to a full scan
      // when the dirty set is null/empty (no filename from the watcher, a
      // burst past the cap, or an explicit manual sync — an explicit call
      // must never trust a possibly-stale dirty set).
      const scanOut = new Map()
      const dirty = lib._dirtyPaths
      lib._dirtyPaths = null // consumed; a re-scan re-populates it
      // Incremental ONLY for watcher-triggered scans (opts.full === false).
      // Every other caller (explicit UI, tick, PEER_CONNECTED, accept, retry)
      // does a full scan — a possibly-stale dirty set must never make an
      // authoritative sync miss files.
      const wantIncremental = opts.full === false && dirty && dirty.size > 0 && dirty.size <= 200
      let wasIncremental = wantIncremental
      if (wantIncremental) {
        const res = await statDirtyPaths(this.fsp, lib.localPath, dirty)
        for (const [rel, meta] of res.entries()) scanOut.set(rel, meta)
        if (res._sawDir) {
          // A dirty path was a directory (new subfolder, rename target) — the
          // subtree may be mid-write during a transfer. Fall back to a full
          // scan so no file is mis-read or tombstoned.
          console.log('[SyncEngine] dirty path is a directory — falling back to full scan')
          wasIncremental = false
          scanOut.clear()
          await scanFolder(this.fsp, lib.localPath, lib.localPath, scanOut)
        } else if (scanOut.size > 0) {
          console.log(`[SyncEngine] incremental scan: ${scanOut.size} dirty path(s)`)
        }
      } else {
        await scanFolder(this.fsp, lib.localPath, lib.localPath, scanOut)
      }
      const myPeerId = this.getPeerId ? this.getPeerId() : ''

      // Missing-source guard: if the folder is gone/unmounted, scanFolder
      // returns empty (read errors are swallowed). Tombstoning the whole index
      // would, in two-way mode, propagate SYNC_DELETE for EVERY file and move
      // the peer's copies to trash. Instead, pause the library and surface a
      // sync:error so the user can restore the path — never silently delete.
      const hadFiles = Object.values(lib.index || {}).some((e) => e && !e.deleted)
      const folderExists = await this.fsp
        .stat(lib.localPath)
        .then((st) => st && st.isDirectory())
        .catch(() => false)
      if (hadFiles && scanOut.size === 0 && !folderExists) {
        lib.paused = true
        lib.status = 'error'
        this._persist(lib).catch(() => {})
        this.sendEvent(EVENTS.SYNC_ERROR, {
          id: lib.id,
          libraryId: lib.id,
          message: `Sync folder is missing or unreachable: ${lib.localPath}. Sync paused.`
        })
        this.sendEvent(EVENTS.SYNC_PHASE, { id: lib.id, phase: 'error' })
        return
      }

      // Incremental mode: seed nextIndex with the existing index so the
      // non-dirty files survive (scanOut only holds the dirty paths). Full
      // scans build nextIndex fresh.
      const nextIndex = wasIncremental ? { ...(lib.index || {}) } : {}
      for (const [rel, meta] of scanOut.entries()) {
        const prev = lib.index[rel]
        // Incremental scans can return deleted:true for a dirty path that
        // vanished — carry the tombstone through instead of reviving it.
        if (meta.deleted) {
          nextIndex[rel] = {
            size: meta.size || (prev && prev.size) || 0,
            mtimeMs: Date.now(),
            sig: (prev && prev.sig) || '',
            authorKey: myPeerId,
            deleted: true,
            hash: (prev && prev.hash) || ''
          }
          continue
        }
        if (prev && !prev.deleted && prev.sig === meta.sig) {
          nextIndex[rel] = prev
        } else {
          nextIndex[rel] = {
            size: meta.size,
            mtimeMs: meta.mtimeMs,
            sig: meta.sig,
            authorKey: myPeerId,
            deleted: false,
            // Content hash for the divergence guard: populated lazily (only
            // when the file is ambiguous in a verify round) so the stat-only
            // scan stays cheap.
            hash: prev && prev.sig === meta.sig ? prev.hash || '' : ''
          }
        }
      }

      // Mark vanished files as deleted (tombstones). In incremental mode only
      // dirty paths can have vanished — statDirtyPaths already returns them
      // as deleted:true, so the loop must NOT tombstone untouched files (they
      // are simply not in scanOut). The dirty set is the allowlist.
      for (const [rel, prev] of Object.entries(lib.index || {})) {
        if (prev && !prev.deleted && !scanOut.has(rel)) {
          if (wasIncremental && !isDirtyOrUnder(dirty, rel)) {
            continue // untouched in an incremental scan — keep as-is
          }
          nextIndex[rel] = {
            size: prev.size,
            mtimeMs: Date.now(),
            sig: prev.sig,
            authorKey: myPeerId,
            deleted: true
          }
        }
      }

      // Persist ONLY the changed per-file rows — the lean incremental
      // manifest. Full-record JSON rewrites made RocksDB accumulate GBs of
      // blob versions; a per-file row put keeps writes tiny and localized.
      const changed = []
      for (const [rel, entry] of Object.entries(nextIndex)) {
        const prev = lib.index[rel]
        if (!prev || prev.sig !== entry.sig || !!prev.deleted !== !!entry.deleted) {
          changed.push([rel, entry])
        }
      }
      if (changed.length > 0) {
        await this._writeRows('index', lib.id, changed)
      }
      lib.index = nextIndex
      lib.lastScanAt = Date.now()
      this._setPhase(lib, 'analyzing', {
        total: Object.keys(nextIndex).length,
        done: Object.keys(nextIndex).length
      })

      // 2. Push/diff when the peer is connected and accepted. The index is only
      // announced for two-way libraries (desktop↔desktop): push owners track
      // their own sentIndex, and receive-only sinks never answer indexes.
      const peerObj = this._findPeer(lib.peerId)
      const isPeerOnline = !!(peerObj && peerObj.connection && !peerObj.incompatible)
      
      if (isPeerOnline && lib.accepted) {
        if (lib.mode === 'two-way') {
          this._sendToPeer(lib.peerId, {
            type: MESSAGES.SYNC_INDEX,
            libraryId: lib.id,
            name: lib.name,
            mode: lib.mode,
            entries: indexToArray(lib.index)
          })
        }

        // 3. Diff and push missing/newer files (no-op for receive_only sinks)
        await this._diffAndPush(lib)

        // Nothing in flight — close the round.
        if (!lib._phasePending) {
          this._finishRound(lib, { pushed: lib._lastPushed || 0, skipped: lib._lastSkipped || 0 })
        }
      } else if (isPeerOnline && !lib.accepted) {
        lib.status = 'waiting_peer'
        // Re-announce the invite now instead of waiting for the next 5-minute
        // tick: resume/trigger must not sit on "waiting for peer" when the
        // peer is online and the accept was lost (e.g. app restart).
        this._sendToPeer(lib.peerId, {
          type: MESSAGES.SYNC_INVITE,
          libraryId: lib.id,
          name: lib.name,
          mode: lib.mode,
          fileCount: Object.keys(lib.index || {}).length,
          totalSize: Object.values(lib.index || {}).reduce((acc, x) => acc + (x?.size || 0), 0)
        })
        // The round is over — never leave a stale "Analyzing" phase on screen.
        this._setPhase(lib, 'synced')
      } else {
        // Peer offline (or none bound): the round compared and transferred
        // nothing. Never claim "Synchronized" — the card must honestly show
        // that the link is waiting for its peer; the next round (tick /
        // PEER_CONNECTED / resume) re-runs the moment it is online.
        lib.status = 'waiting_peer'
        this._setPhase(lib, 'synced')
      }

      this.sendEvent(EVENTS.SYNC_UP_TO_DATE, { id: lib.id, status: lib.status })
    } catch (err) {
      console.warn(`[SyncEngine] syncLibrary failed for ${id}:`, err.message)
      lib.status = 'error'
      this.sendEvent(EVENTS.SYNC_ERROR, { id, error: err.message })
      // An exception must never leave the card stuck on "Analyzing".
      this._setPhase(lib, 'synced')
    } finally {
      this._syncingSet.delete(id)
      if (lib.status === 'syncing') lib.status = 'idle'
      // NOTE: no _pendingRescan re-entry here. The syncLibrary chain already
      // serializes concurrent calls — a caller that arrived during this scan
      // is queued on lib._scanChain and runs right after this one finishes.
    }
  }

  async _diffAndPush(lib) {
    if (!lib || lib.paused || !lib.accepted || lib.mode === 'receive_only') return
    // Re-entrancy guard: invite-accept and index-exchange can both trigger a
    // push round; without this, concurrent rounds push every file twice.
    if (lib._pushing) {
      // A round was dropped because another is in flight — remember to re-run
      // once it finishes so a remote's newly-announced file is never silently
      // left un-pushed until the next 5-minute tick. This is the "hit or miss"
      // two-way bug: both the remote index arrival and our own watcher can
      // fire a push round, and the dropped one lost the remote's file.
      lib._pushingRetry = true
      return
    }
    lib._pushing = true
    try {
      await this._doDiffAndPush(lib)
      // Catch-up: if a round was dropped while we were pushing, run once more.
      if (lib._pushingRetry && !lib.paused && lib.accepted) {
        lib._pushingRetry = false
        await this._doDiffAndPush(lib)
      }
    } finally {
      lib._pushing = false
    }
  }

  async _doDiffAndPush(lib) {
    // Single-owner model:
    //  - push   : the OWNER diffs its folder against its own sentIndex (what it
    //             already pushed). The receiver is a pure sink — no index
    //             exchange, no delete propagation (true backup: the receiver
    //             keeps everything it received).
    //  - two-way: desktop↔desktop only — full bidirectional diff against the
    //             peer's index with delete propagation (unchanged).
    const useSent = lib.mode === 'push'
    const baseline = useSent ? lib.sentIndex || {} : lib.remoteIndex || {}
    const toPush = []

    for (const [rel, localEntry] of Object.entries(lib.index || {})) {
      if (!localEntry || localEntry.deleted) continue
      const known = baseline[rel]
      if (!known) {
        toPush.push(rel)
      } else if (!useSent && !known.deleted && localEntry.mtimeMs > known.mtimeMs + CONFLICT_TOLERANCE_MS) {
        toPush.push(rel)
      } else if (useSent && known.sig !== localEntry.sig) {
        toPush.push(rel)
      }
    }

    // Batch pre-verification — ONE round-trip instead of N per-file skip
    // handshakes. The message covers TWO sets:
    //   1. pending files — does the receiver need them? (verified ones never
    //      become transfer records, so re-syncing an existing folder is clean)
    //   2. already-sent files — does the receiver still HAVE them? This is the
    //      self-healing half of a backup: files deleted or lost on the
    //      receiver's disk are detected here and re-pushed, so the backup
    //      repairs itself instead of silently decaying.
    const toPushSet = new Set(toPush)
    const have = new Set() // pending files the receiver already holds
    const lost = new Set() // sent files the receiver no longer holds → re-push
    const verifyList = []
    for (const rel of toPush) {
      const e = lib.index[rel] || {}
      verifyList.push({ rel, size: e.size || 0, mtimeMs: e.mtimeMs || 0, hash: e.hash || '' })
    }
    const sentRels = Object.keys(baseline).filter((rel) => lib.index[rel] && !lib.index[rel].deleted)
    let sentSlice = sentRels
    if (sentRels.length > MAX_VERIFY_FILES) {
      // Rotate through huge sent sets so every file is re-checked eventually
      // without shipping a multi-MB verify message every round.
      lib._verifyRound = (lib._verifyRound || 0) + 1
      const chunks = Math.ceil(sentRels.length / MAX_VERIFY_FILES)
      const idx = lib._verifyRound % chunks
      sentSlice = sentRels.slice(idx * MAX_VERIFY_FILES, (idx + 1) * MAX_VERIFY_FILES)
    }
    for (const rel of sentSlice) {
      const e = lib.index[rel]
      // Divergence guard: attach the sender's content hash so the receiver can
      // detect same-size/same-mtime files whose CONTENT differs (local edit
      // preserved size, or clock skew). Computed lazily — only for already-sent
      // files in the verify slice, cached in the index entry.
      if (e && !e.hash) {
        const absPath = this.path.join(lib.localPath, ...rel.split('/'))
        e.hash = await this._hashFileFast(absPath)
      }
      verifyList.push({ rel, size: e.size || 0, mtimeMs: e.mtimeMs || 0, hash: (e && e.hash) || '' })
    }

    let verified = null
    if (verifyList.length > 0) {
      const peerObj = this._findPeer(lib.peerId)
      if (peerObj && peerObj.connection && !peerObj.incompatible) {
        verified = await this._verifyWithPeer(lib, verifyList)
      }
    }
    if (verified && verified.size > 0) {
      const markKind = useSent ? 'sent' : 'remote'
      const marks = []
      for (const rel of verified) {
        const e = lib.index[rel]
        if (!e || !toPushSet.has(rel)) continue
        baseline[rel] = e
        marks.push([rel, useSent ? { sig: e.sig || `${e.size || 0}-${e.mtimeMs || 0}` } : e])
        have.add(rel)
      }
      if (marks.length) await this._writeRows(markKind, lib.id, marks)
    }
    if (verified) {
      // Sent files the receiver no longer has are re-pushed — EXCEPT those
      // whose delivery is already in flight (a transfer en route has not
      // reached the receiver's disk yet, so it is not "lost"; re-pushing it
      // would duplicate the transfer and storm the connection).
      const inFlight =
        this.transferEngine && typeof this.transferEngine.inFlightSyncRels === 'function'
          ? this.transferEngine.inFlightSyncRels(lib.id)
          : new Set()
      for (const rel of sentSlice) {
        if (!verified.has(rel) && !inFlight.has(rel)) lost.add(rel)
      }
    }

    // The transferring phase starts BEFORE any transfer record is created:
    // the pending set is fully known after the batch verify, so the UI
    // switches from "Analyzing" to a live transfer counter immediately —
    // never a long "Analyzing N/N" pause while 1000+ records are queued.
    // The pending set = files to push for the first time, minus the ones the
    // receiver already holds, plus any sent files the receiver lost.
    const pendingRels = Array.from(new Set(toPush.filter((rel) => !have.has(rel)).concat(Array.from(lost))))
    if (pendingRels.length > 0) {
      lib._phasePending = (lib._phasePending || 0) + pendingRels.length
      this._setPhase(lib, 'transferring', {
        total: (lib._phaseDone || 0) + lib._phasePending,
        done: lib._phaseDone || 0
      })
    }

    // Create the transfers in parallel chunks (startSend persists a small
    // record per transfer; awaiting 1170 of them sequentially would stall the
    // round for seconds). Records that fail to create are reconciled below so
    // the in-flight counter always reaches zero.
    let pushed = 0
    const markKind = useSent ? 'sent' : 'remote'
    const optimisticMarks = []
    const CREATE_CHUNK = 8
    for (let i = 0; i < pendingRels.length; i += CREATE_CHUNK) {
      if (lib.paused) break
      const chunk = pendingRels.slice(i, i + CREATE_CHUNK)
      await Promise.all(
        chunk.map(async (rel) => {
          const filePath = this.path.join(lib.localPath, ...rel.split('/'))
          try {
            const stats = await this.fsp.stat(filePath).catch(() => null)
            if (!stats) return
            // NEVER create transfers for an unreachable peer: hundreds of parked/
            // failing transfers starve the worklet and flood the logs. The tick
            // re-diffs when the peer comes back.
            const peerObj = this._findPeer(lib.peerId)
            if (!peerObj || !peerObj.connection || peerObj.incompatible) return
            if (this.transferEngine && typeof this.transferEngine.startSend === 'function') {
              const localEntry = lib.index[rel]
              await this.transferEngine.startSend({
                filePath,
                filename: this.path.basename(filePath),
                fileSize: stats.size,
                peerId: lib.peerId,
                peerName: peerObj?.device?.name || 'Peer',
                source: 'sync',
                syncLibraryId: lib.id,
                syncLibraryName: lib.name,
                syncRelPath: rel,
                syncMtimeMs: (localEntry && localEntry.mtimeMs) || stats.mtimeMs,
                syncAuthorKey: (localEntry && localEntry.authorKey) || ''
              })
              // Optimistic: the peer now has (or is receiving) this version, so
              // the next round does not re-push it while the transfer is in
              // flight. The mark is persisted per-file (crash-safe; a failed
              // transfer deletes its row again via handleTransferTerminal).
              if (localEntry) {
                baseline[rel] = localEntry
                optimisticMarks.push([rel, useSent ? { sig: localEntry.sig || `${localEntry.size || 0}-${localEntry.mtimeMs || 0}` } : localEntry])
              }
              pushed++
            }
          } catch (err) {
            console.warn(`[SyncEngine] Push failed for ${rel}:`, err.message)
          }
        })
      )
    }
    if (optimisticMarks.length) await this._writeRows(markKind, lib.id, optimisticMarks)

    // Reconcile: files that could not be turned into transfers (peer dropped,
    // stat failed) must not keep the in-flight counter above zero forever.
    const notCreated = pendingRels.length - pushed
    if (notCreated > 0 && lib._phasePending > 0) {
      lib._phasePending = Math.max(0, lib._phasePending - notCreated)
      this._setPhase(lib, 'transferring', {
        total: (lib._phaseDone || 0) + lib._phasePending,
        done: lib._phaseDone || 0
      })
    }
    lib._lastPushed = pushed
    lib._lastSkipped = have.size

    if (useSent) {
      // Pure backup: the receiver keeps everything, so tombstones are NEVER
      // propagated. Prune sentIndex entries whose files no longer exist, and
      // drop their rows (per-file, no full-record rewrite).
      const sent = lib.sentIndex || {}
      const removed = []
      for (const rel of Object.keys(sent)) {
        if (!lib.index[rel] || lib.index[rel].deleted) {
          delete sent[rel]
          removed.push(rel)
        }
      }
      if (removed.length) await this._delRows('sent', lib.id, removed)
    } else if (lib.mode === 'two-way') {
      // Propagate local deletions to the peer (desktop ↔ desktop mirror).
      // The tombstone travels as SYNC_DELETE; the peer moves its copy to trash.
      for (const [rel, entry] of Object.entries(lib.index || {})) {
        if (!entry || !entry.deleted) continue
        const remoteEntry = baseline[rel]
        if (!remoteEntry || remoteEntry.deleted) continue
        this._sendToPeer(lib.peerId, { type: MESSAGES.SYNC_DELETE, libraryId: lib.id, rel })
        // Optimistic: the peer will trash it; skip re-sending next round.
        baseline[rel] = { size: 0, mtimeMs: Date.now(), sig: '', authorKey: '', deleted: true }
      }
    }
  }

  // One batch pre-verification round-trip: returns the set of rel paths the
  // receiver already holds byte-identical, or null on timeout/error. NULL is
  // deliberately distinct from an empty set: a timed-out round must not look
  // like "the receiver has nothing" (that would re-push every sent file), it
  // simply falls back to the per-file skip handshake.
  _verifyWithPeer(lib, pending) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        if (lib._verifyResolver) lib._verifyResolver = null
        resolve(null)
      }, this.verifyTimeoutMs)
      if (timer.unref) timer.unref()
      lib._verifyResolver = {
        timer,
        resolve: (haveSet) => {
          clearTimeout(timer)
          if (lib._verifyResolver) lib._verifyResolver = null
          resolve(haveSet || null)
        }
      }
      this._sendToPeer(lib.peerId, { type: MESSAGES.SYNC_VERIFY, libraryId: lib.id, files: pending })
    })
  }

  // ─── Sync Run Phases ──────────────────────────────────────────────────────
  // A sync run is split into cleanly distinguishable phases so the UI can
  // separate "analyzing/comparing existing files" from "transferring new
  // payload" instead of lumping verification into the progress bar:
  //   analyzing    — local index scan + batch verification against the receiver
  //   transferring — real payload transfers only (verified files never queue)
  //   synced       — round complete

  _setPhase(lib, phase, extra = {}) {
    lib._phase = { phase, ...extra }
    this.sendEvent(EVENTS.SYNC_PHASE, { id: lib.id, phase, ...extra })
  }

  _finishRound(lib, { pushed = 0, skipped = 0 } = {}) {
    lib._phasePending = 0
    lib._phaseDone = 0
    lib._phase = { phase: 'synced' }
    lib.lastSyncAt = Date.now()
    this._persist(lib).catch(() => {})
    this.sendEvent(EVENTS.SYNC_PHASE, { id: lib.id, phase: 'synced' })
    this.sendEvent(EVENTS.SYNC_COMPLETED, { id: lib.id, name: lib.name, pushed, deleted: 0 })
  }

  // ─── Wire Message Handlers ────────────────────────────────────────────────

  async handleSyncInvite(peerId, msg) {
    if (!msg || !msg.libraryId) return
    if (this.removedLibraries.has(msg.libraryId)) return
    // The link already exists on this side (the peer restarted and re-announced
    // an invite for an already-accepted folder). Re-confirm silently instead of
    // raising a new accept modal for a folder that is already live. The pause
    // state is separate and still gates transfers via isSyncAllowed.
    const existing = this.libraries.get(msg.libraryId)
    if (existing) {
      existing.accepted = true
      existing.status = 'idle'
      this._persist(existing).catch(() => {})
      this._sendToPeer(peerId, { type: MESSAGES.SYNC_INVITE_ACCEPT, libraryId: msg.libraryId })
      return
    }
    const peersMap = this.getPeers()
    let peerObj = peersMap.get(peerId)
    if (!peerObj) {
      for (const [, p] of peersMap.entries()) {
        if (p.device?.id === peerId || p.device?.publicKey === peerId) {
          peerObj = p
          break
        }
      }
    }
    const peerName = peerObj?.device?.name || 'MeshDrop Device'
    const defaultPath = this.path.join(
      this.downloadsDir || '/storage/emulated/0/Download',
      'Sync',
      msg.name || 'Sync-Folder'
    )
    const invite = {
      id: msg.libraryId,
      name: msg.name || 'Sync-Folder',
      peerId,
      peerName,
      defaultPath,
      mode: msg.mode || 'two-way',
      fileCount: msg.fileCount || 0,
      totalSize: msg.totalSize || 0
    }
    this.pendingInvites.set(msg.libraryId, invite)
    this.sendEvent(EVENTS.SYNC_INVITE_RECEIVED, invite)
  }

  // Receiver side of the batch pre-verification: stat each pending file and
  // report which ones we already hold byte-identical (same size + mtime as the
  // sender's index). The owner then marks those as sent instead of pushing
  // them — no per-file skip handshakes, no fake transfers, no data on the wire.
  async handleSyncVerify(peerId, msg) {
    if (!msg || !msg.libraryId || !Array.isArray(msg.files) || msg.files.length === 0) return
    const lib = this.libraries.get(msg.libraryId)
    if (!lib) {
      this._sendToPeer(peerId, { type: MESSAGES.SYNC_VERIFY_RESULT, libraryId: msg.libraryId, have: [] })
      return
    }
    // Surface the comparison phase on the receiver's card too (large folders
    // take a moment to stat). Never stomp an active transferring phase.
    if (lib._phase && lib._phase.phase !== 'transferring') {
      this._setPhase(lib, 'analyzing', { total: msg.files.length, done: 0 })
    }
    const have = []
    if (!lib.paused) {
      let bee = null
      try {
        bee = await this.getBee('sync')
      } catch {}
      const CHUNK = 32
      for (let i = 0; i < msg.files.length; i += CHUNK) {
        const chunk = msg.files.slice(i, i + CHUNK)
        const results = await Promise.all(
          chunk.map(async (f) => {
            if (!f || typeof f.rel !== 'string') return null
            const rel = safeRelPath(f.rel)
            if (!rel) return null
            const abs = this.path.join(lib.localPath, ...rel.split('/'))
            const st = await this.fsp.stat(abs).catch(() => null)
            if (
              st &&
              Number(st.size) === Number(f.size) &&
              Math.abs(Number(st.mtimeMs || 0) - Number(f.mtimeMs || 0)) < VERIFY_TOLERANCE_MS
            ) {
              // Strict match: only trust a stat match when WE delivered this
              // exact version (recorded at transfer completion). A pre-existing
              // file that happens to share size + mtime must not count as
              // synced — that is how stale content stayed "Synchronized"
              // despite rescan on both sides.
              if (!bee) return null
              const node = await bee.get(`delivered/${lib.id}/${rel}`).catch(() => null)
              const del = node && node.value
              if (del && Math.abs(Number(del.mtimeMs || 0) - Number(f.mtimeMs || 0)) < VERIFY_TOLERANCE_MS) {
                // Divergence guard: the file matches the sender's size+mtime AND
                // we delivered a version with that mtime — but was the local
                // file MODIFIED after delivery while preserving size (or under
                // clock skew)? If the current mtime drifted from the delivered
                // mtime beyond tolerance, hash-compare before claiming "have".
                // A content match → skip; a content mismatch → report missing so
                // the owner re-pushes and the newer edit wins deterministically.
                if (del.mtimeMs && Math.abs(Number(st.mtimeMs || 0) - Number(del.mtimeMs || 0)) > VERIFY_TOLERANCE_MS) {
                  const localHash = await this._hashFileFast(abs)
                  const remoteHash = f.hash || ''
                  if (localHash && remoteHash && localHash !== remoteHash) {
                    // Concurrent-edit divergence: local content differs from
                    // what the sender believes is synced. Report as NOT-have so
                    // the owner re-pushes, and surface a conflict event — the
                    // losing local edit is preserved to trash (two-way) by
                    // _finalizeReceive.
                    this.sendEvent(EVENTS.SYNC_CONFLICT, {
                      id: lib.id,
                      libraryId: lib.id,
                      rel,
                      message: `Concurrent edit detected on ${rel} — the newer version will be kept, the local copy moved to trash.`
                    })
                    return null // content differs → do NOT report as have
                  }
                }
                return rel
              }
            }
            return null
          })
        )
        for (const r of results) if (r) have.push(r)
      }
    }
    this._sendToPeer(peerId, { type: MESSAGES.SYNC_VERIFY_RESULT, libraryId: msg.libraryId, have })
    if (lib._phase && lib._phase.phase !== 'transferring') {
      this._setPhase(lib, 'synced')
    }
  }

  // Owner side: the receiver answered the batch verify. Resolve the pending
  // verification (the awaiting sync round then skips those files entirely).
  async handleSyncVerifyResult(peerId, msg) {
    if (!msg || !msg.libraryId) return
    const lib = this.libraries.get(msg.libraryId)
    if (!lib || !lib._verifyResolver) return
    const have = new Set((msg.have || []).map((r) => safeRelPath(r)).filter(Boolean))
    lib._verifyResolver.resolve(have)
  }

  // Strict-verify helper: did WE deliver this exact version of rel to lib?
  // The transfer-level re-sync skip consults this before trusting a size+mtime
  // stat match on the destination file, so a coincidental match never
  // suppresses a real push.
  async isDeliveredVersion(libId, rel, mtimeMs) {
    try {
      const safe = safeRelPath(rel)
      if (!safe || !libId) return false
      const bee = await this.getBee('sync')
      const node = await bee.get(`delivered/${libId}/${safe}`).catch(() => null)
      const del = node && node.value
      return !!(del && Math.abs(Number(del.mtimeMs || 0) - Number(mtimeMs || 0)) < VERIFY_TOLERANCE_MS)
    } catch {
      return false
    }
  }

  async handleSyncInviteAccept(peerId, msg) {
    if (!msg || !msg.libraryId) return
    const lib = this.libraries.get(msg.libraryId)
    if (!lib) return
    lib.accepted = true
    lib.status = 'idle'
    await this._persist(lib)
    this.sendEvent(EVENTS.SYNC_COMPLETED, { id: lib.id, name: lib.name, pushed: 0, deleted: 0, accepted: true })

    // Single-owner model: only two-way (desktop↔desktop) exchanges indexes —
    // one-way owners track their own sentIndex, receive-only sinks need none.
    if (lib.mode === 'two-way') {
      this._sendToPeer(peerId, {
        type: MESSAGES.SYNC_INDEX,
        libraryId: lib.id,
        name: lib.name,
        mode: lib.mode,
        entries: indexToArray(lib.index)
      })
    }
    await this._diffAndPush(lib)
  }

  async handleSyncInviteDecline(peerId, msg) {
    if (!msg || !msg.libraryId) return
    const lib = this.libraries.get(msg.libraryId)
    if (!lib) return
    lib.status = 'paused'
    lib.paused = true
    await this._persist(lib)
    this.sendEvent(EVENTS.SYNC_ERROR, { id: lib.id, error: 'Peer declined sync invitation' })
  }

  async handleSyncIndex(peerId, msg) {
    if (!msg || !msg.libraryId) return
    // A library the user removed must not come back via a peer re-announce.
    if (this.removedLibraries.has(msg.libraryId)) return
    const lib = this.libraries.get(msg.libraryId)
    // Indexes only ever arrive for libraries we already created or accepted —
    // a fresh libraryId here means the invite flow was bypassed; ignore it so
    // files can never land without an explicit accept (custom folder choice).
    if (!lib) return
    // Single-owner model: index exchange only exists for two-way (desktop↔
    // desktop). One-way owners diff against their own sentIndex; receive-only
    // sinks never need the sender's index.
    if (lib.mode !== 'two-way') return

    // MERGE, don't replace: the peer's index can be stale/empty while our
    // pushes are still in flight (its folder rescan lags behind). Replacing
    // wipes our optimistic marks and makes every round re-push everything.
    // Entries the peer does not mention keep their previous value.
    const incoming = indexFromArray(msg.entries)
    const merged = { ...(lib.remoteIndex || {}), ...incoming }
    // Persist only on a real change (the record embeds the full index).
    if (JSON.stringify(merged) !== JSON.stringify(lib.remoteIndex)) {
      lib.remoteIndex = merged
      await this._persist(lib)
    } else {
      lib.remoteIndex = merged
    }

    // Receiver side (two-way): mirror peer tombstones to trash.
    await this._applyRemoteDeletes(lib)
    // Sender side (two-way): if the peer's index is missing or older on a file, push ours.
    if (lib.accepted && !lib.paused) {
      this._diffAndPush(lib).catch(() => {})
    }
  }

  // Move a library file into its .meshdrop-trash and tombstone the index entry
  // (never a hard delete — the peer can always be re-synced from the master).
  async _deleteLocal(lib, rel) {
    if (!lib || !lib.localPath) return
    const safe = safeRelPath(rel)
    if (!safe) return
    const targetFile = this.path.join(lib.localPath, ...safe.split('/'))
    const trashDir = this.path.join(lib.localPath, '.meshdrop-trash')

    try {
      const exists = await this.fsp.stat(targetFile).then(() => true).catch(() => false)
      if (exists) {
        await this.fsp.mkdir(trashDir, { recursive: true })
        const trashDest = this.path.join(trashDir, `${Date.now().toString(36)}_${this.path.basename(safe)}`)
        await this.fsp.rename(targetFile, trashDest)
      }
    } catch (err) {
      console.warn(`[SyncEngine] _deleteLocal error for ${safe}:`, err.message)
    }

    if (lib.index && lib.index[safe]) {
      lib.index[safe] = {
        ...lib.index[safe],
        deleted: true,
        mtimeMs: Date.now()
      }
      await this._persist(lib)
      this.sendEvent(EVENTS.SYNC_DELETED, { id: lib.id, libraryId: lib.id, rel: safe })
    }
  }

  // Receiver-side mirror: apply tombstones announced by the peer — move our
  // live copy to trash so the folder mirrors the master exactly. No index
  // reply here: replying to every incoming index with our own (possibly stale)
  // index makes the peer re-push files that are still in flight. Our next
  // syncLibrary round announces the updated index anyway.
  async _applyRemoteDeletes(lib) {
    const remote = lib.remoteIndex || {}
    for (const [rel, r] of Object.entries(remote)) {
      if (!r || !r.deleted) continue
      const local = lib.index && lib.index[rel]
      if (local && !local.deleted) {
        await this._deleteLocal(lib, rel)
      }
    }
  }

  async handleSyncDelete(peerId, msg) {
    if (!msg || !msg.libraryId) return
    const lib = this.libraries.get(msg.libraryId)
    if (!lib) return
    await this._deleteLocal(lib, msg.rel)
    await this.syncLibrary(lib.id, { full: true }).catch(() => {})
  }

  // Periodic rescan tick across active libraries
  async tick() {
    for (const lib of this.libraries.values()) {
      if (lib.paused) continue
      const peerObj = this._findPeer(lib.peerId)
      if (!peerObj || !peerObj.connection || peerObj.incompatible) continue

      if (!lib.accepted) {
        // The peer is back but never accepted our invite (it was offline when
        // the library was created) — re-announce so it can choose a folder.
        this._sendToPeer(lib.peerId, {
          type: MESSAGES.SYNC_INVITE,
          libraryId: lib.id,
          name: lib.name,
          mode: lib.mode,
          fileCount: Object.keys(lib.index || {}).length,
          totalSize: Object.values(lib.index || {}).reduce((acc, x) => acc + (x?.size || 0), 0)
        })
        continue
      }

      try {
        await this.syncLibrary(lib.id, { full: true })
      } catch {}
    }
  }

  async acceptSyncInvite({ id, customPath }) {
    const invite = this.pendingInvites.get(id)
    const localPath = customPath || (invite && invite.defaultPath) || this.path.join(this.downloadsDir || '/storage/emulated/0/Download', 'Sync', (invite && invite.name) || 'Sync')
    try {
      await this.fsp.mkdir(localPath, { recursive: true })
    } catch {}

    const peerId = invite ? invite.peerId : ''
    const libName = (invite && invite.name) || this.path.basename(localPath) || 'Sync'
    const mode = invite && invite.mode === 'push' ? 'receive_only' : 'two-way'

    let lib = this.libraries.get(id)
    if (!lib) {
      lib = {
        id,
        name: libName,
        localPath,
        peerId,
        mode,
        status: 'idle',
        paused: false,
        accepted: true, // the peer accepted us — it may now push
        index: {},
        remoteIndex: {},
        lastScanAt: Date.now(),
        lastSyncAt: 0
      }
      this.libraries.set(id, lib)
    } else {
      lib.localPath = localPath
      lib.status = 'idle'
      lib.paused = false
      lib.accepted = true
    }
    this.removedLibraries.delete(id)
    await this._persist(lib)
    this._watchLibrary(lib)
    this.pendingInvites.delete(id)

    this.sendEvent(EVENTS.SYNC_LIBRARY_ADDED, {
      id: lib.id,
      name: lib.name,
      localPath: lib.localPath,
      mode: lib.mode,
      status: lib.status,
      fileCount: Object.keys(lib.index).length
    })

    if (peerId) {
      this._sendToPeer(peerId, {
        type: MESSAGES.SYNC_INVITE_ACCEPT,
        libraryId: id
      })
    }
    // Scan our (possibly new) folder and exchange indexes so the sender starts pushing.
    this.syncLibrary(lib.id, { full: true }).catch(() => {})
    return { success: true, id, path: localPath }
  }

  async declineSyncInvite({ id }) {
    const invite = this.pendingInvites.get(id)
    this.pendingInvites.delete(id)
    if (invite && invite.peerId) {
      this._sendToPeer(invite.peerId, {
        type: MESSAGES.SYNC_INVITE_DECLINE,
        libraryId: id
      })
    }
    return { success: true, id }
  }

  listPendingInvites() {
    return Array.from(this.pendingInvites.values())
  }

  // TransferEngine gate: reject incoming sync offers for libraries that are
  // paused or removed — pausing/deleting must stop BOTH directions, and a
  // deleted link must never accept stray files into the default folder.
  isSyncAllowed(offer) {
    if (!offer || !offer.syncLibraryId) return false
    const lib = this.libraries.get(offer.syncLibraryId)
    if (!lib) return false
    return !lib.paused && lib.accepted
  }

  // Any sync transfer reaching a terminal state (completed/failed/cancelled/
  // interrupted): advance the transferring-phase counter, and for failures
  // drop the optimistic mark (sentIndex for one-way owners, remoteIndex for
  // two-way) so the next round re-pushes the file. Rows are deleted per-file —
  // a tiny write, unlike the old full-record rewrites.
  handleTransferTerminal(record) {
    if (!record || record.source !== 'sync') return
    const lib = this.libraries.get(record.syncLibraryId)
    if (!lib) return

    const failed = record.status === 'failed' || record.status === 'cancelled' || record.status === 'interrupted'
    const rel = safeRelPath(record.syncRelPath)
    if (failed && rel) {
      if (lib.sentIndex && lib.sentIndex[rel]) {
        delete lib.sentIndex[rel]
        this._delRows('sent', lib.id, [rel]).catch(() => {})
        console.warn(`[SyncEngine] sync push failed for ${rel} — will retry`)
      } else if (lib.remoteIndex && lib.remoteIndex[rel]) {
        delete lib.remoteIndex[rel]
        this._delRows('remote', lib.id, [rel]).catch(() => {})
        console.warn(`[SyncEngine] sync push failed for ${rel} — will retry`)
      }
      // Fast self-heal: an interrupted/failed send disappears from the sent
      // index, so schedule a re-round with backoff — the file is re-pushed
      // within seconds instead of waiting for the 5-minute tick.
      if (record.direction === 'send') {
        const delay = Math.min(30000, 2000 * Math.pow(2, lib._failRetries || 0))
        lib._failRetries = (lib._failRetries || 0) + 1
        clearTimeout(lib._failRetryTimer)
        lib._failRetryTimer = setTimeout(() => {
          lib._failRetries = Math.max(0, (lib._failRetries || 1) - 1)
          this.syncLibrary(lib.id, { full: true }).catch(() => {})
        }, delay)
        if (lib._failRetryTimer.unref) lib._failRetryTimer.unref()
      }
    } else if (record.status === 'completed') {
      // A clean delivery resets the failure backoff.
      lib._failRetries = 0
    }

    if (record.direction === 'receive') {
      // Record that WE delivered this exact version (size + sender mtime).
      // Strict re-verification only trusts stat matches for these files.
      if (record.status === 'completed' && rel) {
        const size = Number(record.fileSize) || 0
        const mtimeMs = Number(record.syncMtimeMs) || 0
        if (size > 0 && mtimeMs > 0) {
          this._writeRows('delivered', lib.id, [[rel, { size, mtimeMs, sig: `${size}-${mtimeMs}` }]]).catch(() => {})
        }
      }
      // Receiver side: count down active receives; when the last one finishes,
      // the round is done.
      lib._activeReceives = Math.max(0, (lib._activeReceives || 0) - 1)
      if (lib._activeReceives === 0 && lib._phase && lib._phase.phase === 'transferring') {
        this._finishRound(lib, {})
      }
      return
    }

    if (lib._phasePending > 0) {
      // Only genuinely completed transfers advance the counter — interrupted
      // or failed ones just decrement the in-flight count and retry next
      // round, so the number can never claim delivery that did not happen.
      lib._phasePending = Math.max(0, lib._phasePending - 1)
      if (record.status === 'completed') {
        lib._phaseDone = (lib._phaseDone || 0) + 1
      }
      this.sendEvent(EVENTS.SYNC_PHASE, {
        id: lib.id,
        phase: 'transferring',
        total: (lib._phaseDone || 0) + lib._phasePending,
        done: lib._phaseDone || 0
      })
      if (lib._phasePending === 0) {
        this._finishRound(lib, { pushed: lib._phaseDone || 0 })
      }
    }
  }

  // A sync transfer started: the receiver shows a transferring phase while
  // real payload is being written (skips never produce transfers, so this can
  // only fire for actual data).
  handleTransferStarted(record) {
    if (!record || record.source !== 'sync') return
    const lib = this.libraries.get(record.syncLibraryId)
    if (!lib || record.direction !== 'receive') return
    lib._activeReceives = (lib._activeReceives || 0) + 1
    this._setPhase(lib, 'transferring', { total: 0, done: 0 })
  }
}

module.exports = { SyncEngine, safeRelPath, scanFolder }
