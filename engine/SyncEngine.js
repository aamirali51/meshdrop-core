'use strict'

// SyncEngine — fast, zero-duplication, memory-safe folder synchronization
// for @mesh/core (Desktop & Mobile).
//
// Built on a modular 3-Way Baseline Snapshot Architecture:
//  - SnapshotStore: Hyperbee/RocksDB baseline snapshot & manifest persistence
//  - Scanner: Fast directory traversal, dirty-path tracking, and sample hashing
//  - Reconciler: Pure 3-way algebraic reconciliation & action planner

const { EVENTS, MESSAGES } = require('../protocol.js')
const {
  DEFAULT_SCAN_INTERVAL_MS,
  MAX_LIBRARY_FILES,
  DEFAULT_VERIFY_TIMEOUT_MS,
  MAX_VERIFY_FILES,
  VERIFY_TOLERANCE_MS,
  safeRelPath,
  indexToArray,
  indexFromArray
} = require('./sync/SyncConstants.js')
const { SnapshotStore } = require('./sync/SnapshotStore.js')
const { scanFolder, statDirtyPaths, isDirtyOrUnder, hashFileFast } = require('./sync/Scanner.js')
const { Reconciler } = require('./sync/Reconciler.js')

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
      for await (const node of bee.createReadStream({ gte: 'sync-', lte: 'sync-\uffff' })) {
        const cfg = node.value
        if (!cfg || !cfg.id || !cfg.localPath) continue

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
        if (typeof lib.accepted !== 'boolean') lib.accepted = true

        for (const [rel, entry] of await this._iterRows('index', lib.id)) lib.index[rel] = entry
        for (const [rel, entry] of await this._iterRows('sent', lib.id)) lib.sentIndex[rel] = entry
        for (const [rel, entry] of await this._iterRows('remote', lib.id)) lib.remoteIndex[rel] = entry

        // Migrate legacy embedded maps to individual rows
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
          await this._persist(lib)
        }

        this.libraries.set(lib.id, lib)
        this._watchLibrary(lib)
      }
    } catch (err) {
      console.warn('[SyncEngine] loadLibraries warning:', err.message)
    }
  }

  async _persist(lib) {
    await SnapshotStore.persistConfig(this.getBee, lib)
  }

  async _iterRows(kind, libId) {
    return SnapshotStore.iterRows(this.getBee, kind, libId)
  }

  async _writeRows(kind, libId, entries) {
    return SnapshotStore.writeRows(this.getBee, kind, libId, entries)
  }

  async _delRows(kind, libId, rels) {
    return SnapshotStore.delRows(this.getBee, kind, libId, rels)
  }

  async _remove(id) {
    return SnapshotStore.removeLibrary(this.getBee, id)
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
      const watcher = this.fs.watch(lib.localPath, { recursive: true }, (eventType, filename) => {
        this._onFolderChange(lib.id, filename)
      })
      watcher.on('error', () => {
        this._stopWatching(lib.id)
        this._scheduleWatchRetry(lib.id)
      })
      lib.watcher = watcher
    } catch {
      this._scheduleWatchRetry(lib.id)
    }
  }

  _scheduleWatchRetry(id) {
    const lib = this.libraries.get(id)
    if (!lib || lib.watcher || lib._watchRetryTimer) return
    const delay = Math.min((lib._watchRetryCount || 0) * 5000 + 5000, 5 * 60 * 1000)
    lib._watchRetryCount = (lib._watchRetryCount || 0) + 1
    lib._watchRetryTimer = setTimeout(() => {
      lib._watchRetryTimer = null
      const l = this.libraries.get(id)
      if (!l || l.watcher || l.paused) return
      this._watchLibrary(l)
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
    if (typeof filename === 'string' && filename.length > 0) {
      if (!lib._dirtyPaths) lib._dirtyPaths = new Set()
      let rel = filename.split(/[\\/]/).join('/').replace(/^\/+/, '')
      if (rel) {
        lib._dirtyPaths.add(rel)
        if (lib._dirtyPaths.size > 200) lib._dirtyPaths = null
      }
    } else {
      lib._dirtyPaths = null
    }
    clearTimeout(lib.flushTimer)
    lib.flushTimer = setTimeout(() => {
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
      const isLocalMobile = this.isMobile || (typeof process !== 'undefined' && process.platform === 'android')
      const peerObj = this._findPeer(peerId)
      const isPeerMobile = !!(peerObj && (peerObj.device?.type === 'mobile' || peerObj.device?.platform === 'android' || peerObj.device?.platform === 'ios'))
      if (isLocalMobile || isPeerMobile) normMode = 'push'
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
      accepted: !peerId,
      index: {},
      remoteIndex: {},
      sentIndex: {},
      lastScanAt: Date.now(),
      lastSyncAt: 0
    }

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
    if (peerId) {
      this._sendToPeer(peerId, { type: MESSAGES.SYNC_REMOVE, libraryId: id })
    }
    return { id }
  }

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

  getLibraryMode(id) {
    const lib = this.libraries.get(id)
    return lib ? lib.mode || null : null
  }

  // ─── Sync Execution & Diffing ─────────────────────────────────────────────

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
      const scanOut = new Map()
      const dirty = lib._dirtyPaths
      lib._dirtyPaths = null
      const wantIncremental = opts.full === false && dirty && dirty.size > 0 && dirty.size <= 200
      let wasIncremental = wantIncremental
      if (wantIncremental) {
        const res = await statDirtyPaths(this.fsp, lib.localPath, dirty)
        for (const [rel, meta] of res.entries()) scanOut.set(rel, meta)
        if (res._sawDir) {
          wasIncremental = false
          scanOut.clear()
          await scanFolder(this.fsp, lib.localPath, lib.localPath, scanOut)
        }
      } else {
        await scanFolder(this.fsp, lib.localPath, lib.localPath, scanOut)
      }
      const myPeerId = this.getPeerId ? this.getPeerId() : ''

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

      const nextIndex = wasIncremental ? { ...(lib.index || {}) } : {}
      for (const [rel, meta] of scanOut.entries()) {
        const prev = lib.index[rel]
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
            hash: prev && prev.sig === meta.sig ? prev.hash || '' : ''
          }
        }
      }

      for (const [rel, prev] of Object.entries(lib.index || {})) {
        if (prev && !prev.deleted && !scanOut.has(rel)) {
          if (wasIncremental && !isDirtyOrUnder(dirty, rel)) {
            continue
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

        await this._diffAndPush(lib)

        if (!lib._phasePending) {
          this._finishRound(lib, { pushed: lib._lastPushed || 0, skipped: lib._lastSkipped || 0 })
        }
      } else if (isPeerOnline && !lib.accepted) {
        lib.status = 'waiting_peer'
        this._sendToPeer(lib.peerId, {
          type: MESSAGES.SYNC_INVITE,
          libraryId: lib.id,
          name: lib.name,
          mode: lib.mode,
          fileCount: Object.keys(lib.index || {}).length,
          totalSize: Object.values(lib.index || {}).reduce((acc, x) => acc + (x?.size || 0), 0)
        })
        this._setPhase(lib, 'synced')
      } else {
        lib.status = 'waiting_peer'
        this._setPhase(lib, 'synced')
      }

      this.sendEvent(EVENTS.SYNC_UP_TO_DATE, { id: lib.id, status: lib.status })
    } catch (err) {
      console.warn(`[SyncEngine] syncLibrary failed for ${id}:`, err.message)
      lib.status = 'error'
      this.sendEvent(EVENTS.SYNC_ERROR, { id, error: err.message })
      this._setPhase(lib, 'synced')
    } finally {
      this._syncingSet.delete(id)
      if (lib.status === 'syncing') lib.status = 'idle'
    }
  }

  async _diffAndPush(lib) {
    if (!lib || lib.paused || !lib.accepted || lib.mode === 'receive_only') return
    if (lib._pushing) {
      lib._pushingRetry = true
      return
    }
    lib._pushing = true
    try {
      await this._doDiffAndPush(lib)
      if (lib._pushingRetry && !lib.paused && lib.accepted) {
        lib._pushingRetry = false
        await this._doDiffAndPush(lib)
      }
    } finally {
      lib._pushing = false
    }
  }

  async _doDiffAndPush(lib) {
    const useSent = lib.mode === 'push'
    const baseline = useSent ? lib.sentIndex || {} : lib.remoteIndex || {}

    // Pure 3-way reconciliation
    const { toPush, toDeleteRemote } = Reconciler.reconcile({
      localIndex: lib.index || {},
      baseline,
      remoteIndex: lib.remoteIndex || {},
      mode: lib.mode
    })

    const toPushSet = new Set(toPush)
    const have = new Set()
    const lost = new Set()
    const verifyList = []

    for (const rel of toPush) {
      const e = lib.index[rel] || {}
      verifyList.push({ rel, size: e.size || 0, mtimeMs: e.mtimeMs || 0, hash: e.hash || '' })
    }

    const sentRels = Object.keys(baseline).filter((rel) => lib.index[rel] && !lib.index[rel].deleted)
    let sentSlice = sentRels
    if (sentRels.length > MAX_VERIFY_FILES) {
      lib._verifyRound = (lib._verifyRound || 0) + 1
      const chunks = Math.ceil(sentRels.length / MAX_VERIFY_FILES)
      const idx = lib._verifyRound % chunks
      sentSlice = sentRels.slice(idx * MAX_VERIFY_FILES, (idx + 1) * MAX_VERIFY_FILES)
    }

    for (const rel of sentSlice) {
      const e = lib.index[rel]
      if (e && !e.hash) {
        const absPath = this.path.join(lib.localPath, ...rel.split('/'))
        e.hash = await hashFileFast(this.fsp, absPath)
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
      const inFlight =
        this.transferEngine && typeof this.transferEngine.inFlightSyncRels === 'function'
          ? this.transferEngine.inFlightSyncRels(lib.id)
          : new Set()
      for (const rel of sentSlice) {
        if (!verified.has(rel) && !inFlight.has(rel)) lost.add(rel)
      }
    }

    const pendingRels = Array.from(new Set(toPush.filter((rel) => !have.has(rel)).concat(Array.from(lost))))
    if (pendingRels.length > 0) {
      lib._phasePending = (lib._phasePending || 0) + pendingRels.length
      this._setPhase(lib, 'transferring', {
        total: (lib._phaseDone || 0) + lib._phasePending,
        done: lib._phaseDone || 0
      })
    }

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
      for (const rel of toDeleteRemote) {
        this._sendToPeer(lib.peerId, { type: MESSAGES.SYNC_DELETE, libraryId: lib.id, rel })
        baseline[rel] = { size: 0, mtimeMs: Date.now(), sig: '', authorKey: '', deleted: true }
      }
    }
  }

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

  async handleSyncVerify(peerId, msg) {
    if (!msg || !msg.libraryId || !Array.isArray(msg.files) || msg.files.length === 0) return
    const lib = this.libraries.get(msg.libraryId)
    if (!lib) {
      this._sendToPeer(peerId, { type: MESSAGES.SYNC_VERIFY_RESULT, libraryId: msg.libraryId, have: [] })
      return
    }

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
              if (!bee) return null
              const node = await bee.get(`delivered/${lib.id}/${rel}`).catch(() => null)
              const del = node && node.value
              if (del && Math.abs(Number(del.mtimeMs || 0) - Number(f.mtimeMs || 0)) < VERIFY_TOLERANCE_MS) {
                if (del.mtimeMs && Math.abs(Number(st.mtimeMs || 0) - Number(del.mtimeMs || 0)) > VERIFY_TOLERANCE_MS) {
                  const localHash = await hashFileFast(this.fsp, abs)
                  const remoteHash = f.hash || ''
                  if (localHash && remoteHash && localHash !== remoteHash) {
                    this.sendEvent(EVENTS.SYNC_CONFLICT, {
                      id: lib.id,
                      libraryId: lib.id,
                      rel,
                      message: `Concurrent edit detected on ${rel} — the newer version will be kept, the local copy moved to trash.`
                    })
                    return null
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

  async handleSyncVerifyResult(peerId, msg) {
    if (!msg || !msg.libraryId) return
    const lib = this.libraries.get(msg.libraryId)
    if (!lib || !lib._verifyResolver) return
    const have = new Set((msg.have || []).map((r) => safeRelPath(r)).filter(Boolean))
    lib._verifyResolver.resolve(have)
  }

  async isDeliveredVersion(libId, rel, mtimeMs) {
    return SnapshotStore.isDelivered(this.getBee, libId, rel, mtimeMs)
  }

  async handleSyncInviteAccept(peerId, msg) {
    if (!msg || !msg.libraryId) return
    const lib = this.libraries.get(msg.libraryId)
    if (!lib) return
    lib.accepted = true
    lib.status = 'idle'
    await this._persist(lib)
    this.sendEvent(EVENTS.SYNC_COMPLETED, { id: lib.id, name: lib.name, pushed: 0, deleted: 0, accepted: true })

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
    if (this.removedLibraries.has(msg.libraryId)) return
    const lib = this.libraries.get(msg.libraryId)
    if (!lib || lib.mode !== 'two-way') return

    const incoming = indexFromArray(msg.entries)
    const merged = { ...(lib.remoteIndex || {}), ...incoming }
    if (JSON.stringify(merged) !== JSON.stringify(lib.remoteIndex)) {
      lib.remoteIndex = merged
      await this._persist(lib)
    } else {
      lib.remoteIndex = merged
    }

    await this._applyRemoteDeletes(lib)
    if (lib.accepted && !lib.paused) {
      this._diffAndPush(lib).catch(() => {})
    }
  }

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

  async tick() {
    for (const lib of this.libraries.values()) {
      if (lib.paused) continue
      const peerObj = this._findPeer(lib.peerId)
      if (!peerObj || !peerObj.connection || peerObj.incompatible) continue

      if (!lib.accepted) {
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
        accepted: true,
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

  isSyncAllowed(offer) {
    if (!offer || !offer.syncLibraryId) return false
    const lib = this.libraries.get(offer.syncLibraryId)
    if (!lib) return false
    return !lib.paused && lib.accepted
  }

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
      lib._failRetries = 0
    }

    if (record.direction === 'receive') {
      if (record.status === 'completed' && rel) {
        const size = Number(record.fileSize) || 0
        const mtimeMs = Number(record.syncMtimeMs) || 0
        SnapshotStore.recordDelivered(this.getBee, lib.id, rel, size, mtimeMs).catch(() => {})
      }
      lib._activeReceives = Math.max(0, (lib._activeReceives || 0) - 1)
      if (lib._activeReceives === 0 && lib._phase && lib._phase.phase === 'transferring') {
        this._finishRound(lib, {})
      }
      return
    }

    if (lib._phasePending > 0) {
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

  handleTransferStarted(record) {
    if (!record || record.source !== 'sync') return
    const lib = this.libraries.get(record.syncLibraryId)
    if (!lib || record.direction !== 'receive') return
    lib._activeReceives = (lib._activeReceives || 0) + 1
    this._setPhase(lib, 'transferring', { total: 0, done: 0 })
  }
}

module.exports = { SyncEngine, safeRelPath, scanFolder }
