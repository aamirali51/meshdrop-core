'use strict'

// TransferEngine: the transfer state machine (send/receive/resume/cancel),
// composed from the focused modules in transfer/:
//   - transfer/queue.js      : persistent, priority-ordered scheduler
//   - transfer/scheduler.js  : parallel block fetch with adaptive window
//   - transfer/integrity.js  : SHA-256 block manifest + whole-file checksum
//   - transfer/constants.js  : block sizes, limits, statuses, schema version
// Plus the engine-level guarantees:
//   - Resume    : same core, stored (coreKey, byteOffset)
//   - Cancel    : marks `interrupted` (resumable), never `failed`
//   - Collision : per-transfer staging dir + atomic rename on verify
//
// fs/path are injected so the module stays platform-agnostic (node:fs on
// desktop, whatever fs adapter the host provides on mobile). Nothing here
// fabricates data: every status/progress/checksum reflects real bytes read or
// written. Events are emitted through the injected sendEvent (the engine's
// EventEmitter) using the names from ../protocol.js.

const b4a = require('b4a')
const Protomux = require('protomux')
const c = require('compact-encoding')
const { sha256 } = require('../crypto.js')
const { EVENTS, MESSAGES } = require('../protocol.js')

const {
  CHUNK_SIZE,
  DEFAULT_PRIORITY,
  MAX_TRANSFER_SIZE,
  STATUS,
  SCHEMA_VERSION,
  SCHEMA_KEY,
  TERMINAL,
  sleep
} = require('./transfer/constants.js')
const { getFileType, safeFilename, buildManifest, parseManifest } = require('./transfer/integrity.js')
const { TransferQueue } = require('./transfer/queue.js')
const { ChunkScheduler } = require('./transfer/scheduler.js')

// ─── Sync streaming channel ─────────────────────────────────────────────────
// Sync transfers stream blocks directly from the source file over a dedicated
// protomux channel — the payload never touches the exchange store, so a large
// camera/media library is never cloned into the app sandbox (zero duplication).
// Flow control: the receiver ACKs every SYNC_ACK_EVERY blocks; the sender keeps
// at most SYNC_STREAM_WINDOW blocks in flight (bounded memory on WAN links).
const SYNC_STREAM_PROTOCOL = 'meshdrop-sync-v1'
const SYNC_STREAM_WINDOW = 32
const SYNC_ACK_EVERY = 8
const SYNC_HANDSHAKE_TIMEOUT = 60 * 1000
// No-ACK / no-block deadlines. A half-dead connection (dead relay node, phone
// that lost its network without closing TCP cleanly) never fires 'close', so
// without these the flow-control waits would hang the transfer forever at the
// exact byte offset it stalled on. The errors carry "interrupted" so the
// transfer parks as resumable instead of failing hard.
const SYNC_FLOW_TIMEOUT = 60 * 1000 // sender: no ACK progress in this window → interrupt
const SYNC_RECEIVE_IDLE_TIMEOUT = 90 * 1000 // receiver: no blocks after the manifest → interrupt

// Staging-partial retention: a .part file older than this (interrupted transfer
// never resumed) is swept at engine init. 7 days is generous for an active
// transfer backlog while still bounding disk waste.
const STAGING_RETENTION_MS = 7 * 24 * 60 * 60 * 1000

// ─── ChunkScheduler ────────────────────────────────────────────────────────

// Parallel, adaptive block fetcher (see transfer/scheduler.js).
// ─── TransferEngine ────────────────────────────────────────────────────────

class TransferEngine {
  constructor({
    getBee,
    exchangeStore,
    sendEvent,
    getPeers,
    getDeviceIdentity,
    getDownloadDirectory,
    getTransferMethod,
    fsp,
    path,
    getSyncMode,
    getStagingRoot
  }) {
    this.getBee = getBee
    this.exchangeStore = exchangeStore
    this.sendEvent = sendEvent // (eventName, data) => void
    this.getPeers = getPeers // () => Map<peerId, peerObj>
    this.getDeviceIdentity = getDeviceIdentity
    this.getDownloadDirectory = getDownloadDirectory
    this.getTransferMethod = getTransferMethod
    this.fsp = fsp
    this.path = path
    this.getSyncMode = getSyncMode || (() => null) // (syncLibraryId) => 'push'|'two-way'|'receive_only'|null
    this.getStagingRoot = getStagingRoot || (() => null) // () => baseDir for .p2p-staging

    this.queue = new TransferQueue()
    this.runs = new Map() // transferId -> { direction, fd, core, flags, scheduler }
    this.pendingOffers = new Map() // transferId -> { offer, autoAccept }
    // transferId -> { chan, reg } — sync stream channels are created
    // synchronously in receiveOffer so protomux can pair them with the remote's
    // open frame (pairing must happen within a microtask or the channel is
    // rejected); the real handlers are attached when the transfer starts.
    this._syncChannels = new Map()
    // Retention: the transfers bee is a log that would grow forever; prune
    // terminal records periodically (see _pruneTerminalTransfers).
    this._terminalEvents = 0
  }

  // Keep the persisted transfer log bounded: after every PRUNE_EVERY terminal
  // transitions (and at init), drop the oldest terminal records beyond the cap.
  async _pruneTerminalTransfers() {
    try {
      const bee = await this.getBee('transfers')
      const all = []
      for await (const node of bee.createReadStream()) {
        if (node.key === SCHEMA_KEY) continue
        all.push({ key: node.key, value: node.value })
      }
      const terminal = all
        .filter((e) => e.value && TERMINAL.has(e.value.status))
        .sort((a, b) => (b.value.completedAt || b.value.interruptedAt || b.value.createdAt || '') > (a.value.completedAt || a.value.interruptedAt || a.value.createdAt || '') ? 1 : -1)
      const keep = 200
      if (terminal.length <= keep) return
      for (const e of terminal.slice(keep)) {
        await bee.del(e.key).catch(() => {})
      }
      console.log(`[TransferEngine] pruned ${terminal.length - keep} terminal transfer record(s)`)
    } catch (err) {
      console.warn('[TransferEngine] prune failed:', err.message)
    }
  }

  getFileType(filename) {
    return getFileType(filename)
  }

  // Rels with a sync SEND that is queued or actively streaming for a library.
  // Their delivery is already en route — the SyncEngine verify round must not
  // treat them as "lost" just because they are not on the receiver's disk yet
  // (an in-flight transfer is not a missing file; re-pushing it creates a
  // duplicate-transfer storm under rapid rounds).
  inFlightSyncRels(libId) {
    const out = new Set()
    const check = (r) => {
      if (r && r.direction === 'send' && r.source === 'sync' && r.syncLibraryId === libId && r.syncRelPath) {
        out.add(r.syncRelPath)
      }
    }
    if (this.queue) {
      for (const tier of [this.queue.queued.interactive, this.queue.queued.bulk, this.queue.queued.background]) {
        for (const t of tier) check(t)
      }
    }
    for (const [, info] of this.runs) check(info.record)
    return out
  }

  // Throttled retention sweep: called when a transfer reaches a terminal state.
  _noteTerminal() {
    if (++this._terminalEvents < 50) return
    this._terminalEvents = 0
    this._pruneTerminalTransfers().catch(() => {})
  }

  // Sweep stale .p2p-staging partials at engine init. Interrupted transfers
  // that were never resumed leave a .part on disk forever; this bounds the
  // leak without touching partials that are still resumable (fresh ones).
  async _sweepStagingPartials() {
    try {
      const root = this.getStagingRoot ? await this.getStagingRoot() : null
      if (!root) return
      const { fsp, path } = this
      const stagingRoot = path.join(root, '.p2p-staging')
      const entries = await fsp.readdir(stagingRoot, { withFileTypes: true }).catch(() => [])
      const cutoff = Date.now() - STAGING_RETENTION_MS
      let removed = 0
      for (const ent of entries) {
        if (!ent || ent.isFile()) continue
        const dirPath = path.join(stagingRoot, ent.name)
        try {
          const st = await fsp.stat(dirPath)
          if (st.mtimeMs < cutoff) {
            await fsp.rm(dirPath, { recursive: true, force: true })
            removed++
          }
        } catch {}
      }
      if (removed > 0) {
        console.log(`[TransferEngine] swept ${removed} stale staging partial(s)`)
      }
    } catch (err) {
      console.warn('[TransferEngine] staging sweep failed:', err.message)
    }
  }

  // Re-queue persisted transfers that were interrupted by a restart.
  async init() {
    try {
      const bee = await this.getBee('transfers')
      // Schema migration: v1 records (pre-TransferEngine) carried no manifest
      // or checksum and cannot be resumed under the integrity scheme, so we
      // reset the transfer log on upgrade rather than trusting stale offsets.
      const meta = await bee.get(SCHEMA_KEY)
      if (!meta || meta.value?.schema !== SCHEMA_VERSION) {
        const toDelete = []
        for await (const node of bee.createReadStream()) {
          if (node.key !== SCHEMA_KEY) toDelete.push(node.key)
        }
        for (const k of toDelete) await bee.del(k)
        await bee.put(SCHEMA_KEY, { schema: SCHEMA_VERSION })
        console.log(`[TransferEngine] Transfer store migrated to ${SCHEMA_VERSION} (reset)`)
      }

      const queued = []
      for await (const node of bee.createReadStream()) {
        const t = node.value
        if (t && t.id && t.status === STATUS.QUEUED) queued.push(t)
      }
      for (const t of queued) {
        this.queue.enqueue(t)
        this._emit(EVENTS.TRANSFER_QUEUED, t)
      }
      console.log(`[TransferEngine] loaded ${queued.length} queued transfer(s)`)
      this._pruneTerminalTransfers().catch(() => {})
      // Clean up abandoned .part files (interrupted transfers never resumed).
      // Non-blocking: the engine must not wait on a disk sweep to come up.
      this._sweepStagingPartials().catch(() => {})
    } catch (err) {
      console.warn('[TransferEngine] init failed:', err.message)
    }
  }

  async _persist(id, patch) {
    try {
      const bee = await this.getBee('transfers')
      const entry = await bee.get(id)
      const next = { ...(entry?.value || {}), ...patch, id }
      await bee.put(id, next)
      return next
    } catch (err) {
      console.warn('[TransferEngine] persist failed:', err.message)
      return { id, ...patch }
    }
  }

  _emit(event, data) {
    try {
      this.sendEvent(event, data)
    } catch (err) {
      console.warn('[TransferEngine] event emit failed:', err.message)
    }
  }

  _resolveMethod(transferMethod, senderIdentity, isClaim) {
    if (transferMethod) return transferMethod
    return this.getTransferMethod(senderIdentity?.ipAddress || '', isClaim)
  }

  // ── Send ──────────────────────────────────────────────────────────────────

  async startSend(params) {
    const peerId = params.peerId || ''
    const peerObj = peerId ? this.getPeers().get(peerId) : null
    const transferMethod = this._resolveMethod(
      params.transferMethod,
      peerObj?.device || { ipAddress: params.ipAddress },
      false
    )
    const transfer = {
      id:
        params.transferId ||
        `transfer-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
      schema: SCHEMA_VERSION,
      filename: params.filename || 'Unknown',
      fileSize: params.fileSize || 0,
      fileType: params.fileType || getFileType(params.filename || ''),
      direction: 'send',
      status: STATUS.QUEUED,
      priority: params.priority || DEFAULT_PRIORITY,
      progress: 0,
      speed: 0,
      peakSpeed: 0,
      eta: 0,
      duration: 0,
      transferMethod,
      isEncrypted: true,
      peerId,
      peerName: params.peerName || peerObj?.device?.name || 'Unknown',
      coreKey: params.coreKey || '',
      filePath: params.filePath || '',
      destPath: '',
      byteOffset: 0,
      manifestHash: '',
      checksum: '',
      blockSize: CHUNK_SIZE,
      blockCount: 0,
      summary: {},
      createdAt: new Date().toISOString()
    }

    // Sync/stream-tagged transfers keep the source on the record so the offer
    // carries it and the UI can distinguish them from one-shot core staging.
    if (params.source) transfer.source = params.source
    transfer.isSync = !!(params.isSync || params.source === 'sync')
    if (params.source === 'stream') transfer.isStream = true
    if (params.syncLibraryId) transfer.syncLibraryId = params.syncLibraryId
    if (params.syncLibraryName) transfer.syncLibraryName = params.syncLibraryName
    if (params.syncRelPath) transfer.syncRelPath = params.syncRelPath
    if (params.syncMtimeMs) transfer.syncMtimeMs = params.syncMtimeMs
    if (params.syncAuthorKey) transfer.syncAuthorKey = params.syncAuthorKey

    if (!transfer.filePath || typeof transfer.fileSize !== 'number' || transfer.fileSize < 0) {
      throw new Error('Transfer requires a valid filePath and fileSize')
    }

    await this._persist(transfer.id, transfer)
    this._enqueue(transfer)
    return transfer
  }

  // Stage a file into a drop core (no offer). Used by FILES_CREATE_CODE: the
  // code topic is joined separately and the resulting coreKey/manifestHash are
  // stored on the pending share, then served to claimers. A multi-file share
  // stages one core per file (coreName differentiates them).
  async stageDrop({ transferId, coreName = null, filePath, filename, fileSize, fileType }) {
    const core = this.exchangeStore.get({ name: coreName || `file-drop-${transferId}` })
    await core.ready()

    const { manifest, manifestHash } = await buildManifest({
      filePath,
      fsp: this.fsp,
      filename,
      fileSize,
      fileType: fileType || getFileType(filename),
      transferId
    })

    if (core.length === 0) {
      await core.append(Buffer.from(JSON.stringify(manifest)))
    }

    // Append any missing data blocks (resume-safe: same core, byteOffset from
    // actual core length, never from a stale counter).
    let byteOffset = 0
    let bytesWritten = 0
    const fd = await this.fsp.open(filePath, 'r')
    try {
      const appended = Math.max(0, core.length - 1) // manifest is block 0
      byteOffset = appended * CHUNK_SIZE
      bytesWritten = byteOffset
      const buf = Buffer.alloc(CHUNK_SIZE)
      const batchSize = 128 // Batch append 8MB at a time for high disk throughput
      let batch = []
      while (bytesWritten < fileSize) {
        const readRes = await fd.read(buf, 0, CHUNK_SIZE, bytesWritten)
        const bytesRead = typeof readRes === 'number' ? readRes : (readRes?.bytesRead || 0)
        if (bytesRead === 0) break
        const block = Buffer.from(buf.subarray(0, bytesRead))
        batch.push(block)
        bytesWritten += bytesRead
        if (batch.length >= batchSize) {
          await core.append(batch)
          batch = []
        }
      }
      if (batch.length > 0) {
        await core.append(batch)
        batch = []
      }
    } finally {
      await fd.close()
    }

    await core.close().catch(() => {})
    return {
      coreKey: core.key.toString('hex'),
      manifestHash,
      checksum: manifest.checksum,
      blockSize: manifest.blockSize,
      blockCount: manifest.blockCount
    }
  }

  async _runSend(transfer) {
    const { fsp } = this
    const id = transfer.id
    const filePath = transfer.filePath
    const fileSize = transfer.fileSize

    // Sync and stream transfers read the source file directly over a dedicated
    // protomux channel — no exchange-store clone on the sender, no core
    // replication on the receiver.
    if (transfer.source === 'sync' || transfer.isSync || transfer.isStream) {
      return this._runSendSync(transfer)
    }

    const info = {
      direction: 'send',
      fd: null,
      core: null,
      flags: { paused: false, cancelled: false },
      record: transfer
    }
    this.runs.set(id, info)

    let bytesWritten = 0
    try {
      const keyBuf = transfer.coreKey ? Buffer.from(transfer.coreKey, 'hex') : null
      const core = keyBuf
        ? this.exchangeStore.get(keyBuf)
        : this.exchangeStore.get({ name: `file-transfer-${id}` })
      await core.ready()
      info.core = core

      // Build the manifest on first run; read it back on resume so we keep the
      // SAME core and never fabricate a fresh one.
      let manifest = null
      let manifestHash = transfer.manifestHash || ''
      if (core.length === 0) {
        const built = await buildManifest({
          filePath,
          fsp,
          filename: transfer.filename,
          fileSize,
          fileType: transfer.fileType,
          transferId: id,
          shouldCancel: () => info.flags.cancelled
        })
        manifest = built.manifest
        manifestHash = built.manifestHash
        await core.append(Buffer.from(JSON.stringify(manifest)))
        transfer.coreKey = core.key.toString('hex')
        transfer.manifestHash = manifestHash
        transfer.checksum = manifest.checksum
        transfer.blockSize = manifest.blockSize
        transfer.blockCount = manifest.blockCount
        await this._persist(id, {
          coreKey: transfer.coreKey,
          manifestHash,
          checksum: manifest.checksum,
          blockSize: manifest.blockSize,
          blockCount: manifest.blockCount,
          status: STATUS.ACTIVE,
          startedAt: transfer.startedAt || new Date().toISOString()
        })
      } else {
        const manifestTimeout = Math.max(60000, Math.ceil((fileSize || 0) / (5 * 1024 * 1024)) * 1000)
        const raw = await core.get(0, { wait: true, timeout: manifestTimeout })
        manifest = parseManifest(raw)
        if (!manifest) {
          throw new Error('Transfer core is missing a valid manifest (block 0)')
        }
        if (manifestHash && manifestHash !== this._hashManifest(manifest)) {
          throw new Error('Stored manifestHash does not match the core manifest')
        }
        transfer.manifestHash = manifestHash || this._hashManifest(manifest)
        transfer.checksum = manifest.checksum
        transfer.blockSize = manifest.blockSize
        transfer.blockCount = manifest.blockCount
        await this._persist(id, {
          status: STATUS.ACTIVE,
          startedAt: transfer.startedAt || new Date().toISOString()
        })
      }

      this.sendOffer(id, transfer)

      bytesWritten = Math.max(0, core.length - 1) * CHUNK_SIZE
      const startTime = Date.now()
      let peakSpeed = 0
      let lastEmitTime = startTime
      let lastEmitBytes = bytesWritten
      let lastEmittedProgress = fileSize === 0 ? 100 : Math.min(100, Math.round((bytesWritten / fileSize) * 100))

      const fd = await fsp.open(filePath, 'r')
      info.fd = fd
      const buf = Buffer.alloc(CHUNK_SIZE)
      while (bytesWritten < fileSize) {
        if (info.flags.cancelled) throw new Error('interrupted')
        while (info.flags.paused && !info.flags.cancelled) await sleep(50)

        const readRes = await fd.read(buf, 0, CHUNK_SIZE, bytesWritten)
        const bytesRead = typeof readRes === 'number' ? readRes : (readRes?.bytesRead || 0)
        if (bytesRead === 0) break
        const block = Buffer.from(buf.subarray(0, bytesRead))
        await core.append(block)
        bytesWritten += bytesRead

        const now = Date.now()
        const elapsed = (now - lastEmitTime) / 1000
        let speed = 0
        if (elapsed >= 1) {
          speed = Math.round((bytesWritten - lastEmitBytes) / elapsed)
          lastEmitTime = now
          lastEmitBytes = bytesWritten
        } else if (now - startTime > 0) {
          speed = Math.round(bytesWritten / ((now - startTime) / 1000))
        }
        if (speed > peakSpeed) peakSpeed = speed

        const progress = fileSize === 0 ? 100 : Math.min(100, Math.round((bytesWritten / fileSize) * 100))
        const remaining = Math.max(0, fileSize - bytesWritten)
        const eta = speed > 0 ? Math.round(remaining / speed) : 0
        if (progress === 100 || progress - lastEmittedProgress >= 2 || now - lastEmitTime >= 1000) {
          lastEmittedProgress = progress
          await this._persist(id, { progress, speed, peakSpeed, eta, byteOffset: bytesWritten })
          this._emit(EVENTS.TRANSFER_PROGRESS, {
            id,
            filename: transfer.filename,
            direction: transfer.direction || 'send',
            progress,
            speed,
            peakSpeed,
            eta,
            source: transfer.source,
            isSync: !!(transfer.isSync || transfer.source === 'sync'),
            syncLibraryId: transfer.syncLibraryId
          })
        }
      }

      await fd.close()
      info.fd = null

      const totalElapsed = (Date.now() - startTime) / 1000
      const avgSpeed = totalElapsed > 0 ? Math.round(fileSize / totalElapsed) : 0
      const completed = await this._persist(id, {
        status: STATUS.COMPLETED,
        progress: 100,
        speed: avgSpeed,
        peakSpeed,
        eta: 0,
        duration: Math.max(1, Math.round(totalElapsed)),
        byteOffset: fileSize,
        completedAt: new Date().toISOString(),
        summary: {
          checksum: transfer.checksum,
          manifestHash: transfer.manifestHash,
          blocksVerified: manifest.blockCount,
          bytesVerified: fileSize
        }
      })
      this._noteTerminal()
      this._emit(EVENTS.TRANSFER_COMPLETED, completed)
      this._recordHistory(completed, 'Sent')
    } catch (err) {
      if (info.fd) await info.fd.close().catch(() => {})
      const isInterrupt = info.flags.cancelled || /interrupted/i.test(err.message)
      await this._failOrInterrupt(transfer, err, isInterrupt, { info, bytesWritten })
    } finally {
      this.runs.delete(id)
      if (info.core) await info.core.close().catch(() => {})
      this.queue.release(transfer)
      this._kickQueue('send')
    }
  }

  // Tolerant peer lookup: the transfer record may carry the noise key, a device
  // id, publicKey or identityKey — try each candidate exactly, then scan the
  // registry by device fields (the sync UI binds libraries to device ids).
  _findTransferPeer(...keys) {
    const peersMap = this.getPeers()
    for (const key of keys) {
      if (!key || typeof key !== 'string') continue
      const direct = peersMap.get(key)
      if (direct) return direct
      for (const [, p] of peersMap.entries()) {
        if (
          p.device?.id === key ||
          p.device?.publicKey === key ||
          p.device?.identityKey === key
        ) {
          return p
        }
      }
    }
    return null
  }

  sendOffer(id, transfer) {
    const offer = {
      type: MESSAGES.TRANSFER_OFFER,
      transferId: id,
      filename: transfer.filename,
      fileSize: transfer.fileSize,
      fileType: transfer.fileType,
      coreKey: transfer.coreKey,
      manifestHash: transfer.manifestHash,
      checksum: transfer.checksum,
      senderIdentity: this.getDeviceIdentity() || { id: '', name: 'Local Device' },
      transferMethod: transfer.transferMethod
    }
    // Sync offers carry the routing metadata the receiver needs.
    if (transfer.source === 'sync') {
      offer.source = 'sync'
      offer.syncLibraryId = transfer.syncLibraryId
      offer.syncLibraryName = transfer.syncLibraryName
      offer.syncRelPath = transfer.syncRelPath
      offer.syncMtimeMs = transfer.syncMtimeMs
      offer.syncAuthorKey = transfer.syncAuthorKey
    }
    // Stream offers (1:1 sends): the payload arrives over the stream channel,
    // so the receiver must know to expect blocks instead of core blocks.
    if (transfer.source === 'stream') {
      offer.source = 'stream'
    }

    let sent = 0
    const sendTo = (peerId, peerObj) => {
      if (peerObj?.signaling) {
        peerObj.signaling.send(offer)
        sent++
      }
    }

    if (transfer.peerId) {
      const peerObj = this._findTransferPeer(transfer.peerId)
      if (peerObj) sendTo(transfer.peerId, peerObj)
    }
    if (sent === 0) {
      for (const [peerId, peerObj] of this.getPeers().entries()) sendTo(peerId, peerObj)
    }
    if (sent === 0) {
      console.log(`[MeshEngine] No connected peers to deliver TRANSFER_OFFER for ${id}`)
    }
  }

  // ── Receive ───────────────────────────────────────────────────────────────

  async receiveOffer(offer, { autoAccept = false, isClaim = false, keepCoreOpen = false } = {}) {
    const { transferId, filename, fileSize, fileType, coreKey, senderIdentity, transferMethod } =
      offer

    // Sync/stream offers carry no coreKey — the blocks arrive over the stream
    // channel, not from a replicated core.
    const isSyncOffer = offer.source === 'sync'
    const isStreamOffer = offer.source === 'stream'
    if (
      !transferId ||
      (typeof coreKey !== 'string' || coreKey.length !== 64) && !isSyncOffer && !isStreamOffer
    ) {
      console.warn('[TransferEngine] Rejecting malformed TRANSFER_OFFER (bad coreKey/transferId)')
      return null
    }
    if (typeof fileSize !== 'number' || fileSize < 0 || fileSize > MAX_TRANSFER_SIZE) {
      console.warn('[TransferEngine] Rejecting TRANSFER_OFFER with invalid fileSize')
      return null
    }
    if (typeof filename !== 'string' || filename.length === 0 || filename.length > 500) {
      console.warn('[TransferEngine] Rejecting TRANSFER_OFFER with invalid filename')
      return null
    }

    // Pre-create the sync stream channel SYNCHRONOUSLY (before any await):
    // protomux rejects an incoming channel open if a local channel with the
    // same protocol+id is not created within a microtask. The real handlers are
    // attached when the transfer starts (see _runReceiveSync). If the offer is
    // rejected below, the channel is closed again.
    let syncChan = null
    if ((isSyncOffer || isStreamOffer) && offer.senderPeerId) {
      const peerObj = this.getPeers().get(offer.senderPeerId)
      if (peerObj && peerObj.connection) {
        const reg = { control: null, manifest: null, block: null, ack: null, close: null, error: null }
        const chan = this._openSyncStreamChannel(peerObj, transferId, {
          onControl: (msg) => { if (reg.control) reg.control(msg) },
          onManifest: (buf) => { if (reg.manifest) reg.manifest(buf) },
          onBlock: (buf) => { if (reg.block) reg.block(buf) },
          onAck: (n) => { if (reg.ack) reg.ack(n) },
          onClose: () => { if (reg.close) reg.close() },
          onError: (err) => { if (reg.error) reg.error(err) }
        })
        if (chan) {
          syncChan = chan
          this._syncChannels.set(transferId, { chan, reg })
        }
      }
    }

    const rejectSyncChannel = () => {
      if (this._syncChannels.has(transferId)) {
        const entry = this._syncChannels.get(transferId)
        this._syncChannels.delete(transferId)
        try {
          if (entry.chan && typeof entry.chan.channel.close === 'function') entry.chan.channel.close()
        } catch {}
      }
    }

    const bee = await this.getBee('transfers')
    const existing = await bee.get(transferId)
    if (existing) {
      console.log(
        `[TransferEngine] Transfer ${transferId} already exists, skipping duplicate offer`
      )
      rejectSyncChannel()
      return null
    }

    const { os } = require('../compat.js')
    const downloadsDir = (this.getDownloadDirectory ? await this.getDownloadDirectory() : null) || (os.tmpdir ? os.tmpdir() : '/tmp')

    // Pause/delete authority: reject sync offers for libraries that are paused
    // or removed. Without this, the peer's mirror keeps pushing files into a
    // paused folder (or the default folder after the link was deleted).
    if (offer.source === 'sync' && this.syncAllowed && !this.syncAllowed(offer)) {
      console.log(
        `[TransferEngine] Rejecting sync offer for paused/removed library ${offer.syncLibraryId || '?'} (${filename})`
      )
      rejectSyncChannel()
      return null
    }
    
    // Sync-tagged offers route directly into the library folder
    const syncDest = this.resolveSyncDest ? this.resolveSyncDest(offer) : null
    const isSync = !!(offer.source === 'sync' && syncDest)
    const shouldAutoAccept = autoAccept || isSync
    const baseDir = (isSync ? syncDest.baseDir : downloadsDir) || os.tmpdir()
    const safeName = (isSync ? this.path.basename(syncDest.path) : safeFilename(filename)) || 'file'
    const destPath = isSync ? syncDest.path : this.path.join(downloadsDir, safeName)

    const record = {
      id: transferId,
      schema: SCHEMA_VERSION,
      filename,
      fileSize,
      fileType: fileType || getFileType(filename),
      direction: 'receive',
      status: shouldAutoAccept ? STATUS.QUEUED : STATUS.PENDING_APPROVAL,
      priority: offer.priority || DEFAULT_PRIORITY,
      progress: 0,
      speed: 0,
      peakSpeed: 0,
      eta: 0,
      duration: 0,
      transferMethod: this._resolveMethod(transferMethod, senderIdentity, isClaim),
      isEncrypted: true,
      isClaim: !!isClaim,
      // Claims keep their verified core open after completion so the peer
      // can seed the share (swarm distribution).
      keepCoreOpen: !!keepCoreOpen,
      isSync,
      source: isSync ? 'sync' : isStreamOffer ? 'stream' : undefined,
      syncLibraryId: offer.syncLibraryId || '',
      syncRelPath: offer.syncRelPath || '',
      // The sender's recorded mtime for this file. Without it the receiver's
      // re-sync skip can never match (existing identical files would be
      // re-uploaded every round) and received files keep their write-time
      // mtime instead of the sender's original one.
      syncMtimeMs: offer.syncMtimeMs || 0,
      peerId: senderIdentity?.id || '',
      peerName: senderIdentity?.name || 'Remote Peer',
      shareId: offer.shareId || '',
      // The signaling router injects the noise peer key as senderPeerId — the
      // stream channel needs it to open on the right connection.
      peerKey: offer.senderPeerId || offer.peerKey || '',
      coreKey,
      baseDir,
      destPath,
      stagingPath: this.path.join(baseDir, '.p2p-staging', transferId, safeName + '.part'),
      byteOffset: 0,
      manifestHash: offer.manifestHash || '',
      checksum: offer.checksum || '',
      blockSize: CHUNK_SIZE,
      blockCount: 0,
      summary: {},
      createdAt: new Date().toISOString()
    }
    await this._persist(transferId, record)

    if (!isSync) {
      this._emit(EVENTS.TRANSFER_OFFER, {
        transferId,
        filename,
        fileSize,
        fileType: fileType || getFileType(filename),
        senderIdentity: senderIdentity || { name: 'Remote Peer', id: '' },
        status: record.status
      })
    }

    if (shouldAutoAccept) {
      this.pendingOffers.set(transferId, { offer, autoAccept: true })
      const validated = await this._validateReceive(record, offer)
      if (!validated) return null
      this._enqueue(record)
    }
    return record
  }

  async cancelSyncTransfers(syncLibraryId) {
    if (!syncLibraryId) return
    // Fast path: RUNNING transfers stop immediately — their loops poll the
    // cancel flag every ~25ms (and during hashing every 4MB).
    this._cancelledQueue = this._cancelledQueue || new Set()
    for (const [id, run] of this.runs.entries()) {
      const r = run && (run.record || run.transfer)
      if (r && (r.source === 'sync' || r.isSync) && r.syncLibraryId === syncLibraryId) {
        run.flags.cancelled = true
        if (run.scheduler) run.scheduler.cancel()
        this._cancelledQueue.add(id)
      }
    }
    // Sweep the log: queued transfers are marked interrupted (they never start —
    // _startTransfer checks both the guard set and the record status) and
    // WAITING_PEER ones become cancelled (retryWaiting skips them).
    try {
      const bee = await this.getBee('transfers')
      const toCancel = []
      for await (const node of bee.createReadStream()) {
        const t = node.value
        if (t && (t.source === 'sync' || t.isSync) && t.syncLibraryId === syncLibraryId) {
          if (!TERMINAL.has(t.status)) {
            toCancel.push(node)
            this._cancelledQueue.add(node.key)
          }
        }
      }
      for (const node of toCancel) {
        const isWaiting = node.value.status === STATUS.WAITING_PEER
        await this._persist(node.key, {
          status: isWaiting ? STATUS.CANCELLED : STATUS.INTERRUPTED,
          ...(isWaiting ? { cancelledAt: new Date().toISOString() } : { interruptedAt: new Date().toISOString() })
        })
        this._emit(EVENTS.TRANSFER_CANCELLED, { id: node.key, ...node.value })
      }
      // Records now carry their terminal status; the in-memory guard is done.
      this._cancelledQueue.clear()
      return toCancel.length
    } catch (err) {
      console.warn('[TransferEngine] cancelSyncTransfers failed:', err.message)
      return 0
    }
  }

  async _validateReceive(record, offer = null) {
    const { fsp, path } = this
    if (offer) {
      if (
        typeof offer.fileSize !== 'number' ||
        offer.fileSize <= 0 ||
        offer.fileSize > MAX_TRANSFER_SIZE
      ) {
        await this._persist(record.id, {
          status: STATUS.FAILED,
          error: 'Invalid or oversized file'
        })
        return false
      }
    }

    // Path confinement: final destination must stay inside the downloads dir
    // (or the sync folder's root for sync-tagged transfers).
    const downloadsDir = await this.getDownloadDirectory()
    const resolvedBase = path.resolve(record.baseDir || downloadsDir)
    const resolvedDest = path.resolve(record.destPath || '')
    if (!resolvedDest.startsWith(resolvedBase + path.sep)) {
      await this._persist(record.id, {
        status: STATUS.FAILED,
        error: 'Destination path outside downloads directory'
      })
      return false
    }

    // Disk space pre-check.
    try {
      if (typeof fsp.statfs === 'function') {
        const stats = await fsp.statfs(resolvedBase)
        const freeBytes = Number(stats.bavail || stats.bfree || 0) * Number(stats.bsize || 4096)
        if (freeBytes > 0 && freeBytes < record.fileSize) {
          await this._persist(record.id, {
            status: STATUS.FAILED,
            error: `Insufficient disk space: ${Math.round(freeBytes / (1024 * 1024))}MB free`
          })
          return false
        }
      }
    } catch (err) {
      console.warn('[TransferEngine] Disk space pre-check skipped:', err.message)
    }
    return true
  }

  async _runReceive(transfer) {
    const { fsp, path } = this
    const id = transfer.id

    // Sync and stream transfers receive over the stream channel — no replicated core.
    if (transfer.source === 'sync' || transfer.isSync || transfer.source === 'stream') {
      return this._runReceiveSync(transfer)
    }

    const keyBuf = Buffer.from(transfer.coreKey, 'hex')
    const core = this.exchangeStore.get(keyBuf)
    await core.ready()

    const info = {
      direction: 'receive',
      fd: null,
      core,
      flags: { paused: false, cancelled: false },
      scheduler: null,
      download: null,
      record: transfer
    }
    this.runs.set(id, info)

    let bytesWritten = 0
    let verifiedBytes = 0
    try {
      // Integrity gate: the manifest (block 0) must exist and match the offer.
      const manifestTimeout = Math.max(60000, Math.ceil((transfer.fileSize || 0) / (5 * 1024 * 1024)) * 1000)
      const raw = await core.get(0, { wait: true, timeout: manifestTimeout })
      const manifest = parseManifest(raw)
      if (!manifest) throw new Error('Remote core has no valid manifest (block 0)')
      const actualHash = this._hashManifest(manifest)
      if (transfer.manifestHash && actualHash !== transfer.manifestHash) {
        throw new Error('Manifest hash mismatch: sender integrity check failed')
      }

      const blockSize = manifest.blockSize
      const blockCount = manifest.blocks.length
      const stagingPath = transfer.stagingPath
      const destPath = transfer.destPath
      const firstDataBlock = 1
      const lastDataBlock = blockCount // core block indices 1..blockCount
      // Progressive-playback watermark (bytes that must be verified before a
      // player can mount the stream). Set by the sender at stage time.
      const playableAfter = Math.min(
        manifest.fileSize,
        typeof manifest.playableAfter === 'number' ? manifest.playableAfter : manifest.fileSize
      )

      await fsp.mkdir(path.dirname(stagingPath), { recursive: true })
      // Resume: a partial .part file continues from a whole-block boundary.
      try {
        const stat = await fsp.stat(stagingPath)
        bytesWritten = Math.floor(stat.size / blockSize) * blockSize
      } catch {}
      bytesWritten = Math.max(0, Math.min(bytesWritten, manifest.fileSize))
      transfer.byteOffset = bytesWritten

      const flags = bytesWritten > 0 ? 'r+' : 'w'
      info.fd = await fsp.open(stagingPath, flags)

      // Resume from a whole-block boundary. The scheduler indexes `blocks` as
      // `coreIndex - first`, so the array must be sliced to start at the first
      // remaining data block (manifest.blocks[n] is the hash of core block 1+n).
      const resumeBlockIndex = Math.floor(bytesWritten / blockSize)
      const startTime = Date.now()
      let peakSpeed = 0
      let lastEmitTime = startTime
      let lastEmitBytes = bytesWritten
      let lastEmittedProgress = Math.round((bytesWritten / manifest.fileSize) * 100)
      verifiedBytes = bytesWritten

      const scheduler = new ChunkScheduler({
        core,
        firstDataBlock: firstDataBlock + resumeBlockIndex,
        lastDataBlock,
        blocks: manifest.blocks.slice(resumeBlockIndex),
        blockSize,
        isStreaming: !!transfer.isStreaming,
        onBlock: async (coreIndex, block) => {
          const fileOffset = (coreIndex - firstDataBlock) * blockSize
          await info.fd.write(block, 0, block.length, fileOffset)
          verifiedBytes += block.length

          // Progress: real bytes verified, no fabricated speeds.
          const now = Date.now()
          const elapsed = (now - lastEmitTime) / 1000
          let speed = 0
          if (elapsed >= 0.5) {
            speed = Math.round((verifiedBytes - lastEmitBytes) / elapsed)
            lastEmitTime = now
            lastEmitBytes = verifiedBytes
          }
          if (speed > peakSpeed) peakSpeed = speed
          const progress = manifest.fileSize === 0 ? 100 : Math.min(100, Math.round((verifiedBytes / manifest.fileSize) * 100))
          const remaining = Math.max(0, manifest.fileSize - verifiedBytes)
          const eta = speed > 0 ? Math.round(remaining / speed) : 0
          const playable = verifiedBytes >= playableAfter || progress === 100
          if (playable && transfer.playable !== true) {
            transfer.playable = true
            await this._persist(id, { playable: true })
            this._emit(EVENTS.TRANSFER_PROGRESS, {
              id,
              filename: transfer.filename,
              direction: transfer.direction || 'receive',
              progress,
              speed,
              peakSpeed,
              eta,
              source: transfer.source,
              playable: true,
              isSync: !!(transfer.isSync || transfer.source === 'sync'),
              syncLibraryId: transfer.syncLibraryId
            })
          }
          if (
            progress === 100 ||
            progress - lastEmittedProgress >= 2 ||
            now - lastEmitTime >= 1000
          ) {
            lastEmittedProgress = progress
            await this._persist(id, { progress, speed, peakSpeed, eta, byteOffset: verifiedBytes })
            this._emit(EVENTS.TRANSFER_PROGRESS, {
              id,
              filename: transfer.filename,
              direction: transfer.direction || 'receive',
              progress,
              speed,
              peakSpeed,
              eta,
              source: transfer.source,
              isSync: !!(transfer.isSync || transfer.source === 'sync'),
              syncLibraryId: transfer.syncLibraryId
            })
          }
        }
      })
      info.scheduler = scheduler
      const dl = core.download({ start: firstDataBlock, end: lastDataBlock })
      if (dl && typeof dl.destroy === 'function') info.download = dl

      await scheduler.run()

      if (info.flags.cancelled) throw new Error('interrupted')

      await info.fd.close()
      info.fd = null

      // Integrity verification: whole-file checksum from block hashes.
      const fileBytes = verifiedBytes
      if (fileBytes < manifest.fileSize) {
        throw new Error(`Incomplete file: ${fileBytes}/${manifest.fileSize} bytes`)
      }
      if (transfer.checksum && manifest.checksum !== transfer.checksum) {
        throw new Error('Checksum mismatch: file integrity verification failed')
      }

      // Atomic rename from the per-transfer staging dir into the final path,
      // then persist/emit completion (shared with the sync stream path).
      await this._finalizeReceive(transfer, info, {
        manifest,
        verifiedBytes,
        stagingPath,
        destPath,
        blockSize,
        blockCount,
        startTime,
        peakSpeed
      })
    } catch (err) {
      const isInterrupt = info.flags.cancelled || /interrupted/i.test(err.message)
      await this._failOrInterrupt(transfer, err, isInterrupt, {
        info,
        bytesWritten: verifiedBytes || bytesWritten
      })
    } finally {
      if (info.fd) await info.fd.close().catch(() => {})
      if (info.download && typeof info.download.destroy === 'function') info.download.destroy()
      this.runs.delete(id)
      // Claim keeps its verified core OPEN after completion: it becomes a
      // seeder for the share (see connections/claims.js). Closing it here
      // would take the peer out of the swarm.
      if (!transfer.keepCoreOpen) await core.close().catch(() => {})
      this.queue.release(transfer)
      this._kickQueue('receive')
    }
  }

  async _uniqueFinalPath(destPath) {
    const { path } = this
    const dir = path.dirname(destPath)
    const ext = path.extname(destPath)
    const base = path.basename(destPath, ext)
    let candidate = destPath
    let n = 1
    while (await this._exists(candidate)) {
      candidate = path.join(dir, `${base} (${n})${ext}`)
      n++
    }
    return candidate
  }

  async _exists(p) {
    try {
      await this.fsp.stat(p)
      return true
    } catch {
      return false
    }
  }

  _hashManifest(manifest) {
    return b4a.toString(sha256(JSON.stringify(manifest)), 'hex')
  }

  // ─── Sync streaming channel ────────────────────────────────────────────────
  // One protomux channel per sync transfer (id = transferId). Message types
  // must be registered in the SAME order on both sides:
  //   0 control (JSON string)  1 manifest (raw JSON)  2 block (idx+data)  3 ack
  _openSyncStreamChannel(peerObj, transferId, handlers) {
    const mux = Protomux.isProtomux(peerObj.connection)
      ? peerObj.connection
      : Protomux.from(peerObj.connection)
    const channel = mux.createChannel({
      protocol: SYNC_STREAM_PROTOCOL,
      id: Buffer.from(transferId, 'utf8'),
      onopen: () => {
        if (handlers.onOpen) handlers.onOpen()
      },
      onclose: () => {
        if (handlers.onClose) handlers.onClose()
      },
      ondestroy: () => {
        if (handlers.onClose) handlers.onClose()
      }
    })
    if (!channel) return null

    const control = channel.addMessage({
      encoding: c.string,
      onmessage: (raw) => {
        try {
          if (handlers.onControl) handlers.onControl(JSON.parse(raw))
        } catch {}
      }
    })
    const manifest = channel.addMessage({
      encoding: c.raw,
      onmessage: (buf) => {
        if (handlers.onManifest) handlers.onManifest(buf)
      }
    })
    const block = channel.addMessage({
      encoding: c.raw,
      onmessage: (buf) => {
        if (handlers.onBlock) {
          Promise.resolve()
            .then(() => handlers.onBlock(buf))
            .catch((err) => {
              if (handlers.onError) handlers.onError(err)
            })
        }
      }
    })
    const ack = channel.addMessage({
      encoding: c.uint,
      onmessage: (n) => {
        if (handlers.onAck) handlers.onAck(n)
      }
    })
    channel.open()
    return { channel, control, manifest, block, ack }
  }

  // Poll for async channel signals (ready/ack/done/close/error) plus pause and
  // cancel flags. Polling keeps the flow control loop simple and race-free.
  // deadlineFn (optional) is polled alongside: a transfer that is stalled but
  // whose channel has NOT closed (dead relay, silent peer) must still abort.
  async _waitSync(info, predicate, timeoutMs, label, deadlineFn) {
    const deadline = timeoutMs > 0 ? Date.now() + timeoutMs : 0
    for (;;) {
      if (info.flags.cancelled) throw new Error('interrupted')
      if (info.channelClosed) throw new Error('interrupted: peer disconnected')
      if (info.errorMsg) throw new Error(info.errorMsg)
      if (predicate()) return
      if (deadline > 0 && Date.now() >= deadline) throw new Error(label + ' timed out')
      if (deadlineFn && deadlineFn()) throw new Error(label + ' timed out')
      await sleep(25)
    }
  }

  // Wait for the receive-side staging fd to be opened. Blocks can arrive
  // before the fd is assigned (the sender streams right after the manifest;
  // the receiver opens the file between the ready handshake and the first
  // block). Bounded by the receive idle timeout so a stuck sender still
  // interrupts instead of hanging forever.
  async _waitForFd(info, timeoutMs = SYNC_RECEIVE_IDLE_TIMEOUT) {
    const deadline = Date.now() + timeoutMs
    for (;;) {
      if (info.flags.cancelled) throw new Error('interrupted')
      if (info.channelClosed) throw new Error('interrupted: peer disconnected')
      if (info.errorMsg) throw new Error(info.errorMsg)
      if (info.fd) return
      if (Date.now() >= deadline) throw new Error('interrupted: timed out waiting for file open')
      await sleep(10)
    }
  }

  // ─── Sync streaming send ───────────────────────────────────────────────────
  // Reads the file from its source path and streams blocks over the sync
  // channel. Nothing is appended to the exchange store — the payload is never
  // duplicated into the app sandbox. Progress reflects real bytes on the wire
  // and completion waits for the receiver's verified write ('done').
  async _runSendSync(transfer) {
    const { fsp } = this
    const id = transfer.id
    const filePath = transfer.filePath
    const fileSize = transfer.fileSize

    const info = {
      direction: 'send',
      fd: null,
      core: null,
      flags: { paused: false, cancelled: false },
      scheduler: null,
      channel: null,
      record: transfer,
      readyByteOffset: null,
      ackedBlocks: 0,
      lastAckAt: 0,
      doneReceived: false,
      errorMsg: null,
      channelClosed: false
    }
    this.runs.set(id, info)

    // Peer offline: park the transfer instead of churning the queue. It is
    // re-enqueued by retryWaiting() when the peer comes back online.
    const peerObj = this._findTransferPeer(transfer.peerId, transfer.peerKey)
    if (peerObj && peerObj.incompatible) {
      // The peer runs a different wire protocol version — sync cannot work.
      await this._persist(id, { status: STATUS.WAITING_PEER, error: 'Peer protocol incompatible — update both apps' })
      this.runs.delete(id)
      this.queue.release(transfer)
      this._kickQueue('send')
      return
    }
    if (!peerObj || !peerObj.connection) {
      await this._persist(id, { status: STATUS.WAITING_PEER, error: 'Peer offline' })
      this.runs.delete(id)
      this.queue.release(transfer)
      this._kickQueue('send')
      return
    }

    let bytesSent = 0
    const startTime = Date.now()
    let peakSpeed = 0
    let lastEmitTime = startTime
    let lastEmitBytes = 0
    let lastEmittedProgress = 0

    try {
      // 1. Announce the offer AND open the stream channel BEFORE hashing. The
      //    channel open frame travels right behind the offer on the same
      //    connection, so protomux pairs it the instant the receiver creates
      //    its side in receiveOffer. Hashing a large file must never delay
      //    pairing — a late channel open is orphaned and the transfer dies.
      this.sendOffer(id, transfer)
      const chan = this._openSyncStreamChannel(peerObj, id, {
        onControl: (msg) => {
          if (!msg || typeof msg !== 'object') return
          if (msg.type === 'ready') info.readyByteOffset = Number(msg.byteOffset) || 0
          else if (msg.type === 'done') info.doneReceived = true
          else if (msg.type === 'error') info.errorMsg = String(msg.message || 'peer reported an error')
        },
        onAck: (n) => {
          info.ackedBlocks = Math.max(info.ackedBlocks, Number(n) || 0)
          info.lastAckAt = Date.now()
        },
        onClose: () => {
          info.channelClosed = true
        }
      })
      if (!chan) throw new Error('Could not open sync stream channel')
      info.channel = chan
      // Anchor the no-ACK deadline at channel open: a peer that dies before
      // the first ACK must not hold the flow-control wait open forever.
      info.lastAckAt = Date.now()
      const { manifest: manifestMsg, block: blockMsg } = chan

      // 2. Integrity manifest — streamed hash of the file (memory-safe).
      const built = await buildManifest({
        filePath,
        fsp,
        filename: transfer.filename,
        fileSize,
        fileType: transfer.fileType,
        transferId: id,
        shouldCancel: () => info.flags.cancelled
      })
      const { manifest, manifestHash } = built
      transfer.manifestHash = manifestHash
      transfer.checksum = manifest.checksum
      transfer.blockSize = manifest.blockSize
      transfer.blockCount = manifest.blockCount
      await this._persist(id, {
        manifestHash,
        checksum: manifest.checksum,
        blockSize: manifest.blockSize,
        blockCount: manifest.blockCount,
        status: STATUS.ACTIVE,
        startedAt: transfer.startedAt || new Date().toISOString()
      })

      // 3. Wait for the receiver's ready handshake (its resume byte offset).
      await this._waitSync(
        info,
        () => info.readyByteOffset !== null,
        SYNC_HANDSHAKE_TIMEOUT,
        'waiting for receiver handshake'
      )
      const startBlock = Math.floor(info.readyByteOffset / manifest.blockSize)

      // 4. Send the manifest, then stream blocks from the resume point.
      manifestMsg.send(Buffer.from(JSON.stringify(manifest)))
      const fd = await fsp.open(filePath, 'r')
      info.fd = fd
      const buf = Buffer.alloc(manifest.blockSize)
      bytesSent = startBlock * manifest.blockSize
      lastEmitBytes = bytesSent
      lastEmittedProgress = fileSize === 0 ? 100 : Math.min(100, Math.round((bytesSent / fileSize) * 100))

      for (let i = startBlock + 1; i <= manifest.blockCount; i++) {
        if (info.flags.cancelled) throw new Error('interrupted')
        while (info.flags.paused && !info.flags.cancelled) await sleep(50)

        const offset = (i - 1) * manifest.blockSize
        const readRes = await fd.read(buf, 0, manifest.blockSize, offset)
        const bytesRead = typeof readRes === 'number' ? readRes : (readRes?.bytesRead || 0)
        if (bytesRead === 0) break

        const payload = Buffer.alloc(4 + bytesRead)
        payload.writeUInt32BE(i, 0)
        buf.subarray(0, bytesRead).copy(payload, 4)
        blockMsg.send(payload)
        bytesSent += bytesRead

        // Windowed flow control: keep at most SYNC_STREAM_WINDOW blocks in flight.
        const sentBlocks = i - startBlock
        if (sentBlocks - info.ackedBlocks >= SYNC_STREAM_WINDOW) {
          await this._waitSync(
            info,
            () => info.ackedBlocks >= sentBlocks - Math.floor(SYNC_STREAM_WINDOW / 2),
            0,
            'interrupted: flow control (no ACKs from peer)',
            () => Date.now() - info.lastAckAt > SYNC_FLOW_TIMEOUT
          )
        }

        // Progress: real bytes put on the wire.
        const now = Date.now()
        const elapsed = (now - lastEmitTime) / 1000
        let speed = 0
        if (elapsed >= 1) {
          speed = Math.round((bytesSent - lastEmitBytes) / elapsed)
          lastEmitTime = now
          lastEmitBytes = bytesSent
        } else if (now - startTime > 0) {
          speed = Math.round(bytesSent / ((now - startTime) / 1000))
        }
        if (speed > peakSpeed) peakSpeed = speed
        const progress = fileSize === 0 ? 100 : Math.min(100, Math.round((bytesSent / fileSize) * 100))
        if (progress === 100 || progress - lastEmittedProgress >= 2 || now - lastEmitTime >= 1000) {
          lastEmittedProgress = progress
          const remaining = Math.max(0, fileSize - bytesSent)
          const eta = speed > 0 ? Math.round(remaining / speed) : 0
          await this._persist(id, { progress, speed, peakSpeed, eta, byteOffset: bytesSent })
          this._emit(EVENTS.TRANSFER_PROGRESS, {
            id,
            filename: transfer.filename,
            direction: transfer.direction || 'send',
            progress,
            speed,
            peakSpeed,
            eta,
            source: transfer.source,
            isSync: !!(transfer.isSync || transfer.source === 'sync'),
            syncLibraryId: transfer.syncLibraryId
          })
        }
      }

      await fd.close()
      info.fd = null

      // 6. Completion is real: wait for the receiver's verified write + 'done'.
      await this._waitSync(info, () => info.doneReceived, SYNC_HANDSHAKE_TIMEOUT, 'waiting for receiver completion')

      const totalElapsed = (Date.now() - startTime) / 1000
      const avgSpeed = totalElapsed > 0 ? Math.round(fileSize / totalElapsed) : 0
      const completed = await this._persist(id, {
        status: STATUS.COMPLETED,
        progress: 100,
        speed: avgSpeed,
        peakSpeed,
        eta: 0,
        duration: Math.max(1, Math.round(totalElapsed)),
        byteOffset: fileSize,
        completedAt: new Date().toISOString(),
        summary: {
          checksum: transfer.checksum,
          manifestHash: transfer.manifestHash,
          blocksVerified: manifest.blockCount,
          bytesVerified: fileSize
        }
      })
      this._emit(EVENTS.TRANSFER_COMPLETED, completed)
      this._recordHistory(completed, 'Sent')
    } catch (err) {
      if (info.fd) await info.fd.close().catch(() => {})
      const isInterrupt = info.flags.cancelled || /interrupted/i.test(err.message)
      await this._failOrInterrupt(transfer, err, isInterrupt, { info, bytesWritten: bytesSent })
    } finally {
      this.runs.delete(id)
      // info.channel is the { channel, control, manifest, block, ack } wrapper;
      // close the underlying protomux channel (also signals the receiver).
      if (info.channel && info.channel.channel && typeof info.channel.channel.close === 'function') {
        try {
          info.channel.channel.close()
        } catch {}
      }
      this.queue.release(transfer)
      this._kickQueue('send')
    }
  }

  // ─── Sync streaming receive ────────────────────────────────────────────────
  // Receives the streamed blocks, verifies each against the manifest, writes
  // them to the staging .part at whole-block offsets, then atomically renames
  // into the sync folder — no exchange core is ever involved.
  async _runReceiveSync(transfer) {
    const { fsp, path } = this
    const id = transfer.id

    const info = {
      direction: 'receive',
      fd: null,
      core: null,
      flags: { paused: false, cancelled: false },
      scheduler: null,
      channel: null,
      record: transfer,
      manifest: null,
      blockSize: 0,
      blockCount: 0,
      receivedBlocks: 0,
      lastBlockAt: 0,
      verifiedBytes: 0,
      pendingWrites: 0,
      finalized: false,
      errorMsg: null,
      channelClosed: false
    }
    this.runs.set(id, info)

    const stagingPath = transfer.stagingPath
    const destPath = transfer.destPath
    const startTime = Date.now()
    let peakSpeed = 0
    let lastEmitTime = startTime
    let lastEmitBytes = 0

    const failReceive = async (err) => {
      if (info.finalized) return
      info.finalized = true
      try {
        if (info.channel && typeof info.channel.control.send === 'function') {
          info.channel.control.send(JSON.stringify({ type: 'error', message: String(err?.message || err) }))
        }
      } catch {}
      const isInterrupt = info.flags.cancelled || /interrupted/i.test(err.message)
      await this._failOrInterrupt(transfer, err, isInterrupt, { info, bytesWritten: info.verifiedBytes })
      // A hard failure leaves garbage in the library folder — remove the
      // staging .part (interrupted transfers keep it for resume).
      if (!isInterrupt) {
        try {
          await fsp.rm(stagingPath, { force: true }).catch(() => {})
          await fsp.rmdir(path.dirname(stagingPath)).catch(() => {})
        } catch {}
      }
    }

    const finalizeReceive = async () => {
      if (info.finalized) return
      info.finalized = true
      while (info.pendingWrites > 0) await sleep(5)
      if (info.flags.cancelled) throw new Error('interrupted')
      await this._finalizeReceive(transfer, info, {
        manifest: info.manifest,
        verifiedBytes: info.verifiedBytes,
        stagingPath,
        destPath,
        blockSize: info.blockSize,
        blockCount: info.blockCount,
        startTime,
        peakSpeed
      })
      try {
        info.channel.control.send(JSON.stringify({ type: 'done' }))
      } catch {}
      // Do NOT close the channel here: a close frame can overtake the 'done'
      // message and protomux drops pending inbound messages on close, which
      // makes the sender interrupt an already-completed transfer. The sender
      // closes the channel after it receives 'done'.
      info.completed = true
    }

    const handleManifest = async (buf) => {
      const manifest = parseManifest(buf)
      if (!manifest) throw new Error('Remote stream has no valid manifest')
      if (transfer.manifestHash && this._hashManifest(manifest) !== transfer.manifestHash) {
        throw new Error('Manifest hash mismatch: sender integrity check failed')
      }
      info.manifest = manifest
      info.blockSize = manifest.blockSize
      info.blockCount = manifest.blockCount
      // Anchor the no-block deadline: from the manifest on, blocks must keep
      // arriving or the receive is interrupted (the sender may have died on a
      // silent link that never closes the channel).
      info.lastBlockAt = Date.now()
      // Re-sync skip: the destination already holds this exact version — the
      // sender was told byteOffset = fileSize, so no blocks will arrive.
      if (manifest.blockCount === 0 || info.skipFile) await finalizeReceive()
    }

    // Serialize file writes: protomux delivers messages without awaiting the
    // handlers, and concurrent positional writes on one fd are not safe on all
    // platforms (bare-fs on Android). The core path's scheduler writes blocks
    // one at a time; the stream path chains writes the same way.
    let lastWrite = Promise.resolve()

    const handleBlock = async (buf) => {
      const manifest = info.manifest
      if (!manifest) throw new Error('Received block before manifest')
      const index = buf.readUInt32BE(0)
      // COPY the block immediately: the buffer is a view into the transport's
      // shared receive buffer, which is reused on the next data event — holding
      // it across an await would write corrupted bytes.
      const block = Buffer.from(buf.subarray(4))
      if (index < 1 || index > manifest.blockCount) throw new Error('Invalid block index: ' + index)

      // Per-block integrity: the manifest carries every block hash.
      const expected = manifest.blocks[index - 1]
      const actual = b4a.toString(sha256(block), 'hex')
      if (expected && actual !== expected) throw new Error('Block hash mismatch at index ' + index)

      // A block can arrive before the fd is open (the sender streams as soon as
      // it has the manifest; the receiver opens the staging file between the
      // ready handshake and the first block — and on the skip path the fd is
      // never opened because zero blocks are expected). Buffer the payload and
      // wait for the fd instead of crashing on a null dereference.
      if (!info.fd) {
        await this._waitForFd(info)
        if (!info.fd) throw new Error('interrupted: receive aborted before file open')
      }

      const offset = (index - 1) * manifest.blockSize
      info.pendingWrites++
      const write = lastWrite.then(() => info.fd.write(block, 0, block.length, offset))
      lastWrite = write.catch(() => {})
      await write
      info.pendingWrites--
      info.receivedBlocks++
      info.verifiedBytes += block.length
      info.lastBlockAt = Date.now()

      // Progress: real verified bytes written to disk.
      const now = Date.now()
      const elapsed = (now - lastEmitTime) / 1000
      let speed = 0
      if (elapsed >= 0.5) {
        speed = Math.round((info.verifiedBytes - lastEmitBytes) / elapsed)
        lastEmitTime = now
        lastEmitBytes = info.verifiedBytes
      }
      if (speed > peakSpeed) peakSpeed = speed
      const progress = manifest.fileSize === 0 ? 100 : Math.min(100, Math.round((info.verifiedBytes / manifest.fileSize) * 100))
      if (progress === 100 || now - lastEmitTime >= 1000) {
        const remaining = Math.max(0, manifest.fileSize - info.verifiedBytes)
        const eta = speed > 0 ? Math.round(remaining / speed) : 0
        await this._persist(id, { progress, speed, peakSpeed, eta, byteOffset: info.verifiedBytes })
        this._emit(EVENTS.TRANSFER_PROGRESS, {
          id,
          filename: transfer.filename,
          direction: transfer.direction || 'receive',
          progress,
          speed,
          peakSpeed,
          eta,
          source: transfer.source,
          isSync: !!(transfer.isSync || transfer.source === 'sync'),
          syncLibraryId: transfer.syncLibraryId
        })
      }

      // Flow control: tell the sender how many whole blocks we hold.
      if (info.receivedBlocks % SYNC_ACK_EVERY === 0) {
        try {
          info.channel.ack.send(info.receivedBlocks)
        } catch {}
      }

      if (info.receivedBlocks >= manifest.blockCount) {
          await finalizeReceive()
      }
    }

    try {
      // Tolerant lookup: the offer may carry the noise key or a device id.
      // If the peer is momentarily unreachable, park the receive (WAITING_PEER)
      // instead of failing it — a transient lookup miss must never burn the
      // transfer or flood the log with failures.
      const peerObj = this._findTransferPeer(transfer.peerKey, transfer.peerId)
      if (!peerObj || !peerObj.connection) {
        await this._persist(id, { status: STATUS.WAITING_PEER, error: 'Peer offline' })
        const pre = this._syncChannels.get(id)
        if (pre) {
          this._syncChannels.delete(id)
          try {
            pre.chan.channel.close()
          } catch {}
        }
        this.queue.release(transfer)
        return
      }

      // Resume: a partial .part file continues from a whole-block boundary.
      await fsp.mkdir(path.dirname(stagingPath), { recursive: true })
      let resumeBytes = 0
      try {
        const stat = await fsp.stat(stagingPath)
        resumeBytes = Math.floor(stat.size / (transfer.blockSize || CHUNK_SIZE)) * (transfer.blockSize || CHUNK_SIZE)
      } catch {}
      resumeBytes = Math.max(0, Math.min(resumeBytes, transfer.fileSize))
      transfer.byteOffset = resumeBytes
      info.verifiedBytes = resumeBytes
      lastEmitBytes = resumeBytes

      // Re-sync skip: if the DESTINATION already has this exact version (same
      // size + the sender's mtime was stamped on it), answer the handshake
      // with byteOffset = fileSize so the sender sends zero blocks and the
      // transfer completes instantly. This makes re-creating a sync folder
      // against an existing folder skip all identical files instead of
      // re-uploading the whole backlog.
      let skipFile = false
      if (transfer.fileSize > 0 && transfer.syncMtimeMs && resumeBytes === 0) {
        try {
          const dst = await fsp.stat(destPath)
          if (dst.size === transfer.fileSize && Math.abs(dst.mtimeMs - transfer.syncMtimeMs) < 1500) {
            // Only skip when WE delivered this exact version (strict verify
            // records it). A same-size / close-mtime coincidence must never
            // suppress a real push — that is how content silently desynced.
            const delivered =
              !this.syncDelivered ||
              (await this.syncDelivered(transfer.syncLibraryId, transfer.syncRelPath, transfer.syncMtimeMs))
            if (delivered) {
              skipFile = true
              info.skipFile = true
              resumeBytes = transfer.fileSize
              transfer.byteOffset = resumeBytes
              info.verifiedBytes = resumeBytes
              lastEmitBytes = resumeBytes
            }
          }
        } catch {}
      }

      if (!skipFile) {
        info.fd = await fsp.open(stagingPath, resumeBytes > 0 ? 'r+' : 'w')
      }

      // Consume the channel pre-created in receiveOffer (protomux pairing must
      // happen synchronously with the offer); fall back to creating it here if
      // the offer arrived without a sender peer key.
      const pre = this._syncChannels.get(id)
      let chan
      if (pre) {
        this._syncChannels.delete(id)
        chan = pre.chan
        const reg = pre.reg
        reg.control = (msg) => {
          if (msg && msg.type === 'error') info.errorMsg = String(msg.message || 'peer error')
        }
        reg.manifest = (buf) => handleManifest(buf).catch((err) => failReceive(err))
        reg.block = (buf) => handleBlock(buf).catch((err) => failReceive(err))
        reg.ack = () => {}
        reg.close = () => { info.channelClosed = true }
        reg.error = (err) => failReceive(err)
        info.channel = chan
        // Tell the sender where to resume from (0 = fresh). Nothing is sent by
        // the sender before this handshake, so no messages are lost.
        chan.control.send(JSON.stringify({ type: 'ready', byteOffset: resumeBytes }))
      } else {
        const peerObj2 = this._findTransferPeer(transfer.peerKey, transfer.peerId)
        if (!peerObj2 || !peerObj2.connection) throw new Error('Peer connection unavailable')
        chan = this._openSyncStreamChannel(peerObj2, id, {
          onOpen: () => {
            try {
              chan.control.send(JSON.stringify({ type: 'ready', byteOffset: resumeBytes }))
            } catch {}
          },
          onControl: (msg) => {
            if (msg && msg.type === 'error') info.errorMsg = String(msg.message || 'peer error')
          },
          onManifest: (buf) => {
            handleManifest(buf).catch((err) => failReceive(err))
          },
          onBlock: (buf) => handleBlock(buf).catch((err) => failReceive(err)),
          onError: (err) => failReceive(err),
          onClose: () => {
            info.channelClosed = true
          }
        })
        if (!chan) throw new Error('Could not open sync stream channel')
        info.channel = chan
      }

      // Wait for finalization — driven by the incoming block stream.
      for (;;) {
        if (info.flags.cancelled) throw new Error('interrupted')
        if (info.finalized) break
        if (info.channelClosed) throw new Error('interrupted: peer disconnected')
        if (info.errorMsg) throw new Error(info.errorMsg)
        // A silent sender (dead relay, lost network — channel never closes)
        // must not hold the receive open forever. The sender aborts first via
        // its own no-ACK timeout; this bounds the receiver either way.
        if (info.manifest && Date.now() - info.lastBlockAt > SYNC_RECEIVE_IDLE_TIMEOUT) {
          throw new Error('interrupted: no blocks received from peer')
        }
        await sleep(25)
      }
    } catch (err) {
      await failReceive(err)
    } finally {
      if (info.fd) await info.fd.close().catch(() => {})
      this.runs.delete(id)
      // On success the channel stays open until the sender closes it after
      // receiving 'done' (closing here could drop the 'done' in transit).
      if (!info.completed && info.channel && typeof info.channel.channel.close === 'function') {
        try {
          info.channel.channel.close()
        } catch {}
      }
      this.queue.release(transfer)
      this._kickQueue('receive')
    }
  }

  // Shared completion path for core-based and streamed receives: atomic rename,
  // sync conflict handling, mtime stamping, persistence + events.
  async _finalizeReceive(transfer, info, { manifest, verifiedBytes, stagingPath, destPath, blockSize, blockCount, startTime, peakSpeed }) {
    const { fsp, path } = this
    const id = transfer.id

    if (info.flags.cancelled) throw new Error('interrupted')
    if (info.fd) {
      await info.fd.close().catch(() => {})
      info.fd = null
    }
    if (verifiedBytes < manifest.fileSize) {
      throw new Error(`Incomplete file: ${verifiedBytes}/${manifest.fileSize} bytes`)
    }
    if (transfer.checksum && manifest.checksum !== transfer.checksum) {
      throw new Error('Checksum mismatch: file integrity verification failed')
    }

    // Atomic rename from the per-transfer staging dir into the final path.
    // Sync transfers OVERWRITE their target (the file is expected to change —
    // single-owner backup: the owner's copy always wins, the receiver is a
    // pure sink); regular transfers resolve collisions with a unique suffix.
    let finalPath = destPath
    if (transfer.isSync && info.skipFile) {
      // Re-sync skip: the destination already holds this exact version —
      // nothing to write. Just clean up the (empty) staging file.
      try {
        await fsp.rm(stagingPath, { force: true })
        await fsp.rmdir(path.dirname(stagingPath)).catch(() => {})
      } catch {}
    } else if (transfer.isSync) {
      await fsp.mkdir(path.dirname(destPath), { recursive: true })
      // Sync overwrite policy:
      //  - push / receive_only (single-owner backup): the owner's copy always
      //    wins, the receiver is a pure sink — replace unconditionally.
      //  - two-way (bidirectional mirror): a differing local file is a
      //    concurrent-edit conflict. Preserve the local copy in .meshdrop-trash
      //    BEFORE overwriting so the losing edit is never silently destroyed
      //    (Last-Write-Wins by completion order, but recoverable).
      const syncMode = transfer.syncLibraryId ? this.getSyncMode(transfer.syncLibraryId) : null
      const existing = await fsp.stat(destPath).then(() => true).catch(() => false)
      if (syncMode === 'two-way' && existing) {
        try {
          const trashDir = path.join(transfer.baseDir || path.dirname(destPath), '.meshdrop-trash')
          await fsp.mkdir(trashDir, { recursive: true })
          const trashName = `${Date.now().toString(36)}_${path.basename(destPath)}`
          await fsp.rename(destPath, path.join(trashDir, trashName))
        } catch (err) {
          // Trash move failed (locked file, cross-device) — fall back to the
          // old overwrite rather than failing the whole transfer.
          console.warn('[TransferEngine] two-way conflict trash failed, overwriting:', err.message)
          await fsp.rm(destPath, { force: true }).catch(() => {})
        }
      } else {
        await fsp.rm(destPath, { force: true }).catch(() => {})
      }
      await fsp.rename(stagingPath, finalPath)
      await fsp.rmdir(path.dirname(stagingPath)).catch(() => {})
    } else {
      finalPath = await this._uniqueFinalPath(destPath)
      await fsp.rename(stagingPath, finalPath)
      await fsp.rmdir(path.dirname(stagingPath)).catch(() => {})
    }

    // Sync convergence: stamp the sender's mtime onto the received file so
    // both sides' indices agree and the file is not re-pushed forever.
    if (transfer.isSync && transfer.syncMtimeMs && !info.skipFile) {
      await fsp.utimes(finalPath, new Date(transfer.syncMtimeMs), new Date(transfer.syncMtimeMs)).catch(() => {})
    }

    const totalElapsed = (Date.now() - startTime) / 1000
    const avgSpeed = totalElapsed > 0 ? Math.round(manifest.fileSize / totalElapsed) : 0
    const completed = await this._persist(id, {
      status: STATUS.COMPLETED,
      progress: 100,
      speed: avgSpeed,
      peakSpeed,
      eta: 0,
      duration: Math.max(1, Math.round(totalElapsed)),
      destPath: finalPath,
      byteOffset: manifest.fileSize,
      blockSize,
      blockCount,
      completedAt: new Date().toISOString(),
      summary: {
        checksum: manifest.checksum,
        manifestHash: transfer.manifestHash || this._hashManifest(manifest),
        blocksVerified: blockCount,
        bytesVerified: manifest.fileSize
      }
    })
    this._noteTerminal()
    this._emit(EVENTS.TRANSFER_COMPLETED, completed)
    this._recordHistory(completed, 'Received')
    this._recordShared(completed, finalPath)
    return finalPath
  }

  // Re-enqueue sync sends parked as WAITING_PEER (peer was offline). Called on
  // PEER_CONNECTED so an offline push resumes the moment the peer comes back.
  // Only transfers whose peer is ACTUALLY connected are re-enqueued — otherwise
  // connection flaps would re-enqueue the whole backlog on every reconnect.
  async retryWaiting() {
    try {
      const bee = await this.getBee('transfers')
      const waiting = []
      for await (const node of bee.createReadStream()) {
        const t = node.value
        if (t && t.direction === 'send' && t.status === STATUS.WAITING_PEER) waiting.push(t)
      }
      for (const t of waiting) {
        const peerObj = this._findTransferPeer(t.peerId, t.peerKey)
        if (!peerObj || !peerObj.connection) continue
        await this._persist(t.id, { status: STATUS.QUEUED })
        this._enqueue(t)
      }
      if (waiting.length > 0) {
        console.log(`[TransferEngine] re-enqueued ${waiting.length} waiting send(s)`)
      }
    } catch (err) {
      console.warn('[TransferEngine] retryWaiting failed:', err.message)
    }
  }

  async _failOrInterrupt(transfer, err, isInterrupt, { info, bytesWritten }) {
    this._noteTerminal()
    const id = transfer.id
    const msg = String(err?.message || err)
    console.warn(`[TransferEngine] ${isInterrupt ? 'interrupt' : 'fail'} ${id}: ${msg}`)

    if (isInterrupt) {
      const record = await this._persist(id, {
        status: STATUS.INTERRUPTED,
        byteOffset: bytesWritten || 0,
        progress:
          transfer.fileSize > 0
            ? Math.min(100, Math.round(((bytesWritten || 0) / transfer.fileSize) * 100))
            : 0,
        error: msg,
        interruptedAt: new Date().toISOString()
      })
      this._emit(EVENTS.TRANSFER_CANCELLED, record)
    } else {
      const record = await this._persist(id, {
        status: STATUS.FAILED,
        error: msg,
        completedAt: new Date().toISOString()
      })
      this._emit(EVENTS.TRANSFER_FAILED, record)
    }
  }

  async _recordHistory(transfer, action) {
    try {
      const bee = await this.getBee('history')
      const entry = {
        id: `hist-${transfer.id}`,
        type: 'transfer',
        title: `${action} ${transfer.filename}`,
        description: `${transfer.fileSize} bytes ${action.toLowerCase()} ${
          action === 'Sent'
            ? `to ${transfer.peerName || 'Unknown'}`
            : `from ${transfer.peerName || 'Remote Peer'}`
        }`,
        timestamp: new Date().toISOString(),
        transferId: transfer.id,
        transferMethod: transfer.transferMethod
      }
      await bee.put(entry.id, entry)
    } catch (err) {
      console.warn('[TransferEngine] history write failed:', err.message)
    }
  }

  // ─── Receive ───────────────────────────────────────────────────────────────
  async _recordShared(transfer, finalPath) {
    try {
      const bee = await this.getBee('shared')
      const entry = {
        id: transfer.id,
        name: transfer.filename,
        size: transfer.fileSize,
        type: transfer.fileType,
        modifiedAt: new Date().toISOString(),
        sharedWith: [],
        isFavorite: false,
        path: finalPath
      }
      await bee.put(transfer.id, entry)
    } catch (err) {
      console.warn('[TransferEngine] shared write failed:', err.message)
    }
  }

  // ── Queue scheduling ──────────────────────────────────────────────────────

  _enqueue(transfer) {
    if (this.queue._hasSlot(transfer)) {
      this._startTransfer(transfer)
    } else {
      this.queue.enqueue(transfer)
      this._emit(EVENTS.TRANSFER_QUEUED, transfer)
    }
  }

  _kickQueue(direction) {
    const next = this.queue.popNext(direction)
    if (!next) return
    this._startTransfer(next)
  }

  async _startTransfer(transfer) {
    this.queue.claim(transfer)
    const id = transfer.id
    try {
      // A transfer cancelled while queued (pause/delete → cancelSyncTransfers)
      // must never start: the cancel only marks the record, but the queue may
      // still pop it. Check the in-memory guard (fast) and the persisted status.
      if (this._cancelledQueue && this._cancelledQueue.has(id)) {
        this.queue.release(transfer)
        this._kickQueue(transfer.direction)
        return
      }
      const bee = await this.getBee('transfers')
      const entry = await bee.get(id)
      const stored = entry && entry.value
      if (
        stored &&
        (stored.status === STATUS.CANCELLED ||
          stored.status === STATUS.INTERRUPTED ||
          stored.status === STATUS.FAILED)
      ) {
        this.queue.release(transfer)
        this._kickQueue(transfer.direction)
        return
      }
      if (transfer.direction === 'send') {
        const record = await this._persist(id, {
          status: STATUS.ACTIVE,
          startedAt: new Date().toISOString()
        })
        const fullRecord = { ...transfer, ...record }
        this._emit(EVENTS.TRANSFER_STARTED, fullRecord)
        this._runSend(fullRecord).catch(() => {})
      } else {
        const record = await this._persist(id, {
          status: STATUS.ACTIVE,
          startedAt: new Date().toISOString()
        })
        const fullRecord = { ...transfer, ...record }
        this._emit(EVENTS.TRANSFER_STARTED, fullRecord)
        this._runReceive(fullRecord).catch(() => {})
      }
    } catch (err) {
      console.warn(`[TransferEngine] start ${id} failed:`, err.message)
      await this._persist(id, { status: STATUS.FAILED, error: err.message })
      this._emit(EVENTS.TRANSFER_FAILED, { id, error: err.message })
      this.queue.release(transfer)
      this._kickQueue(transfer.direction)
    }
  }

  // ── Public operations ─────────────────────────────────────────────────────

  async approve(transferId) {
    const bee = await this.getBee('transfers')
    const entry = await bee.get(transferId)
    if (!entry) throw new Error('Transfer offer not found')

    const record = entry.value
    if (record.direction !== 'receive') throw new Error('Only incoming transfers can be approved')

    const pending = this.pendingOffers.get(transferId)
    const ok = await this._validateReceive(record, pending?.offer || null)
    if (!ok) throw new Error('Transfer rejected during validation')
    this.pendingOffers.delete(transferId)

    await this._persist(transferId, {
      status: STATUS.QUEUED,
      priority: record.priority || DEFAULT_PRIORITY
    })
    const updated = { ...record, status: STATUS.QUEUED }
    this._enqueue(updated)
    return updated
  }

  async decline(transferId) {
    this.pendingOffers.delete(transferId)
    const bee = await this.getBee('transfers')
    const entry = await bee.get(transferId)
    const record = entry?.value || { id: transferId }
    const cancelled = await this._persist(transferId, {
      status: STATUS.CANCELLED,
      cancelledAt: new Date().toISOString()
    })
    this._emit(EVENTS.TRANSFER_CANCELLED, cancelled)
    return record
  }

  async pause(transferId) {
    const bee = await this.getBee('transfers')
    const entry = await bee.get(transferId)
    if (!entry) throw new Error('Transfer not found')

    const info = this.runs.get(transferId)
    if (info) info.flags.paused = true

    const record = await this._persist(transferId, { status: STATUS.PAUSED })
    this._emit(EVENTS.TRANSFER_PAUSED, record)
    return record
  }

  async resume(transferId) {
    const bee = await this.getBee('transfers')
    const entry = await bee.get(transferId)
    if (!entry) throw new Error('Transfer not found')

    const record = { ...entry.value, priority: entry.value.priority || DEFAULT_PRIORITY }
    if (this.runs.has(transferId)) {
      const info = this.runs.get(transferId)
      info.flags.paused = false
      if (info.scheduler) info.scheduler.resume()
      const updated = await this._persist(transferId, { status: STATUS.ACTIVE })
      this._emit(EVENTS.TRANSFER_RESUMED, updated)
      return updated
    }

    // Resume from an interrupted/paused record: reuse the same stored core.
    await this._persist(transferId, { status: STATUS.QUEUED })
    const updated = { ...record, status: STATUS.QUEUED }
    this._enqueue(updated)
    return updated
  }

  async cancel(transferId) {
    const bee = await this.getBee('transfers')
    const entry = await bee.get(transferId)
    if (!entry) throw new Error('Transfer not found')

    const info = this.runs.get(transferId)
    if (info) {
      info.flags.cancelled = true
      if (info.scheduler) info.scheduler.cancel()
    }
    // The active loop's finally block persists 'interrupted' + emits + frees the
    // queue slot; if nothing was running, do it here.
    if (!info) {
      // A pending claim placeholder has no running loop: cancel it cleanly.
      const isWaiting = entry.value.status === STATUS.WAITING_PEER
      const record = await this._persist(transferId, {
        status: isWaiting ? STATUS.CANCELLED : STATUS.INTERRUPTED,
        ...(isWaiting ? { cancelledAt: new Date().toISOString() } : { interruptedAt: new Date().toISOString() })
      })
      this._emit(EVENTS.TRANSFER_CANCELLED, record)
      return record
    }
    return null
  }

  async retry(transferId) {
    const bee = await this.getBee('transfers')
    const entry = await bee.get(transferId)
    if (!entry) throw new Error('Transfer not found')

    const record = { ...entry.value, priority: entry.value.priority || DEFAULT_PRIORITY }
    await this._persist(transferId, {
      status: STATUS.QUEUED,
      progress: 0,
      speed: 0,
      eta: 0,
      byteOffset: 0
    })
    const updated = {
      ...record,
      status: STATUS.QUEUED,
      progress: 0,
      speed: 0,
      eta: 0,
      byteOffset: 0
    }
    this._enqueue(updated)
    return updated
  }

  async list() {
    const bee = await this.getBee('transfers')
    const results = []
    for await (const node of bee.createReadStream()) {
      if (node.value && node.value.id) results.push(node.value)
    }
    results.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt))
    return results
  }

  setPlayhead(transferId, blockIndex) {
    const info = this.runs.get(transferId)
    if (info && info.scheduler && typeof info.scheduler.setPlayhead === 'function') {
      info.scheduler.setPlayhead(blockIndex)
      return true
    }
    return false
  }

  setPlayheadByte(transferId, byteOffset) {
    const info = this.runs.get(transferId)
    if (info && info.scheduler && typeof info.scheduler.setPlayhead === 'function') {
      const blockSize = info.blockSize || CHUNK_SIZE
      const blockIndex = Math.floor(byteOffset / blockSize) + 1
      info.scheduler.setPlayhead(blockIndex)
      return true
    }
    return false
  }

  async delete(transferId) {
    if (!transferId) return false
    try {
      await this.cancel(transferId).catch(() => {})
    } catch {}
    const bee = await this.getBee('transfers')
    await bee.del(transferId)
    this._emit(EVENTS.TRANSFER_CANCELLED, { id: transferId, deleted: true })
    return true
  }

  async clear(options = {}) {
    const includePending = typeof options === 'boolean' ? options : Boolean(options && options.includePending)
    const status = options && typeof options === 'object' ? options.status : null
    const bee = await this.getBee('transfers')
    const keys = []
    for await (const node of bee.createReadStream()) {
      if (!node.value || !node.key) continue
      if (status && node.value.status === status) {
        keys.push(node.key)
      } else if (includePending || TERMINAL.has(node.value.status)) {
        keys.push(node.key)
      }
    }
    for (const k of keys) {
      try {
        await this.cancel(k).catch(() => {})
      } catch {}
      await bee.del(k)
    }
    return { success: true, count: keys.length }
  }

  // ── Claimer-side pending claims ──────────────────────────────────────────
  // When a DROP code is claimed while the host is offline there is no offer,
  // hence no real transfer — but the UI must show an honest "waiting for
  // sender" row instead of an empty list. These placeholders are persisted
  // like any transfer record and cleared the moment the claim resolves.

  async createWaitingClaim({ code }) {
    const id = `claim-wait-${String(code)
      .replace(/[^A-Z0-9]/g, '')
      .toLowerCase()}-${Date.now().toString(36)}`
    const record = {
      id,
      schema: SCHEMA_VERSION,
      filename: 'Waiting for sender…',
      fileSize: 0,
      fileType: '',
      direction: 'receive',
      status: STATUS.WAITING_PEER,
      priority: DEFAULT_PRIORITY,
      progress: 0,
      speed: 0,
      peakSpeed: 0,
      eta: 0,
      duration: 0,
      transferMethod: 'internet',
      isEncrypted: true,
      isClaim: true,
      peerId: '',
      peerName: '',
      shareId: '',
      claimCode: String(code),
      coreKey: '',
      destPath: '',
      stagingPath: '',
      byteOffset: 0,
      manifestHash: '',
      checksum: '',
      blockSize: CHUNK_SIZE,
      blockCount: 0,
      summary: {},
      createdAt: new Date().toISOString()
    }
    await this._persist(id, record)
    // Surface the row immediately (the upsert path treats this like any
    // transfer record arriving on the wire).
    this._emit(EVENTS.TRANSFER_QUEUED, record)
    return record
  }

  async clearWaitingClaims({ code }) {
    try {
      const bee = await this.getBee('transfers')
      const toDelete = []
      for await (const node of bee.createReadStream()) {
        const t = node.value
        if (t && t.status === STATUS.WAITING_PEER && t.claimCode === String(code)) {
          toDelete.push(node.key)
        }
      }
      for (const k of toDelete) await bee.del(k)
      return toDelete.length
    } catch (err) {
      console.warn('[TransferEngine] clearWaitingClaims failed:', err.message)
      return 0
    }
  }

  async shutdown() {
    for (const [, info] of this.runs.entries()) {
      info.flags.cancelled = true
      if (info.scheduler) info.scheduler.cancel()
      if (info.fd) await info.fd.close().catch(() => {})
      await info.core.close().catch(() => {})
    }
    this.runs.clear()
  }
}

module.exports = { TransferEngine, TransferQueue, ChunkScheduler, CHUNK_SIZE, STATUS, getFileType }
