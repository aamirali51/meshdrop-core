'use strict'

const { path, fsp, EventEmitter } = require('../compat.js')
const { generateDropCode } = require('../crypto')
const { EVENTS } = require('../protocol.js')
const { STATUS } = require('./transfer/constants.js')

const PARTY_EVENTS = {
  ROOM_CREATED: 'party:room:created',
  ROOM_JOINED: 'party:room:joined',
  ROOM_UPDATED: 'party:room:updated',
  ROOM_LEFT: 'party:room:left',
  ROOM_CLOSED: 'party:room:closed',
  PEER_JOINED: 'party:peer:joined',
  PEER_LEFT: 'party:peer:left',
  PEER_STATUS: 'party:peer:status',
  STATE_SYNC: 'party:state:sync',
  REACTION: 'party:reaction',
  DISCOVERED_ROOMS: 'party:rooms:discovered',
  MEDIA_OFFER: 'party:media:offer',
  MEDIA_READY: 'party:media:ready',
  MEDIA_ERROR: 'party:media:error',
  CHAT: 'party:chat',
  CHAT_HISTORY: 'party:chat:history',
  VOICE: 'party:voice',
  MODERATED: 'party:moderated'
}

// Chat log kept per room (replayed to late joiners) and per-message caps.
const CHAT_LOG_MAX = 100
const CHAT_HISTORY_REPLAY = 50
const CHAT_TEXT_MAX = 1000
// Subtitle sidecars ride INSIDE the media offer (tiny text files, no second
// transfer pipeline). 1 MB covers even feature-length SRTs with margin.
const SUBTITLE_MAX_BYTES = 1024 * 1024

// A guest that hears nothing back from the host (wrong code, host vanished,
// host still staging) is told to leave instead of lingering in a phantom room.
const JOIN_TIMEOUT_MS = 30000
// A guest whose host has been silent this long (no media offer, no state sync,
// no close) drops the room instead of sitting in a zombie state forever.
const HOST_SILENCE_MS = 120000

class WatchPartyManager extends EventEmitter {
  constructor(options = {}) {
    super()
    this.engine = options.engine
    this.activeRoom = null // { roomCode, shareId, title, filePath, isHost, controlsMode, coreKey, participants: Map }
    this.discoveredRooms = new Map() // roomCode -> { roomCode, title, hostName, hostPeerId, duration, timestamp }
    this.reactionListeners = new Set()
    // peerId -> replication streams serving this room's media core (torn down
    // on leaveRoom / peer disconnect / engine stop).
    this._mediaStreams = new Map()
    this._mediaReadyNotified = false
    // Serializes public transitions (create/join/leave) so overlapping calls
    // can never interleave two half-built rooms on the same activeRoom slot.
    this._transitionChain = Promise.resolve()
    this._joinTimers = new Set()
    // Throttle roster snapshots emitted on high-frequency peer-status updates.
    this._lastRosterEmitAt = 0
    // Engine listeners are instance fields so stop() can remove them.
    this._onTransferProgress = null
    this._onTransferCompleted = null
    this._onPeerDisconnected = null

    // Guest-side media state: surface an explicit source-ready / source-error
    // signal for the party transfer so UIs never have to poll the staging dir.
    if (this.engine && typeof this.engine.on === 'function') {
      this._onTransferProgress = (t) => {
        const room = this.activeRoom
        if (!room || room.isHost || !room.shareId) return
        if (!t || t.id !== room.shareId) return
        if (this._mediaReadyNotified) return
        // Only surface "source ready" once the transfer has verified enough of
        // the file head to be playable (the manifest's moov/prefix watermark).
        // Firing on the first progress tick handed the player a near-empty
        // .part — the "timeline but no video" failure.
        if (t.playable !== true && !(t.status === 'completed')) return
        this._mediaReadyNotified = true
        this.emit(PARTY_EVENTS.MEDIA_READY, {
          roomCode: room.roomCode,
          shareId: room.shareId,
          filename: t.filename,
          fileSize: room.fileSize,
          destPath: t.destPath || null,
          playable: true
        })
      }
      this._onTransferCompleted = (t) => {
        const room = this.activeRoom
        if (!room || room.isHost || !room.shareId) return
        if (!t || t.id !== room.shareId) return
        this._mediaReadyNotified = true
        this.emit(PARTY_EVENTS.MEDIA_READY, {
          roomCode: room.roomCode,
          shareId: room.shareId,
          filename: t.filename,
          fileSize: room.fileSize,
          destPath: t.destPath || null
        })
      }
      const onError = (t) => {
        const room = this.activeRoom
        if (!room || room.isHost || !room.shareId) return
        if (!t || t.id !== room.shareId) return
        this.emit(PARTY_EVENTS.MEDIA_ERROR, {
          roomCode: room.roomCode,
          shareId: room.shareId,
          status: t.status,
          error: t.error || 'party media transfer failed'
        })
      }
      this.engine.on(EVENTS.TRANSFER_PROGRESS, this._onTransferProgress)
      this.engine.on(EVENTS.TRANSFER_COMPLETED, this._onTransferCompleted)
      this.engine.on(EVENTS.TRANSFER_FAILED, onError)
      this.engine.on(EVENTS.TRANSFER_CANCELLED, onError)
      // A dropped connection must never leave a ghost participant in a live
      // room. This is the single source of truth for peer departure.
      this._onPeerDisconnected = (info) => {
        const peerId = (info && (info.peerId || info.id)) || ''
        if (!peerId || !this.activeRoom) return
        this._handlePeerGone(peerId)
      }
      this.engine.on(EVENTS.PEER_DISCONNECTED, this._onPeerDisconnected)
    }

    // Periodic room maintenance: host re-announcement + guest host-silence
    // watchdog. Unref'd so it never keeps the process alive.
    this._roomMaintenanceTimer = setInterval(() => {
      this._maintainRoom()
    }, 10000)
    if (this._roomMaintenanceTimer.unref) this._roomMaintenanceTimer.unref()
  }

  _serialize(fn) {
    const run = this._transitionChain.then(fn, fn)
    // Keep the chain alive even when one transition throws.
    this._transitionChain = run.catch(() => {})
    return run
  }

  // Canonicalize a room code: uppercase, strip separators, then restore the
  // canonical PARTY-XXXX-XXXX shape (same approach as normalizeDropCode).
  _normalizeRoomCode(code) {
    const clean = String(code || '')
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9]/g, '')
    let body = clean
    if (body.startsWith('PARTY')) body = body.slice(5)
    else if (body.startsWith('DROPGRP')) body = body.slice(7)
    if (body.length !== 8) return clean.startsWith('PARTY') ? clean : (clean ? `PARTY-${clean}` : '')
    return `PARTY-${body.slice(0, 4)}-${body.slice(4)}`
  }

  _armJoinTimer(room) {
    this._clearJoinTimer()
    const t = setTimeout(() => {
      this._joinTimers.delete(t)
      const cur = this.activeRoom
      if (!cur || cur.isHost || cur.roomCode !== room.roomCode) return
      if (cur._hostHeardAt) return
      this._failJoinTimeout(cur)
    }, JOIN_TIMEOUT_MS)
    if (t.unref) t.unref()
    this._joinTimers.add(t)
  }

  _clearJoinTimer() {
    for (const t of this._joinTimers) clearTimeout(t)
    this._joinTimers.clear()
  }

  _maintainRoom() {
    const room = this.activeRoom
    if (!room) return
    if (room.isHost) {
      this._broadcastAnnouncement('create')
      return
    }
    // Guest watchdog: if the host has been silent for a long time (its
    // connection is gone and no leave/close ever arrived), drop the room.
    const now = Date.now()
    const lastHostActivity = Math.max(room.joinedAt || 0, room._hostHeardAt || 0)
    if (now - lastHostActivity > HOST_SILENCE_MS) {
      this._autoLeaveStaleRoom(room, 'host unreachable')
    }
  }

  /**
   * Host creates a new Watch Party room.
   */
  createRoom(params = {}) {
    return this._serialize(async () => {
      if (this.activeRoom) {
        throw new Error('Already in a watch party — leave it before starting another')
      }
      const { title, filePath, controlsMode = 'host', isPrivate = false, subtitlePath = null, rewindWindowSec = 0 } = params
      if (!filePath) throw new Error('File path required to create a watch party')

      const filename = path.basename(filePath)
      const roomCode = generateDropCode({ isGroup: true }).replace('DROP-GRP-', 'PARTY-')

      let fileSize = 0
      try {
        const stat = await fsp.stat(filePath)
        fileSize = stat.size
      } catch (err) {
        throw new Error(`Party media file not readable: ${err.message}`)
      }
      if (!(fileSize > 0)) {
        throw new Error('Party media file is empty')
      }

      const shareId = `watch-${roomCode.toLowerCase()}`
      const hostIdentity = this.engine.storage?.getDeviceIdentity?.() || { name: 'Host Device' }
      const transferEngine = this.engine && this.engine.transferEngine
      const fileType = transferEngine && typeof transferEngine.getFileType === 'function'
        ? transferEngine.getFileType(filename)
        : ''

      // Join the swarm topic BEFORE announcing, so a joiner's topic join races
      // the announcement rather than the reverse.
      const topic = `p2p-watch-${roomCode}`
      if (this.engine && this.engine.topicRegistry) {
        this.engine.topicRegistry.join(topic, { client: true, server: true })
      }

      const room = {
        roomCode,
        shareId,
        title: title || filename,
        filename,
        filePath,
        fileSize,
        fileType,
        coreKey: null,
        manifestHash: '',
        checksum: '',
        isHost: true,
        controlsMode,
        isPrivate: Boolean(isPrivate),
        // Guests may only rewind this many seconds behind the playback master
        // (0 = unlimited). Enforced host-side on incoming seeks.
        rewindWindowSec: Math.max(0, Number(rewindWindowSec) || 0),
        // Playback authority in 'host' controls mode (can be promoted away
        // from the media host via moderation).
        playbackPeerId: hostIdentity.id || 'host',
        // Latest authoritative position reported by the playback master —
        // used for the rewind-window rule and guest catch-up.
        hostPositionSec: null,
        muted: new Set(),
        kicked: new Set(),
        chatLog: [],
        queue: [],
        mediaEpoch: 1,
        subtitle: null,
        hostPeerId: hostIdentity.id || 'host',
        hostName: hostIdentity.name || 'Host',
        participants: new Map(),
        createdAt: Date.now()
      }
      this.activeRoom = room

      // Stage the party media under the room's DETERMINISTIC id. Guests resolve
      // media by this id (desktop: /stream/transfer?id=<shareId>, mobile:
      // .p2p-staging/<shareId>), so the staging pass must exist before the room
      // is announced — the announcement implies a playable source.
      if (transferEngine && typeof transferEngine.stageDrop === 'function') {
        try {
          const staged = await transferEngine.stageDrop({
            transferId: shareId,
            coreName: shareId,
            filePath,
            filename,
            fileSize,
            fileType
          })
          room.coreKey = staged.coreKey
          room.manifestHash = staged.manifestHash
          room.checksum = staged.checksum
        } catch (err) {
          // Fail the room instead of announcing a party whose media can never
          // arrive. UIs get a thrown error from createRoom and stay in lobby.
          this._teardownRoomState(room)
          throw new Error(`Party media staging failed: ${err.message}`)
        }
      }

      if (!room.coreKey) {
        this._teardownRoomState(room)
        throw new Error('Party media staging did not produce a playable source')
      }

      // Optional subtitle sidecar: read + normalize to WebVTT now and ride it
      // inside every media offer — no second transfer pipeline needed.
      if (subtitlePath) {
        try {
          room.subtitle = await this._loadSubtitle(subtitlePath)
        } catch (err) {
          console.warn('[WatchPartyManager] subtitle load failed:', err.message)
        }
      }

      // Broadcast the initial room announcement to all connected peers. The
      // periodic 10s maintenance tick re-announces while the room stays live,
      // so late joiners / rediscovery keep working.
      this._broadcastAnnouncement('create')

      this.emit(PARTY_EVENTS.ROOM_CREATED, this.getRoomInfo())
      return this.getRoomInfo()
    })
  }

  /**
   * Join an existing Watch Party room as a peer.
   */
  joinRoom(params = {}) {
    return this._serialize(async () => {
      if (this.activeRoom) {
        throw new Error('Already in a watch party — leave it before joining another')
      }
      const { roomCode } = params
      if (!roomCode) throw new Error('Room code required to join')

      const cleanCode = this._normalizeRoomCode(roomCode)
      if (!cleanCode) throw new Error('Room code required to join')

      const topic = `p2p-watch-${cleanCode}`
      if (this.engine.topicRegistry) {
        this.engine.topicRegistry.join(topic, { client: true, server: true })
      }

      const myIdentity = this.engine.storage?.getDeviceIdentity?.() || { name: 'Peer Device' }

      const room = {
        roomCode: cleanCode,
        shareId: `watch-${cleanCode.toLowerCase()}`,
        title: cleanCode,
        isHost: false,
        controlsMode: 'host',
        isMuted: false,
        muted: new Set(),
        kicked: new Set(),
        chatLog: [],
        mediaEpoch: 1,
        subtitle: null,
        playbackPeerId: null,
        hostPositionSec: null,
        participants: new Map(),
        joinedAt: Date.now()
      }
      this.activeRoom = room
      this._mediaReadyNotified = false

      // Send join message to the swarm
      this._broadcastRoomMessage({
        type: 'WATCH_ROOM_JOIN',
        roomCode: cleanCode,
        peer: {
          id: myIdentity.id || 'peer',
          name: myIdentity.name || 'Peer'
        }
      })

      this.emit(PARTY_EVENTS.ROOM_JOINED, this.getRoomInfo())
      // If the host never answers (wrong code / host gone), surface it.
      this._armJoinTimer(room)
      return this.getRoomInfo()
    })
  }

  _failJoinTimeout(room) {
    this._clearJoinTimer()
    const prev = room
    this._teardownRoomState(prev)
    this.emit(PARTY_EVENTS.ROOM_CLOSED, {
      roomCode: prev.roomCode,
      reason: 'join-timeout',
      error: 'No host responded — check the room code and try again.'
    })
  }

  _autoLeaveStaleRoom(room) {
    if (this.activeRoom !== room) return
    const prev = room
    this._teardownRoomState(prev)
    if (!prev.isHost) {
      this.emit(PARTY_EVENTS.ROOM_CLOSED, {
        roomCode: prev.roomCode,
        reason: 'host-lost',
        error: 'The host is no longer reachable — the party ended.'
      })
    }
  }

  /**
   * Leave the currently active Watch Party room.
   */
  leaveRoom() {
    return this._serialize(async () => {
      if (!this.activeRoom) return null

      const room = this.activeRoom

      if (room.isHost) {
        this._broadcastAnnouncement('close')
        this.emit(PARTY_EVENTS.ROOM_CLOSED, {
          roomCode: room.roomCode,
          reason: 'host-left'
        })
      } else {
        const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
        this._broadcastRoomMessage({
          type: 'WATCH_ROOM_LEAVE',
          roomCode: room.roomCode,
          peerId: myIdentity.id
        })
      }

      this._teardownRoomState(room)

      if (!room.isHost) {
        this.emit(PARTY_EVENTS.ROOM_LEFT, { roomCode: room.roomCode })
      }
      return { success: true }
    })
  }

  _teardownRoomState(room) {
    // Idempotent teardown of everything tied to a specific room object.
    if (this.activeRoom === room) this.activeRoom = null
    this._clearJoinTimer()

    if (this.engine && this.engine.topicRegistry) {
      try {
        this.engine.topicRegistry.leave(`p2p-watch-${room.roomCode}`)
      } catch {}
    }

    // Tear down the per-peer media replication streams this room opened.
    for (const [, streams] of this._mediaStreams) {
      for (const s of streams) {
        try {
          s.destroy()
        } catch {}
      }
    }
    this._mediaStreams.clear()

    // Drop references to this room's streams on the peer objects.
    if (this.engine && this.engine.peers) {
      for (const [, peerObj] of this.engine.peers.entries()) {
        if (peerObj && peerObj.partyMediaStream) {
          try {
            peerObj.partyMediaStream.destroy()
          } catch {}
          peerObj.partyMediaStream = null
        }
        if (peerObj && peerObj.dropStreams) {
          peerObj.dropStreams = peerObj.dropStreams.filter((s) => {
            if (!s || !room || s._watchPartyRoom !== room.roomCode) return true
            try {
              s.destroy()
            } catch {}
            return false
          })
        }
      }
    }

    this._mediaReadyNotified = false
  }

  _handlePeerGone(peerId) {
    const room = this.activeRoom
    if (!room) return
    if (room.participants && room.participants.has(peerId)) {
      room.participants.delete(peerId)
      this.emit(PARTY_EVENTS.PEER_LEFT, {
        roomCode: room.roomCode,
        peerId,
        reason: 'disconnect'
      })
      this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
    }
    // Drop any per-peer media streams for this peer.
    const streams = this._mediaStreams.get(peerId)
    if (streams) {
      for (const s of streams) {
        try {
          s.destroy()
        } catch {}
      }
      this._mediaStreams.delete(peerId)
    }
    const peerObj = this.engine && this.engine.peers ? this.engine.peers.get(peerId) : null
    if (peerObj) {
      if (peerObj.partyMediaStream) {
        try {
          peerObj.partyMediaStream.destroy()
        } catch {}
        peerObj.partyMediaStream = null
      }
      if (peerObj.dropStreams) {
        peerObj.dropStreams = peerObj.dropStreams.filter((s) => !s || s.destroyed)
      }
    }
  }

  /**
   * Broadcast playback state (play/pause/seek) within the active room.
   */
  broadcastPlaybackState(params = {}) {
    if (!this.activeRoom) return false

    const { action, positionSec, playbackRate = 1.0 } = params
    const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}

    // The host is its own playback master by default: record the position
    // locally (self-broadcasts never loop back through handleMessage).
    const room0 = this.activeRoom
    const masterId0 = room0.playbackPeerId || room0.hostPeerId
    if (typeof positionSec === 'number' && (!masterId0 || masterId0 === myIdentity.id)) {
      room0.hostPositionSec = positionSec
    }
    // Remember the last action so a late-joiner push reflects the host's real
    // state (a paused host must not join-push action 'play').
    room0._lastAction = action || 'play'

    const payload = {
      type: 'WATCH_STATE_SYNC',
      roomCode: this.activeRoom.roomCode,
      action: action || 'play',
      positionSec: typeof positionSec === 'number' ? positionSec : 0,
      playbackRate,
      timestampMs: Date.now(),
      sender: {
        id: myIdentity.id,
        name: myIdentity.name
      }
    }

    this._broadcastRoomMessage(payload)
    this.emit(PARTY_EVENTS.STATE_SYNC, payload)
    return true
  }

  /**
   * Broadcast peer status (buffering, current playhead).
   */
  broadcastPeerStatus(params = {}) {
    if (!this.activeRoom) return false

    const { positionSec, buffering, bufferedPercent = 0 } = params
    const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}

    // Track the playback master's authoritative position (host reports its
    // own playhead here; guests see it via the same message).
    const room0 = this.activeRoom
    const masterId0 = room0.playbackPeerId || room0.hostPeerId
    if (typeof positionSec === 'number' && (!masterId0 || masterId0 === myIdentity.id)) {
      room0.hostPositionSec = positionSec
    }

    const payload = {
      type: 'WATCH_PEER_STATUS',
      roomCode: this.activeRoom.roomCode,
      peerId: myIdentity.id,
      peerName: myIdentity.name,
      positionSec: typeof positionSec === 'number' ? positionSec : 0,
      buffering: Boolean(buffering),
      bufferedPercent
    }

    this._broadcastRoomMessage(payload)
    return true
  }

  /**
   * Send a real-time emoji reaction (🍿, 🔥, 👏, ❤️, 😂).
   * positionSec stamps the reaction at the sender's current playhead so UIs
   * can pop it when playback reaches that moment.
   */
  sendReaction(emoji, positionSec = null) {
    if (!this.activeRoom) return false

    const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
    const payload = {
      type: 'WATCH_REACTION',
      roomCode: this.activeRoom.roomCode,
      reactionId: `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
      emoji: emoji || '🍿',
      positionSec: typeof positionSec === 'number' ? positionSec : null,
      senderName: myIdentity.name || 'Peer',
      timestamp: Date.now()
    }

    this._broadcastRoomMessage(payload)
    this.emit(PARTY_EVENTS.REACTION, payload)
    return true
  }

  // ─── Chat ─────────────────────────────────────────────────────────────────

  /**
   * Send a room chat message. Broadcast to peers, kept in the local log
   * (host keeps the authoritative copy for late-joiner replay), emitted to
   * the local UI. Muted guests may still chat — mute is voice-only.
   */
  sendChat(text) {
    const room = this.activeRoom
    if (!room) return false
    const clean = String(text || '').trim().slice(0, CHAT_TEXT_MAX)
    if (!clean) return false

    const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
    const payload = {
      type: 'WATCH_CHAT',
      roomCode: room.roomCode,
      messageId: `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
      text: clean,
      sender: { id: myIdentity.id, name: myIdentity.name || 'Peer' },
      timestamp: Date.now()
    }

    this._appendChat(room, payload)
    this._broadcastRoomMessage(payload)
    this.emit(PARTY_EVENTS.CHAT, payload)
    return true
  }

  getChatHistory() {
    return this.activeRoom ? this.activeRoom.chatLog.slice() : []
  }

  _appendChat(room, msg) {
    // The same message
    // can legitimately arrive twice. messageId is the dedup capability.
    if (msg.messageId && room.chatLog.some((m) => m && m.messageId === msg.messageId)) return
    room.chatLog.push(msg)
    if (room.chatLog.length > CHAT_LOG_MAX) room.chatLog.splice(0, room.chatLog.length - CHAT_LOG_MAX)
  }

  // ─── Moderation (host-authoritative) ──────────────────────────────────────

  /**
   * Host: kick a guest (connection dropped + code stays valid for others),
   * mute/unmute their voice, or promote them to playback master (they drive
   * playback; media stays staged here).
   */
  moderate(params = {}) {
    const room = this.activeRoom
    if (!room) return { success: false, error: 'not in a room' }
    if (!room.isHost) return { success: false, error: 'host only' }
    const { action, targetPeerId } = params
    if (!action || !targetPeerId) return { success: false, error: 'action and targetPeerId required' }
    if (targetPeerId === room.hostPeerId) return { success: false, error: 'cannot moderate the host' }
    if (!room.participants.has(targetPeerId)) return { success: false, error: 'peer not in room' }

    const msg = {
      type: 'WATCH_MODERATE',
      roomCode: room.roomCode,
      action,
      targetPeerId,
      by: { id: room.hostPeerId, name: room.hostName },
      timestamp: Date.now()
    }

    if (action === 'kick') {
      room.kicked.add(targetPeerId)
      room.participants.delete(targetPeerId)
      this._broadcastRoomMessage(msg)
      this._destroyPeerMedia(targetPeerId)
      const peerObj = this.engine && this.engine.peers ? this.engine.peers.get(targetPeerId) : null
      if (peerObj && peerObj.connection && typeof peerObj.connection.destroy === 'function') {
        try {
          peerObj.connection.destroy()
        } catch {}
      }
      this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
    } else if (action === 'mute' || action === 'unmute') {
      if (action === 'mute') room.muted.add(targetPeerId)
      else room.muted.delete(targetPeerId)
      const p = room.participants.get(targetPeerId)
      if (p) p.isMuted = action === 'mute'
      this._broadcastRoomMessage(msg)
      this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
    } else if (action === 'promote') {
      room.playbackPeerId = targetPeerId
      this._broadcastRoomMessage(msg)
      this._broadcastAnnouncement('create')
      this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
    } else {
      return { success: false, error: `unknown action: ${action}` }
    }

    this.emit(PARTY_EVENTS.MODERATED, { ...msg, applied: true })
    return { success: true }
  }

  /** Host: let guests rewind at most this many seconds behind the master. */
  setRewindWindow(seconds) {
    const room = this.activeRoom
    if (!room || !room.isHost) return false
    room.rewindWindowSec = Math.max(0, Number(seconds) || 0)
    this._broadcastAnnouncement('create')
    this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
    return true
  }

  // ─── Queue (host) ─────────────────────────────────────────────────────────

  /** Host: append a local file to the party queue. */
  async queueAdd(params = {}) {
    const room = this.activeRoom
    if (!room) throw new Error('not in a room')
    if (!room.isHost) throw new Error('host only')
    const { filePath, title } = params
    if (!filePath) throw new Error('filePath required')
    const filename = path.basename(filePath)
    let fileSize = 0
    try {
      const stat = await fsp.stat(filePath)
      fileSize = stat.size
    } catch (err) {
      throw new Error(`Queue file not readable: ${err.message}`)
    }
    if (!(fileSize > 0)) throw new Error('Queue file is empty')
    room.queue.push({ filePath, filename, title: title || filename, fileSize })
    this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
    return this.getRoomInfo()
  }

  /** Host: drop a queued item by index. */
  queueRemove(index) {
    const room = this.activeRoom
    if (!room || !room.isHost) return { success: false }
    const i = Number(index) || 0
    if (i < 0 || i >= room.queue.length) return { success: false, error: 'bad index' }
    room.queue.splice(i, 1)
    this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
    return { success: true }
  }

  /**
   * Host: retire the current media and stage the next queued item. Bumps the
   * room's mediaEpoch — every epoch derives a fresh deterministic shareId, so
   * guests who still hold the previous file keep it while a NEW core (and
   * transfer record) is created for this one. The new hypercore multiplexes
   * onto the peers' existing replication session (hypercore 11 channel mux).
   */
  async queueNext() {
    const room = this.activeRoom
    if (!room || !room.isHost) throw new Error('host only')
    const item = room.queue.shift()
    if (!item) return null

    const transferEngine = this.engine && this.engine.transferEngine
    if (!transferEngine || typeof transferEngine.stageDrop !== 'function') {
      throw new Error('transfer engine unavailable')
    }

    room.mediaEpoch += 1
    room.shareId = this._shareIdForEpoch(room.roomCode, room.mediaEpoch)
    room.filePath = item.filePath
    room.filename = item.filename
    room.title = item.title
    room.fileSize = item.fileSize
    room.fileType = typeof transferEngine.getFileType === 'function' ? transferEngine.getFileType(item.filename) : ''
    room.subtitle = null
    room._hostHeardAt = Date.now()

    const staged = await transferEngine.stageDrop({
      transferId: room.shareId,
      coreName: room.shareId,
      filePath: room.filePath,
      filename: room.filename,
      fileSize: room.fileSize,
      fileType: room.fileType
    })
    room.coreKey = staged.coreKey
    room.manifestHash = staged.manifestHash
    room.checksum = staged.checksum

    this._mediaReadyNotified = false
    // Re-announce (title changed) and re-offer the new media to everyone in
    // the room. Guests get a fresh MEDIA_OFFER → MEDIA_READY cycle per epoch.
    this._broadcastAnnouncement('create')
    for (const peerId of room.participants.keys()) {
      try {
        await this._serveMediaToPeer(peerId)
      } catch (err) {
        console.warn('[WatchPartyManager] queue re-offer failed:', err.message)
      }
    }
    this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
    return this.getRoomInfo()
  }

  // ─── Voice (push-to-talk over the signaling channel) ──────────────────────

  /**
   * Broadcast a raw PCM voice chunk (base64, mono 16kHz s16le). Chunks are
   * small (~3KB per 100ms) and infrequent (only while someone holds PTT), so
   * they ride the regular signaling channel fine on LAN. Muted senders are
   * stopped locally; the host also drops chunks from muted peers.
   */
  sendVoiceChunk(params = {}) {
    const room = this.activeRoom
    if (!room) return false
    if (room.isMuted) return false
    if (typeof params.audioB64 !== 'string' || !params.audioB64 || params.audioB64.length > 200000) return false

    const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
    this._broadcastRoomMessage({
      type: 'WATCH_VOICE',
      roomCode: room.roomCode,
      seq: params.seq || 0,
      durationMs: params.durationMs || 100,
      audioB64: params.audioB64,
      sender: { id: myIdentity.id, name: myIdentity.name || 'Peer' },
      timestamp: Date.now()
    })
    return true
  }

  /** Guest: after being muted by the host, stop sending voice locally too. */
  setLocalMuted(muted) {
    const room = this.activeRoom
    if (!room) return false
    room.isMuted = Boolean(muted)
    return true
  }

  // ─── Subtitles ────────────────────────────────────────────────────────────

  /** Host: attach/replace the subtitle sidecar (converted to VTT). */
  async setSubtitle(subtitlePath) {
    const room = this.activeRoom
    if (!room || !room.isHost) return { success: false, error: 'host only' }
    if (!subtitlePath) {
      room.subtitle = null
    } else {
      room.subtitle = await this._loadSubtitle(subtitlePath)
    }
    // Push the updated track with the (unchanged) media offer.
    for (const peerId of room.participants.keys()) {
      try {
        await this._serveMediaToPeer(peerId)
      } catch {}
    }
    this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
    return { success: true, subtitle: room.subtitle ? { filename: room.subtitle.filename } : null }
  }

  /** Current subtitle sidecar ({filename, vtt}) or null — served to local UI. */
  getSubtitle() {
    return this.activeRoom && this.activeRoom.subtitle ? { ...this.activeRoom.subtitle } : null
  }

  async _loadSubtitle(subtitlePath) {
    const raw = await fsp.readFile(subtitlePath, 'utf8')
    if (Buffer.byteLength(raw, 'utf8') > SUBTITLE_MAX_BYTES) {
      throw new Error('subtitle file too large')
    }
    const filename = path.basename(subtitlePath)
    const isVtt = /\.vtt$/i.test(filename)
    return { filename, vtt: isVtt ? raw : this._srtToVtt(raw) }
  }

  _srtToVtt(srt) {
    const body = String(srt)
      .replace(/\r+/g, '')
      .replace(/^\uFEFF/, '')
      .replace(/(\d{2}:\d{2}:\d{2}),(\d{3})/g, '$1.$2')
    return `WEBVTT\n\n${body.trim()}\n`
  }

  _shareIdForEpoch(roomCode, epoch) {
    const base = `watch-${String(roomCode).toLowerCase()}`
    return epoch > 1 ? `${base}-e${epoch}` : base
  }

  _sendToPeer(peerId, msg) {
    const peerObj = this.engine && this.engine.peers ? this.engine.peers.get(peerId) : null
    if (peerObj && peerObj.signaling && typeof peerObj.signaling.send === 'function') {
      try {
        peerObj.signaling.send(msg)
      } catch {}
    }
  }

  _destroyPeerMedia(peerId) {
    const streams = this._mediaStreams.get(peerId)
    if (streams) {
      for (const s of streams) {
        try {
          s.destroy()
        } catch {}
      }
      this._mediaStreams.delete(peerId)
    }
    const peerObj = this.engine && this.engine.peers ? this.engine.peers.get(peerId) : null
    if (peerObj && peerObj.partyMediaStream) {
      try {
        peerObj.partyMediaStream.destroy()
      } catch {}
      peerObj.partyMediaStream = null
    }
  }

  /**
   * List all discovered active rooms hosted across the swarm.
   */
  listDiscoveredRooms() {
    const now = Date.now()
    const valid = []
    for (const [code, room] of this.discoveredRooms.entries()) {
      // The host re-announces every 10s (_maintainRoom). A stale entry means
      // the room is gone or unreachable — offering it in "Join with Room
      // Code" lists a party that can never answer the join.
      if (now - room.timestamp < 35000) {
        valid.push(room)
      } else {
        this.discoveredRooms.delete(code)
      }
    }
    return valid
  }

  /**
   * Get active room details.
   */
  getRoomInfo() {
    if (!this.activeRoom) return null
    return {
      roomCode: this.activeRoom.roomCode,
      shareId: this.activeRoom.shareId || null,
      title: this.activeRoom.title,
      filename: this.activeRoom.filename,
      filePath: this.activeRoom.filePath,
      fileSize: this.activeRoom.fileSize,
      isHost: this.activeRoom.isHost,
      controlsMode: this.activeRoom.controlsMode,
      isPrivate: Boolean(this.activeRoom.isPrivate),
      rewindWindowSec: this.activeRoom.rewindWindowSec || 0,
      playbackPeerId: this.activeRoom.playbackPeerId || this.activeRoom.hostPeerId,
      mediaEpoch: this.activeRoom.mediaEpoch || 1,
      queue: (this.activeRoom.queue || []).map((q) => ({
        title: q.title,
        filename: q.filename,
        fileSize: q.fileSize
      })),
      subtitleName: this.activeRoom.subtitle ? this.activeRoom.subtitle.filename : null,
      hostPositionSec: typeof this.activeRoom.hostPositionSec === 'number' ? this.activeRoom.hostPositionSec : null,
      isLocalMuted: Boolean(this.activeRoom.isMuted),
      hostPeerId: this.activeRoom.hostPeerId,
      hostName: this.activeRoom.hostName,
      participantCount: this.activeRoom.participants.size + 1,
      participants: Array.from(this.activeRoom.participants.values())
    }
  }

  /**
   * Handle incoming signaling message from a peer.
   */
  handleMessage(peerId, msg) {
    if (!msg || typeof msg.type !== 'string') return

    if (msg.type === 'WATCH_ROOM_ANNOUNCE') {
      if (msg.action === 'close') {
        this.discoveredRooms.delete(msg.roomCode)
        // If we're a guest in the room whose host just closed it, leave.
        this._handleHostClosed(msg.roomCode)
      } else if (msg.roomCode && msg.title) {
        // A guest already in this room is still hearing the host —
        // count it as host activity so the silence watchdog never drops a
        // room whose direct link merely stuttered.
        if (this.activeRoom && !this.activeRoom.isHost && this.activeRoom.roomCode === msg.roomCode) {
          this.activeRoom._hostHeardAt = Date.now()
        }
        // Private rooms are code-only: joinable, never discoverable.
        if (!msg.isPrivate) {
          this.discoveredRooms.set(msg.roomCode, {
            roomCode: msg.roomCode,
            title: msg.title,
            hostName: msg.hostName,
            hostPeerId: peerId,
            timestamp: Date.now()
          })
        }
      }
      this.emit(PARTY_EVENTS.DISCOVERED_ROOMS, this.listDiscoveredRooms())
    } else if (msg.type === 'WATCH_ROOM_JOIN') {
      if (this.activeRoom && this.activeRoom.isHost && this.activeRoom.roomCode === msg.roomCode) {
        // Kick enforcement: the join capability (room code) never expires, so
        // a removed peer's id is denylisted for the life of the room.
        if (this.activeRoom.kicked.has(peerId)) {
          this._sendToPeer(peerId, {
            type: 'WATCH_MODERATE',
            roomCode: msg.roomCode,
            action: 'kick',
            targetPeerId: peerId,
            by: { id: this.activeRoom.hostPeerId, name: this.activeRoom.hostName },
            timestamp: Date.now()
          })
          return
        }
        const isRejoin = this.activeRoom.participants.has(peerId)
        this.activeRoom.participants.set(peerId, {
          peerId,
          name: msg.peer?.name || 'Peer',
          joinedAt: Date.now(),
          status: 'connected',
          isMuted: this.activeRoom.muted.has(peerId)
        })
        this.emit(PARTY_EVENTS.PEER_JOINED, {
          roomCode: msg.roomCode,
          peer: { id: peerId, name: msg.peer?.name || 'Peer' }
        })
        // Roster changed: push a fresh room snapshot so the host UI roster
        // reflects the new member immediately.
        this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
        // Authorized by room membership: this peer just proved knowledge of the
        // room code. Hand it the staged media descriptor + per-core replication
        // so its data plane comes up alongside the control plane it joined for.
        // A rejoin on a live connection reuses the existing stream (no-op).
        if (!isRejoin) {
          // Late joiner: replay the recent chat so the panel isn't empty.
          if (this.activeRoom.chatLog.length > 0) {
            this._sendToPeer(peerId, {
              type: 'WATCH_CHAT_HISTORY',
              roomCode: msg.roomCode,
              messages: this.activeRoom.chatLog.slice(-CHAT_HISTORY_REPLAY)
            })
          }
          // Push the host's current playback state so the guest starts where
          // the party actually is instead of at 0:00 while waiting for the
          // next sync.
          if (typeof this.activeRoom.hostPositionSec === 'number') {
            this._sendToPeer(peerId, {
              type: 'WATCH_STATE_SYNC',
              roomCode: msg.roomCode,
              action: this.activeRoom._lastAction || 'play',
              positionSec: this.activeRoom.hostPositionSec,
              playbackRate: 1,
              timestampMs: Date.now(),
              sender: { id: this.activeRoom.hostPeerId, name: this.activeRoom.hostName }
            })
          }
          this._serveMediaToPeer(peerId).catch((err) => {
            this.emit(PARTY_EVENTS.MEDIA_ERROR, {
              roomCode: this.activeRoom && this.activeRoom.roomCode,
              shareId: this.activeRoom && this.activeRoom.shareId,
              error: err && err.message ? err.message : 'media serve failed'
            })
          })
        }
      }
    } else if (msg.type === 'WATCH_MEDIA_OFFER') {
      this._handleMediaOffer(peerId, msg).catch(() => {})
    } else if (msg.type === 'WATCH_ROOM_LEAVE') {
      if (this.activeRoom && this.activeRoom.isHost && this.activeRoom.roomCode === msg.roomCode) {
        this.activeRoom.participants.delete(peerId)
        this.emit(PARTY_EVENTS.PEER_LEFT, { roomCode: msg.roomCode, peerId })
        this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
      }
    } else if (msg.type === 'WATCH_PEER_STATUS') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        const room = this.activeRoom
        const p = room.participants.get(peerId)
        if (p) {
          p.positionSec = msg.positionSec
          p.buffering = msg.buffering
          p.bufferedPercent = msg.bufferedPercent
          // Throttle the roster snapshot so rapid peer-status messages do not
          // spam the UI at 60Hz.
          const now = Date.now()
          if (!this._lastRosterEmitAt || now - this._lastRosterEmitAt > 500) {
            this._lastRosterEmitAt = now
            this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
          }
        }
        // Guests learn the master's authoritative playhead from its status.
        const masterId = room.playbackPeerId || room.hostPeerId
        if (!room.isHost && masterId && msg.peerId && msg.peerId === masterId && typeof msg.positionSec === 'number') {
          room.hostPositionSec = msg.positionSec
          room._hostHeardAt = Date.now()
        }
        this.emit(PARTY_EVENTS.PEER_STATUS, msg)
      }
    } else if (msg.type === 'WATCH_STATE_SYNC') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        const room = this.activeRoom
        const masterId = room.playbackPeerId || room.hostPeerId
        const senderId = (msg.sender && msg.sender.id) || peerId
        // The host spoke — a guest must not be watchdog-dropped while
        // its state-sync heartbeat keeps arriving.
        if (!room.isHost && senderId === (room.hostPeerId || masterId)) {
          room._hostHeardAt = Date.now()
        }
        // Host-authority: in 'host' mode only the playback master may drive
        // playback. Guests never match the master id, so their syncs are
        // dropped — unless the room is 'open' (collaborative).
        if (room.controlsMode === 'host' && room.isHost) {
          if (masterId && senderId !== masterId) return
        }
        // Track the playback master's authoritative position (used by the
        // rewind-window rule and guest catch-up).
        if (masterId && senderId === masterId && typeof msg.positionSec === 'number') {
          room.hostPositionSec = msg.positionSec
        }
        // Rewind window: in open rooms the host rejects guest seeks further
        // back than the window and re-asserts the authoritative position so
        // the seeking guest snaps forward again.
        if (
          room.isHost &&
          room.rewindWindowSec > 0 &&
          senderId !== masterId &&
          msg.action === 'seek' &&
          typeof msg.positionSec === 'number' &&
          typeof room.hostPositionSec === 'number' &&
          msg.positionSec < room.hostPositionSec - room.rewindWindowSec
        ) {
          this.broadcastPlaybackState({ action: 'seek', positionSec: room.hostPositionSec })
          return
        }
        // Guest-side guard: outside 'open' rooms only accept syncs from the
        // master, so a rogue guest cannot drive other guests.
        if (!room.isHost && room.controlsMode !== 'open' && masterId && senderId !== masterId) return
        // Track host liveness for the guest host-silence watchdog.
        if (!room.isHost) room._hostHeardAt = Date.now()
        this.emit(PARTY_EVENTS.STATE_SYNC, msg)
      }
    } else if (msg.type === 'WATCH_REACTION') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        // Room traffic can be duplicated across peers — the same reaction
        // arrives twice. Dedup on reactionId so the UI pops it once.
        if (msg.reactionId) {
          const rid = msg.reactionId
          if (!this._seenReactions) this._seenReactions = new Set()
          if (this._seenReactions.has(rid)) return
          this._seenReactions.add(rid)
          if (this._seenReactions.size > 512) {
            this._seenReactions = new Set(Array.from(this._seenReactions).slice(-256))
          }
        }
        this.emit(PARTY_EVENTS.REACTION, msg)
      }
    } else if (msg.type === 'WATCH_CHAT') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode && msg.text) {
        // Both sides log received messages; the host's log additionally feeds
        // the late-joiner history replay. _appendChat dedups by messageId —
        // the same message can arrive more than once —
        // and a duplicate must not re-emit to the UI either.
        const seen = this.activeRoom.chatLog.some((m) => m && m.messageId && m.messageId === msg.messageId)
        this._appendChat(this.activeRoom, msg)
        if (!seen) this.emit(PARTY_EVENTS.CHAT, msg)
      }
    } else if (msg.type === 'WATCH_CHAT_HISTORY') {
      if (this.activeRoom && !this.activeRoom.isHost && this.activeRoom.roomCode === msg.roomCode) {
        const room = this.activeRoom
        if (Array.isArray(msg.messages)) {
          room.chatLog = msg.messages.filter((m) => m && m.text).slice(-CHAT_LOG_MAX)
        }
        this.emit(PARTY_EVENTS.CHAT_HISTORY, { roomCode: msg.roomCode, messages: room.chatLog.slice() })
      }
    } else if (msg.type === 'WATCH_MODERATE') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode && msg.targetPeerId) {
        const room = this.activeRoom
        if (room.isHost) return
        const me = this.engine.storage?.getDeviceIdentity?.() || {}
        if (msg.action === 'kick' && msg.targetPeerId === me.id) {
          this.emit(PARTY_EVENTS.MODERATED, { ...msg, reason: 'kicked' })
          this._clearJoinTimer()
          const prev = room
          this._teardownRoomState(prev)
          this.emit(PARTY_EVENTS.ROOM_CLOSED, {
            roomCode: prev.roomCode,
            reason: 'kicked',
            error: 'You were removed from the party by the host.'
          })
          return
        }
        if (msg.action === 'mute' && msg.targetPeerId === me.id) {
          room.isMuted = true
        } else if (msg.action === 'unmute' && msg.targetPeerId === me.id) {
          room.isMuted = false
        } else if (msg.action === 'promote') {
          // Playback authority moved: the promoted guest now drives playback.
          room.playbackPeerId = msg.targetPeerId
        }
        this.emit(PARTY_EVENTS.MODERATED, msg)
        this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
      }
    } else if (msg.type === 'WATCH_VOICE') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode && msg.audioB64) {
        const room = this.activeRoom
        const senderId = (msg.sender && msg.sender.id) || peerId
        // Defense in depth: muted senders stop locally; host and peers also
        // drop their chunks.
        if (senderId && room.muted.has(senderId)) return
        this.emit(PARTY_EVENTS.VOICE, msg)
      }
    }
  }

  _handleHostClosed(roomCode) {
    const room = this.activeRoom
    if (!room || room.isHost || room.roomCode !== roomCode) return
    this._clearJoinTimer()
    const prev = room
    this._teardownRoomState(prev)
    this.emit(PARTY_EVENTS.ROOM_CLOSED, {
      roomCode: prev.roomCode,
      reason: 'host-left'
    })
  }

  /**
   * Host: serve the staged party media to a joining peer. Opens a per-core
   * replication stream (the exchange store is never exposed) and sends the
   * media descriptor to that peer ONLY — room membership is the authorization.
   */
  async _serveMediaToPeer(peerId) {
    const room = this.activeRoom
    if (!room || !room.isHost || !room.coreKey || !room.shareId) return

    const peerObj = this.engine && this.engine.peers ? this.engine.peers.get(peerId) : null
    const exchangeStore = this.engine.storage && this.engine.storage.exchangeStore
    if (!peerObj || !peerObj.connection || !exchangeStore) return

    // A party media connection may never verify a pairing challenge (guests
    // join by room code, not pairing): drop the watchdog so a long transfer is
    // not killed mid-stream — the same contract the claims flow uses.
    if (peerObj.pairing && peerObj.pairing.timeout) {
      clearTimeout(peerObj.pairing.timeout)
      peerObj.pairing.timeout = null
    }

    try {
      const core = exchangeStore.get({ name: room.shareId })
      await core.ready()
      // One protomux session per peer connection (hypercore 11 multiplexes
      // core channels on the cached session): a rejoining guest reuses the
      // same connection, and queue-advance attaches the NEXT epoch's core to
      // it instead of opening a second noise layer.
      const streamKey = room.coreKey
      if (peerObj.partyMediaStreamKey !== streamKey || !peerObj.partyMediaStream || peerObj.partyMediaStream.destroyed) {
        const stream = core.replicate(peerObj.connection, { live: true })
        stream._watchPartyRoom = room.roomCode
        peerObj.partyMediaStream = stream
        peerObj.partyMediaStreamKey = streamKey
        peerObj.dropStreams = peerObj.dropStreams || []
        peerObj.dropStreams.push(stream)
        this._trackMediaStream(peerId, stream)
      }
      console.log(
        `[WatchPartyManager] serving party media to ${peerId.slice(0, 12)}... (${room.filename})`
      )
    } catch (err) {
      console.warn('[WatchPartyManager] party media replication failed:', err.message)
      throw err
    }

    if (peerObj.signaling && typeof peerObj.signaling.send === 'function') {
      peerObj.signaling.send({
        type: 'WATCH_MEDIA_OFFER',
        roomCode: room.roomCode,
        shareId: room.shareId,
        transferId: room.shareId,
        filename: room.filename,
        fileSize: room.fileSize,
        fileType: room.fileType || '',
        coreKey: room.coreKey,
        manifestHash: room.manifestHash || '',
        checksum: room.checksum || '',
        // Host-authority mode rides on the offer so guests know whether they
        // may drive playback ('open') or must follow the host ('host').
        controlsMode: room.controlsMode || 'host',
        // Queue epoch: guests derive their shareId from this, so a new epoch
        // creates a fresh transfer instead of colliding with the old file.
        mediaEpoch: room.mediaEpoch || 1,
        // Which peer drives playback ('host' mode) — can be promoted away
        // from the media host.
        playbackPeerId: room.playbackPeerId || room.hostPeerId,
        rewindWindowSec: room.rewindWindowSec || 0,
        subtitle: room.subtitle || null,
        hostPositionSec: typeof room.hostPositionSec === 'number' ? room.hostPositionSec : null,
        sender: { id: room.hostPeerId, name: room.hostName }
      })
    }
  }

  /**
   * Guest: accept a party media descriptor from the host. Opens the receive
   * side of the per-core replication and starts the transfer under the room's
   * deterministic id so the UIs' existing resolution paths work unchanged.
   */
  async _handleMediaOffer(peerId, msg) {
    const room = this.activeRoom
    if (!room || room.isHost) return
    if (!msg || msg.roomCode !== room.roomCode) return

    const epoch = Number.isFinite(msg.mediaEpoch) && msg.mediaEpoch > 0 ? Math.floor(msg.mediaEpoch) : 1
    const shareId = this._shareIdForEpoch(room.roomCode, epoch)
    const transferId = typeof msg.transferId === 'string' && msg.transferId ? msg.transferId : ''
    // Only the room's own deterministic id (for this epoch) is this party's media.
    if (transferId && transferId !== shareId) return

    const { filename, fileSize, coreKey } = msg
    if (typeof coreKey !== 'string' || coreKey.length !== 64) return
    if (typeof fileSize !== 'number' || fileSize < 0) return
    if (typeof filename !== 'string' || filename.length === 0 || filename.length > 500) return

    room.shareId = shareId
    room.filename = filename
    room.fileSize = fileSize
    room.fileType = msg.fileType || room.fileType || ''
    room.mediaEpoch = epoch
    room.subtitle = msg.subtitle || null
    if (msg.controlsMode === 'host' || msg.controlsMode === 'open') {
      room.controlsMode = msg.controlsMode
    }
    if (typeof msg.rewindWindowSec === 'number' && msg.rewindWindowSec >= 0) {
      room.rewindWindowSec = msg.rewindWindowSec
    }
    if (typeof msg.hostPositionSec === 'number') {
      room.hostPositionSec = msg.hostPositionSec
    }
    if (typeof msg.playbackPeerId === 'string' && msg.playbackPeerId) {
      room.playbackPeerId = msg.playbackPeerId
    }
    // The host spoke with a VALID offer — it is definitely reachable. Clear
    // the join timeout and record host identity so getRoomInfo() is complete.
    room._hostHeardAt = Date.now()
    this._clearJoinTimer()
    let roomChanged = false
    if (msg.sender && typeof msg.sender === 'object') {
      if (msg.sender.id && msg.sender.id !== room.hostPeerId) {
        room.hostPeerId = msg.sender.id
        roomChanged = true
      }
      if (msg.sender.name && msg.sender.name !== room.hostName) {
        room.hostName = msg.sender.name
        roomChanged = true
      }
    }
    // The optimistic guest room's title is the bare code — once the offer tells
    // us the real media filename, surface the enriched room to the UI.
    if (room.title === room.roomCode || !room.title) {
      room.title = filename
      roomChanged = true
    }
    // The guest room now carries real media metadata + host identity. Emit a
    // fresh snapshot so the UI can show the host name / media info instead of
    // placeholders from the optimistic join.
    if (roomChanged) {
      this.emit(PARTY_EVENTS.ROOM_UPDATED, this.getRoomInfo())
    }

    const transferEngine = this.engine && this.engine.transferEngine
    if (!transferEngine) return

    // Already hold the finished media (rejoin / app restart): nothing to
    // transfer — report the source we have on disk.
    const existing = await this._getTransferRecord(shareId)
    if (existing && existing.status === STATUS.COMPLETED) {
      this._mediaReadyNotified = true
      this.emit(PARTY_EVENTS.MEDIA_READY, {
        roomCode: room.roomCode,
        shareId,
        filename: existing.filename,
        fileSize: existing.fileSize,
        destPath: existing.destPath || null
      })
      return
    }

    // Receive side of the per-core replication (pairs with the host's open).
    // One stream per peer per connection — a rejoin must reuse it. Like the
    // host side (and claims), drop the pairing watchdog: an unpaired guest's
    // media transfer must not be killed mid-stream by the 30s challenge timer.
    try {
      const peerObj = this.engine.peers ? this.engine.peers.get(peerId) : null
      const exchangeStore = this.engine.storage && this.engine.storage.exchangeStore
      if (peerObj && peerObj.connection && exchangeStore) {
        if (peerObj.pairing && peerObj.pairing.timeout) {
          clearTimeout(peerObj.pairing.timeout)
          peerObj.pairing.timeout = null
        }
        const core = exchangeStore.get(Buffer.from(coreKey, 'hex'))
        await core.ready()
        // Attach the new core's channel to the existing protomux session
        // (epoch changes reuse the connection; hypercore 11 multiplexes).
        if (peerObj.partyMediaStreamKey !== coreKey || !peerObj.partyMediaStream || peerObj.partyMediaStream.destroyed) {
          const stream = core.replicate(peerObj.connection, { live: true })
          stream._watchPartyRoom = room.roomCode
          peerObj.partyMediaStream = stream
          peerObj.partyMediaStreamKey = coreKey
          peerObj.dropStreams = peerObj.dropStreams || []
          peerObj.dropStreams.push(stream)
          this._trackMediaStream(peerId, stream)
        }
      }
    } catch (err) {
      console.warn('[WatchPartyManager] party media receive stream failed:', err.message)
    }

    try {
      const record = await transferEngine.receiveOffer(
        {
          transferId: shareId,
          filename,
          fileSize,
          fileType: msg.fileType || (typeof transferEngine.getFileType === 'function' ? transferEngine.getFileType(filename) : ''),
          coreKey,
          manifestHash: msg.manifestHash || '',
          checksum: msg.checksum || '',
          senderIdentity: msg.sender || { id: peerId, name: 'Host' },
          peerKey: peerId,
          shareId,
          source: 'watch'
        },
        { autoAccept: true }
      )

      if (!record) {
        // Duplicate offer: resume a parked receive if it can still deliver.
        if (
          existing &&
          [STATUS.PENDING_APPROVAL, STATUS.INTERRUPTED, STATUS.FAILED, STATUS.WAITING_PEER, STATUS.QUEUED, STATUS.PAUSED].indexOf(existing.status) !== -1 &&
          typeof transferEngine.approve === 'function'
        ) {
          await transferEngine.approve(shareId)
        }
        return
      }

      this.emit(PARTY_EVENTS.MEDIA_OFFER, {
        roomCode: room.roomCode,
        shareId,
        filename,
        fileSize
      })
    } catch (err) {
      console.warn('[WatchPartyManager] party media accept failed:', err.message)
      this.emit(PARTY_EVENTS.MEDIA_ERROR, {
        roomCode: room.roomCode,
        shareId,
        error: err.message
      })
    }
  }

  _trackMediaStream(peerId, stream) {
    const list = this._mediaStreams.get(peerId) || []
    list.push(stream)
    this._mediaStreams.set(peerId, list)
  }

  async _getTransferRecord(id) {
    try {
      const bee = await this.engine.getBee('transfers')
      const entry = await bee.get(id)
      return entry && entry.value
    } catch {
      return null
    }
  }

  _broadcastAnnouncement(action = 'create') {
    const room = this.activeRoom
    if (!room || !room.isHost) return
    const hostIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
    // Discovery needs the room code (it IS the join capability), but the
    // human-readable details only go to PAIRED peers — untrusted strangers on
    // the swarm learn that a party exists, not who hosts it or what's playing.
    for (const [, peerObj] of this.engine.peers?.entries() || []) {
      if (!peerObj || !peerObj.signaling || typeof peerObj.signaling.send !== 'function') continue
      const trusted = !!(peerObj.pairing && peerObj.pairing.trusted)
      try {
        peerObj.signaling.send({
          type: 'WATCH_ROOM_ANNOUNCE',
          action,
          roomCode: room.roomCode,
          title: trusted || action === 'close' ? room.title : 'Watch Party',
          hostName: trusted ? hostIdentity.name || 'Host' : undefined,
          isPrivate: Boolean(room.isPrivate),
          timestamp: Date.now()
        })
      } catch {}
    }
  }

  _broadcastRoomMessage(msg) {
    this._broadcastToAllPeers(msg)
  }

  _broadcastToAllPeers(msg) {
    if (!this.engine || !this.engine.peers) return
    for (const [, peerObj] of this.engine.peers.entries()) {
      if (peerObj && peerObj.signaling && typeof peerObj.signaling.send === 'function') {
        try {
          peerObj.signaling.send(msg)
        } catch {}
      }
    }
  }

  /**
   * Release everything this manager holds. Called from engine.stop() so a
   * restart never inherits a half-open room, timer, or listener.
   */
  stop() {
    this._clearJoinTimer()
    if (this._roomMaintenanceTimer) {
      clearInterval(this._roomMaintenanceTimer)
      this._roomMaintenanceTimer = null
    }
    if (this.engine && typeof this.engine.removeListener === 'function') {
      if (this._onTransferProgress) this.engine.removeListener(EVENTS.TRANSFER_PROGRESS, this._onTransferProgress)
      if (this._onTransferCompleted) this.engine.removeListener(EVENTS.TRANSFER_COMPLETED, this._onTransferCompleted)
      if (this._onPeerDisconnected) this.engine.removeListener(EVENTS.PEER_DISCONNECTED, this._onPeerDisconnected)
    }
    if (this.activeRoom) {
      const room = this.activeRoom
      this._teardownRoomState(room)
    }
  }
}

module.exports = { WatchPartyManager, PARTY_EVENTS }
