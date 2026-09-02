'use strict'

const { path, fsp, EventEmitter } = require('../compat.js')
const { generateDropCode } = require('../crypto')
const { EVENTS } = require('../protocol.js')
const { MESSAGES } = require('../connections/signaling')
const { STATUS } = require('./transfer/constants.js')

const PARTY_EVENTS = {
  ROOM_CREATED: 'party:room:created',
  ROOM_JOINED: 'party:room:joined',
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
  MEDIA_ERROR: 'party:media:error'
}

class WatchPartyManager extends EventEmitter {
  constructor(options = {}) {
    super()
    this.engine = options.engine
    this.activeRoom = null // { roomCode, shareId, title, filePath, isHost, controlsMode, coreKey, participants: Map }
    this.discoveredRooms = new Map() // roomCode -> { roomCode, title, hostName, hostPeerId, duration, timestamp }
    this.reactionListeners = new Set()
    // peerId -> replication streams serving this room's media core (torn down
    // on leaveRoom; per-peer disconnect cleanup also happens in connections).
    this._mediaStreams = new Map()
    this._mediaReadyNotified = false

    // Guest-side media state: surface an explicit source-ready / source-error
    // signal for the party transfer so UIs never have to poll the staging dir.
    if (this.engine && typeof this.engine.on === 'function') {
      this._onTransferProgress = (t) => {
        const room = this.activeRoom
        if (!room || room.isHost || !room.shareId) return
        if (!t || t.id !== room.shareId) return
        if (this._mediaReadyNotified) return
        this._mediaReadyNotified = true
        this.emit(PARTY_EVENTS.MEDIA_READY, {
          roomCode: room.roomCode,
          shareId: room.shareId,
          filename: t.filename,
          fileSize: room.fileSize
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
    }
  }

  /**
   * Host creates a new Watch Party room.
   */
  async createRoom(params = {}) {
    const { title, filePath, controlsMode = 'host' } = params
    if (!filePath) throw new Error('File path required to create a watch party')

    const filename = path.basename(filePath)
    const roomCode = generateDropCode({ isGroup: true }).replace('DROP-GRP-', 'PARTY-')

    let fileSize = 0
    try {
      const stat = await fsp.stat(filePath)
      fileSize = stat.size
    } catch (err) {
      console.warn('[WatchPartyManager] stat failed:', err.message)
    }

    const shareId = `watch-${roomCode.toLowerCase()}`

    // Join swarm topic for this room
    const topic = `p2p-watch-${roomCode}`
    if (this.engine && this.engine.topicRegistry) {
      this.engine.topicRegistry.join(topic, { client: true, server: true })
    }

    const hostIdentity = this.engine.storage?.getDeviceIdentity?.() || { name: 'Host Device' }
    const transferEngine = this.engine && this.engine.transferEngine
    const fileType = transferEngine && typeof transferEngine.getFileType === 'function'
      ? transferEngine.getFileType(filename)
      : ''

    this.activeRoom = {
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
      hostPeerId: hostIdentity.id || 'host',
      hostName: hostIdentity.name || 'Host',
      participants: new Map(),
      createdAt: Date.now()
    }

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
        this.activeRoom.coreKey = staged.coreKey
        this.activeRoom.manifestHash = staged.manifestHash
        this.activeRoom.checksum = staged.checksum
      } catch (err) {
        console.warn('[WatchPartyManager] party media staging failed:', err.message)
      }
    }

    // Broadcast room announcement to all connected peers
    this._broadcastAnnouncement('create')

    this.emit(PARTY_EVENTS.ROOM_CREATED, this.getRoomInfo())
    return this.getRoomInfo()
  }

  /**
   * Join an existing Watch Party room as a peer.
   */
  async joinRoom(params = {}) {
    const { roomCode } = params
    if (!roomCode) throw new Error('Room code required to join')

    const cleanCode = roomCode.trim().toUpperCase()
    const topic = `p2p-watch-${cleanCode}`

    if (this.engine.topicRegistry) {
      this.engine.topicRegistry.join(topic, { client: true, server: true })
    }

    const myIdentity = this.engine.storage?.getDeviceIdentity?.() || { name: 'Peer Device' }

    this.activeRoom = {
      roomCode: cleanCode,
      shareId: `watch-${cleanCode.toLowerCase()}`,
      title: cleanCode,
      isHost: false,
      controlsMode: 'host',
      participants: new Map(),
      joinedAt: Date.now()
    }
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
    return this.getRoomInfo()
  }

  /**
   * Leave the currently active Watch Party room.
   */
  async leaveRoom() {
    if (!this.activeRoom) return null

    const room = this.activeRoom
    const topic = `p2p-watch-${room.roomCode}`

    if (room.isHost) {
      this._broadcastAnnouncement('close')
      this.emit(PARTY_EVENTS.ROOM_CLOSED, { roomCode: room.roomCode })
    } else {
      const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
      this._broadcastRoomMessage({
        type: 'WATCH_ROOM_LEAVE',
        roomCode: room.roomCode,
        peerId: myIdentity.id
      })
    }

    if (this.engine.topicRegistry) {
      try {
        this.engine.topicRegistry.leave(topic)
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
    this._mediaReadyNotified = false

    this.activeRoom = null
    this.emit(PARTY_EVENTS.ROOM_LEFT, { roomCode: room.roomCode })
    return { success: true }
  }

  /**
   * Broadcast playback state (play/pause/seek) within the active room.
   */
  broadcastPlaybackState(params = {}) {
    if (!this.activeRoom) return false

    const { action, positionSec, playbackRate = 1.0 } = params
    const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}

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
   */
  sendReaction(emoji) {
    if (!this.activeRoom) return false

    const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
    const payload = {
      type: 'WATCH_REACTION',
      roomCode: this.activeRoom.roomCode,
      emoji: emoji || '🍿',
      senderName: myIdentity.name || 'Peer',
      timestamp: Date.now()
    }

    this._broadcastRoomMessage(payload)
    this.emit(PARTY_EVENTS.REACTION, payload)
    return true
  }

  /**
   * List all discovered active rooms hosted across the swarm.
   */
  listDiscoveredRooms() {
    const now = Date.now()
    const valid = []
    for (const [code, room] of this.discoveredRooms.entries()) {
      if (now - room.timestamp < 120000) { // Valid within 2 mins of last keepalive
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
      } else if (msg.roomCode && msg.title) {
        this.discoveredRooms.set(msg.roomCode, {
          roomCode: msg.roomCode,
          title: msg.title,
          hostName: msg.hostName,
          hostPeerId: peerId,
          timestamp: Date.now()
        })
      }
      this.emit(PARTY_EVENTS.DISCOVERED_ROOMS, this.listDiscoveredRooms())
    } else if (msg.type === 'WATCH_ROOM_JOIN') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        this.activeRoom.participants.set(peerId, {
          peerId,
          name: msg.peer?.name || 'Peer',
          joinedAt: Date.now(),
          status: 'connected'
        })
        this.emit(PARTY_EVENTS.PEER_JOINED, {
          roomCode: msg.roomCode,
          peer: { id: peerId, name: msg.peer?.name || 'Peer' }
        })
        // Authorized by room membership: this peer just proved knowledge of the
        // room code. Hand it the staged media descriptor + per-core replication
        // so its data plane comes up alongside the control plane it joined for.
        if (this.activeRoom.isHost) {
          this._serveMediaToPeer(peerId).catch(() => {})
        }
      }
    } else if (msg.type === 'WATCH_MEDIA_OFFER') {
      this._handleMediaOffer(peerId, msg).catch(() => {})
    } else if (msg.type === 'WATCH_ROOM_LEAVE') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        this.activeRoom.participants.delete(peerId)
        this.emit(PARTY_EVENTS.PEER_LEFT, { roomCode: msg.roomCode, peerId })
      }
    } else if (msg.type === 'WATCH_PEER_STATUS') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        const p = this.activeRoom.participants.get(peerId)
        if (p) {
          p.positionSec = msg.positionSec
          p.buffering = msg.buffering
          p.bufferedPercent = msg.bufferedPercent
        }
        this.emit(PARTY_EVENTS.PEER_STATUS, msg)
      }
    } else if (msg.type === 'WATCH_STATE_SYNC') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        this.emit(PARTY_EVENTS.STATE_SYNC, msg)
      }
    } else if (msg.type === 'WATCH_REACTION') {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        this.emit(PARTY_EVENTS.REACTION, msg)
      }
    }
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
      // One live replication stream per peer per connection: a rejoining guest
      // reuses the same mesh connection, and a second noise-wrapped replicate
      // over it would corrupt the wire.
      let stream = peerObj.partyMediaStream
      if (!stream || stream.destroyed) {
        stream = core.replicate(peerObj.connection, { live: true })
        peerObj.partyMediaStream = stream
        peerObj.dropStreams = peerObj.dropStreams || []
        peerObj.dropStreams.push(stream)
        this._trackMediaStream(peerId, stream)
      }
      console.log(
        `[WatchPartyManager] serving party media to ${peerId.slice(0, 12)}... (${room.filename})`
      )
    } catch (err) {
      console.warn('[WatchPartyManager] party media replication failed:', err.message)
      return
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

    const shareId = `watch-${room.roomCode.toLowerCase()}`
    const transferId = typeof msg.transferId === 'string' && msg.transferId ? msg.transferId : ''
    // Only the room's own deterministic id is this party's media.
    if (transferId && transferId !== shareId) return

    const { filename, fileSize, coreKey } = msg
    if (typeof coreKey !== 'string' || coreKey.length !== 64) return
    if (typeof fileSize !== 'number' || fileSize < 0) return
    if (typeof filename !== 'string' || filename.length === 0 || filename.length > 500) return

    room.shareId = shareId
    room.filename = filename
    room.fileSize = fileSize

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
        let stream = peerObj.partyMediaStream
        if (!stream || stream.destroyed) {
          stream = core.replicate(peerObj.connection, { live: true })
          peerObj.partyMediaStream = stream
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
          [STATUS.PENDING_APPROVAL, STATUS.INTERRUPTED, STATUS.FAILED, STATUS.WAITING_PEER].indexOf(existing.status) !== -1 &&
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
    if (!this.activeRoom) return
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
          roomCode: this.activeRoom.roomCode,
          title: trusted || action === 'close' ? this.activeRoom.title : 'Watch Party',
          hostName: trusted ? hostIdentity.name || 'Host' : undefined,
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
}

module.exports = { WatchPartyManager, PARTY_EVENTS }
