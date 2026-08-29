'use strict'

const { path, fsp, EventEmitter } = require('../compat.js')
const { generateDropCode } = require('../crypto')
const { MESSAGES } = require('../protocol.js')
const { normalizeCapabilities } = require('./watchCapabilities.js')

const PARTY_EVENTS = {
  ROOM_CREATED: 'party:room:created',
  ROOM_JOINED: 'party:room:joined',
  ROOM_LEFT: 'party:room:left',
  ROOM_CLOSED: 'party:room:closed',
  HOST_CHANGED: 'party:host:changed',
  PEER_JOINED: 'party:peer:joined',
  PEER_LEFT: 'party:peer:left',
  PEER_STATUS: 'party:peer:status',
  STATE_SYNC: 'party:state:sync',
  REACTION: 'party:reaction',
  CHAT: 'party:chat',
  DISCOVERED_ROOMS: 'party:rooms:discovered'
}

// Hard resync threshold (seconds). Drift above this snaps the player; below
// it the follower corrects gradually with playbackRate on desktop.
const DRIFT_SNAP_THRESHOLD = 2.0
// How long a discovered room announcement stays valid.
const DISCOVER_TTL_MS = 120000
// Min interval between chat messages from one sender (anti-flood).
const CHAT_THROTTLE_MS = 1000
// Cap on chat history retained per room.
const CHAT_HISTORY_MAX = 200

class WatchPartyManager extends EventEmitter {
  constructor(options = {}) {
    super()
    this.engine = options.engine
    this.activeRoom = null // { roomCode, title, filePath, isHost, controlsMode, coreKey, participants: Map, lastPlayback, fileChecksum, chatHistory }
    this.discoveredRooms = new Map() // roomCode -> { roomCode, title, hostName, hostPeerId, filename, fileSize, durationSec, timestamp }
    this.reactionListeners = new Set()
    this._chatThrottle = new Map() // peerId -> last chat timestamp
    this._resyncCount = 0
    this._driftSamples = []
  }

  /**
   * Host creates a new Watch Party room.
   */
  async createRoom(params = {}) {
    const { title, filePath, controlsMode = 'host', hostCapabilities, fileCodecs } = params
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

    this.activeRoom = {
      roomCode,
      title: title || filename,
      filename,
      filePath,
      fileSize,
      coreKey: null,
      isHost: true,
      controlsMode,
      hostPeerId: hostIdentity.id || 'host',
      hostName: hostIdentity.name || 'Host',
      participants: new Map(),
      createdAt: Date.now(),
      lastPlayback: null, // { action, positionSec, playbackRate, timestampMs }
      fileChecksum: null,
      fileCodecs: fileCodecs || null, // { videoCodec, audioCodec, container } from ffprobe
      hostCapabilities: normalizeCapabilities(hostCapabilities),
      chatHistory: []
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

    // The joiner's device capabilities (what it can actually play). Optional
    // and untrusted — normalized before it touches the wire.
    const capabilities = normalizeCapabilities(params.capabilities)

    this.activeRoom = {
      roomCode: cleanCode,
      title: cleanCode,
      isHost: false,
      controlsMode: 'host',
      participants: new Map(),
      joinedAt: Date.now(),
      lastPlayback: null,
      fileChecksum: null,
      chatHistory: []
    }

    // Send join message to the swarm
    this._broadcastRoomMessage({
      type: MESSAGES.WATCH_ROOM_JOIN,
      roomCode: cleanCode,
      peer: {
        id: myIdentity.id || 'peer',
        name: myIdentity.name || 'Peer',
        capabilities
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
      // Hand the room off to the longest-joined participant if one exists;
      // only close the room when nobody is left to take over.
      const successor = this._pickSuccessor()
      if (successor) {
        this._broadcastRoomMessage({
          type: MESSAGES.WATCH_ROOM_HANDOFF,
          roomCode: room.roomCode,
          newHostPeerId: successor.peerId, // noise public key
          newHostName: successor.name,
          oldHostPeerId: room.hostPeerId,
          oldHostName: room.hostName,
          lastPlayback: room.lastPlayback,
          roomMeta: this._roomMeta()
        })
        this._broadcastAnnouncement('handoff')
        this.emit(PARTY_EVENTS.ROOM_CLOSED, {
          roomCode: room.roomCode,
          handedOff: true,
          newHostPeerId: successor.peerId
        })
      } else {
        this._broadcastAnnouncement('close')
        this.emit(PARTY_EVENTS.ROOM_CLOSED, { roomCode: room.roomCode })
      }
    } else {
      const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
      this._broadcastRoomMessage({
        type: MESSAGES.WATCH_ROOM_LEAVE,
        roomCode: room.roomCode,
        peerId: myIdentity.id
      })
    }

    if (this.engine.topicRegistry) {
      try {
        this.engine.topicRegistry.leave(topic)
      } catch {}
    }

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
      type: MESSAGES.WATCH_STATE_SYNC,
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

    // Track the host's authoritative playback state for join snapshots.
    this.activeRoom.lastPlayback = {
      action: payload.action,
      positionSec: payload.positionSec,
      playbackRate: payload.playbackRate,
      timestampMs: payload.timestampMs
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
      type: MESSAGES.WATCH_PEER_STATUS,
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
      type: MESSAGES.WATCH_REACTION,
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
   * Send a text chat message to the room.
   */
  sendChat(text) {
    if (!this.activeRoom) return false
    const clean = typeof text === 'string' ? text.trim().slice(0, 500) : ''
    if (!clean) return false

    const myIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
    const payload = {
      type: MESSAGES.WATCH_CHAT,
      roomCode: this.activeRoom.roomCode,
      sender: { id: myIdentity.id, name: myIdentity.name || 'Peer' },
      text: clean,
      timestamp: Date.now()
    }

    this._broadcastRoomMessage(payload)
    this._appendChat(payload)
    this.emit(PARTY_EVENTS.CHAT, payload)
    return true
  }

  /**
   * List all discovered active rooms hosted across the swarm.
   */
  listDiscoveredRooms() {
    const now = Date.now()
    const valid = []
    for (const [code, room] of this.discoveredRooms.entries()) {
      if (now - room.timestamp < DISCOVER_TTL_MS) {
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
      title: this.activeRoom.title,
      filename: this.activeRoom.filename,
      filePath: this.activeRoom.filePath,
      fileSize: this.activeRoom.fileSize,
      isHost: this.activeRoom.isHost,
      controlsMode: this.activeRoom.controlsMode,
      hostPeerId: this.activeRoom.hostPeerId,
      hostName: this.activeRoom.hostName,
      participantCount: this.activeRoom.participants.size + 1,
      participants: Array.from(this.activeRoom.participants.values()),
      lastPlayback: this.activeRoom.lastPlayback,
      fileChecksum: this.activeRoom.fileChecksum,
      chatHistory: this.activeRoom.chatHistory || []
    }
  }

  /**
   * Metrics for the diagnostics collector.
   */
  getMetrics() {
    if (!this.activeRoom) return null
    return {
      active: true,
      isHost: this.activeRoom.isHost,
      roomCode: this.activeRoom.roomCode,
      participantCount: this.activeRoom.participants.size + 1,
      resyncCount: this._resyncCount,
      avgDriftSec: this._driftSamples.length
        ? this._driftSamples.reduce((a, b) => a + b, 0) / this._driftSamples.length
        : 0
    }
  }

  /**
   * Handle incoming signaling message from a peer.
   */
  handleMessage(peerId, msg) {
    if (!msg || typeof msg.type !== 'string') return

    if (msg.type === MESSAGES.WATCH_ROOM_ANNOUNCE) {
      if (msg.action === 'close') {
        this.discoveredRooms.delete(msg.roomCode)
      } else if (msg.action === 'handoff') {
        // A room changed hosts; keep it discoverable but refresh host info.
        const existing = this.discoveredRooms.get(msg.roomCode)
        if (existing) {
          existing.hostName = msg.newHostName || existing.hostName
          existing.hostPeerId = msg.newHostPeerId || existing.hostPeerId
          existing.timestamp = Date.now()
        }
      } else if (msg.roomCode && msg.title) {
        this.discoveredRooms.set(msg.roomCode, {
          roomCode: msg.roomCode,
          title: msg.title,
          hostName: msg.hostName,
          hostPeerId: peerId,
          filename: msg.filename,
          fileSize: msg.fileSize,
          durationSec: msg.durationSec,
          timestamp: Date.now()
        })
      }
      this.emit(PARTY_EVENTS.DISCOVERED_ROOMS, this.listDiscoveredRooms())
    } else if (msg.type === MESSAGES.WATCH_ROOM_JOIN) {
      if (this.activeRoom && this.activeRoom.isHost && this.activeRoom.roomCode === msg.roomCode) {
        this.activeRoom.participants.set(peerId, {
          peerId,
          name: msg.peer?.name || 'Peer',
          joinedAt: Date.now(),
          status: 'connected',
          capabilities: normalizeCapabilities(msg.peer?.capabilities)
        })
        this.emit(PARTY_EVENTS.PEER_JOINED, {
          roomCode: msg.roomCode,
          peer: {
            id: peerId,
            name: msg.peer?.name || 'Peer',
            capabilities: normalizeCapabilities(msg.peer?.capabilities)
          }
        })
        // Send an immediate snapshot so the joiner syncs to the current
        // position instead of waiting for the next broadcast tick.
        if (this.activeRoom.lastPlayback) {
          this._sendRoomSnapshot(peerId)
        }
      }
    } else if (msg.type === MESSAGES.WATCH_ROOM_HANDOFF) {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode && !this.activeRoom.isHost) {
        // newHostPeerId is the successor's noise public key (the key peers
        // use on the wire). Compare against our own noise public key.
        const myKey = String((this.engine.getIdentity && this.engine.getIdentity().publicKey) || '').toLowerCase()
        if (String(msg.newHostPeerId || '').toLowerCase() === myKey) {
          this._adoptHost(msg)
        }
      }
    } else if (msg.type === MESSAGES.WATCH_ROOM_LEAVE) {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        this.activeRoom.participants.delete(peerId)
        this.emit(PARTY_EVENTS.PEER_LEFT, { roomCode: msg.roomCode, peerId })
      }
    } else if (msg.type === MESSAGES.WATCH_PEER_STATUS) {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        const p = this.activeRoom.participants.get(peerId)
        if (p) {
          p.positionSec = msg.positionSec
          p.buffering = msg.buffering
          p.bufferedPercent = msg.bufferedPercent
        }
        this.emit(PARTY_EVENTS.PEER_STATUS, msg)
      }
    } else if (msg.type === MESSAGES.WATCH_STATE_SYNC) {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        // Host-authority: in 'host' mode only the host may drive playback;
        // in 'open' mode any participant may, but strangers cannot.
        if (this.activeRoom.isHost) {
          // We are the host: ignore state sync from anyone else (feedback
          // loop guard). Our own broadcasts are the authority.
          return
        }
        const controlsMode = this.activeRoom.controlsMode || 'host'
        if (controlsMode === 'host') {
          const hostPeerId = String(this.activeRoom.hostPeerId || '').toLowerCase()
          const senderId = String(msg.sender?.id || '').toLowerCase()
          if (senderId !== hostPeerId) return
        } else if (msg.sender?.id && !this.activeRoom.participants.has(msg.sender.id)) {
          // open mode: must be a known participant
          return
        }
        this.emit(PARTY_EVENTS.STATE_SYNC, msg)
      }
    } else if (msg.type === MESSAGES.WATCH_REACTION) {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        this.emit(PARTY_EVENTS.REACTION, msg)
      }
    } else if (msg.type === MESSAGES.WATCH_CHAT) {
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode) {
        const last = this._chatThrottle.get(peerId) || 0
        const now = Date.now()
        if (now - last < CHAT_THROTTLE_MS) {
          console.warn('[WatchPartyManager] dropping chat: flood from', peerId.slice(0, 8))
          return
        }
        this._chatThrottle.set(peerId, now)
        this._appendChat(msg)
        this.emit(PARTY_EVENTS.CHAT, msg)
      }
    } else if (msg.type === MESSAGES.WATCH_CAPABILITIES_REQUEST) {
      // A peer joined a running room and needs the file's codecs to decide how
      // (or whether) it can play. Only the host knows the file. Additive: an
      // old host never answers and the requester falls back to filename-only
      // metadata.
      if (this.activeRoom && this.activeRoom.isHost && this.activeRoom.roomCode === msg.roomCode) {
        const peerObj = this.engine.peers?.get(peerId)
        if (peerObj && typeof peerObj.signaling?.send === 'function') {
          try {
            peerObj.signaling.send({
              type: MESSAGES.WATCH_CAPABILITIES_RESPONSE,
              roomCode: this.activeRoom.roomCode,
              filename: this.activeRoom.filename || null,
              fileSize: this.activeRoom.fileSize || null,
              fileCodecs: this.activeRoom.fileCodecs || null
            })
          } catch (err) {
            console.warn('[WatchPartyManager] capabilities response failed:', err.message)
          }
        }
      }
    } else if (msg.type === MESSAGES.WATCH_CAPABILITIES_RESPONSE) {
      // We asked; the host answered. Adopt the file's real codecs for a local
      // decide() call.
      if (this.activeRoom && this.activeRoom.roomCode === msg.roomCode && !this.activeRoom.isHost) {
        if (msg.filename) this.activeRoom.filename = msg.filename
        if (msg.fileSize) this.activeRoom.fileSize = msg.fileSize
        if (msg.fileCodecs) this.activeRoom.fileCodecs = msg.fileCodecs
      }
    }
  }

  // ─── Internal helpers ─────────────────────────────────────────────────────

  _sendRoomSnapshot(peerId) {
    const room = this.activeRoom
    if (!room || !room.isHost || !room.lastPlayback) return
    const hostIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
    const lp = room.lastPlayback
    const peerObj = this.engine.peers?.get(peerId)
    if (!peerObj || !peerObj.signaling || typeof peerObj.signaling.send !== 'function') return
    try {
      peerObj.signaling.send({
        type: MESSAGES.WATCH_STATE_SYNC,
        roomCode: room.roomCode,
        action: lp.action,
        positionSec: lp.positionSec,
        playbackRate: lp.playbackRate,
        timestampMs: Date.now(),
        sender: { id: hostIdentity.id, name: hostIdentity.name },
        roomMeta: this._roomMeta()
      })
    } catch (err) {
      console.warn('[WatchPartyManager] snapshot send failed:', err.message)
    }
  }

  _roomMeta() {
    const room = this.activeRoom
    if (!room) return null
    return {
      roomCode: room.roomCode,
      title: room.title,
      filename: room.filename,
      fileSize: room.fileSize,
      controlsMode: room.controlsMode,
      hostName: room.hostName,
      hostPeerId: room.hostPeerId,
      fileChecksum: room.fileChecksum,
      fileCodecs: room.fileCodecs || null,
      participantCount: room.participants.size + 1,
      participants: Array.from(room.participants.values()).map((p) => ({
        peerId: p.peerId,
        name: p.name,
        status: p.status,
        capabilities: p.capabilities || null
      }))
    }
  }

  _pickSuccessor() {
    const room = this.activeRoom
    if (!room || room.participants.size === 0) return null
    let best = null
    for (const p of room.participants.values()) {
      if (!best || p.joinedAt < best.joinedAt) best = p
    }
    return best
  }

  _adoptHost(msg) {
    const room = this.activeRoom
    if (!room) return
    const meta = msg.roomMeta || {}
    room.isHost = true
    room.title = meta.title || room.title
    room.filename = meta.filename
    room.fileSize = meta.fileSize
    room.fileChecksum = meta.fileChecksum
    room.controlsMode = meta.controlsMode || room.controlsMode
    room.hostPeerId = meta.hostPeerId || room.hostPeerId
    room.hostName = meta.hostName || room.hostName
    room.lastPlayback = msg.lastPlayback || room.lastPlayback
    // The old host is gone; drop any leftover roster entry for it (keyed by
    // device id in participants when it joined other rooms).
    const oldHostId = msg.oldHostPeerId
    if (oldHostId) {
      for (const [pid, p] of room.participants.entries()) {
        if (p.peerId === oldHostId || p.name === msg.oldHostName) room.participants.delete(pid)
      }
    }
    // Re-announce so peers + the lobby discover the new host.
    this._broadcastAnnouncement('create')
    this.emit(PARTY_EVENTS.HOST_CHANGED, this.getRoomInfo())
    this.emit(PARTY_EVENTS.ROOM_CREATED, this.getRoomInfo())
  }

  _appendChat(msg) {
    const room = this.activeRoom
    if (!room) return
    if (!Array.isArray(room.chatHistory)) room.chatHistory = []
    room.chatHistory.push({
      sender: msg.sender || { name: 'Peer' },
      text: String(msg.text || '').slice(0, 500),
      timestamp: msg.timestamp || Date.now()
    })
    if (room.chatHistory.length > CHAT_HISTORY_MAX) {
      room.chatHistory.splice(0, room.chatHistory.length - CHAT_HISTORY_MAX)
    }
  }

  _broadcastAnnouncement(action = 'create') {
    if (!this.activeRoom) return
    const hostIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
    const room = this.activeRoom
    const msg = {
      type: MESSAGES.WATCH_ROOM_ANNOUNCE,
      action,
      roomCode: room.roomCode,
      title: room.title,
      hostName: hostIdentity.name || 'Host',
      filename: room.filename,
      fileSize: room.fileSize,
      timestamp: Date.now()
    }
    if (action === 'handoff') {
      msg.newHostName = hostIdentity.name || 'Host'
      msg.newHostPeerId = hostIdentity.id || 'host'
    }
    this._broadcastToAllPeers(msg)
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
