'use strict'

const { path, fsp, EventEmitter } = require('../compat.js')
const { generateDropCode } = require('../crypto')
const { MESSAGES } = require('../connections/signaling')

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
  DISCOVERED_ROOMS: 'party:rooms:discovered'
}

class WatchPartyManager extends EventEmitter {
  constructor(options = {}) {
    super()
    this.engine = options.engine
    this.activeRoom = null // { roomCode, title, filePath, isHost, controlsMode, coreKey, participants: Map }
    this.discoveredRooms = new Map() // roomCode -> { roomCode, title, hostName, hostPeerId, duration, timestamp }
    this.reactionListeners = new Set()
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
      createdAt: Date.now()
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
      title: cleanCode,
      isHost: false,
      controlsMode: 'host',
      participants: new Map(),
      joinedAt: Date.now()
    }

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
      }
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

  _broadcastAnnouncement(action = 'create') {
    if (!this.activeRoom) return
    const hostIdentity = this.engine.storage?.getDeviceIdentity?.() || {}
    const msg = {
      type: 'WATCH_ROOM_ANNOUNCE',
      action,
      roomCode: this.activeRoom.roomCode,
      title: this.activeRoom.title,
      hostName: hostIdentity.name || 'Host',
      timestamp: Date.now()
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
