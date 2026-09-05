'use strict'

// Watch Party v2 feature tests (control plane): chat, moderation, private
// rooms, rewind window, voice chunks, timestamped reactions, queue
// bookkeeping. Runs two manager "sides" against each other over a direct
// message pipe (no media replication — covered by test-watch-party.js).

const { EventEmitter } = require('events')
const { WatchPartyManager, PARTY_EVENTS } = require('./engine/WatchPartyManager.js')

let passed = 0
let failed = 0

function assert(name, cond, detail = '') {
  if (cond) {
    passed++
    console.log(`  ✓ ${name}`)
  } else {
    failed++
    console.error(`  ✗ ${name}${detail ? ' — ' + detail : ''}`)
    process.exitCode = 1
  }
}

function makeSide(id, name) {
  const engine = new EventEmitter()
  engine.peers = new Map()
  engine.topicRegistry = { join() {}, leave() {} }
  engine.storage = { getDeviceIdentity: () => ({ id, name }) }
  // Control-plane tests skip real media staging: fake a coreKey so
  // createRoom passes its playable-source check (64 hex chars).
  engine.transferEngine = {
    stageDrop: async (p) => ({
      coreKey: Buffer.alloc(32, 7).toString('hex'),
      manifestHash: 'fake',
      checksum: 'fake'
    }),
    getFileType: () => 'video'
  }
  const wp = new WatchPartyManager({ engine })
  const outbox = []
  // Route a message from this side to the other manager.
  wp.onOutgoing = (msg) => outbox.push(msg)
  wp._broadcastRoomMessage = (msg) => wp.onOutgoing(msg)
  wp._broadcastAnnouncement = (action) => wp.onOutgoing({ type: 'WATCH_ROOM_ANNOUNCE', action, roomCode: wp.activeRoom?.roomCode, title: wp.activeRoom?.title, hostName: name, isPrivate: wp.activeRoom?.isPrivate })
  wp._sendToPeer = (peerId, msg) => wp.onOutgoing({ ...msg, _to: peerId })
  return { engine, wp, outbox }
}

async function main() {
  console.log('\nTest 1: Chat round-trip + history replay')
  {
    const host = makeSide('host-1', 'Host')
    const guest = makeSide('guest-1', 'Guest')
    const chatEvents = []
    guest.wp.on(PARTY_EVENTS.CHAT, (m) => chatEvents.push(m))

    await host.wp.createRoom({ filePath: __filename })
    const room = host.wp.getRoomInfo()
    // Join over the pipe
    await guest.wp.joinRoom({ roomCode: room.roomCode })
    for (const m of guest.outbox.splice(0)) host.wp.handleMessage('guest-1', m)

    host.wp.sendChat('hello party')
    const chatMsg = host.outbox.find((m) => m.type === 'WATCH_CHAT')
    assert('chat broadcast carries text + sender', !!chatMsg && chatMsg.text === 'hello party' && chatMsg.sender.name === 'Host')
    guest.wp.handleMessage('host-1', chatMsg)
    assert('guest received chat', chatEvents.length === 1 && chatEvents[0].text === 'hello party')
    assert('host chat log kept', host.wp.getChatHistory().length === 1)
    assert('guest chat log kept', guest.wp.getChatHistory().length === 1)

    // Late joiner history replay
    const guest2 = makeSide('guest-2', 'Late')
    await guest2.wp.joinRoom({ roomCode: room.roomCode })
    for (const m of guest2.outbox.splice(0)) host.wp.handleMessage('guest-2', m)
    const hist = host.outbox.find((m) => m.type === 'WATCH_CHAT_HISTORY' && m._to === 'guest-2')
    assert('chat history replayed to late joiner', !!hist && hist.messages.length === 1)
    const histEvents = []
    guest2.wp.on(PARTY_EVENTS.CHAT_HISTORY, (m) => histEvents.push(m))
    guest2.wp.handleMessage('host-1', hist)
    assert('guest applied history', histEvents.length === 1 && guest2.wp.getChatHistory().length === 1)
  }

  console.log('\nTest 2: Moderation — mute, promote, kick')
  {
    const host = makeSide('host-2', 'Host')
    const guest = makeSide('guest-2', 'Guest')
    await host.wp.createRoom({ filePath: __filename })
    const room = host.wp.getRoomInfo()
    await guest.wp.joinRoom({ roomCode: room.roomCode })
    for (const m of guest.outbox.splice(0)) host.wp.handleMessage('guest-2', m)
    const moderated = []
    guest.wp.on(PARTY_EVENTS.MODERATED, (m) => moderated.push(m))

    // Mute
    const r1 = host.wp.moderate({ action: 'mute', targetPeerId: 'guest-2' })
    assert('mute applied host-side', r1.success && host.wp.getRoomInfo().participants.find((p) => p.peerId === 'guest-2')?.isMuted === true)
    const muteMsg = host.outbox.find((m) => m.type === 'WATCH_MODERATE' && m.action === 'mute')
    guest.wp.handleMessage('host-2', muteMsg)
    assert('guest knows it is muted', guest.wp.getRoomInfo().isLocalMuted === true)
    assert('muted guest cannot send voice', guest.wp.sendVoiceChunk({ audioB64: 'AAAA' }) === false)

    // Unmute
    host.wp.moderate({ action: 'unmute', targetPeerId: 'guest-2' })
    guest.wp.handleMessage('host-2', host.outbox.find((m) => m.type === 'WATCH_MODERATE' && m.action === 'unmute'))
    assert('unmuted guest can send voice', guest.wp.sendVoiceChunk({ audioB64: 'AAAA', seq: 1, durationMs: 100 }) === true)
    const voiceMsg = guest.outbox.find((m) => m.type === 'WATCH_VOICE')
    assert('voice chunk broadcast', !!voiceMsg && voiceMsg.audioB64 === 'AAAA')

    // Promote: guest becomes playback master in host mode
    host.wp.moderate({ action: 'promote', targetPeerId: 'guest-2' })
    const promoteMsg = host.outbox.find((m) => m.type === 'WATCH_MODERATE' && m.action === 'promote')
    guest.wp.handleMessage('host-2', promoteMsg)
    // In host mode, host must now ACCEPT guest state sync (guest is master)
    host.wp.handleMessage('guest-2', {
      type: 'WATCH_STATE_SYNC', roomCode: room.roomCode, action: 'play',
      positionSec: 42, sender: { id: 'guest-2', name: 'Guest' }
    })
    let guestGotSync = false
    guest.wp.on(PARTY_EVENTS.STATE_SYNC, () => { guestGotSync = true })
    // And guest rejects third-party sync (sender is neither master nor host)
    host.wp.handleMessage('intruder', {
      type: 'WATCH_STATE_SYNC', roomCode: room.roomCode, action: 'play',
      positionSec: 1, sender: { id: 'someone-else', name: 'X' }
    })
    assert('promoted master sync accepted by host', host.wp.getRoomInfo().hostPositionSec === 42)
    assert('guest room knows the master', guest.wp.getRoomInfo().playbackPeerId === 'guest-2')

    // Kick
    const r3 = host.wp.moderate({ action: 'kick', targetPeerId: 'guest-2' })
    assert('kick applied', r3.success && !host.wp.getRoomInfo().participants.some((p) => p.peerId === 'guest-2'))
    const kickMsg = host.outbox.find((m) => m.type === 'WATCH_MODERATE' && m.action === 'kick')
    const closed = []
    guest.wp.on(PARTY_EVENTS.ROOM_CLOSED, (e) => closed.push(e))
    guest.wp.handleMessage('host-2', kickMsg)
    assert('kicked guest left room with reason kicked', closed.length === 1 && closed[0].reason === 'kicked')
    // Kicked peer cannot rejoin
    await guest.wp.joinRoom({ roomCode: room.roomCode })
    for (const m of guest.outbox.splice(0)) host.wp.handleMessage('guest-2', m)
    const rejoinKick = host.outbox.find((m) => m.type === 'WATCH_MODERATE' && m.action === 'kick' && m._to === 'guest-2')
    assert('kicked peer rejoin rejected', !!rejoinKick)
    assert('kicked peer not in roster', !host.wp.getRoomInfo().participants.some((p) => p.peerId === 'guest-2'))
    assert('moderation events surfaced to guest', moderated.length >= 3)
  }

  console.log('\nTest 3: Private rooms are not discoverable')
  {
    const host = makeSide('host-3', 'Host')
    await host.wp.createRoom({ filePath: __filename, isPrivate: true })
    const announce = host.outbox.find((m) => m.type === 'WATCH_ROOM_ANNOUNCE' && m.action === 'create')
    assert('private announce flagged', announce.isPrivate === true)
    const guest = makeSide('guest-3', 'Guest')
    const discovered = []
    guest.wp.on(PARTY_EVENTS.DISCOVERED_ROOMS, (rooms) => discovered.push(rooms))
    guest.wp.handleMessage('host-3', announce)
    assert('private room not added to discovery', guest.wp.listDiscoveredRooms().length === 0)
    // But joinable by code
    await guest.wp.joinRoom({ roomCode: host.wp.getRoomInfo().roomCode })
    for (const m of guest.outbox.splice(0)) host.wp.handleMessage('guest-3', m)
    assert('private room joinable by code', host.wp.getRoomInfo().participants.some((p) => p.peerId === 'guest-3'))
  }

  console.log('\nTest 4: Rewind window enforcement (open mode)')
  {
    const host = makeSide('host-4', 'Host')
    const guest = makeSide('guest-4', 'Guest')
    await host.wp.createRoom({ filePath: __filename, controlsMode: 'open', rewindWindowSec: 30 })
    const room = host.wp.getRoomInfo()
    assert('rewind window stored', room.rewindWindowSec === 30)
    await guest.wp.joinRoom({ roomCode: room.roomCode })
    for (const m of guest.outbox.splice(0)) host.wp.handleMessage('guest-4', m)

    // Host at 10:00; guest seeks back to 3:00 — rejected + corrective resync
    host.wp.broadcastPlaybackState({ action: 'play', positionSec: 600 })
    const syncs = []
    guest.wp.on(PARTY_EVENTS.STATE_SYNC, (s) => syncs.push(s))
    host.wp.handleMessage('guest-4', {
      type: 'WATCH_STATE_SYNC', roomCode: room.roomCode, action: 'seek',
      positionSec: 180, sender: { id: 'guest-4', name: 'Guest' }
    })
    const corrective = host.outbox.filter((m) => m.type === 'WATCH_STATE_SYNC' && m.action === 'seek')
    assert('over-window rewind rejected', corrective.length === 1 && corrective[0].positionSec === 600)
    // Guest seeks within window (580) — accepted, no corrective
    host.outbox.length = 0
    const hostSyncs = []
    host.wp.on(PARTY_EVENTS.STATE_SYNC, (s) => hostSyncs.push(s))
    host.wp.handleMessage('guest-4', {
      type: 'WATCH_STATE_SYNC', roomCode: room.roomCode, action: 'seek',
      positionSec: 580, sender: { id: 'guest-4', name: 'Guest' }
    })
    assert('within-window seek accepted', host.outbox.filter((m) => m.type === 'WATCH_STATE_SYNC').length === 0 && hostSyncs.length === 1 && hostSyncs[0].positionSec === 580)
    // With window 0 (default), same seek is fine
    const host2 = makeSide('host-4b', 'Host')
    await host2.wp.createRoom({ filePath: __filename, controlsMode: 'open' })
    host2.wp.broadcastPlaybackState({ action: 'play', positionSec: 600 })
    host2.wp.handleMessage('g', {
      type: 'WATCH_STATE_SYNC', roomCode: host2.wp.getRoomInfo().roomCode, action: 'seek',
      positionSec: 10, sender: { id: 'g', name: 'Guest' }
    })
    assert('window 0 = unlimited rewind', host2.outbox.filter((m) => m.type === 'WATCH_STATE_SYNC' && m.action === 'seek').length === 0)
  }

  console.log('\nTest 5: Timestamped reactions + voice relay')
  {
    const host = makeSide('host-5', 'Host')
    const guest = makeSide('guest-5', 'Guest')
    await host.wp.createRoom({ filePath: __filename })
    const room = host.wp.getRoomInfo()
    await guest.wp.joinRoom({ roomCode: room.roomCode })
    for (const m of guest.outbox.splice(0)) host.wp.handleMessage('guest-5', m)

    guest.wp.sendReaction('🔥', 321.5)
    const react = guest.outbox.find((m) => m.type === 'WATCH_REACTION')
    assert('reaction carries positionSec', react && react.positionSec === 321.5 && react.emoji === '🔥')
    const gotReact = []
    host.wp.on(PARTY_EVENTS.REACTION, (r) => gotReact.push(r))
    host.wp.handleMessage('guest-5', react)
    assert('host received timestamped reaction', gotReact.length === 1 && gotReact[0].positionSec === 321.5)

    guest.wp.sendVoiceChunk({ seq: 7, durationMs: 120, audioB64: 'QUJD' })
    const voice = guest.outbox.find((m) => m.type === 'WATCH_VOICE')
    const gotVoice = []
    host.wp.on(PARTY_EVENTS.VOICE, (v) => gotVoice.push(v))
    host.wp.handleMessage('guest-5', voice)
    assert('voice chunk relayed', gotVoice.length === 1 && gotVoice[0].audioB64 === 'QUJD' && gotVoice[0].seq === 7)
    // Oversized chunk refused
    assert('oversized voice chunk refused', guest.wp.sendVoiceChunk({ audioB64: 'A'.repeat(300000) }) === false)
  }

  console.log('\nTest 6: Queue bookkeeping + epoch shareIds')
  {
    const host = makeSide('host-6', 'Host')
    await host.wp.createRoom({ filePath: __filename })
    const room = host.wp.getRoomInfo()
    assert('epoch starts at 1', room.mediaEpoch === 1)
    const added = await host.wp.queueAdd({ filePath: __filename, title: 'Episode 2' })
    assert('queue add reflected in room info', added.queue.length === 1 && added.queue[0].title === 'Episode 2')
    const removed = host.wp.queueRemove(5)
    assert('bad index rejected', removed.success === false)
    assert('epoch shareId derivation', host.wp._shareIdForEpoch(room.roomCode, 1) === room.shareId && host.wp._shareIdForEpoch(room.roomCode, 2).endsWith('-e2'))
    // Guests cannot touch the queue
    const guest = makeSide('guest-6', 'Guest')
    await guest.wp.joinRoom({ roomCode: room.roomCode })
    for (const m of guest.outbox.splice(0)) host.wp.handleMessage('guest-6', m)
    let threw = false
    try {
      await guest.wp.queueAdd({ filePath: __filename })
    } catch {
      threw = true
    }
    assert('guest queue add rejected', threw)
  }

  console.log('\nTest 7: SRT → VTT conversion')
  {
    const host = makeSide('host-7', 'Host')
    const vtt = host.wp._srtToVtt('1\r\n00:00:01,000 --> 00:00:03,500\r\nHello\r\n')
    assert('WEBVTT header present', vtt.startsWith('WEBVTT'))
    assert('comma timing converted', vtt.includes('00:00:01.000 --> 00:00:03.500'))
  }

  console.log(`\n======================================================`)
  console.log(`  WATCH PARTY v2 FEATURES: ${passed} PASSED, ${failed} FAILED`)
  console.log(`======================================================`)
}

main().catch((err) => {
  console.error('Test harness crashed:', err)
  process.exit(1)
})
