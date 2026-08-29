'use strict'

// End-to-end verification for the Watch Party feature — no Electron, no IPC.
//
//   node test-watch-party.js
//
// Spawns TWO real MeshEngine instances as separate child processes (separate
// storage dirs), pairs them over the public DHT, then drives the room flow:
//
//   1. host creates a party room (PARTY-XXXX)
//   2. joiner joins the room -> host replies with a join snapshot
//   3. host broadcasts play state -> joiner receives it
//   4. joiner chat message -> host receives it
//   5. host leaves -> joiner is promoted to host (handoff)
//   6. host-authority: in host mode a non-host state sync is dropped
//
// Exit code 0 on success, 1 on any failure.

const { spawn } = require('child_process')
const path = require('path')
const os = require('os')
const fs = require('fs')

const SCRIPT = __filename
const GLOBAL_TIMEOUT_MS = 180 * 1000
const PAIR_TIMEOUT_MS = 90 * 1000
const ROOM_TIMEOUT_MS = 30 * 1000

let checks = 0
let failures = 0

function check(name, cond, extra) {
  checks++
  if (cond) {
    console.log('PASS  ' + name + (extra ? ' — ' + extra : ''))
  } else {
    failures++
    console.error('FAIL  ' + name + (extra ? ' — ' + extra : ''))
  }
}

function waitFor(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

// ─── Child process (one per MeshEngine instance) ────────────────────────────

async function runChild(role, config) {
  const origError = console.error.bind(console)
  console.log = (...args) => origError(`[${role}]`, ...args)
  console.warn = (...args) => origError(`[${role}]`, ...args)
  console.error = (...args) => origError(`[${role}]`, ...args)

  const { MeshEngine } = require('./index.js')
  const send = (msg) => process.stdout.write(JSON.stringify(msg) + '\n')

  const engine = new MeshEngine({
    storageDir: config.storageDir,
    downloadsDir: config.downloadsDir,
    deviceName: config.deviceName,
    autoAcceptOffers: true,
    autoTrustLAN: false,
    lanDiscovery: false
  })

  engine.on('error', (err) => {
    send({ type: 'error', message: String((err && err.message) || err) })
  })

  let pairedKey = null
  engine.on('trust:paired', ({ peer }) => {
    pairedKey = pairedKey || peer.publicKey
    send({ type: 'paired', peer })
  })

  // Forward watch-party events so the orchestrator can assert on them.
  const partyEvents = [
    'party:room:created', 'party:room:joined', 'party:room:left', 'party:room:closed',
    'party:host:changed', 'party:peer:joined', 'party:peer:left', 'party:state:sync',
    'party:reaction', 'party:chat', 'party:rooms:discovered'
  ]
  for (const evt of partyEvents) {
    engine.on(evt, (data) => send({ type: 'partyEvent', event: evt, data }))
  }

  await engine.start()
  const identity = engine.getIdentity()
  send({ type: 'code', code: identity.pairingCode })

  // The joiner pairs automatically at boot (mirrors test.js: the code is
  // passed via argv, pairing runs as soon as the engine is up).
  if (config.code) {
    try {
      const peer = await engine.pairWithCode(config.code, { timeoutMs: PAIR_TIMEOUT_MS })
      send({ type: 'paired', peer })
    } catch (err) {
      send({ type: 'error', message: 'pair failed: ' + String((err && err.message) || err) })
    }
  }

  process.stdin.on('data', async (chunk) => {
    const text = String(chunk)
    for (const line of text.split('\n')) {
      const trimmed = line.trim()
      if (!trimmed) continue
      if (trimmed === 'stop') {
        await engine.stop()
        send({ type: 'bye' })
        process.exit(0)
        return
      }
      let cmd = null
      try {
        cmd = JSON.parse(trimmed)
      } catch {
        continue
      }
      try {
        if (cmd.cmd === 'pair') {
          const peer = await engine.pairWithCode(cmd.code, { timeoutMs: cmd.timeout || PAIR_TIMEOUT_MS })
          send({ type: 'paired', peer })
        } else if (cmd.cmd === 'createroom') {
          const room = await engine.createPartyRoom({
            title: cmd.title,
            filePath: cmd.filePath,
            controlsMode: cmd.controlsMode || 'host'
          })
          send({ type: 'roomCreated', room })
        } else if (cmd.cmd === 'joinroom') {
          const room = await engine.joinPartyRoom({ roomCode: cmd.code })
          send({ type: 'roomJoined', room })
        } else if (cmd.cmd === 'leaveroom') {
          await engine.leavePartyRoom()
          send({ type: 'roomLeft' })
        } else if (cmd.cmd === 'broadcast') {
          engine.broadcastWatchState({
            roomCode: cmd.roomCode,
            action: cmd.action || 'play',
            positionSec: cmd.positionSec || 0
          })
          send({ type: 'broadcastSent' })
        } else if (cmd.cmd === 'chat') {
          engine.sendPartyChat(cmd.text)
          send({ type: 'chatSent' })
        } else if (cmd.cmd === 'reaction') {
          engine.sendPartyReaction(cmd.emoji)
          send({ type: 'reactionSent' })
        } else if (cmd.cmd === 'getroom') {
          send({ type: 'roomInfo', room: engine.getPartyRoom() })
        }
      } catch (err) {
        send({ type: 'error', message: String((err && err.message) || err) })
      }
    }
  })
}

// ─── Orchestrator ───────────────────────────────────────────────────────────

function spawnChild(role, config) {
  const args = [SCRIPT, '--child', role, config.storageDir, config.downloadsDir, config.deviceName]
  if (config.code) args.push(config.code)
  const child = spawn(process.execPath, args, {
    stdio: ['pipe', 'pipe', 'inherit']
  })
  child.config = config
  child._lines = ''
  child._pending = []
  child._history = [] // replay buffer so waitFor can catch already-emitted messages
  child.stdout.on('data', (d) => {
    child._lines += d.toString()
    let idx
    while ((idx = child._lines.indexOf('\n')) >= 0) {
      const line = child._lines.slice(0, idx).trim()
      child._lines = child._lines.slice(idx + 1)
      if (!line) continue
      let msg
      try {
        msg = JSON.parse(line)
      } catch {
        continue
      }
      child._history.push(msg)
      if (child._history.length > 500) child._history.shift()
      child._pending.forEach((p) => p(msg))
    }
  })
  child.waitFor = (predicate, timeoutMs) =>
    new Promise((resolve, reject) => {
      // Replay: if the message already arrived, resolve immediately.
      const existing = child._history.find(predicate)
      if (existing) {
        resolve(existing)
        return
      }
      const handler = (msg) => {
        if (predicate(msg)) {
          cleanup()
          resolve(msg)
        }
      }
      const timer = setTimeout(() => {
        cleanup()
        reject(new Error(`timeout waiting for ${role} message`))
      }, timeoutMs || GLOBAL_TIMEOUT_MS)
      const cleanup = () => {
        clearTimeout(timer)
        const i = child._pending.indexOf(handler)
        if (i >= 0) child._pending.splice(i, 1)
      }
      child._pending.push(handler)
    })
  child.sendCmd = (cmd) => child.stdin.write(JSON.stringify(cmd) + '\n')
  child.stop = () => child.stdin.write('stop\n')
  return child
}

async function main() {
  const isChild = process.argv[2] === '--child'
  if (isChild) {
    const role = process.argv[3]
    await runChild(role, {
      storageDir: process.argv[4],
      downloadsDir: process.argv[5],
      deviceName: process.argv[6],
      code: process.argv[7] || null
    })
    return
  }

  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-watchparty-test-'))
  console.log('[test] tmp root:', tmpRoot)
  const hostDir = path.join(tmpRoot, 'host')
  const joinerDir = path.join(tmpRoot, 'joiner')
  fs.mkdirSync(path.join(hostDir, 'downloads'), { recursive: true })
  fs.mkdirSync(path.join(joinerDir, 'downloads'), { recursive: true })

  // A small fake video file for the host to "stream".
  const videoPath = path.join(tmpRoot, 'movie.mp4')
  fs.writeFileSync(videoPath, Buffer.alloc(256 * 1024, 7))

  const host = spawnChild('host', { storageDir: hostDir, downloadsDir: path.join(hostDir, 'downloads'), deviceName: 'PartyHost' })
  let joiner = null

  try {
    // 1. Host boots and prints its pairing code.
    const hostBoot = await host.waitFor((m) => m.type === 'code', GLOBAL_TIMEOUT_MS).catch(() => null)
    check('host booted (identity present)', !!hostBoot && !!hostBoot.code)

    if (!hostBoot || !hostBoot.code) throw new Error('host did not boot')

    // Spawn the joiner with the host's code so it pairs at boot.
    joiner = spawnChild('joiner', {
      storageDir: joinerDir,
      downloadsDir: path.join(joinerDir, 'downloads'),
      deviceName: 'PartyJoiner',
      code: hostBoot.code
    })
    const joinerBoot = await joiner.waitFor((m) => m.type === 'code', GLOBAL_TIMEOUT_MS).catch(() => null)
    check('joiner booted (identity present)', !!joinerBoot && !!joinerBoot.code)

    // 2. Wait for mutual pairing (both engines emit trust:paired). Retry once
    //    if the first attempt is slow (fresh DHT state after the other suites).
    let pairedOk = false
    for (let attempt = 0; attempt < 2 && !pairedOk; attempt++) {
      try {
        await Promise.all([
          joiner.waitFor((m) => m.type === 'paired', PAIR_TIMEOUT_MS),
          host.waitFor((m) => m.type === 'paired', PAIR_TIMEOUT_MS)
        ])
        pairedOk = true
      } catch (err) {
        if (attempt === 1) throw err
        console.log('[test] pairing attempt timed out, retrying...')
        joiner.sendCmd({ cmd: 'pair', code: hostBoot.code, timeout: PAIR_TIMEOUT_MS })
      }
    }
    check('host and joiner paired', pairedOk)

    // 3. Host creates a party room.
    host.sendCmd({ cmd: 'createroom', title: 'Test Movie Night', filePath: videoPath })
    const roomMsg = await host.waitFor((m) => m.type === 'roomCreated', ROOM_TIMEOUT_MS)
    const room = roomMsg.room
    check('room created with PARTY code', /^PARTY-/.test(room.roomCode || ''), room.roomCode)
    check('room carries title', room.title === 'Test Movie Night', room.title)
    check('room carries filename + size', room.filename === 'movie.mp4' && room.fileSize === 256 * 1024)

    // 4. Joiner joins -> host replies with an immediate snapshot.
    joiner.sendCmd({ cmd: 'joinroom', code: room.roomCode })
    await joiner.waitFor((m) => m.type === 'roomJoined', ROOM_TIMEOUT_MS)
    check('joiner joined room', true)

    // 5. Host broadcasts play state; joiner receives the state sync.
    host.sendCmd({ cmd: 'broadcast', roomCode: room.roomCode, action: 'play', positionSec: 42 })
    await joiner.waitFor((m) => m.type === 'partyEvent' && m.event === 'party:state:sync' && m.data.action === 'play', ROOM_TIMEOUT_MS)
    check('joiner received play state sync (position 42)', true)

    // 6. Host sends chat; joiner receives it.
    host.sendCmd({ cmd: 'chat', text: 'hello party!' })
    const chatMsg = await joiner.waitFor((m) => m.type === 'partyEvent' && m.event === 'party:chat', ROOM_TIMEOUT_MS)
    check('joiner received chat message', chatMsg.data && chatMsg.data.text === 'hello party!', chatMsg.data && chatMsg.data.text)

    // 7. Host reaction; joiner receives it.
    host.sendCmd({ cmd: 'reaction', emoji: '🔥' })
    await joiner.waitFor((m) => m.type === 'partyEvent' && m.event === 'party:reaction', ROOM_TIMEOUT_MS)
    check('joiner received reaction', true)

    // 8. Host-authority: joiner (non-host) broadcasts state in host mode —
    //    the host must NOT re-emit it (feedback-loop guard).
    let hostGotPeerSync = false
    host._pending.push((m) => {
      if (m.type === 'partyEvent' && m.event === 'party:state:sync') hostGotPeerSync = true
    })
    joiner.sendCmd({ cmd: 'broadcast', roomCode: room.roomCode, action: 'pause', positionSec: 7 })
    await waitFor(1500)
    check('host-authority: host dropped non-host state sync', !hostGotPeerSync)

    // 9. Host leaves -> joiner is promoted to host (handoff).
    host.sendCmd({ cmd: 'leaveroom' })
    const hostChanged = await joiner.waitFor(
      (m) => m.type === 'partyEvent' && m.event === 'party:host:changed',
      ROOM_TIMEOUT_MS
    )
    check('joiner promoted to host on handoff', !!hostChanged && hostChanged.data && hostChanged.data.isHost === true)

    // 10. New host's room info reflects host state.
    joiner.sendCmd({ cmd: 'getroom' })
    const info = await joiner.waitFor((m) => m.type === 'roomInfo', ROOM_TIMEOUT_MS)
    check('promoted room isHost=true', info.room && info.room.isHost === true)

    // Cleanup
    joiner.sendCmd({ cmd: 'leaveroom' })
    host.stop()
    joiner.stop()
    await Promise.all([
      host.waitFor((m) => m.type === 'bye', 10000).catch(() => {}),
      joiner.waitFor((m) => m.type === 'bye', 10000).catch(() => {})
    ])
  } catch (err) {
    failures++
    console.error('[test] ERROR:', (err && err.stack) || err)
  } finally {
    try {
      host.kill()
      if (joiner) joiner.kill()
    } catch {}
    await new Promise((r) => setTimeout(r, 500))
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        fs.rmSync(tmpRoot, { recursive: true, force: true })
        console.log('[test] temp dirs removed')
        break
      } catch (err) {
        if (attempt === 2) {
          console.warn(`[test] could not remove ${tmpRoot}: ${err.message}`)
        } else {
          await new Promise((r) => setTimeout(r, 1000))
        }
      }
    }
  }

  console.log(`\n${checks - failures}/${checks} watch-party checks passed`)
  process.exit(failures === 0 ? 0 : 1)
}

main()
