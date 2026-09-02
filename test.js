'use strict'

// End-to-end verification for @mesh/core — no Electron, no IPC.
//
//   node core/test.js
//
// The orchestrator spawns TWO real MeshEngine instances as separate child
// processes (separate storage dirs), then drives the full flow over the
// public DHT:
//
//   1. host starts and prints its pairing code
//   2. joiner calls pairWithCode(code)         -> challenge-response pairing
//   3. joiner calls offerFile(hostPeerId, src) -> file transfer
//   4. host auto-accepts, receives the file,   -> content hash verified
//
// Exit code 0 on success, 1 on any failure. Child processes keep stdout
// clean: every line on stdout is one JSON protocol message; engine logs are
// redirected to stderr.

const { spawn } = require('child_process')
const path = require('path')
const os = require('os')
const fs = require('fs')
const crypto = require('crypto')

const SCRIPT = path.join(__dirname, 'test.js')
const GLOBAL_TIMEOUT_MS = 240 * 1000
const PAIR_TIMEOUT_MS = 90 * 1000
const TRANSFER_TIMEOUT_MS = 60 * 1000
const SOURCE_SIZE = 1280 * 1024 // 1.25 MiB -> 20 blocks of 64 KiB (integer)

// ─── Child process (one per MeshEngine instance) ────────────────────────────

async function runChild(role, config) {
  // Route all engine console noise to stderr so stdout stays pure JSON.
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
    autoAcceptOffers: true, // explicit auto-accept for automated test runner
    autoTrustLAN: false, // force the challenge-response pairing path
    lanDiscovery: false // pairing + transfers go over the DHT
  })

  engine.on('error', (err) => {
    console.error('[engine error]', err && err.message)
    send({ type: 'error', message: String((err && err.message) || err) })
  })

  let hostPeerKey = null

  engine.on('trust:paired', ({ peer }) => {
    hostPeerKey = hostPeerKey || peer.publicKey
    send({ type: 'paired', peer })
  })
  engine.on('transfer:offer', (offer) => {
    send({ type: 'offer', transferId: offer.transferId, filename: offer.filename })
  })
  engine.on('transfer:completed', (record) => {
    send({ type: 'completed', transferId: record.id, destPath: record.destPath })
  })

  // Graceful stop on request from the orchestrator.
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
      if (cmd.cmd === 'createdrop') {
        // Host side: stage a WeTransfer-style one-time share and advertise it.
        // Accepts a single file ({path}) or multiple ({files: [{path, filename, size}]}).
        try {
          const share = await engine.createDropShare({
            filePath: cmd.path,
            filename: cmd.filename || undefined,
            fileSize: cmd.size || 0,
            files: Array.isArray(cmd.files)
              ? cmd.files.map((f) => ({
                  filePath: f.path,
                  filename: f.filename,
                  fileSize: f.size || 0
                }))
              : undefined,
            folderPath: cmd.folderPath || undefined,
            expirationPreset: cmd.preset || '30m'
          })
          send({ type: 'dropCreated', code: share.code })
        } catch (err) {
          send({ type: 'error', message: String((err && err.message) || err) })
        }
      } else if (cmd.cmd === 'claim') {
        // Claimer side: claim a DROP code (no pairing involved).
        try {
          await engine.claimDropCode(cmd.code)
          send({ type: 'claimed', code: cmd.code })
        } catch (err) {
          send({ type: 'error', message: String((err && err.message) || err) })
        }
      } else if (cmd.cmd === 'rmdevice') {
        // Delete a device + permanently revoke its key (mirrors the Electron
        // DEVICES_REMOVE handler) and tear down any live session with it.
        try {
          const bee = await engine.getBee('devices')
          for await (const node of bee.createReadStream()) {
            const dev = node.value
            if (dev && dev.publicKey === cmd.key) {
              await bee.del(node.key)
            }
          }
          engine.trustManager.removeTrustedKey(cmd.key)
          await engine.trustManager.revokeKey(cmd.key)
          await engine.trustManager.rotateHostPairingCode()
          for (const [pid, peerObj] of engine.peers.entries()) {
            if (pid === cmd.key || (peerObj.device && peerObj.device.publicKey === cmd.key)) {
              try {
                peerObj.connection.destroy()
              } catch {}
            }
          }
          send({ type: 'rmdevice-done', key: cmd.key })
        } catch (err) {
          send({ type: 'error', message: String((err && err.message) || err) })
        }
      } else if (cmd.cmd === 'reconnect') {
        // Force a reconnect pass (the 15s interval also runs this): the peer
        // still holds a trusted record for the other side, so this exercises
        // exactly the auto re-add path.
        try {
          await engine.connections.reconnectKnownPeers()
          send({ type: 'reconnected' })
        } catch (err) {
          send({ type: 'error', message: String((err && err.message) || err) })
        }
      } else if (cmd.cmd === 'listdevices') {
        try {
          const bee = await engine.getBee('devices')
          const devices = []
          for await (const node of bee.createReadStream()) {
            devices.push(node.value)
          }
          send({ type: 'devices', devices })
        } catch (err) {
          send({ type: 'error', message: String((err && err.message) || err) })
        }
      } else if (cmd.cmd === 'getcode') {
        send({ type: 'code', code: engine.trustManager.getActiveHostCode() || '' })
      } else if (cmd.cmd === 'pair') {
        // Explicit pairing with a code (the "use the key again" path).
        try {
          const peer = await engine.pairWithCode(cmd.code, { timeoutMs: PAIR_TIMEOUT_MS })
          send({ type: 'paired', peer })
        } catch (err) {
          send({ type: 'error', message: String((err && err.message) || err) })
        }
      } else if (cmd.cmd === 'mksync') {
        try {
          const targetPeer = cmd.peerId || hostPeerKey || Array.from(engine.peers.keys())[0]
          const lib = await engine.addSyncLibrary({
            path: cmd.path,
            peerId: targetPeer,
            name: cmd.name,
            mode: cmd.mode
          })
          send({ type: 'syncAdded', id: lib.id, status: lib.status, fileCount: lib.fileCount })
        } catch (err) {
          console.error(`[${role}] mksync failed:`, err)
          send({ type: 'error', message: String((err && err.message) || err) })
        }
      } else if (cmd.cmd === 'acceptsync') {
        try {
          const res = await engine.acceptSyncInvite({ id: cmd.id, customPath: cmd.customPath })
          send({ type: 'syncAccepted', id: cmd.id, path: res.path })
        } catch (err) {
          send({ type: 'error', message: String((err && err.message) || err) })
        }
      } else if (cmd.cmd === 'syncnow') {
        try {
          await engine.syncLibrary(cmd.id)
          send({ type: 'synced', id: cmd.id })
        } catch (err) {
          send({ type: 'error', message: String((err && err.message) || err) })
        }
      } else if (cmd.cmd === 'listsync') {
        try {
          send({ type: 'syncLibs', libs: engine.listSyncLibraries() })
        } catch (err) {
          send({ type: 'error', message: String((err && err.message) || err) })
        }
      }
    }
  })

  await engine.start()
  // Pairing intent: in the app this fires when the pairing screen opens —
  // the relay fallback engages immediately (lazy 'auto' mode) instead of
  // waiting out the 30s zero-peer window, so pairing timing matches the UX.
  engine.setPairingIntent(true)
  send({ type: 'ready', identity: engine.getIdentity() })

  if (role === 'joiner') {
    send({ type: 'pairing', code: config.code })
    const peer = await engine.pairWithCode(config.code, { timeoutMs: PAIR_TIMEOUT_MS })
    hostPeerKey = peer.publicKey
    send({ type: 'paired', peer })
    const record = await engine.offerFile(peer.publicKey, config.sourcePath)
    send({ type: 'offered', transferId: record.id, filename: record.filename })
  }

  // Keep the process alive until the orchestrator says stop.
  await new Promise(() => {})
}

// ─── Orchestrator ───────────────────────────────────────────────────────────

class Child {
  constructor(name, args) {
    this.name = name
    this.proc = spawn(process.execPath, [SCRIPT, '--child', ...args], {
      stdio: ['pipe', 'pipe', 'pipe']
    })
    this.lines = []
    this.waiters = []
    this.proc.stdout.on('data', (d) => this._onData(d))
    this.proc.stderr.on('data', (d) => process.stderr.write(`[${name}] ${d}`))
    this.proc.on('exit', (code) => {
      this.exited = code
      for (const w of this.waiters.splice(0)) w.reject(new Error(`${name} exited early (code ${code})`))
    })
  }

  _onData(d) {
    const text = d.toString()
    for (const line of text.split('\n')) {
      if (!line.trim()) continue
      let msg
      try {
        msg = JSON.parse(line)
      } catch {
        process.stderr.write(`[${this.name}] non-JSON stdout: ${line}\n`)
        continue
      }
      this.lines.push(msg)
      // Resolve the FIRST matching waiter but keep the others: earlier code
      // spliced ALL waiters on every message, silently dropping any waiter
      // whose test did not match THIS message (it was never re-registered, so
      // the awaited message later found no waiter and the timer timed out).
      for (let i = 0; i < this.waiters.length; i++) {
        if (this.waiters[i].test(msg)) {
          const [w] = this.waiters.splice(i, 1)
          w.resolve(msg)
          break
        }
      }
    }
  }

  waitFor(test, timeoutMs, label, fromIndex = 0) {
    // fromIndex: ignore messages that arrived before it (pass a snapshot of
    // this.lines.length) so stale matches from earlier phases are skipped.
    const at = (m) => this.lines.indexOf(m) >= fromIndex && test(m)
    const existing = this.lines.slice(fromIndex).find(test)
    if (existing) return Promise.resolve(existing)
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`${this.name} timed out waiting for ${label}`)),
        timeoutMs
      )
      this.waiters.push({
        test: at,
        resolve: (msg) => {
          clearTimeout(timer)
          resolve(msg)
        },
        reject: () => clearTimeout(timer)
      })
    })
  }

  stop() {
    return new Promise((resolve) => {
      if (this.exited !== undefined) return resolve()
      this.proc.stdin.write('stop\n')
      const timer = setTimeout(() => {
        try {
          this.proc.kill()
        } catch {}
        resolve()
      }, 8000)
      this.proc.on('exit', () => {
        clearTimeout(timer)
        resolve()
      })
    })
  }
}

function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const h = crypto.createHash('sha256')
    const s = fs.createReadStream(filePath)
    s.on('data', (d) => h.update(d))
    s.on('end', () => resolve(h.digest('hex')))
    s.on('error', reject)
  })
}

// Wait until `count` distinct messages matching `test` have arrived since
// `fromIndex` (the child's line buffer grows; pass a snapshot of
// child.lines.length to ignore older messages). Resolves with the matching
// messages; throws on timeout.
// `dedupKey`: optional function(msg) => string — when provided, messages with
// the same key are treated as the SAME logical event (only the most recent
// version is kept). Useful for sync completions where a retry for the same
// file should not count as two distinct deliveries.
async function waitForCount(child, test, count, timeoutMs, label, fromIndex = 0, dedupKey = null) {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const matching = child.lines.slice(fromIndex).filter((m) => test(m))
    let effective
    if (dedupKey) {
      // Keep only the most recent completion per logical key (last write wins).
      const byKey = new Map()
      for (const m of matching) byKey.set(dedupKey(m), m)
      effective = Array.from(byKey.values())
    } else {
      // Default: deduplicate by full JSON identity (original behaviour).
      const seen = new Set()
      effective = []
      for (const m of matching) {
        const k = JSON.stringify(m)
        if (!seen.has(k)) { seen.add(k); effective.push(m) }
      }
    }
    if (effective.length >= count) return effective.slice(0, count)
    await new Promise((r) => setTimeout(r, 250))
  }
  throw new Error(`${child.name} timed out waiting for ${label}`)
}

async function main() {
  const started = Date.now()
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-core-test-'))
  const hostDir = path.join(tmpRoot, 'host')
  const joinDir = path.join(tmpRoot, 'joiner')
  const sourcePath = path.join(tmpRoot, 'source.bin')
  const results = []

  const ok = (name, cond, detail = '') => {
    results.push({ name, pass: !!cond, detail })
    console.log(`${cond ? 'PASS' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}`)
    return !!cond
  }

  let host = null
  let joiner = null
  const watchdog = setTimeout(() => {
    console.error('GLOBAL TIMEOUT — aborting')
    if (host) host.proc.kill()
    if (joiner) joiner.proc.kill()
    process.exit(1)
  }, GLOBAL_TIMEOUT_MS)

  try {
    console.log(`[test] tmp root: ${tmpRoot}`)
    console.log('[test] writing source file...')
    const chunk = Buffer.alloc(65536)
    const stream = fs.createWriteStream(sourcePath)
    let remaining = SOURCE_SIZE
    while (remaining > 0) {
      crypto.randomFillSync(chunk)
      const n = Math.min(chunk.length, remaining)
      stream.write(chunk.subarray(0, n))
      remaining -= n
    }
    await new Promise((r) => stream.end(r))
    const sourceHash = await sha256File(sourcePath)
    ok('source file written', fs.statSync(sourcePath).size === SOURCE_SIZE)

    // ── 1. Host boots, reveals pairing code ──────────────────────────────
    console.log('[test] spawning host engine...')
    host = new Child('host', [
      'host',
      path.join(hostDir, 'storage'),
      path.join(hostDir, 'downloads'),
      'TestHost'
    ])
    const hostReady = await host.waitFor(
      (m) => m.type === 'ready' && m.identity && m.identity.pairingCode,
      90 * 1000,
      'host ready (identity + pairing code)'
    )
    const hostIdentity = hostReady.identity
    ok('host has deviceId', typeof hostIdentity.deviceId === 'string' && hostIdentity.deviceId.length > 0)
    ok('host has noise publicKey', /^[0-9a-f]{64}$/.test(hostIdentity.publicKey))
    ok('host has pairing code', /^MD-[A-Z2-9]{4}(-[A-Z2-9]{4}){3}$/.test(hostIdentity.pairingCode))
    console.log(`[test] host code: ${hostIdentity.pairingCode}`)

    // ── 2. Joiner boots and pairs with the code ──────────────────────────
    console.log('[test] spawning joiner engine...')
    joiner = new Child('joiner', [
      'joiner',
      path.join(joinDir, 'storage'),
      path.join(joinDir, 'downloads'),
      'TestJoiner',
      hostIdentity.pairingCode,
      sourcePath
    ])
    const joinerReady = await joiner.waitFor(
      (m) => m.type === 'ready',
      90 * 1000,
      'joiner ready'
    )
    ok('joiner ready', !!joinerReady)

    console.log('[test] waiting for mutual pairing...')
    const hostPaired = await host.waitFor(
      (m) => m.type === 'paired' && m.peer && m.peer.publicKey,
      PAIR_TIMEOUT_MS + 30 * 1000,
      'host trust:paired'
    )
    const joinerPaired = await joiner.waitFor(
      (m) => m.type === 'paired' && m.peer && m.peer.publicKey,
      PAIR_TIMEOUT_MS + 30 * 1000,
      'joiner trust:paired'
    )
    const hostSawJoiner = ok(
      'host paired with joiner',
      hostPaired.peer.name === 'TestJoiner' && hostPaired.peer.isTrusted === true,
      `${hostPaired.peer.name} (${hostPaired.peer.id})`
    )
    const joinerSawHost = ok(
      'joiner paired with host',
      joinerPaired.peer.name === 'TestHost' && joinerPaired.peer.isTrusted === true,
      `${joinerPaired.peer.name} (${joinerPaired.peer.id})`
    )
    const joinerPeerKey = joinerPaired.peer.publicKey
    ok('joiner learned host peer key', joinerPeerKey === hostIdentity.publicKey)
    const hostJoinerKey = hostPaired.peer.publicKey // joiner's noise key as the host sees it

    // ── 3. Joiner offers a file to the host ──────────────────────────────
    if (hostSawJoiner && joinerSawHost) {
      console.log('[test] offering file from joiner -> host...')
      const offered = await joiner.waitFor((m) => m.type === 'offered', 30 * 1000, 'joiner offerFile')
      const offerReceived = await host.waitFor(
        (m) => m.type === 'offer',
        TRANSFER_TIMEOUT_MS,
        'host transfer:offer'
      )
      ok('host received transfer:offer', offerReceived.transferId === offered.transferId)
      ok('offer carries filename', offerReceived.filename === 'source.bin')

      console.log('[test] waiting for transfer to complete...')
      const completed = await host.waitFor(
        (m) => m.type === 'completed' && m.transferId === offered.transferId,
        TRANSFER_TIMEOUT_MS,
        'host transfer:completed'
      )
      ok('host completed transfer', typeof completed.destPath === 'string' && completed.destPath.length > 0)

      // ── 4. Content verification ────────────────────────────────────────
      const destHash = await sha256File(completed.destPath)
      const received = fs.statSync(completed.destPath).size
      ok('received file size matches', received === SOURCE_SIZE, `${received} bytes`)
      ok('received file content matches (sha256)', destHash === sourceHash, destHash.slice(0, 16))
    } else {
      ok('file transfer attempted (pairing failed)', false, 'skipped: pairing did not complete')
    }

    // ── 5. One-time DROP claim (WeTransfer-style, no pairing) ────────────
    console.log('[test] writing drop source file...')
    const dropSourcePath = path.join(tmpRoot, 'drop-source.bin')
    const DROP_SIZE = 640 * 1024 // 640 KiB -> 10 blocks
    const dstream = fs.createWriteStream(dropSourcePath)
    let dremaining = DROP_SIZE
    while (dremaining > 0) {
      crypto.randomFillSync(chunk)
      const n = Math.min(chunk.length, dremaining)
      dstream.write(chunk.subarray(0, n))
      dremaining -= n
    }
    await new Promise((r) => dstream.end(r))
    const dropHash = await sha256File(dropSourcePath)
    ok('drop source file written', fs.statSync(dropSourcePath).size === DROP_SIZE)

    console.log('[test] host creates a drop share...')
    host.proc.stdin.write(
      JSON.stringify({ cmd: 'createdrop', path: dropSourcePath, filename: 'drop-source.bin', size: DROP_SIZE }) + '\n'
    )
    const dropCreated = await host.waitFor((m) => m.type === 'dropCreated' && m.code, 60 * 1000, 'host drop share created')
    ok('host created DROP code', /^DROP-[A-Z2-9]{4}-[A-Z2-9]{4}$/.test(dropCreated.code), dropCreated.code)

    console.log('[test] joiner claims the drop code...')
    joiner.proc.stdin.write(JSON.stringify({ cmd: 'claim', code: dropCreated.code }) + '\n')
    const claimed = await joiner.waitFor((m) => m.type === 'claimed', 60 * 1000, 'joiner claim accepted')
    ok('joiner claimed the code', claimed.code === dropCreated.code)

    console.log('[test] waiting for claimed transfer to complete...')
    const claimCompleted = await joiner.waitFor(
      (m) => m.type === 'completed' && m.transferId && m.transferId.startsWith('claim-'),
      TRANSFER_TIMEOUT_MS + 30 * 1000,
      'joiner claim transfer:completed'
    )
    ok(
      'joiner completed claimed transfer',
      typeof claimCompleted.destPath === 'string' && claimCompleted.destPath.length > 0
    )
    const claimDestHash = await sha256File(claimCompleted.destPath)
    const claimReceived = fs.statSync(claimCompleted.destPath).size
    ok('claimed file size matches', claimReceived === DROP_SIZE, `${claimReceived} bytes`)
    ok('claimed file content matches (sha256)', claimDestHash === dropHash, claimDestHash.slice(0, 16))

    // ── 6. Multi-file DROP claim (folder-style share, 2 files) ──────────
    console.log('[test] writing multi-file drop sources...')
    const dropA = path.join(tmpRoot, 'drop-a.bin')
    const dropB = path.join(tmpRoot, 'drop-b.bin')
    const SIZE_A = 400 * 1024
    const SIZE_B = 512 * 1024
    for (const [p, size] of [[dropA, SIZE_A], [dropB, SIZE_B]]) {
      const s = fs.createWriteStream(p)
      let rem = size
      while (rem > 0) {
        crypto.randomFillSync(chunk)
        const n = Math.min(chunk.length, rem)
        s.write(chunk.subarray(0, n))
        rem -= n
      }
      await new Promise((r) => s.end(r))
    }
    const hashA = await sha256File(dropA)
    const hashB = await sha256File(dropB)
    ok('multi-file drop sources written', fs.statSync(dropA).size === SIZE_A && fs.statSync(dropB).size === SIZE_B)

    console.log('[test] host creates a 2-file drop share...')
    const hostBeforeMulti = host.lines.length
    const joinBeforeMulti = joiner.lines.length
    host.proc.stdin.write(
      JSON.stringify({
        cmd: 'createdrop',
        files: [
          { path: dropA, filename: 'drop-a.bin', size: SIZE_A },
          // Nested relative path: the receive side flattens it to a safe name.
          { path: dropB, filename: 'sub/drop-b.bin', size: SIZE_B }
        ]
      }) + '\n'
    )
    const multiDrop = await host.waitFor(
      (m) => m.type === 'dropCreated' && m.code,
      60 * 1000,
      'host multi-file drop created',
      hostBeforeMulti
    )
    ok('host created multi-file DROP code', /^DROP-[A-Z2-9]{4}-[A-Z2-9]{4}$/.test(multiDrop.code), multiDrop.code)

    console.log('[test] joiner claims the multi-file code...')
    joiner.proc.stdin.write(JSON.stringify({ cmd: 'claim', code: multiDrop.code }) + '\n')
    const claimedMulti = await joiner.waitFor(
      (m) => m.type === 'claimed',
      60 * 1000,
      'joiner multi-file claim accepted',
      joinBeforeMulti
    )
    ok('joiner claimed the multi-file code', claimedMulti.code === multiDrop.code)

    console.log('[test] waiting for BOTH claimed files to complete...')
    const claimCompletions = await waitForCount(
      joiner,
      (m) => m.type === 'completed' && m.transferId && m.transferId.startsWith('claim-'),
      2,
      TRANSFER_TIMEOUT_MS + 60 * 1000,
      'both multi-file claim transfers',
      joinBeforeMulti
    )
    ok('both claimed files completed', claimCompletions.length === 2, `${claimCompletions.length}/2`)

    const destByHash = new Map()
    for (const m of claimCompletions) {
      const h = await sha256File(m.destPath)
      destByHash.set(h, m.destPath)
    }
    ok('multi-file #1 content matches (sha256)', destByHash.has(hashA), hashA.slice(0, 16))
    ok('multi-file #2 content matches (sha256)', destByHash.has(hashB), hashB.slice(0, 16))
    // The nested relative path must have been flattened to a safe filename.
    const flatName = claimCompletions.find((m) => m.destPath.endsWith('sub_drop-b.bin'))
    ok('nested path flattened to safe filename', !!flatName, flatName ? flatName.destPath : 'missing')

    // ── 7. Folder DROP claim (folderPath → recursive enumeration) ───────
    console.log('[test] writing folder share (root file + nested file)...')
    const folderRoot = path.join(tmpRoot, 'share-folder')
    const nestedDir = path.join(folderRoot, 'nested')
    fs.mkdirSync(nestedDir, { recursive: true })
    const rootFile = path.join(folderRoot, 'root.txt')
    const deepFile = path.join(nestedDir, 'deep.bin')
    const ROOT_SIZE = 96 * 1024
    const DEEP_SIZE = 224 * 1024
    for (const [p, size] of [[rootFile, ROOT_SIZE], [deepFile, DEEP_SIZE]]) {
      const s = fs.createWriteStream(p)
      let rem = size
      while (rem > 0) {
        crypto.randomFillSync(chunk)
        const n = Math.min(chunk.length, rem)
        s.write(chunk.subarray(0, n))
        rem -= n
      }
      await new Promise((r) => s.end(r))
    }
    const rootHash = await sha256File(rootFile)
    const deepHash = await sha256File(deepFile)
    ok('folder share files written', fs.statSync(rootFile).size === ROOT_SIZE && fs.statSync(deepFile).size === DEEP_SIZE)

    console.log('[test] host creates a folder drop share...')
    const hostBeforeFolder = host.lines.length
    const joinBeforeFolder = joiner.lines.length
    host.proc.stdin.write(JSON.stringify({ cmd: 'createdrop', folderPath: folderRoot }) + '\n')
    const folderDrop = await host.waitFor(
      (m) => m.type === 'dropCreated' && m.code,
      60 * 1000,
      'host folder drop created',
      hostBeforeFolder
    )
    ok('host created folder DROP code', /^DROP-[A-Z2-9]{4}-[A-Z2-9]{4}$/.test(folderDrop.code), folderDrop.code)

    console.log('[test] joiner claims the folder code...')
    joiner.proc.stdin.write(JSON.stringify({ cmd: 'claim', code: folderDrop.code }) + '\n')
    const claimedFolder = await joiner.waitFor(
      (m) => m.type === 'claimed',
      60 * 1000,
      'joiner folder claim accepted',
      joinBeforeFolder
    )
    ok('joiner claimed the folder code', claimedFolder.code === folderDrop.code)

    console.log('[test] waiting for BOTH folder files to complete...')
    const folderCompletions = await waitForCount(
      joiner,
      (m) => m.type === 'completed' && m.transferId && m.transferId.startsWith('claim-'),
      2,
      TRANSFER_TIMEOUT_MS + 60 * 1000,
      'both folder claim transfers',
      joinBeforeFolder
    )
    ok('both folder files completed', folderCompletions.length === 2, `${folderCompletions.length}/2`)

    const folderDestByHash = new Map()
    for (const m of folderCompletions) {
      const h = await sha256File(m.destPath)
      folderDestByHash.set(h, m.destPath)
    }
    ok('folder root file content matches (sha256)', folderDestByHash.has(rootHash), rootHash.slice(0, 16))
    ok('folder nested file content matches (sha256)', folderDestByHash.has(deepHash), deepHash.slice(0, 16))
    const flatNested = folderCompletions.find((m) => m.destPath.endsWith('nested_deep.bin'))
    ok('nested folder file flattened to safe name', !!flatNested, flatNested ? flatNested.destPath : 'missing')

    // ── 7b. One-way folder sync (joiner -> host) ─────────────────────────
    // Fast stat indexing + zero-duplication direct streaming over the DHT.
    console.log('[test] writing sync source folder...')
    const syncDir = path.join(tmpRoot, 'sync-src')
    const syncNested = path.join(syncDir, 'sub')
    fs.mkdirSync(syncNested, { recursive: true })
    const syncRootFile = path.join(syncDir, 'note.txt')
    const syncDeepFile = path.join(syncNested, 'photo.bin')
    fs.writeFileSync(syncRootFile, `sync root file ${Date.now()}`)
    const SYNCDEEP_SIZE = 256 * 1024
    const sstream = fs.createWriteStream(syncDeepFile)
    let sremaining = SYNCDEEP_SIZE
    while (sremaining > 0) {
      crypto.randomFillSync(chunk)
      const n = Math.min(chunk.length, sremaining)
      sstream.write(chunk.subarray(0, n))
      sremaining -= n
    }
    await new Promise((r) => sstream.end(r))
    const syncRootHash = await sha256File(syncRootFile)
    const syncDeepHash = await sha256File(syncDeepFile)
    ok(
      'sync source files written',
      fs.statSync(syncRootFile).size > 0 && fs.statSync(syncDeepFile).size === SYNCDEEP_SIZE
    )

    const hostBeforeSync = host.lines.length
    const joinerBeforeSync = joiner.lines.length
    console.log('[test] joiner starts syncing the folder to the host...')
    joiner.proc.stdin.write(
      JSON.stringify({ cmd: 'mksync', path: syncDir, name: 'TestSync' }) + '\n'
    )
    const syncAdded = await joiner.waitFor(
      (m) => m.type === 'syncAdded',
      30 * 1000,
      'joiner sync library added',
      joinerBeforeSync
    )
    ok('joiner added sync library', syncAdded.status !== 'error', `${syncAdded.fileCount} file(s) indexed`)

    console.log('[test] host accepts the sync invite...')
    host.proc.stdin.write(
      JSON.stringify({ cmd: 'acceptsync', id: syncAdded.id }) + '\n'
    )
    await host.waitFor((m) => m.type === 'syncAccepted', 30 * 1000, 'host accepted sync invite')

    console.log('[test] waiting for both sync files to complete on the host...')
    const syncCompletions = await waitForCount(
      host,
      (m) => m.type === 'completed' && m.destPath && m.destPath.includes(path.join('Sync', 'TestSync')),
      2,
      TRANSFER_TIMEOUT_MS,
      'host sync transfers',
      hostBeforeSync,
      (m) => m.destPath  // deduplicate by destination file path — retries for the same file count once
    )
    ok('both sync files completed', syncCompletions.length === 2, `${syncCompletions.length}/2`)

    const syncDestByHash = new Map()
    for (const m of syncCompletions) {
      const h = await sha256File(m.destPath)
      syncDestByHash.set(h, m.destPath)
    }
    ok('sync root file content matches (sha256)', syncDestByHash.has(syncRootHash), syncRootHash.slice(0, 16))
    ok('sync nested file content matches (sha256)', syncDestByHash.has(syncDeepHash), syncDeepHash.slice(0, 16))

    // ── 7b. One-way push (single-owner backup) ────────────────────────────────
    // The simplified mobile model: a send-only library's owner diffs against
    // its own sentIndex; the receiver is a pure sink (no index exchange, no
    // echo). This is the phone→desktop backup path.
    console.log('[test] one-way push sync (single-owner backup)...')
    const pushDir = path.join(tmpRoot, 'push-src')
    fs.mkdirSync(path.join(pushDir, 'nested'), { recursive: true })
    const pushRootFile = path.join(pushDir, 'backup.txt')
    const pushDeepFile = path.join(pushDir, 'nested', 'photo.bin')
    fs.writeFileSync(pushRootFile, `backup root ${Date.now()}`)
    fs.writeFileSync(pushDeepFile, crypto.randomBytes(128 * 1024))
    const pushRootHash = await sha256File(pushRootFile)
    const pushDeepHash = await sha256File(pushDeepFile)

    const joinerBeforePush = joiner.lines.length
    const hostBeforePush = host.lines.length
    joiner.proc.stdin.write(
      JSON.stringify({ cmd: 'mksync', path: pushDir, name: 'PushSync', mode: 'send-only' }) + '\n'
    )
    const pushAdded = await joiner.waitFor(
      (m) => m.type === 'syncAdded',
      30 * 1000,
      'joiner push library added',
      joinerBeforePush
    )
    ok('joiner added push library', pushAdded.status !== 'error', `${pushAdded.fileCount} file(s) indexed`)

    host.proc.stdin.write(
      JSON.stringify({ cmd: 'acceptsync', id: pushAdded.id }) + '\n'
    )
    await host.waitFor((m) => m.type === 'syncAccepted', 30 * 1000, 'host accepted push sync invite')

    const pushCompletions = await waitForCount(
      host,
      (m) => m.type === 'completed' && m.destPath && m.destPath.includes(path.join('Sync', 'PushSync')),
      2,
      TRANSFER_TIMEOUT_MS,
      'host push sync transfers',
      hostBeforePush
    )
    ok('both push files completed', pushCompletions.length === 2, `${pushCompletions.length}/2`)
    const pushDestByHash = new Map()
    for (const m of pushCompletions) {
      const h = await sha256File(m.destPath)
      pushDestByHash.set(h, m.destPath)
    }
    ok('push root file content matches (sha256)', pushDestByHash.has(pushRootHash), pushRootHash.slice(0, 16))
    ok('push nested file content matches (sha256)', pushDestByHash.has(pushDeepHash), pushDeepHash.slice(0, 16))

    // The receiver must stay a pure sink: verify its library is receive_only
    // (no echo — the peer's folder can never push back to the owner).
    // The receiver's index updates via its watcher (1s debounce), so poll —
    // each attempt scans only NEW lines for the latest listsync response.
    let pushLib = null
    for (let attempt = 0; attempt < 15 && (!pushLib || pushLib.fileCount !== 2); attempt++) {
      if (attempt > 0) await new Promise((r) => setTimeout(r, 1000))
      const from = host.lines.length
      host.proc.stdin.write(JSON.stringify({ cmd: 'listsync' }) + '\n')
      const hostLibs = await host.waitFor(
        (m) => m.type === 'syncLibs' && Array.isArray(m.libs),
        10 * 1000,
        'host lists push library',
        from
      )
      pushLib = hostLibs.libs.find((l) => l.name === 'PushSync')
    }
    ok('receiver library is receive_only (pure sink)', pushLib && pushLib.mode === 'receive_only', pushLib && pushLib.mode)
    ok('receiver library has both files', pushLib && pushLib.fileCount === 2, pushLib && pushLib.fileCount)

    // ── 7c. Re-sync over an existing folder: receiver skip + mtime stamping ──
    // Regression: syncMtimeMs must reach the receiver. When it does not, the
    // receiver's skip can never match (existing files re-upload every round),
    // and received files keep their write-time mtime instead of the sender's
    // original — the exact "re-sync re-sends everything" bug.
    console.log('[test] re-sync over an existing folder (receiver skip)...')
    const oldStamp = new Date('2024-01-01T00:00:00Z')
    fs.utimesSync(pushRootFile, oldStamp, oldStamp)
    fs.utimesSync(pushDeepFile, oldStamp, oldStamp)

    // Re-creating the link over the same folder = fresh library id + empty
    // sentIndex → the owner re-diffs every file.
    const joinerBeforeResync = joiner.lines.length
    joiner.proc.stdin.write(
      JSON.stringify({ cmd: 'mksync', path: pushDir, name: 'PushSync2', mode: 'send-only' }) + '\n'
    )
    const resyncAdded = await joiner.waitFor(
      (m) => m.type === 'syncAdded',
      30 * 1000,
      'joiner re-sync library added',
      joinerBeforeResync
    )
    ok('joiner added re-sync library', resyncAdded.status !== 'error')
    const hostBeforeResync = host.lines.length
    host.proc.stdin.write(JSON.stringify({ cmd: 'acceptsync', id: resyncAdded.id }) + '\n')
    await host.waitFor((m) => m.type === 'syncAccepted', 30 * 1000, 'host accepted re-sync invite')

    const resyncCompletions = await waitForCount(
      host,
      (m) => m.type === 'completed' && m.destPath && m.destPath.includes(path.join('Sync', 'PushSync2')),
      2,
      TRANSFER_TIMEOUT_MS,
      'host re-sync transfers',
      hostBeforeResync
    )
    ok('both re-sync files completed', resyncCompletions.length === 2, `${resyncCompletions.length}/2`)

    // The receiver must stamp the sender's ORIGINAL mtime onto the received
    // file. With the regression the dest keeps its write-time mtime and every
    // re-sync re-uploads it forever.
    for (const m of resyncCompletions) {
      const st = fs.statSync(m.destPath)
      ok(
        `dest mtime stamped to sender's original (${path.basename(m.destPath)})`,
        Math.abs(st.mtimeMs - oldStamp.getTime()) < 2000,
        `mtime=${st.mtimeMs} want≈${oldStamp.getTime()}`
      )
    }

    // The batch pre-verification closes the loop: a further round confirms
    // everything is already present (stamped mtimes match), so zero new
    // transfers are created and the destination files are untouched.
    const push2Dir = path.dirname(resyncCompletions[0].destPath)
    const push2RootPath = path.join(push2Dir, 'backup.txt')
    const birthBeforeVerify = fs.statSync(push2RootPath).birthtimeMs
    joiner.proc.stdin.write(JSON.stringify({ cmd: 'syncnow', id: resyncAdded.id }) + '\n')
    await joiner.waitFor((m) => m.type === 'synced' && m.id === resyncAdded.id, 30 * 1000, 'joiner verify round done')
    const verifyTransfers = host.lines
      .slice(hostBeforeResync)
      .filter((m) => m.type === 'completed' && m.destPath && m.destPath.includes(path.join('Sync', 'PushSync2')))
      .filter((m) => m.destPath !== resyncCompletions[0].destPath && m.destPath !== resyncCompletions[1].destPath)
    ok('re-sync verify: zero transfers created for identical files', verifyTransfers.length === 0, `${verifyTransfers.length} transfers`)
    const birthAfterVerify = fs.statSync(push2RootPath).birthtimeMs
    ok(
      're-sync verify: identical existing file was not rewritten',
      Math.abs(birthAfterVerify - birthBeforeVerify) < 500,
      `birthBefore=${birthBeforeVerify} birthAfter=${birthAfterVerify}`
    )

    // ── 7d. Self-healing backup: files lost on the receiver are re-pushed ────
    // The backup must repair itself: delete a received file on the receiver
    // and the next round must re-push it (the verify re-checks already-sent
    // files). Interrupted re-pushes self-heal within ~2s (backoff re-round).
    console.log('[test] self-healing: receiver file deleted → re-pushed...')
    const healedPath = push2RootPath
    const hostBeforeHeal = host.lines.length
    fs.rmSync(healedPath)
    const healCompletions = await waitForCount(
      host,
      (m) => m.type === 'completed' && m.destPath === healedPath,
      1,
      TRANSFER_TIMEOUT_MS,
      'host self-heal re-push',
      hostBeforeHeal
    )
    ok('self-heal: deleted receiver file was re-pushed', healCompletions.length === 1, `${healCompletions.length}/1`)
    ok('self-heal: file content restored', fs.readFileSync(healedPath, 'utf8') === fs.readFileSync(pushRootFile, 'utf8'))

    // ── 8. Deleted device must not re-add itself; the key re-admits it ───
    // Regression: a device deleted from the device list used to re-add itself
    // seconds later — the live pairing code re-challenged the reconnecting
    // peer and re-granted trust. Deletion must break the CURRENT trust without
    // blacklisting the key: re-adding works only via a fresh pairing with the
    // (rotated) current code.
    console.log('[test] deleting joiner from host and forcing a reconnect...')
    const hostBeforeRm = host.lines.length
    host.proc.stdin.write(JSON.stringify({ cmd: 'rmdevice', key: hostJoinerKey }) + '\n')
    const rmDone = await host.waitFor(
      (m) => m.type === 'rmdevice-done' && m.key === hostJoinerKey,
      30 * 1000,
      'host device removed',
      hostBeforeRm
    )
    ok('host removed the joiner device', !!rmDone)
    // Snapshot AFTER the delete handler completed: any trust:paired emitted
    // before it is ordered before this index on the same stdout stream.
    const hostAfterRm = host.lines.length

    // The joiner still holds a trusted record for the host, so it tries to
    // reconnect (as its 15s interval does). The host must refuse it — the
    // pairing code was rotated, so the joiner can no longer answer challenges.
    joiner.proc.stdin.write(JSON.stringify({ cmd: 'reconnect' }) + '\n')
    await joiner.waitFor((m) => m.type === 'reconnected', 30 * 1000, 'joiner reconnect attempted')
    // Give the reconnect window time to land: connection + unanswered challenge.
    await new Promise((r) => setTimeout(r, 8000))

    const rePaired = host.lines
      .slice(hostAfterRm)
      .find((m) => m.type === 'paired' && m.peer && m.peer.publicKey === hostJoinerKey)
    ok(
      'deleted device did NOT auto re-add after reconnect',
      !rePaired,
      rePaired ? `re-paired ${rePaired.peer.name}` : 'no re-pair event'
    )

    const hostDevicesAt = host.lines.length
    host.proc.stdin.write(JSON.stringify({ cmd: 'listdevices' }) + '\n')
    const hostDevicesMsg = await host.waitFor(
      (m) => m.type === 'devices',
      30 * 1000,
      'host device list',
      hostDevicesAt
    )
    const stillPresent = (hostDevicesMsg.devices || []).some((d) => d.publicKey === hostJoinerKey)
    ok('deleted device absent from host device list', !stillPresent, stillPresent ? 'record still present' : '')

    // ── 8b. Pairing with the CURRENT (rotated) code re-admits the device ──
    // The deletion must NOT blacklist the key: using the key again is the
    // legitimate way to re-add. The host rotated its code on delete, so the
    // joiner must pair with the new code.
    console.log('[test] re-pairing the joiner with the rotated code...')
    const hostCodeAt = host.lines.length
    host.proc.stdin.write(JSON.stringify({ cmd: 'getcode' }) + '\n')
    const hostCodeMsg = await host.waitFor((m) => m.type === 'code', 30 * 1000, 'host current code', hostCodeAt)
    const rotatedCode = hostCodeMsg.code
    ok('host rotated its pairing code on deletion', rotatedCode && rotatedCode !== hostIdentity.pairingCode, rotatedCode)

    const hostBeforeRepair = host.lines.length
    joiner.proc.stdin.write(JSON.stringify({ cmd: 'pair', code: rotatedCode }) + '\n')
    const rePairedMsg = await host.waitFor(
      (m) => m.type === 'paired' && m.peer && m.peer.publicKey === hostJoinerKey,
      PAIR_TIMEOUT_MS + 30 * 1000,
      'host re-paired with joiner',
      hostBeforeRepair
    )
    ok('deleted device re-added by pairing with the current code', !!rePairedMsg, rePairedMsg ? rePairedMsg.peer.name : '')

    // applyHandshake persists the device record asynchronously (the paired
    // event is emitted before the bee.put settles), so poll until it lands.
    let reAdded = false
    const repairDeadline = Date.now() + 15000
    while (Date.now() < repairDeadline && !reAdded) {
      const hostRepairedAt = host.lines.length
      host.proc.stdin.write(JSON.stringify({ cmd: 'listdevices' }) + '\n')
      const hostRepairedMsg = await host.waitFor(
        (m) => m.type === 'devices',
        30 * 1000,
        'host device list after re-pair',
        hostRepairedAt
      )
      reAdded = (hostRepairedMsg.devices || []).some((d) => d.publicKey === hostJoinerKey)
      if (!reAdded) await new Promise((r) => setTimeout(r, 1000))
    }
    ok('re-paired device present in host device list', !!reAdded)

    // ── 9. Clean shutdown ────────────────────────────────────────────────
    console.log('[test] stopping engines...')
    const joinerBye = joiner.waitFor((m) => m.type === 'bye', 15 * 1000, 'joiner bye')
    const hostBye = host.waitFor((m) => m.type === 'bye', 15 * 1000, 'host bye')
    await Promise.all([joiner.stop(), host.stop()])
    await Promise.all([joinerBye, hostBye])
    ok('joiner stopped cleanly', !!joiner.exited === false || joiner.exited === 0, `exit=${joiner.exited}`)
    ok('host stopped cleanly', !!host.exited === false || host.exited === 0, `exit=${host.exited}`)
  } catch (err) {
    ok('unexpected failure', false, err.message)
  } finally {
    clearTimeout(watchdog)
    const failed = results.filter((r) => !r.pass).length
    console.log(`\n${results.length - failed}/${results.length} checks passed in ${((Date.now() - started) / 1000).toFixed(1)}s`)
    // Make sure no child process is still holding the temp dir open.
    for (const child of [host, joiner]) {
      if (child && child.exited === undefined) {
        try {
          child.proc.kill()
        } catch {}
      }
    }
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
    process.exit(failed > 0 ? 1 : 0)
  }
}

// ─── Entry ──────────────────────────────────────────────────────────────────

if (process.argv[2] === '--child') {
  const role = process.argv[3]
  const config = {
    storageDir: process.argv[4],
    downloadsDir: process.argv[5],
    deviceName: process.argv[6]
  }
  if (role === 'joiner') {
    config.code = process.argv[7]
    config.sourcePath = process.argv[8]
  }
  runChild(role, config).catch((err) => {
    console.error(`[${role}] child failed:`, err && err.stack)
    process.exit(1)
  })
} else {
  main()
}
