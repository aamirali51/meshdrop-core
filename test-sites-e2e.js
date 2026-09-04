'use strict'

// End-to-end DHT test: MeshDrop Sites allowlist + live read, real engines.
//
//   node test-sites-e2e.js
//
// Spawns TWO real MeshEngine child processes over the public DHT (separate
// storage dirs), then drives:
//   1. host boots, publishes a folder site
//   2. visitor boots; host allowlists the visitor by its MD- code (MAC proof)
//   3. visitor enters the SITE- code -> discovers siteId, SITE_HELLO, allowed
//   4. visitor lists the drive root
//   5. visitor reads a file (full + byte Range) and SHA-256-verifies
//   6. a third party (stranger) that is NOT allowlisted is refused
//
// Exit code 0 on success, 1 on any failure. Child stdout = JSON only (engine
// logs redirected to stderr).

const { spawn } = require('child_process')
const path = require('path')
const os = require('os')
const fs = require('fs')
const crypto = require('crypto')

const SCRIPT = __filename
const GLOBAL_TIMEOUT_MS = 180 * 1000
const STEP_TIMEOUT_MS = 60 * 1000
const PAIR_TIMEOUT_MS = 90 * 1000

// ─── Child (one per engine) ──────────────────────────────────────────────────

async function runChild(role, config) {
  const origError = console.error.bind(console)
  console.log = (...a) => origError(`[${role}]`, ...a)
  console.warn = (...a) => origError(`[${role}]`, ...a)
  console.error = (...a) => origError(`[${role}]`, ...a)

  const { MeshEngine } = require('./index.js')
  const send = (m) => process.stdout.write(JSON.stringify(m) + '\n')

  const engine = new MeshEngine({
    storageDir: config.storageDir,
    downloadsDir: config.downloadsDir,
    deviceName: config.deviceName,
    autoAcceptOffers: false,
    autoTrustLAN: false,
    lanDiscovery: false
  })
  engine.on('error', (err) => send({ type: 'error', message: String((err && err.message) || err) }))
  engine.on('trust:paired', ({ peer }) => send({ type: 'paired', peer }))
  engine.on('site:visitor:added', (d) => send({ type: 'siteVisitorAdded', ...d }))
  engine.on('site:visit:started', (d) => send({ type: 'siteVisitStarted', ...d }))
  engine.on('site:visit:stopped', (d) => send({ type: 'siteVisitStopped', ...d }))

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
        if (cmd.cmd === 'getcode') {
          send({ type: 'code', code: engine.trustManager.getActiveHostCode() || '' })
        } else if (cmd.cmd === 'publish') {
          const site = await engine.publishSite({ folderPath: cmd.folderPath, name: cmd.name || 'Test Drive' })
          send({ type: 'published', site })
        } else if (cmd.cmd === 'addvisitor') {
          const res = await engine.addSiteVisitor(cmd.siteId, cmd.code, { timeoutMs: PAIR_TIMEOUT_MS })
          send({ type: 'visitorAdded', ...res })
        } else if (cmd.cmd === 'visit') {
          const res = await engine.visitSite(cmd.code)
          send({ type: 'visited', ...res })
        } else if (cmd.cmd === 'list') {
          const entries = await engine.listSitePath(cmd.path || '/')
          send({ type: 'listed', entries })
        } else if (cmd.cmd === 'read') {
          const res = await engine.readSiteFile(cmd.path, cmd.range ? { range: cmd.range } : undefined)
          // Don't ship the whole body over stdout for a big file — ship digest.
          send({ type: 'readResult', ...res, bodyDigest: crypto.createHash('sha256').update(res.body || Buffer.alloc(0)).digest('hex'), bodyLen: res.body ? res.body.length : 0, body: undefined })
        } else if (cmd.cmd === 'getidentity') {
          send({ type: 'identity', identity: engine.getIdentity() })
        } else {
          send({ type: 'error', message: 'unknown cmd ' + cmd.cmd })
        }
      } catch (err) {
        send({ type: 'error', message: String((err && err.message) || err), cmd: cmd.cmd })
      }
    }
  })

  await engine.start()
  engine.setPairingIntent(true)
  send({ type: 'ready', identity: engine.getIdentity() })
  await new Promise(() => {})
}

// ─── Orchestrator ────────────────────────────────────────────────────────────

class Child {
  constructor(name, args) {
    this.name = name
    this.proc = spawn(process.execPath, [SCRIPT, '--child', ...args], { stdio: ['pipe', 'pipe', 'pipe'] })
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
    for (const line of d.toString().split('\n')) {
      if (!line.trim()) continue
      let msg
      try {
        msg = JSON.parse(line)
      } catch {
        continue
      }
      this.lines.push(msg)
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
    const existing = this.lines.slice(fromIndex).find(test)
    if (existing) return Promise.resolve(existing)
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`${this.name} timed out waiting for ${label}`)), timeoutMs)
      this.waiters.push({
        test: (m) => this.lines.indexOf(m) >= fromIndex && test(m),
        resolve: (msg) => { clearTimeout(timer); resolve(msg) },
        reject: () => clearTimeout(timer)
      })
    })
  }
  stop() {
    return new Promise((resolve) => {
      if (this.exited !== undefined) return resolve()
      this.proc.stdin.write('stop\n')
      const timer = setTimeout(() => { try { this.proc.kill() } catch {}; resolve() }, 8000)
      this.proc.on('exit', () => { clearTimeout(timer); resolve() })
    })
  }
  send(cmd) { this.proc.stdin.write(JSON.stringify(cmd) + '\n') }
}

function sha256(buf) { return crypto.createHash('sha256').update(buf).digest('hex') }

async function main() {
  const started = Date.now()
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-sites-e2e-'))
  const results = []
  const ok = (name, cond, detail = '') => {
    results.push({ name, pass: !!cond })
    console.log(`${cond ? 'PASS' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}`)
    return !!cond
  }
  let host = null
  let visitor = null
  let stranger = null
  const watchdog = setTimeout(() => {
    console.error('GLOBAL TIMEOUT — aborting')
    for (const c of [host, visitor, stranger]) if (c) try { c.proc.kill() } catch {}
    process.exit(1)
  }, GLOBAL_TIMEOUT_MS)

  try {
    // Host site folder with a few files.
    const siteDir = path.join(tmpRoot, 'drive')
    fs.mkdirSync(path.join(siteDir, 'nested'), { recursive: true })
    const fileA = path.join(siteDir, 'hello.txt')
    const fileB = path.join(siteDir, 'nested', 'blob.bin')
    fs.writeFileSync(fileA, 'hello meshdrive')
    const BLOB = crypto.randomBytes(300 * 1024)
    fs.writeFileSync(fileB, BLOB)
    const secret = path.join(siteDir, '.secret')
    fs.writeFileSync(secret, 'hidden') // dotfile must be hidden from listing

    console.log('[test] spawning host...')
    host = new Child('host', [path.join(tmpRoot, 'host', 'storage'), path.join(tmpRoot, 'host', 'dl'), 'Host'])
    const hostReady = await host.waitFor((m) => m.type === 'ready', STEP_TIMEOUT_MS, 'host ready')
    const hostIdentity = hostReady.identity
    ok('host ready', !!hostIdentity && /^MD-/.test(hostIdentity.pairingCode))

    console.log('[test] spawning visitor...')
    visitor = new Child('visitor', [path.join(tmpRoot, 'visitor', 'storage'), path.join(tmpRoot, 'visitor', 'dl'), 'Visitor'])
    const visReady = await visitor.waitFor((m) => m.type === 'ready', STEP_TIMEOUT_MS, 'visitor ready')
    ok('visitor ready', !!visReady)
    const visitorIdentity = visReady.identity

    console.log('[test] spawning stranger (3rd device, NOT allowlisted)...')
    stranger = new Child('stranger', [path.join(tmpRoot, 'stranger', 'storage'), path.join(tmpRoot, 'stranger', 'dl'), 'Stranger'])
    const strangerReady = await stranger.waitFor((m) => m.type === 'ready', STEP_TIMEOUT_MS, 'stranger ready')
    ok('stranger ready', !!strangerReady)
    const strangerIdentity = strangerReady.identity

    // 1. Host publishes a site over the drive folder.
    console.log('[test] host publishes site...')
    host.send({ cmd: 'publish', folderPath: siteDir, name: 'My Mesh Drive' })
    const pub = await host.waitFor((m) => m.type === 'published' && m.site, STEP_TIMEOUT_MS, 'site published')
    const site = pub.site
    ok('site published with SITE- code', /^SITE-/.test(site.code), site.code)

    // 2. Host allowlists the visitor by its MD- code (MAC proof over the DHT).
    console.log('[test] host allowlists visitor by code...')
    host.send({ cmd: 'addvisitor', siteId: site.siteId, code: visitorIdentity.pairingCode })
    const added = await host.waitFor((m) => m.type === 'visitorAdded', PAIR_TIMEOUT_MS + 30 * 1000, 'visitor allowlisted')
    ok('visitor allowlisted by MAC proof', !!added.publicKey, added.publicKey && added.publicKey.slice(0, 12))
    ok('allowlisted key == visitor noise key', added.publicKey === visitorIdentity.publicKey)

    // 3. Visitor enters the SITE- code -> discover + hello.
    console.log('[test] visitor visits site...')
    visitor.send({ cmd: 'visit', code: site.code })
    const visited = await visitor.waitFor((m) => m.type === 'visited' || m.type === 'error', STEP_TIMEOUT_MS, 'site visit')
    ok('visitor visited site', visited.type === 'visited', visited.siteId || visited.message)

    // 4. Visitor lists the drive root.
    console.log('[test] visitor lists drive root...')
    visitor.send({ cmd: 'list', path: '/' })
    const listed = await visitor.waitFor((m) => m.type === 'listed', STEP_TIMEOUT_MS, 'list root')
    const names = (listed.entries || []).map((e) => e.name)
    ok('list sees hello.txt', names.includes('hello.txt'))
    ok('list sees nested dir', names.includes('nested'))
    ok('list hides dotfile', !names.includes('.secret'), names.join(','))

    // 5. Visitor reads a file (full) + verifies hash.
    console.log('[test] visitor reads nested file (full)...')
    const readFrom = visitor.lines.length
    visitor.send({ cmd: 'read', path: '/nested/blob.bin' })
    const readFull = await visitor.waitFor((m) => m.type === 'readResult', STEP_TIMEOUT_MS, 'read full', readFrom)
    ok('read full ok', readFull.ok !== false && readFull.status === 'ok', readFull.error || '')
    ok('read full size matches', readFull.size === BLOB.length, `${readFull.size}/${BLOB.length}`)
    ok('read full content verified (sha256)', readFull.bodyDigest === sha256(BLOB))

    // 6. Visitor reads a byte Range of a file + verifies.
    console.log('[test] visitor reads byte range...')
    const readRangeFrom = visitor.lines.length
    visitor.send({ cmd: 'read', path: '/nested/blob.bin', range: 'bytes=100-199' })
    const readRange = await visitor.waitFor((m) => m.type === 'readResult', STEP_TIMEOUT_MS, 'read range', readRangeFrom)
    ok('read range ok', readRange.status === 'ok', readRange.error || '')
    const wantSlice = BLOB.subarray(100, 200)
    ok('read range start/end', readRange.start === 100 && readRange.end === 199, `${readRange.start}-${readRange.end}`)
    ok('read range content verified', readRange.bodyDigest === sha256(wantSlice))

    // 7. Stranger (NOT allowlisted) must be refused.
    console.log('[test] stranger tries to visit (should fail)...')
    const strangerFrom = stranger.lines.length
    stranger.send({ cmd: 'visit', code: site.code })
    const strangerVisit = await stranger.waitFor(
      (m) => m.type === 'visited' || (m.type === 'error' && m.cmd === 'visit') || m.type === 'siteVisitStopped',
      STEP_TIMEOUT_MS,
      'stranger visit refused',
      strangerFrom
    )
    const refused = strangerVisit.type === 'error' || (strangerVisit.type === 'siteVisitStopped' && /not.?allowed|not allowed/i.test(strangerVisit.reason || ''))
    ok('stranger refused (not allowlisted)', refused, `${strangerVisit.type}: ${strangerVisit.message || strangerVisit.reason || ''}`)

    // Clean shutdown.
    const hb = host.waitFor((m) => m.type === 'bye', 15 * 1000, 'host bye')
    const vb = visitor.waitFor((m) => m.type === 'bye', 15 * 1000, 'visitor bye')
    const sb = stranger.waitFor((m) => m.type === 'bye', 15 * 1000, 'stranger bye')
    await Promise.all([host.stop(), visitor.stop(), stranger.stop()])
    await Promise.all([hb, vb, sb])
  } catch (err) {
    ok('unexpected failure', false, err.message)
    console.error(err)
  } finally {
    clearTimeout(watchdog)
    const failed = results.filter((r) => !r.pass).length
    console.log(`\n${results.length - failed}/${results.length} checks passed in ${((Date.now() - started) / 1000).toFixed(1)}s`)
    for (const c of [host, visitor, stranger]) {
      if (c && c.exited === undefined) { try { c.proc.kill() } catch {} }
    }
    await new Promise((r) => setTimeout(r, 500))
    for (let attempt = 0; attempt < 3; attempt++) {
      try { fs.rmSync(tmpRoot, { recursive: true, force: true }); break } catch (err) {
        if (attempt === 2) console.warn(`could not remove ${tmpRoot}: ${err.message}`)
        else await new Promise((r) => setTimeout(r, 1000))
      }
    }
    process.exit(failed > 0 ? 1 : 0)
  }
}

if (process.argv[2] === '--child') {
  const role = process.argv[3]
  runChild(role, {
    storageDir: process.argv[4],
    downloadsDir: process.argv[5],
    deviceName: process.argv[6]
  }).catch((err) => {
    console.error(`[${role}] child failed:`, err && err.stack)
    process.exit(1)
  })
} else {
  main()
}
