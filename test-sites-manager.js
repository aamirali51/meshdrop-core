'use strict'

// Test: SiteManager + TrustManager scoped allowlist verification.
//
// Simulates the REAL wire flow on the HOST side only (no DHT, no real second
// engine): the host registers a visitor's code, the "visitor" answers the
// challenge with mac(code, nonce) — exactly as the visitor's engine would —
// and we assert the host's MAC check admits the right key and rejects a
// stranger, WITHOUT ever granting device trust.
//
// This keeps the trust-scoping invariant under test: a verified site visitor
// must never appear in the host's trustedPeerKeys.

const assert = require('assert')
const crypto = require('crypto')
const os = require('os')
const path = require('path')
const fsp = require('fs/promises')

const Corestore = require('corestore')
const Hyperbee = require('hyperbee')

const { SiteManager } = require('./engine/SiteManager.js')
const { generatePairingCode, mac } = require('./crypto.js')
const b4a = require('b4a')

async function makeStorage(dir) {
  const store = new Corestore(dir)
  await store.ready()
  const bees = new Map()
  const getBee = async (name) => {
    if (bees.has(name)) return bees.get(name)
    const core = store.get({ name })
    await core.ready()
    const bee = new Hyperbee(core, { keyEncoding: 'utf-8', valueEncoding: 'json' })
    bees.set(name, bee)
    return bee
  }
  return { store, getBee }
}

// A minimal engine double with just enough surface for SiteManager + a
// TrustManager that has registerHostVerificationCode/leaveHostVerificationCode.
function makeEngine({ getBee }) {
  const pairingSecrets = new Map()
  const trustedPeerKeys = new Set()
  const listeners = new Map()
  const emit = (evt, data) => {
    const arr = listeners.get(evt) || []
    for (const fn of arr) fn(data)
  }
  const on = (evt, fn) => {
    if (!listeners.has(evt)) listeners.set(evt, [])
    listeners.get(evt).push(fn)
  }

  const fakeTrustManager = {
    pairingSecrets,
    trustedPeerKeys,
    getActiveHostCode() {
      for (const [, s] of pairingSecrets) if (s.role === 'host') return s.code
      return null
    },
    registerHostVerificationCode(rawCode) {
      // Mirrors the real TrustManager: join topic + return a challenge. We
      // don't need real topics here, just the code/challenge mechanics.
      const clean = require('./crypto.js').normalizePairingCode(rawCode)
      if (!clean) return null
      const cid = require('./crypto.js').codeId(clean)
      pairingSecrets.set(cid, { code: clean, role: 'verification', codeId: cid })
      const nonce = crypto.randomBytes(16)
      return { code: clean, codeId: cid, challenge: { codeId: cid, nonce: nonce.toString('hex') }, nonce: nonce.toString('hex') }
    },
    leaveHostVerificationCode(code) {
      if (!code) return
      const cid = require('./crypto.js').codeId(code)
      pairingSecrets.delete(cid)
    }
  }

  const engine = {
    getBee,
    trustManager: fakeTrustManager,
    peers: new Map(),
    relayClient: null,
    emit,
    on,
    connections: {
      setBeforePeerMessage() {}
    }
  }
  return engine
}

async function run() {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'mesh-sites-mgr-'))
  const { store, getBee } = await makeStorage(dir)
  const engine = makeEngine({ getBee })

  const sm = new SiteManager({ engine, getBee })
  await sm.init()
  sm.installSignalHook()

  // Host creates a site.
  const site = await sm.createSite({ name: 'My Drive', folderPath: 'E:\\Drive' })

  // Visitor shares its MD- code with the host.
  const visitorCode = generatePairingCode()
  const visitorKey = crypto.randomBytes(32).toString('hex') // noise pubkey hex
  const strangerKey = crypto.randomBytes(32).toString('hex')

  // Host pastes the code -> challenge created + registered (host joined the
  // visitor's pairing topic in the real TrustManager).
  let challenge = null
  const addPromise = sm.addSiteVisitor(site.siteId, visitorCode, { timeoutMs: 5000 }).then((res) => {
    return res
  })

  // Give addSiteVisitor a beat to register + create the challenge. In the real
  // flow the challenge is broadcast; here we drive the visitor's answer by
  // grabbing the pending entry.
  await new Promise((r) => setTimeout(r, 20))
  const pending = Array.from(sm.pendingVisitors.values())[0]
  assert.ok(pending, 'a pending visitor verification exists')
  challenge = pending.challenge

  // The visitor answers with mac(code, nonce) — exactly what the visitor's
  // engine does when it holds the code and receives a PAIRING_CHALLENGE.
  const visitorAnswer = b4a.toString(mac(visitorCode, b4a.from(challenge.nonce, 'hex')), 'hex')

  // Deliver the visitor's PAIRING_RESP through the hook (the visitor's noise
  // key = the transport-authenticated peerId on the host).
  const addedEvent = new Promise((resolve) => engine.on('site:visitor:added', resolve))
  sm._handleVisitorResponse(visitorKey, {
    type: 'PAIRING_RESP',
    codeId: challenge.codeId,
    nonce: challenge.nonce,
    mac: visitorAnswer
  })
  const added = await addedEvent
  assert.strictEqual(added.peerId, visitorKey, 'event carries the verified visitor key')

  const result = await addPromise
  assert.strictEqual(result.publicKey, visitorKey, 'addSiteVisitor resolves with the verified key')

  // The visitor is on the allowlist.
  assert.strictEqual(await sm.store.isAllowed(site.siteId, visitorKey), true, 'visitor allowlisted')
  assert.strictEqual(await sm.store.isAllowed(site.siteId, strangerKey), false, 'stranger NOT allowlisted')

  // CRITICAL INVARIANT: no device trust was granted. The visitor's key must
  // NOT be in the host's trustedPeerKeys, and no devices record was written.
  assert.strictEqual(engine.trustManager.trustedPeerKeys.has(visitorKey), false, 'no device trust granted')

  // A wrong answer (stranger guessing) must be rejected and NOT allowlisted.
  const strangerCode = generatePairingCode()
  const sm2 = new SiteManager({ engine, getBee })
  await sm2.init()
  const site2 = await sm2.createSite({ name: 'S2' })
  const addP2 = sm2.addSiteVisitor(site2.siteId, strangerCode, { timeoutMs: 5000 })
  await new Promise((r) => setTimeout(r, 20))
  const pending2 = Array.from(sm2.pendingVisitors.values())[0]
  const badAnswer = b4a.toString(mac('MD-WRONG-WRONG-WRONG-WRNG', b4a.from(pending2.challenge.nonce, 'hex')), 'hex')
  let rejected = false
  sm2._handleVisitorResponse(strangerKey, {
    type: 'PAIRING_RESP',
    codeId: pending2.challenge.codeId,
    nonce: pending2.challenge.nonce,
    mac: badAnswer
  })
  try {
    await addP2
  } catch (err) {
    rejected = true
  }
  assert.strictEqual(rejected, true, 'bad MAC rejects the add')
  assert.strictEqual(await sm2.store.isAllowed(site2.siteId, strangerKey), false, 'stranger not allowlisted after bad MAC')

  // Removal revokes immediately.
  await sm.removeSiteVisitor(site.siteId, visitorKey)
  assert.strictEqual(await sm.store.isAllowed(site.siteId, visitorKey), false, 'removed visitor refused')

  await store.close()
  await fsp.rm(dir, { recursive: true, force: true }).catch(() => {})
  console.log('PASS: SiteManager scoped allowlist verification (no device trust granted)')
}

run().catch((err) => {
  console.error('FAIL:', err)
  process.exit(1)
})
