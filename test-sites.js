'use strict'

// Spike test: MeshDrop Sites slices 1-2 (registry + allowlist MAC gate).
//
// Exercises the host flow end to end against a REAL Hyperbee (no mocks):
//   1. Host creates a site (SITE- code, folder, empty allowlist).
//   2. Host pastes a visitor's MD- code and challenges it.
//   3. Visitor answers with the code it holds -> MAC verifies.
//   4. Host adds the visitor's Noise public key to the allowlist.
//   5. isAllowed() admits the verified key and refuses a stranger.
//   6. Removing the key revokes access immediately.
//   7. Persistence: a second store over the same directory still sees the site.

const assert = require('assert')
const os = require('os')
const path = require('path')
const fsp = require('fs/promises')

const Corestore = require('corestore')
const Hyperbee = require('hyperbee')

const { createSitesStore } = require('./sites.js')
const { generatePairingCode } = require('./crypto.js')
const siteAccess = require('./siteAccess.js')

async function makeBee(dir) {
  const store = new Corestore(dir)
  await store.ready()
  const core = store.get({ name: 'sites' })
  await core.ready()
  const bee = new Hyperbee(core, { keyEncoding: 'utf-8', valueEncoding: 'json' })
  return { bee, store }
}

function fakeVisitorKey(seed) {
  // Deterministic 32-byte stand-in for a visitor's verified Noise public key
  // (the real flow gets this from the transport after MAC verification).
  const h = require('crypto').createHash('sha256').update(seed).digest('hex')
  return h
}

async function run() {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'mesh-sites-spike-'))
  let { bee, store } = await makeBee(dir)

  const hostStore = createSitesStore({ getBee: async () => bee })
  await hostStore.hydrate()

  // 1. Host creates a site.
  const site = await hostStore.createSite({ name: 'My Drive', folderPath: 'E:\\Drive' })
  assert.ok(/^SITE-[A-Z2-9]{4}-[A-Z2-9]{4}$/.test(site.code), 'site code format')
  assert.strictEqual(site.allowlist.length, 0, 'new site allowlist is empty')

  // 2. Visitor shares their MD- pairing code; host starts the challenge.
  const visitorCode = generatePairingCode()
  const visitorKey = fakeVisitorKey('visitor-1')
  const nonce = require('crypto').randomBytes(16)
  const challenge = siteAccess.createChallenge(visitorCode, nonce)

  // 3. Visitor answers with the code it holds; host verifies the MAC.
  const answer = siteAccess.answerChallenge(visitorCode, challenge)
  assert.ok(answer, 'visitor can answer a challenge for its own code')
  assert.ok(siteAccess.verifyAnswer(visitorCode, challenge, answer), 'MAC verifies')

  // A wrong code (a different visitor guessing) must NOT verify.
  const wrongAnswer = siteAccess.answerChallenge(generatePairingCode(), challenge)
  assert.ok(!siteAccess.verifyAnswer(visitorCode, challenge, wrongAnswer), 'wrong code rejected')
  assert.ok(!siteAccess.verifyAnswer(visitorCode, challenge, 'deadbeef'.repeat(8)), 'garbage MAC rejected')

  // 4. Host adds the visitor's VERIFIED Noise public key to the allowlist.
  await hostStore.addToAllowlist(site.siteId, visitorKey)

  // 5. The verified key is allowed; a stranger is not.
  assert.strictEqual(await hostStore.isAllowed(site.siteId, visitorKey), true, 'verified visitor allowed')
  const strangerKey = fakeVisitorKey('stranger')
  assert.strictEqual(await hostStore.isAllowed(site.siteId, strangerKey), false, 'stranger refused')

  // 6. Revocation is immediate.
  await hostStore.removeFromAllowlist(site.siteId, visitorKey)
  assert.strictEqual(await hostStore.isAllowed(site.siteId, visitorKey), false, 'revoked visitor refused')

  // Re-add, then prove persistence: close the first store, then a second store
  // over the SAME directory still sees the site + allowlist (the engine
  // rehydrates sites at boot).
  await hostStore.addToAllowlist(site.siteId, visitorKey)
  await store.close()
  const { bee: bee2, store: store2 } = await makeBee(dir)
  const reloadedStore = createSitesStore({ getBee: async () => bee2 })
  await reloadedStore.hydrate()
  const reloadedSite = await reloadedStore.getSite(site.siteId)
  assert.ok(reloadedSite, 'site survives reload')
  assert.strictEqual(reloadedSite.name, 'My Drive', 'site metadata survives reload')
  assert.strictEqual(reloadedSite.folderPath, 'E:\\Drive', 'folder survives reload')
  assert.strictEqual(reloadedSite.code, site.code, 'site code survives reload')
  assert.ok(reloadedSite.allowlist.includes(visitorKey), 'allowlist survives reload')

  // Lookup by human code also survives.
  const byCode = await reloadedStore.getSiteByCode(site.code)
  assert.strictEqual(byCode.siteId, site.siteId, 'site findable by SITE- code')

  await store2.close()
  await fsp.rm(dir, { recursive: true, force: true }).catch(() => {})
  console.log('PASS: sites slice 1-2 spike (registry + allowlist MAC gate)')
}

run().catch((err) => {
  console.error('FAIL:', err)
  process.exit(1)
})
