'use strict'

// Acceptance tests — audit fix F1 (SYNC_* router gate) + F3 (SYNC_DENIED).
// 1. Lan-level peer SYNC_* → dropped + rate-limited SYNC_DENIED reply.
// 2. Trusted partner SYNC_* for its own library → dispatched.
// 3. Trusted peer using ANOTHER library's id → dropped, no reply, no oracle.
// 4. Untrusted non-lan (pairing-mode) peer → dropped in silence.
// 5. SYNC_INVITE (not library-bound) from a trusted peer → dispatched.

const { createSignaling } = require('./connections/signaling.js')
const { MESSAGES } = require('./protocol.js')

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

function makeBee() {
  const map = new Map()
  return {
    async put(key, value) { map.set(key, value) },
    async get(key) { return map.has(key) ? { key, value: map.get(key) } : null },
    async del(key) { map.delete(key) },
    async *createReadStream() {
      for (const key of Array.from(map.keys()).sort()) yield { key, value: map.get(key) }
    }
  }
}

const OWNER = 'o'.repeat(64)
const PARTNER = 'a'.repeat(64)
const OTHER_TRUSTED = 'b'.repeat(64)
const STRANGER = 'c'.repeat(64)
const LIB_OURS = 'sync-ours-1'
const LIB_FOREIGN = 'sync-foreign-9'

async function buildHarness() {
  const peers = new Map()
  const sentTo = new Map()
  const dispatched = [] // { handler, peerId, msg }

  const engine = {
    trustManager: {
      isRevoked: () => false,
      isTrustedPublicKey: () => false
    },
    // Fake sync engine: only PARTNER owns LIB_OURS; OTHER_TRUSTED owns LIB_FOREIGN.
    syncEngine: {
      isLibraryPeer: (libId, peerId) =>
        (libId === LIB_OURS && peerId === PARTNER) || (libId === LIB_FOREIGN && peerId === OTHER_TRUSTED)
    },
    autoAcceptOffers: true,
    getBee: async () => makeBee(),
    emit: () => {},
    getPeers: () => peers,
    notificationStore: null
  }

  const ctx = { engine, peers, activeClaims: new Set(), refs: {} }
  const signaling = createSignaling(ctx)

  ctx.refs = {
    ...signaling,
    handleSyncIndex: async (peerId, msg) => dispatched.push(['handleSyncIndex', peerId, msg]),
    handleSyncDelete: async (peerId, msg) => dispatched.push(['handleSyncDelete', peerId, msg]),
    handleSyncRemove: async (peerId, msg) => dispatched.push(['handleSyncRemove', peerId, msg]),
    handleSyncVerify: async (peerId, msg) => dispatched.push(['handleSyncVerify', peerId, msg]),
    handleSyncVerifyResult: async (peerId, msg) => dispatched.push(['handleSyncVerifyResult', peerId, msg]),
    handleSyncInviteAccept: async (peerId, msg) => dispatched.push(['handleSyncInviteAccept', peerId, msg]),
    handleSyncInviteDecline: async (peerId, msg) => dispatched.push(['handleSyncInviteDecline', peerId, msg]),
    handleSyncInvite: async (peerId, msg) => dispatched.push(['handleSyncInvite', peerId, msg]),
    handleSyncDenied: async (peerId, msg) => dispatched.push(['handleSyncDenied', peerId, msg])
  }

  function sent(peerId) {
    const arr = sentTo.get(peerId) || []
    sentTo.set(peerId, arr)
    return arr
  }

  function addPeer(peerId, { mode = 'pairing', trusted = false } = {}) {
    peers.set(peerId, {
      id: peerId,
      connection: { destroy() {} },
      device: { id: 'dev-' + peerId.slice(0, 6), publicKey: peerId, identityKey: '', name: 'P', os: 'win32', isTrusted: trusted, isOnline: true },
      signaling: { send: (obj) => sent(peerId).push(obj) },
      transferMethod: 'lan',
      pairing: { mode, trusted, complete: trusted, outstanding: [], pendingChallenges: [], pendingHandshake: null, timeout: null, code: null }
    })
  }

  return { ctx, signaling, dispatched, sent, addPeer }
}

async function runAllTests() {
  console.log('\n======================================================')
  console.log('      SYNC ROUTER GATE ACCEPTANCE TESTS (F1/F3)      ')
  console.log('======================================================\n')

  const h = await buildHarness()
  h.addPeer(PARTNER, { mode: 'pairing', trusted: true })
  h.addPeer(OTHER_TRUSTED, { mode: 'pairing', trusted: true })
  h.addPeer(STRANGER, { mode: 'pairing', trusted: false })
  h.addPeer('d'.repeat(64), { mode: 'lan', trusted: false })
  const LAN_PEER = 'd'.repeat(64)

  const peerObj = (id) => h.ctx.peers.get(id)

  // 1. Lan-level peer → dropped + SYNC_DENIED, then rate-limited.
  console.log('Test 1: lan-level peer SYNC_INDEX → SYNC_DENIED (rate-limited)')
  h.signaling.handlePeerMessage(LAN_PEER, { type: MESSAGES.SYNC_INDEX, libraryId: LIB_OURS, entries: [] })
  assert('SYNC_INDEX from lan peer dropped', h.dispatched.length === 0)
  const denials1 = h.sent(LAN_PEER).filter((m) => m.type === MESSAGES.SYNC_DENIED)
  assert('SYNC_DENIED sent with reason unpaired', denials1.length === 1 && denials1[0].reason === 'unpaired' && denials1[0].libraryId === LIB_OURS, JSON.stringify(denials1))
  h.signaling.handlePeerMessage(LAN_PEER, { type: MESSAGES.SYNC_INDEX, libraryId: LIB_OURS, entries: [] })
  assert('immediate repeat is rate-limited (no 2nd denial)', h.sent(LAN_PEER).filter((m) => m.type === MESSAGES.SYNC_DENIED).length === 1)
  h.signaling.handlePeerMessage(LAN_PEER, { type: MESSAGES.SYNC_DELETE, libraryId: LIB_OURS })
  assert('different message type gets its own denial slot', h.sent(LAN_PEER).filter((m) => m.type === MESSAGES.SYNC_DENIED).length === 2)
  assert('still no dispatches for lan peer', h.dispatched.length === 0)

  // 2. Trusted partner, own library → dispatched.
  console.log('Test 2: trusted partner SYNC_* for its own library → dispatched')
  h.signaling.handlePeerMessage(PARTNER, { type: MESSAGES.SYNC_INDEX, libraryId: LIB_OURS, entries: [] })
  assert('SYNC_INDEX dispatched for partner+own lib', h.dispatched.some((d) => d[0] === 'handleSyncIndex' && d[1] === PARTNER))
  h.signaling.handlePeerMessage(PARTNER, { type: MESSAGES.SYNC_DELETE, libraryId: LIB_OURS, rel: 'x' })
  assert('SYNC_DELETE dispatched for partner+own lib', h.dispatched.some((d) => d[0] === 'handleSyncDelete' && d[1] === PARTNER))
  h.signaling.handlePeerMessage(PARTNER, { type: MESSAGES.SYNC_REMOVE, libraryId: LIB_OURS })
  assert('SYNC_REMOVE dispatched for partner+own lib', h.dispatched.some((d) => d[0] === 'handleSyncRemove' && d[1] === PARTNER))
  h.signaling.handlePeerMessage(PARTNER, { type: MESSAGES.SYNC_VERIFY, libraryId: LIB_OURS })
  assert('SYNC_VERIFY dispatched for partner+own lib', h.dispatched.some((d) => d[0] === 'handleSyncVerify' && d[1] === PARTNER))
  h.signaling.handlePeerMessage(PARTNER, { type: MESSAGES.SYNC_INVITE_ACCEPT, libraryId: LIB_OURS })
  assert('SYNC_INVITE_ACCEPT dispatched for partner+own lib', h.dispatched.some((d) => d[0] === 'handleSyncInviteAccept' && d[1] === PARTNER))

  // 3. Trusted peer using ANOTHER library's id → dropped silently.
  console.log('Test 3: trusted peer with foreign libraryId → dropped, silence')
  const before = h.dispatched.length
  h.signaling.handlePeerMessage(OTHER_TRUSTED, { type: MESSAGES.SYNC_INDEX, libraryId: LIB_OURS, entries: [] })
  assert('foreign-lib SYNC_INDEX dropped', h.dispatched.length === before)
  assert('no SYNC_DENIED to trusted peer (no oracle)', h.sent(OTHER_TRUSTED).filter((m) => m.type === MESSAGES.SYNC_DENIED).length === 0)
  h.signaling.handlePeerMessage(OTHER_TRUSTED, { type: MESSAGES.SYNC_DELETE, libraryId: LIB_OURS, rel: 'victim.txt' })
  assert('foreign-lib SYNC_DELETE dropped (the audit repro kill)', h.dispatched.length === before)
  h.signaling.handlePeerMessage(OTHER_TRUSTED, { type: MESSAGES.SYNC_INDEX, libraryId: LIB_FOREIGN, entries: [] })
  assert('own-foreign-lib SYNC_INDEX still dispatched for its true owner', h.dispatched.length === before + 1)

  // 4. Untrusted non-lan (pairing-mode) peer → silence, no reply.
  console.log('Test 4: untrusted pairing-mode peer → silent drop')
  h.signaling.handlePeerMessage(STRANGER, { type: MESSAGES.SYNC_INDEX, libraryId: LIB_OURS, entries: [] })
  assert('stranger SYNC_INDEX dropped', h.dispatched.every((d) => d[1] !== STRANGER))
  assert('stranger gets NO SYNC_DENIED (silence)', h.sent(STRANGER).length === 0)

  // 5. SYNC_INVITE is trust-gated but not library-bound.
  console.log('Test 5: SYNC_INVITE from trusted peer → dispatched')
  h.signaling.handlePeerMessage(PARTNER, { type: MESSAGES.SYNC_INVITE, libraryId: 'sync-new-lib', name: 'New' })
  assert('SYNC_INVITE dispatched (invite carries sender-owned id)', h.dispatched.some((d) => d[0] === 'handleSyncInvite' && d[1] === PARTNER))

  // 6. SYNC_DENIED routing reaches the handler.
  console.log('Test 6: inbound SYNC_DENIED reaches handleSyncDenied')
  h.signaling.handlePeerMessage(PARTNER, { type: MESSAGES.SYNC_DENIED, libraryId: LIB_OURS, reason: 'unpaired' })
  assert('SYNC_DENIED dispatched to sync engine', h.dispatched.some((d) => d[0] === 'handleSyncDenied' && d[1] === PARTNER))

  console.log(`\n${passed} passed, ${failed} failed`)
}

runAllTests().catch((e) => { console.error('FATAL', e); process.exit(1) })
