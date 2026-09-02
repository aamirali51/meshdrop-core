'use strict'

// Unit tests for the unanswered-pairing backoff (issue #2):
// 1. A fresh peer gets an automatic challenge and the watchdog arms.
// 2. A recorded 'challenge never verified' cycle suppresses AUTOMATIC
//    challenges (no challenge sent, watchdog not armed) — the silent
//    connect/challenge/destroy loop is broken.
// 3. Forced challenges (user intent — a code was entered) bypass suppression.
// 4. An incoming challenge we CAN answer resets suppression, is answered, and
//    is reciprocated so trust completes on BOTH sides (ping-pong guarded).
// 5. registerJoinerCode (user entered a code) clears suppression for everyone.
// 6. A verified response clears the backoff and grants trust.

const { TrustManager } = require('./engine/TrustManager.js')
const { mac, randomBytes, codeId } = require('./crypto.js')
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

function makeTrustManager() {
  const peers = new Map()
  const sent = [] // { peerId, msg }
  const tm = new TrustManager({
    getBee: async () => makeBee(),
    computeTopicHash: (label) => Buffer.from(label),
    swarm: { join() {}, flush: async () => {} },
    topicRegistry: null,
    relayClient: null,
    getPeers: () => peers,
    sendHandshake: () => {},
    emit: () => {},
    isRefreshing: () => false,
    onTrustGranted: () => {},
    getDeviceIdentity: () => ({ id: 'self-dev', name: 'Self' }),
    getPeerId: () => 'self-key'
  })
  return { tm, peers, sent }
}

function addPeer(peers, peerId) {
  peers.set(peerId, {
    connection: {},
    signaling: {
      send: (msg) => {
        if (globalThis.__capture) globalThis.__capture.push({ peerId, msg })
      }
    },
    device: { id: 'dev-' + peerId, publicKey: peerId, name: peerId },
    pairing: { mode: 'pairing', trusted: false, complete: false, outstanding: [], pendingChallenges: [] }
  })
}

const challengesTo = (peerId) =>
  globalThis.__capture.filter((s) => s.peerId === peerId && s.msg.type === MESSAGES.PAIRING_CHALLENGE).length
const responsesTo = (peerId) =>
  globalThis.__capture.filter((s) => s.peerId === peerId && s.msg.type === MESSAGES.PAIRING_RESP).length

async function runAllTests() {
  console.log('\n======================================================')
  console.log('      PAIRING BACKOFF UNIT TESTS (issue #2)          ')
  console.log('======================================================\n')

  globalThis.__capture = []
  const { tm, peers } = makeTrustManager()

  const code = await tm.getOrCreatePairingCode()
  assert('Host pairing code generated', typeof code === 'string' && code.startsWith('MD-'), code)

  // ── 1. Fresh peer: auto challenge + watchdog ─────────────────────────────
  addPeer(peers, 'peer-a')
  tm.sendChallenges('peer-a')
  const peerA = peers.get('peer-a')
  assert('Auto challenge sent for fresh peer', challengesTo('peer-a') === 1)
  assert('Watchdog armed after challenge', !!peerA.pairing.timeout)
  assert('Challenge recorded as outstanding', peerA.pairing.outstanding.length === 1)

  // ── 2. Unanswered cycle suppresses automatic challenges ──────────────────
  tm._recordUnansweredPairing('peer-a')
  assert('Suppression active after unanswered cycle', tm._isPairingSuppressed('peer-a'))
  peerA.pairing.timeout = null
  tm.sendChallenges('peer-a')
  assert('Suppressed peer gets NO automatic challenge', challengesTo('peer-a') === 1)
  assert('Suppressed peer watchdog not armed', peerA.pairing.timeout === null)

  // ── 3. User intent (force) bypasses suppression ──────────────────────────
  // While a challenge is already outstanding, force does NOT duplicate it
  // (one outstanding per (peer, secret) — the first still awaits an answer).
  tm.sendChallenges('peer-a', { force: true })
  assert('Force does not duplicate an outstanding challenge', challengesTo('peer-a') === 1)
  assert('Force clears suppression', !tm._isPairingSuppressed('peer-a'))
  addPeer(peers, 'peer-f')
  tm._recordUnansweredPairing('peer-f')
  tm.sendChallenges('peer-f', { force: true })
  assert('Forced challenge bypasses suppression', challengesTo('peer-f') === 1)

  // ── 4. Answerable incoming challenge: answer + reciprocate ───────────────
  addPeer(peers, 'peer-b')
  const cid = codeId(code)
  tm.handleChallenge('peer-b', { codeId: cid, nonce: randomBytes(16).toString('hex') })
  assert('Challenge we can answer is answered', responsesTo('peer-b') === 1)
  assert('Answerable challenge resets suppression', !tm._isPairingSuppressed('peer-b'))
  assert('Answerable challenge is reciprocated (mutual trust)', challengesTo('peer-b') === 1)

  // Reciprocity must not ping-pong: answering the reciprocated challenge
  // must NOT trigger another challenge back (we already have outstanding).
  const reciprocated = peers.get('peer-b').pairing.outstanding[0]
  tm.handleChallenge('peer-b', { codeId: cid, nonce: randomBytes(16).toString('hex') })
  assert('No challenge ping-pong', challengesTo('peer-b') === 1)
  assert('Still answers every challenge', responsesTo('peer-b') === 2)
  void reciprocated

  // Unanswerable challenge (unknown code): pendingChallenge stored, no answer.
  tm.handleChallenge('peer-b', { codeId: codeId('MD-0000-0000-0000-0000'), nonce: randomBytes(16).toString('hex') })
  assert('Unanswerable challenge is not answered', responsesTo('peer-b') === 2)
  assert('Unanswerable challenge stored as pending', peers.get('peer-b').pairing.pendingChallenges.length === 1)

  // ── 5. registerJoinerCode (user entered a code) clears suppression ───────
  addPeer(peers, 'peer-c')
  tm._recordUnansweredPairing('peer-c')
  assert('peer-c suppressed', tm._isPairingSuppressed('peer-c'))
  const joined = await tm.registerJoinerCode('MD-AAAA-BBBB-CCCC-DDDD')
  assert('Joiner code accepted', typeof joined === 'string' && joined.startsWith('MD-'), String(joined))
  assert('registerJoinerCode clears suppression for all peers', !tm._isPairingSuppressed('peer-c'))

  // ── 6. Verified response clears backoff and grants trust ─────────────────
  const outstanding = peers.get('peer-a').pairing.outstanding[0]
  tm._recordUnansweredPairing('peer-a')
  assert('peer-a re-suppressed for the test', tm._isPairingSuppressed('peer-a'))
  tm.handleResponse('peer-a', {
    nonce: outstanding.nonce.toString('hex'),
    mac: mac(outstanding.code, outstanding.nonce).toString('hex')
  })
  assert('Verified response grants trust', peers.get('peer-a').pairing.trusted === true)
  assert('Verified response clears the backoff', !tm._isPairingSuppressed('peer-a'))

  console.log('\n======================================================')
  console.log(`  PAIRING BACKOFF RESULTS: ${passed} PASSED, ${failed} FAILED`)
  console.log('======================================================\n')

  if (failed > 0) process.exit(1)
  runRelayHttpTests()
}

// ── Relay HTTP transport (Android Bare worklet path) ───────────────────────
// The worklet has no global fetch/WebSocket; RelayClient must work through an
// injected { post, get } transport. This is what makes cellular pairing work.
function runRelayHttpTests() {
  console.log('\n======================================================')
  console.log('         RELAY HTTP TRANSPORT TESTS                  ')
  console.log('======================================================\n')

  const { RelayClient } = require('./connections/relayClient.js')

  const posted = []
  let getResponse = { messages: [] }
  let getPicked = null
  const delivered = []
  const client = new RelayClient({
    mode: 'auto',
    localPeerId: 'self-key',
    http: {
      post: async (url, body) => {
        posted.push({ url, body })
        return { ok: true }
      },
      get: async (url) => {
        getPicked = url
        return getResponse
      }
    }
  })
  client.onMessage = (topic, data, fromPeerId) => delivered.push({ topic, data, fromPeerId })
  client.setPeerId('self-key')

  // Seed an inbound message BEFORE joining: the immediate first poll must
  // deliver it through the injected transport.
  getResponse = {
    messages: [
      {
        id: 'in1',
        topic: 'p2p-pair-MD-TEST-TEST-TEST-TEST',
        fromPeerId: 'other-key',
        data: { type: 'PAIRING_RESP', nonce: 'n', mac: 'm' },
        ts: 1
      }
    ]
  }

  client.start()
  client.join('p2p-pair-MD-TEST-TEST-TEST-TEST')
  client.send('p2p-pair-MD-TEST-TEST-TEST-TEST', {
    type: 'PAIRING_CHALLENGE',
    codeId: 'cid',
    nonce: randomBytes(16).toString('hex')
  })

  setTimeout(() => {
    assert(
      'send() goes through the injected http.post',
      posted.length === 1 && /p2p-pair-MD-TEST/.test(posted[0].url) && posted[0].body.data.type === 'PAIRING_CHALLENGE'
    )
    assert('poll() goes through the injected http.get', !!getPicked && /p2p-pair-MD-TEST/.test(getPicked))
    assert(
      'Inbound message delivered via injected transport',
      delivered.length === 1 && delivered[0].data.type === 'PAIRING_RESP' && delivered[0].fromPeerId === 'other-key'
    )

    console.log('  RELAY HTTP TRANSPORT: 3 checks')
    process.exit(0)
  }, 300)
}

runAllTests().catch((err) => {
  console.error('Test runner fatal error:', err)
  process.exit(1)
})
