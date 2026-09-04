'use strict'

// Site access gate (slice 2 of the Sites design; see meshdrop-app/docs/SITES.md).
//
// The v1 allowlist flow: a visitor shares their MeshDrop pairing code
// (MD-XXXX-…); the host pastes it into the site's "Allowed users" list. The
// host challenges the visitor with a random nonce; the visitor answers
// mac(theirCode, nonce); a match proves they HOLD the code, so the host adds
// the visitor's verified Noise public key to the site allowlist.
//
// CRITICAL: this is a SCOPED grant. It proves code ownership and populates the
// site allowlist ONLY. It must never call addTrustedKey() / onTrustGranted()
// (that stays exclusively in TrustManager.handleResponse — KB invariant 3).
// A visitor allowed into a site is NOT a paired device and gains no exchange
// replication.
//
// mac() here mirrors crypto.mac(): BLAKE2b-256 over [key, message] via
// hypercore-crypto, so the visitor code never travels the wire — the host
// derives codeId from the code it was given, and the visitor MACs with the
// code string it holds.

const b4a = require('b4a')
const hcrypto = require('hypercore-crypto')
const { codeId } = require('./crypto.js')

function mac(key, message) {
  const keyBuf = b4a.isBuffer(key) ? key : b4a.from(String(key), 'utf8')
  const msgBuf = b4a.isBuffer(message) ? message : b4a.from(String(message), 'utf8')
  return hcrypto.hash([keyBuf, msgBuf])
}

// Host side: build a challenge for a visitor whose code the host is adding.
function createChallenge(visitorCode, nonceBuf) {
  const nonce = b4a.isBuffer(nonceBuf) ? nonceBuf : b4a.from(nonceBuf, 'utf8')
  return {
    type: 'SITE_VISITOR_CHALLENGE',
    codeId: codeId(visitorCode), // non-secret identifier only — never the code
    nonce: b4a.toString(nonce, 'hex')
  }
}

// Host side: register a visitor code the host is verifying, and return the
// challenge to broadcast on the visitor's pairing topic. Joining the topic
// and relaying the challenge are the caller's job (engine/TrustManager); this
// stays pure so it is unit-testable and never touches engine state.
function createHostChallenge(visitorCode, nonceBuf) {
  const nonce = b4a.isBuffer(nonceBuf) ? nonceBuf : b4a.from(nonceBuf, 'utf8')
  return {
    codeId: codeId(visitorCode),
    nonce: b4a.toString(nonce, 'hex')
  }
}

// Host side: verify a visitor's answer. On success the host may add the
// visitor's verified Noise public key (from the transport) to the allowlist.
// `challenge` is the object returned by createHostChallenge (or the wire
// PAIRING_CHALLENGE the host sent), and `answerMac` is the visitor's mac.
function verifyHostAnswer(visitorCode, challenge, answerMac) {
  if (!challenge || typeof answerMac !== 'string') return false
  const expected = mac(visitorCode, b4a.from(String(challenge.nonce), 'hex'))
  const expectedHex = b4a.toString(expected, 'hex')
  return timingSafeEqualHex(expectedHex, answerMac)
}

// Visitor side: answer a challenge with the visitor code it holds.
// Returns the MAC hex, or null if the challenge is not for a code we hold.
function answerChallenge(visitorCode, challenge) {
  if (!challenge || challenge.type !== 'SITE_VISITOR_CHALLENGE') return null
  if (typeof challenge.nonce !== 'string' || challenge.nonce.length !== 32) return null
  if (codeId(visitorCode) !== challenge.codeId) return null
  const sig = mac(visitorCode, b4a.from(challenge.nonce, 'hex'))
  return b4a.toString(sig, 'hex')
}

// Host side: verify a visitor's answer. On success the host may add the
// visitor's verified Noise public key (from the transport) to the allowlist.
function verifyAnswer(visitorCode, challenge, answerMac) {
  if (!challenge || typeof answerMac !== 'string') return false
  const expected = answerChallenge(visitorCode, challenge)
  if (!expected) return false
  // Constant-time compare: the MAC is a capability proof; a timing side
  // channel here would let an attacker test codes byte-by-byte.
  return timingSafeEqualHex(expected, answerMac)
}

function timingSafeEqualHex(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string' || a.length !== b.length) return false
  const nodeCrypto = require('crypto')
  return nodeCrypto.timingSafeEqual(b4a.from(a, 'hex'), b4a.from(b, 'hex'))
}

module.exports = {
  mac,
  createChallenge,
  createHostChallenge,
  verifyHostAnswer,
  answerChallenge,
  verifyAnswer
}
