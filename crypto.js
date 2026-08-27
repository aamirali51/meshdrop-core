'use strict'

// Cryptographic primitives for @mesh/core.
//
// Everything here is built on node:crypto plus hypercore-crypto (which ships
// with the hyperswarm stack and is already required for noise keypairs and DHT
// topic hashing). No sodium-native bindings are required, so the module runs
// anywhere Node runs (desktop, mobile Node threads, CI).

let nodeCrypto = null
try {
  const mod = 'crypto'
  nodeCrypto = require(mod)
} catch {}
const b4a = require('b4a')
const hcrypto = require('hypercore-crypto')

function randomBytes(n) {
  if (nodeCrypto && typeof nodeCrypto.randomBytes === 'function') {
    return nodeCrypto.randomBytes(n)
  }
  return hcrypto.randomBytes(n)
}

// Keyed MAC over the challenge nonce. The pairing code is the key; the peer
// proves knowledge of the code by returning the MAC over our random nonce.
// HMAC-SHA256 (node:crypto) or keyed BLAKE2b (hypercore-crypto).
function mac(key, message) {
  const keyBuf = b4a.isBuffer(key) ? key : b4a.from(String(key), 'utf8')
  const msgBuf = b4a.isBuffer(message) ? message : b4a.from(String(message), 'utf8')
  return hcrypto.hash([keyBuf, msgBuf])
}

// BLAKE2b-256 via hypercore-crypto — the same hash used for DHT topics, so
// topics remain stable across the old worker and the core.
function hash(data) {
  const buf = b4a.isBuffer(data) ? data : b4a.from(String(data), 'utf8')
  return hcrypto.hash([buf])
}

// Cryptographic 256-bit hash for transfer integrity manifests (BLAKE2b-256 via hypercore-crypto).
// Consistent across all platforms (Node.js, Bare, Android, iOS).
function sha256(data) {
  return hash(data)
}

// Stable, non-derivable device id derived from a public key (BLAKE2b).
function deriveDeviceId(publicKey) {
  const pk = typeof publicKey === 'string' ? b4a.from(publicKey, 'hex') : publicKey
  return b4a.toString(hash(pk), 'hex').slice(0, 16)
}

// ─── Pairing codes ──────────────────────────────────────────────────────────

const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
const CODE_GROUPS = 4
const CODE_GROUP_SIZE = 4
const CODE_LENGTH = CODE_GROUPS * CODE_GROUP_SIZE // 16 chars * 5 bits = 80 bits

function formatCode(raw) {
  const groups = []
  for (let i = 0; i < CODE_GROUPS; i++) {
    groups.push(raw.slice(i * CODE_GROUP_SIZE, (i + 1) * CODE_GROUP_SIZE))
  }
  return 'MD-' + groups.join('-')
}

// Random 80-bit pairing code, e.g. MD-ABCD-EFGH-JKLM-NPQR
function generatePairingCode() {
  const bytes = randomBytes(CODE_LENGTH)
  let code = ''
  for (let i = 0; i < CODE_LENGTH; i++) {
    code += ALPHABET[bytes[i] % 32]
  }
  return formatCode(code)
}

// Returns the canonical 'MD-XXXX-XXXX-XXXX-XXXX' or null if invalid.
function normalizePairingCode(raw) {
  if (typeof raw !== 'string') return null
  let clean = raw
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
  if (clean.startsWith('MD')) clean = clean.slice(2)
  if (clean.length !== CODE_LENGTH) return null
  return formatCode(clean)
}

// Non-secret identifier for a pairing secret, used in challenge messages so
// the responder knows which code to MAC with, without revealing the code.
function codeId(code) {
  return b4a.toString(hash(code), 'hex').slice(0, 16)
}

// ─── Drop codes (one-time file shares) ──────────────────────────────────────

const DROP_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789' // no I/O/0/1 ambiguity
const DROP_GROUP_SIZE = 4
const DROP_GROUPS = 2
const DROP_LENGTH = DROP_GROUPS * DROP_GROUP_SIZE // 8 chars * 5 bits = 40 bits

// Random 40-bit one-time share code, e.g. DROP-ABCD-EFGH or DROP-GRP-ABCD-EFGH
function generateDropCode(opts = {}) {
  const isGroup = opts && opts.isGroup === true
  const bytes = randomBytes(DROP_LENGTH)
  let code = ''
  for (let i = 0; i < DROP_LENGTH; i++) {
    code += DROP_ALPHABET[bytes[i] % 32]
  }
  const body = code.slice(0, DROP_GROUP_SIZE) + '-' + code.slice(DROP_GROUP_SIZE)
  return isGroup ? `DROP-GRP-${body}` : `DROP-${body}`
}

// Returns the canonical 'DROP-XXXX-XXXX' or 'DROP-GRP-XXXX-XXXX' or null if invalid.
function normalizeDropCode(raw) {
  if (typeof raw !== 'string') return null
  let clean = raw
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
  let isGroup = false
  if (clean.startsWith('DROPGRP')) {
    isGroup = true
    clean = clean.slice(7)
  } else if (clean.startsWith('GRP')) {
    isGroup = true
    clean = clean.slice(3)
  } else if (clean.startsWith('DROP')) {
    clean = clean.slice(4)
  }
  if (clean.length !== DROP_LENGTH) return null
  const body = clean.slice(0, DROP_GROUP_SIZE) + '-' + clean.slice(DROP_GROUP_SIZE)
  return isGroup ? `DROP-GRP-${body}` : `DROP-${body}`
}

module.exports = {
  randomBytes,
  mac,
  hash,
  sha256,
  deriveDeviceId,
  generatePairingCode,
  normalizePairingCode,
  formatCode,
  codeId,
  ALPHABET,
  CODE_LENGTH,
  generateDropCode,
  normalizeDropCode,
  DROP_ALPHABET,
  DROP_LENGTH
}
