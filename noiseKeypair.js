'use strict'

// Persistent swarm (noise) keypair. The noise public key doubles as this
// node's peer id: it is what peers use for trust (trustedPeerKeys), device
// records (device.publicKey), and direct reconnects (joinPeer). A fresh
// keypair per boot orphans every previously paired device — its stored key no
// longer matches, so reconnection falls back to the pairing handshake (and
// without a code, the watchdog times out in a loop). Persisting the keypair
// keeps the identity stable across restarts.

const { fs, path } = require('./compat.js')
const hcrypto = require('hypercore-crypto')

const KEYPAIR_FILE = 'noise-keypair.json'

// Load the saved keypair, or generate + persist a new one on first boot.
// Synchronous: the swarm is constructed at start(), before any awaits.
function loadOrCreateNoiseKeypair(storageDir) {
  try {
    fs.mkdirSync(storageDir, { recursive: true })
  } catch {}
  const keyFile = path.join(storageDir, KEYPAIR_FILE)
  try {
    const saved = JSON.parse(fs.readFileSync(keyFile, 'utf8'))
    if (
      saved &&
      typeof saved.publicKey === 'string' &&
      typeof saved.secretKey === 'string' &&
      saved.publicKey.length === 64 &&
      saved.secretKey.length === 128
    ) {
      return {
        publicKey: Buffer.from(saved.publicKey, 'hex'),
        secretKey: Buffer.from(saved.secretKey, 'hex')
      }
    }
    console.warn('[MeshEngine] Ignoring malformed noise keypair, generating a fresh one')
  } catch {}
  const keyPair = hcrypto.keyPair()
  const b4a = require('b4a')
  try {
    fs.writeFileSync(
      keyFile,
      JSON.stringify({
        publicKey: b4a.toString(keyPair.publicKey, 'hex'),
        secretKey: b4a.toString(keyPair.secretKey, 'hex')
      })
    )
  } catch (err) {
    console.warn('[MeshEngine] Failed to persist noise keypair:', err.message)
  }
  return keyPair
}

module.exports = { loadOrCreateNoiseKeypair }
