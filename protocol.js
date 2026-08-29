'use strict'

// Single protocol schema for the @mesh/core P2P wire.
//
// This file is the one source of truth for:
//   - peer message types exchanged over the p2p-signal-v1 channel
//   - event names emitted by MeshEngine (EventEmitter)
//   - DHT topic labels
//
// The legacy worker scattered these literals across connections.js,
// TrustManager.js and TransferEngine.js and routed events through the Electron
// IPC protocol (src/shared/protocol.js). The core keeps the wire format
// (JSON objects over the protomux channel) but owns its own schema so the
// package has no dependency on the Electron app.

// ─── Peer wire messages (JSON over p2p-signal-v1) ───────────────────────────

// Wire protocol version, carried in the HANDSHAKE. Bump when the peer message
// schema or transfer/sync semantics change in a way that breaks older peers.
// Peers that report a different (or missing) version are marked incompatible:
// sync is gated so apps never silently mis-sync across versions.
const PROTOCOL_VERSION = 2

const MESSAGES = {
  HANDSHAKE: 'HANDSHAKE',
  PAIRING_CHALLENGE: 'PAIRING_CHALLENGE',
  PAIRING_RESP: 'PAIRING_RESP',
  // Sent by a host to a peer it is deleting so the peer's UI can react
  // immediately ("you were removed") instead of discovering it on next
  // reconnect. The receiving side revokes the host's key and destroys the
  // connection; re-admission requires a fresh pairing with the current code.
  DEVICE_REMOVED: 'DEVICE_REMOVED',
  TRANSFER_OFFER: 'TRANSFER_OFFER',
  PING: 'PING',
  PONG: 'PONG',
  CLAIM_FILE_REQ: 'CLAIM_FILE_REQ',
  CLAIM_FILE_RES: 'CLAIM_FILE_RES',
  CLAIM_FILE_DONE: 'CLAIM_FILE_DONE',
  SYNC_INDEX: 'SYNC_INDEX',
  SYNC_DELETE: 'SYNC_DELETE',
  SYNC_INVITE: 'SYNC_INVITE',
  SYNC_INVITE_ACCEPT: 'SYNC_INVITE_ACCEPT',
  SYNC_INVITE_DECLINE: 'SYNC_INVITE_DECLINE',
  SYNC_REMOVE: 'SYNC_REMOVE',
  // Batch pre-verification: the owner asks which pending files the receiver
  // already holds byte-identical, so already-synced files never become
  // transfer records (additive — old peers simply never answer, and the owner
  // falls back to the per-file skip handshake).
  SYNC_VERIFY: 'SYNC_VERIFY',
  SYNC_VERIFY_RESULT: 'SYNC_VERIFY_RESULT',
  // Watch Party synchronized playback & group swarm state
  WATCH_STATE_SYNC: 'WATCH_STATE_SYNC',
  WATCH_PEER_STATUS: 'WATCH_PEER_STATUS'
}

// ─── Engine events (what MeshEngine emits) ──────────────────────────────────

const EVENTS = {
  PEER_CONNECTED: 'peer:connected',
  PEER_DISCONNECTED: 'peer:disconnected',
  TRUST_PAIRED: 'trust:paired',
  TRUST_REVOKED: 'trust:revoked',
  PAIRING_FAILED: 'pairing:failed',
  DEVICE_REMOVED: 'device:removed',
  TRANSFER_OFFER: 'transfer:offer',
  TRANSFER_QUEUED: 'transfer:queued',
  TRANSFER_STARTED: 'transfer:started',
  TRANSFER_PROGRESS: 'transfer:progress',
  TRANSFER_PAUSED: 'transfer:paused',
  TRANSFER_RESUMED: 'transfer:resumed',
  TRANSFER_CANCELLED: 'transfer:cancelled',
  TRANSFER_COMPLETED: 'transfer:completed',
  TRANSFER_FAILED: 'transfer:failed',
  SYNC_LIBRARY_ADDED: 'sync:library:added',
  SYNC_LIBRARY_REMOVED: 'sync:library:removed',
  SYNC_SCAN: 'sync:scan',
  SYNC_UP_TO_DATE: 'sync:up_to_date',
  SYNC_COMPLETED: 'sync:completed',
  SYNC_DELETED: 'sync:deleted',
  SYNC_CONFLICT: 'sync:conflict',
  SYNC_ERROR: 'sync:error',
  SYNC_INVITE_RECEIVED: 'sync:invite:received',
  // Sync run phase: 'analyzing' | 'transferring' | 'synced' with counters —
  // lets the UI separate file comparison from actual payload transfer.
  SYNC_PHASE: 'sync:phase',
  CLAIM_PREVIEW: 'claim:preview',
  WATCH_STATE_UPDATED: 'watch:state:updated',
  NOTIFICATION_RECEIVED: 'notification:received',
  ERROR: 'error'
}

// ─── DHT topic labels ───────────────────────────────────────────────────────

// Pairing topic for a code: both the code host and every joiner announce on
// it, so entering a code on the joiner side converges on the host.
function pairingTopic(code) {
  return `p2p-pair-${code}`
}

// Identity topic: a device announces on a topic derived from its stable
// identity core key so previously paired devices can find it again.
function peerTopic(publicKey) {
  return `p2p-peer-${publicKey}`
}

// One-time share (drop) topic for a claim code.
function dropTopic(code) {
  return `p2p-file-${code}`
}

const TOPIC_PREFIXES = {
  PAIR: 'p2p-pair-',
  PEER: 'p2p-peer-',
  DROP: 'p2p-file-'
}

// ─── Wire validation ────────────────────────────────────────────────────────

function isPairingMessage(msg) {
  return (
    msg &&
    (msg.type === MESSAGES.PAIRING_CHALLENGE ||
      msg.type === MESSAGES.PAIRING_RESP ||
      msg.type === MESSAGES.HANDSHAKE)
  )
}

module.exports = {
  PROTOCOL_VERSION,
  MESSAGES,
  EVENTS,
  pairingTopic,
  peerTopic,
  dropTopic,
  TOPIC_PREFIXES,
  isPairingMessage
}
