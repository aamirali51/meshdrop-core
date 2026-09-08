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
  // Sent to connected peers when this machine renames a device row, so every
  // trusted machine that also stores that identity adopts the new canonical
  // name (receiver no-ops when it has no row for the identity). Additive —
  // older peers without this branch simply ignore the message type.
  DEVICE_UPDATED: 'DEVICE_UPDATED',
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
  // Audit fix F1: explicit refusal for SYNC_* traffic from a lan-level
  // (recognized but unpaired) peer, so the sender's UI can explain why sync
  // never starts. Untrusted/unknown senders get silence — no oracle.
  SYNC_DENIED: 'SYNC_DENIED',
  // Watch Party synchronized playback & group swarm state
  WATCH_STATE_SYNC: 'WATCH_STATE_SYNC',
  WATCH_PEER_STATUS: 'WATCH_PEER_STATUS',
  // Holesail-style tunnel — Tier1 paired + Tier2 ephemeral code (TUNNEL-XXXX)
  // TUNNEL_CLAIM is metered by the host: a per-peer token bucket (5 tokens,
  // +1 per 12s) plus per-peer / per-share concurrency caps. TUNNEL_CLAIM_RES
  // failure is deliberately uniform — every denial (unknown code, expired,
  // max-uses, throttled, at capacity) is exactly { code, ok:false } with no
  // reason field, so CLAIM_RES never acts as a code-validity oracle. The
  // guest therefore treats any ok:false as inconclusive and keeps its claim
  // drive running until the join's own timeout; hosts log the specific
  // reason locally.
  TUNNEL_CLAIM: 'TUNNEL_CLAIM',
  TUNNEL_CLAIM_RES: 'TUNNEL_CLAIM_RES',
  TUNNEL_OFFER: 'TUNNEL_OFFER',
  TUNNEL_ACCEPT: 'TUNNEL_ACCEPT',
  TUNNEL_REJECT: 'TUNNEL_REJECT',
  TUNNEL_OPEN: 'TUNNEL_OPEN',
  TUNNEL_CLOSE: 'TUNNEL_CLOSE',
  TUNNEL_ERROR: 'TUNNEL_ERROR'
}

// ─── Tunnel channel framing (meshdrop-tunnel-v1 / meshdrop-tunnel-udp-v1) ───
// The TUNNEL_* messages above are rendezvous/control messages over the
// p2p-signal channel. The byte pipe itself rides one Protomux channel per
// tunnelId (protocols defined in engine/TunnelManager.js) with two messages:
//
//   data (raw bytes): the stream payload — TCP bytes, or framed UDP datagrams
//                     on the udp-v1 protocol.
//   ctrl (JSON string), parsed by TunnelManager:
//     { type:'close', reason }  — whole-tunnel teardown requested by the peer
//                                  (mirrors TUNNEL_CLOSE over signaling).
//     { type:'eof', reason? }   — the sender's LOCAL stream ended or never
//         started (guest client disconnected, host upstream closed — e.g. an
//         HTTP server ending an idle keep-alive — or upstream connect refused).
//         The receiver finishes ITS local stream with a clean FIN. Socket-only
//         teardown: the tunnel stays open and the next guest stream reuses the
//         channel. No further data frames follow an EOF from the same stream;
//         'eof' never closes the tunnel (no meshdrop-tunnel-v1 message may).
//
// EOF is how TCP tunnel lifetime is decoupled from any single local-socket
// lifetime: the tunnel is the long-lived object, local sockets are
// per-stream sub-resources that come and go under EOF.

// ─── Engine events (what MeshEngine emits) ──────────────────────────────────

const EVENTS = {
  PEER_CONNECTED: 'peer:connected',
  PEER_DISCONNECTED: 'peer:disconnected',
  TRUST_PAIRED: 'trust:paired',
  TRUST_REVOKED: 'trust:revoked',
  // Two-tier trust: a peer heard on the LAN (autoTrustLAN enabled) is
  // recognized at the 'lan' level only — identity exchange + display. This
  // event asks the user to explicitly confirm full pairing.
  DEVICE_DETECTED_LAN: 'device:detected:lan',
  PAIRING_FAILED: 'pairing:failed',
  DEVICE_REMOVED: 'device:removed',
  DEVICE_UPDATED: 'device:updated',
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
  SYNC_DENIED: 'sync:denied',
  // Sync run phase: 'analyzing' | 'transferring' | 'synced' with counters —
  // lets the UI separate file comparison from actual payload transfer.
  SYNC_PHASE: 'sync:phase',
  CLAIM_PREVIEW: 'claim:preview',
  // Host-side pending-share lifecycle: a drop code expired (swept by the
  // periodic expiration check or a listing) or was claimed for the first time.
  // The renderer refreshes its share grid on these so expiry/claim state is
  // visible without a manual reload.
  PENDING_SHARE_EXPIRED: 'pending:share:expired',
  PENDING_SHARE_CLAIMED: 'pending:share:claimed',
  WATCH_STATE_UPDATED: 'watch:state:updated',
  NOTIFICATION_RECEIVED: 'notification:received',
  // MeshDrop Sites — a visitor proved ownership of the pasted code and was
  // added to (or removed from) a site's allowlist. No device trust is granted.
  SITE_VISITOR_ADDED: 'site:visitor:added',
  SITE_VISITOR_REMOVED: 'site:visitor:removed',
  // A visitor's allowlist challenge failed (wrong code / wrong device).
  SITE_VISITOR_FAILED: 'site:visitor:failed',
  // A visitor joined/left a site session (allowlisted + verified + connected).
  SITE_VISIT_STARTED: 'site:visit:started',
  SITE_VISIT_STOPPED: 'site:visit:stopped',
  SITE_INVITE_RECEIVED: 'site:invite:received',
  // Private tunnel (Holesail-style pipe between paired peers)
  TUNNEL_OFFER: 'tunnel:offer',
  TUNNEL_OPENED: 'tunnel:opened',
  TUNNEL_CLOSED: 'tunnel:closed',
  TUNNEL_ERROR: 'tunnel:error',
  TUNNEL_DATA: 'tunnel:data',
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

// MeshDrop Sites — topic for a published site, announced by the host and
// joined by allowlisted visitors to reach it (sibling of the drop/watch topics).
function siteTopic(siteId) {
  return `p2p-site-${siteId}`
}

function tunnelTopic(code) {
  return `p2p-tunnel-${code}`
}

const TOPIC_PREFIXES = {
  PAIR: 'p2p-pair-',
  PEER: 'p2p-peer-',
  DROP: 'p2p-file-',
  SITE: 'p2p-site-',
  TUNNEL: 'p2p-tunnel-'
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
  siteTopic,
  tunnelTopic,
  TOPIC_PREFIXES,
  isPairingMessage
}
