'use strict'

// Transfer constants: block sizes, scheduling limits, transfer statuses, and
// the schema version for the persisted transfer log.

const CHUNK_SIZE = 64 * 1024 // block size used for the integrity manifest
const MANIFEST_V = 1
const MIN_WINDOW = 8
const MAX_WINDOW = 64
const DEFAULT_PRIORITY = 'bulk'
const MAX_CONCURRENT = 2 // max active transfers per direction (global)
const MAX_PER_PEER = 1 // max active transfers per direction per peer
const MAX_TRANSFER_SIZE = 500 * 1024 * 1024 * 1024 // 500 GB hard cap

// ─── Head/tail priority fetch (video startup latency) ───────────────────────
// Progressive-playback containers (mp4/mkv/ts...) need the file HEAD (format
// headers, moov, init segments) before a player can mount the stream, and the
// TAIL for containers whose seek/cue metadata lives at the end (Matroska Cues,
// mp4 'sidx'/'mfra'). Priority fetching pulls both windows first so playback
// starts (and seeks) before the sequential sweep reaches them. All values are
// overridable per runtime via the network profile (desktop vs mobile-wifi vs
// mobile-cellular).
const DEFAULT_HEAD_BYTES = 4 * 1024 * 1024 // head window (matches integrity PROBE_BYTES)
const DEFAULT_TAIL_BYTES = 2 * 1024 * 1024 // desktop / wi-fi tail window
const MIN_TAIL_BYTES = 512 * 1024 // cellular floor; also the small-file minimum
const PREFETCH_BATCH = 4 // bounded parallel core.gets during the prefetch pass
const LRU_CAP_BYTES = 64 * 1024 * 1024 // in-memory verified head/tail block cache
const LRU_TTL_MS = 60 * 1000

const STATUS = {
  QUEUED: 'queued',
  ACTIVE: 'active',
  PAUSED: 'paused',
  INTERRUPTED: 'interrupted',
  COMPLETED: 'completed',
  FAILED: 'failed',
  CANCELLED: 'cancelled',
  PENDING_APPROVAL: 'pending_approval',
  // Claimer-side placeholder while the DROP host is offline: the offer has not
  // arrived yet, so there is no real transfer — but the user must SEE that the
  // claim is pending, not an empty page.
  WAITING_PEER: 'waiting_peer'
}

const SCHEMA_VERSION = 'transfer.2'
const SCHEMA_KEY = '__meta__'

const TERMINAL = new Set([STATUS.COMPLETED, STATUS.FAILED, STATUS.CANCELLED, STATUS.INTERRUPTED])

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

module.exports = {
  CHUNK_SIZE,
  MANIFEST_V,
  MIN_WINDOW,
  MAX_WINDOW,
  DEFAULT_PRIORITY,
  MAX_CONCURRENT,
  MAX_PER_PEER,
  MAX_TRANSFER_SIZE,
  DEFAULT_HEAD_BYTES,
  DEFAULT_TAIL_BYTES,
  MIN_TAIL_BYTES,
  PREFETCH_BATCH,
  LRU_CAP_BYTES,
  LRU_TTL_MS,
  STATUS,
  SCHEMA_VERSION,
  SCHEMA_KEY,
  TERMINAL,
  sleep
}
