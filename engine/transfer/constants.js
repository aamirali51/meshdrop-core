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
  STATUS,
  SCHEMA_VERSION,
  SCHEMA_KEY,
  TERMINAL,
  sleep
}
