'use strict'

// SyncConstants — Shared constants, ignore lists, path normalizers for SyncEngine

const DEFAULT_SCAN_INTERVAL_MS = 60 * 1000
const MAX_LIBRARY_FILES = 50000
const CONFLICT_TOLERANCE_MS = 3000
const VERIFY_TOLERANCE_MS = 1500
const DEFAULT_VERIFY_TIMEOUT_MS = 15000
const MAX_VERIFY_FILES = 4000
const MAX_CONCURRENT_TRANSFERS = 4

const IGNORED_NAMES = new Set([
  '.p2p-staging',
  '.meshdrop-trash',
  '.thumbnails',
  '.nomedia',
  '.DS_Store',
  'Thumbs.db',
  '.git',
  '.',
  '..'
])

function isIgnored(name) {
  if (!name || typeof name !== 'string') return true
  if (IGNORED_NAMES.has(name)) return true
  if (name.startsWith('.trashed-') || name.startsWith('.pending-')) return true
  if (name.includes('(conflicted copy)')) return true
  return false
}

const INVALID_FS_CHARS = /[<>:"/\\|?*\u0000-\u001F]/g
const MAX_COMPONENT_LEN = 240

function safeRelPath(relPath) {
  if (typeof relPath !== 'string' || relPath.length === 0 || relPath.length > 1024) return ''
  const segments = relPath.split(/[\\/]+/).filter((s) => s !== '' && s !== '.' && s !== '..')
  if (segments.length === 0) return ''
  return segments
    .map((seg) => {
      let s = seg.replace(INVALID_FS_CHARS, '_').replace(/[. ]+$/, '')
      if (s.length > MAX_COMPONENT_LEN) {
        const dot = s.lastIndexOf('.')
        const ext = dot > 0 ? s.slice(dot) : ''
        if (ext && ext.length < MAX_COMPONENT_LEN) {
          s = s.slice(0, MAX_COMPONENT_LEN - ext.length) + ext
        } else {
          s = s.slice(0, MAX_COMPONENT_LEN)
        }
      }
      return s === '' ? '_' : s
    })
    .join('/')
}

function indexToArray(index) {
  const out = []
  if (!index) return out
  for (const [rel, e] of Object.entries(index)) {
    if (!e) continue
    out.push({
      rel,
      size: e.size || 0,
      mtimeMs: e.mtimeMs || 0,
      sig: e.sig || `${e.size || 0}-${e.mtimeMs || 0}`,
      authorKey: e.authorKey || '',
      deleted: !!e.deleted
    })
  }
  return out
}

function indexFromArray(entries) {
  const out = {}
  if (!Array.isArray(entries)) return out
  for (const e of entries) {
    if (!e || typeof e.rel !== 'string') continue
    const rel = safeRelPath(e.rel)
    if (!rel) continue
    out[rel] = {
      size: e.size || 0,
      mtimeMs: e.mtimeMs || 0,
      sig: typeof e.sig === 'string' && e.sig ? e.sig : `${e.size || 0}-${e.mtimeMs || 0}`,
      authorKey: typeof e.authorKey === 'string' ? e.authorKey : '',
      deleted: !!e.deleted
    }
  }
  return out
}

module.exports = {
  DEFAULT_SCAN_INTERVAL_MS,
  MAX_LIBRARY_FILES,
  CONFLICT_TOLERANCE_MS,
  VERIFY_TOLERANCE_MS,
  DEFAULT_VERIFY_TIMEOUT_MS,
  MAX_VERIFY_FILES,
  MAX_CONCURRENT_TRANSFERS,
  IGNORED_NAMES,
  isIgnored,
  safeRelPath,
  indexToArray,
  indexFromArray
}
