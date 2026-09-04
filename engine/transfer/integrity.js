'use strict'

// Integrity layer: SHA-256 block manifests + whole-file checksum.
//
// The canonical manifest is block 0 of a transfer/drop core. It streams the
// file once: every CHUNK_SIZE block gets a SHA-256 hash, and the whole-file
// checksum is the SHA-256 of the concatenated block hashes.

const b4a = require('b4a')
const { sha256 } = require('../../crypto.js')
const { CHUNK_SIZE, MANIFEST_V, sleep } = require('./constants.js')

function getFileType(filename) {
  const ext = String(filename || '')
    .split('.')
    .pop()
    .toLowerCase()
  const mime = {
    jpg: 'image/jpeg',
    jpeg: 'image/jpeg',
    png: 'image/png',
    gif: 'image/gif',
    webp: 'image/webp',
    svg: 'image/svg+xml',
    pdf: 'application/pdf',
    zip: 'application/zip',
    gz: 'application/gzip',
    mp4: 'video/mp4',
    mp3: 'audio/mpeg',
    txt: 'text/plain',
    json: 'application/json',
    js: 'application/javascript',
    html: 'text/html',
    css: 'text/css',
    md: 'text/markdown'
  }
  return mime[ext] || 'application/octet-stream'
}

function safeFilename(name) {
  return String(name || 'file').replace(/[^a-zA-Z0-9._-]/g, '_')
}

// How much of the file head the receiver needs before a generic container is
// considered playable (headers + init + early samples). 4 MiB is a safe
// over-estimate for TS/MKV/WebM/AVI; for MP4-family the real moov watermark is
// used when the metadata box is found in this window.
const PROBE_BYTES = 4 * 1024 * 1024

// MP4-family files are progressively playable only once the moov (metadata)
// atom has been received; mdat can precede it. Locate the moov box within the
// first PROBE bytes of the file so the receiver knows how much prefix it must
// verify before a player can mount the stream. Returns the byte offset where
// moov ends, or null when the file is not MP4-family / moov is not found in the
// probe window (caller then falls back to a conservative prefix threshold).
function scanMp4MoovEnd(buf) {
  // box: 4-byte size + 4-byte type
  if (buf.length < 8) return null
  let off = 0
  let lastMoovEnd = null
  while (off + 8 <= buf.length && off < PROBE_BYTES) {
    const size = buf.readUInt32BE(off)
    const type = buf.toString('latin1', off + 4, off + 8)
    if (size === 1) {
      // 64-bit size: read the 8-byte largesize that follows.
      if (off + 16 > buf.length) break
      const hi = buf.readUInt32BE(off + 8)
      const lo = buf.readUInt32BE(off + 12)
      const large = hi * 4294967296 + lo
      if (large < 16) break
      off += large
      continue
    }
    if (size === 0) break // box extends to EOF
    if (size < 8) break // corrupt
    if (type === 'moov') {
      lastMoovEnd = off + size
      break
    }
    off += size
  }
  return lastMoovEnd
}

// Progressive-playability metadata computed ONCE on the sender at stage time
// and carried in the manifest so every receiver gates "source ready" on the
// same watermark. For MP4-family the watermark is the end of the moov box (the
// metadata that lets a player build a timeline and start decoding). For
// everything else we conservatively require the first PROBE_BYTES prefix, which
// comfortably covers headers, init segments, and the first few seconds of
// samples for TS/MKV/WebM/AVI.
async function computePlayableWatermark({ filePath, fsp, fileSize }) {
  try {
    const fd = await fsp.open(filePath, 'r')
    try {
      const probe = b4a.alloc(Math.min(PROBE_BYTES, Math.max(0, fileSize)))
      if (probe.length === 0) return null
      const readRes = await fd.read(probe, 0, probe.length, 0)
      const bytesRead = typeof readRes === 'number' ? readRes : (readRes?.bytesRead || 0)
      const head = probe.subarray(0, bytesRead)
      if (head.length >= 12) {
        const boxType = head.toString('latin1', 4, 8)
        if (boxType === 'ftyp' || boxType === 'styp') {
          const moovEnd = scanMp4MoovEnd(head)
          // moov found in the probe window: playable once it is fully received.
          if (moovEnd) return { container: 'mp4', watermark: moovEnd }
          // moov after the probe window (or fragmented): require the probe
          // prefix; fragmented MP4 (fMP4) stays playable with init + moof.
          return { container: 'mp4', watermark: PROBE_BYTES }
        }
      }
      return { container: 'other', watermark: Math.min(PROBE_BYTES, fileSize) }
    } finally {
      await fd.close().catch(() => {})
    }
  } catch (err) {
    return null
  }
}

// Build the canonical manifest block (block 0 of a transfer core) by streaming
// the file once: every CHUNK_SIZE block gets a SHA-256 hash, and the whole-file
// checksum is the SHA-256 of the concatenated block hashes.
async function buildManifest({ filePath, fsp, filename, fileSize, fileType, transferId, shouldCancel }) {
  const blocks = []
  const blockSize = CHUNK_SIZE
  const fd = await fsp.open(filePath, 'r')
  try {
    const buf = b4a.alloc(blockSize)
    let offset = 0
    let chunkCount = 0
    while (offset < fileSize) {
      const readRes = await fd.read(buf, 0, blockSize, offset)
      const bytesRead = typeof readRes === 'number' ? readRes : (readRes?.bytesRead || 0)
      if (bytesRead === 0) break
      blocks.push(b4a.toString(sha256(buf.subarray(0, bytesRead)), 'hex'))
      offset += bytesRead
      // Yield periodically so the single worklet thread can still answer IPC
      // while hashing large files (a 4K video would otherwise block the UI).
      if (++chunkCount % 256 === 0) {
        // A pause/delete during the hash must abort immediately instead of
        // waiting for the whole file to be hashed (that was the multi-second
        // pause delay on big videos).
        if (shouldCancel && shouldCancel()) throw new Error('interrupted')
        await new Promise((resolve) => setImmediate(resolve))
      }
    }
  } finally {
    await fd.close()
  }

  const manifest = {
    v: MANIFEST_V,
    type: 'file-transfer',
    transferId,
    filename,
    fileSize,
    fileType: fileType || getFileType(filename),
    blockSize,
    blockCount: blocks.length,
    blocks,
    checksum: b4a.toString(sha256(b4a.concat(blocks.map((h) => b4a.from(h, 'hex')))), 'hex'),
    createdAt: Date.now()
  }

  // Progressive-playback watermark: bytes of the file head that must be on disk
  // (and verified) before a player can mount a playable stream. Computed once
  // here from the real file so the receiver can gate "source ready" on it.
  const watermark = await computePlayableWatermark({ filePath, fsp, fileSize }).catch(() => null)
  if (watermark && typeof watermark.watermark === 'number') {
    manifest.playableAfter = watermark.watermark
    manifest.container = watermark.container || 'other'
  }

  const manifestHash = b4a.toString(sha256(JSON.stringify(manifest)), 'hex')
  return { manifest, manifestHash }
}

function parseManifest(raw) {
  try {
    const manifest = JSON.parse(raw)
    if (
      !manifest ||
      manifest.v !== MANIFEST_V ||
      typeof manifest.blockSize !== 'number' ||
      !Array.isArray(manifest.blocks)
    ) {
      return null
    }
    return manifest
  } catch {
    return null
  }
}

module.exports = {
  getFileType,
  safeFilename,
  buildManifest,
  parseManifest,
  computePlayableWatermark,
  scanMp4MoovEnd,
  PROBE_BYTES
}
