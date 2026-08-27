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

module.exports = { getFileType, safeFilename, buildManifest, parseManifest }
