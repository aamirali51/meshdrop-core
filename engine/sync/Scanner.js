'use strict'

// Scanner — Fast, memory-safe directory walker and signature/hash generator

const { MAX_LIBRARY_FILES, isIgnored, safeRelPath } = require('./SyncConstants.js')
const { sha256 } = require('../../crypto.js')

// Fast stat-only directory walker with bounded concurrency.
async function scanFolder(fsp, dir, baseDir, out, limit = MAX_LIBRARY_FILES) {
  const { path: p } = require('../../compat.js')
  const WALKERS = 4
  const queue = [dir]

  async function worker() {
    while (queue.length > 0) {
      if (out.size >= limit) return
      const currentDir = queue.shift()
      if (!currentDir) continue

      let entries
      try {
        entries = await fsp.readdir(currentDir, { withFileTypes: true })
      } catch {
        try {
          entries = await fsp.readdir(currentDir)
        } catch {
          continue
        }
      }
      if (!Array.isArray(entries)) continue

      const pendingStats = []
      for (const ent of entries) {
        if (out.size >= limit) return
        const name = typeof ent === 'string' ? ent : (ent && ent.name) || String(ent)
        if (isIgnored(name)) continue

        const abs = p.join(currentDir, name)
        if (typeof ent === 'object' && ent !== null) {
          if (typeof ent.isDirectory === 'function' && ent.isDirectory()) {
            if (out.size < limit) queue.push(abs)
            continue
          } else if (typeof ent.isFile === 'function' && ent.isFile()) {
            pendingStats.push(abs)
            continue
          }
        }
        pendingStats.push(abs)
      }

      const stats = await Promise.all(
        pendingStats.map((abs) =>
          fsp
            .lstat(abs)
            .then((st) => ({ abs, st }))
            .catch(() => null)
        )
      )

      for (const item of stats) {
        if (item && item.st && out.size < limit) {
          const isDir = typeof item.st.isDirectory === 'function' ? item.st.isDirectory() : false
          const isSym = typeof item.st.isSymbolicLink === 'function' ? item.st.isSymbolicLink() : false
          if (isSym) continue // Issue 5 fix: never follow symlinks — avoids infinite cycles
          if (isDir) {
            queue.push(item.abs)
          } else {
            const rel = safeRelPath(p.relative(baseDir, item.abs).split(p.sep).join('/'))
            if (!rel) continue
            const rawMtime =
              item.st.mtimeMs ||
              (item.st.mtime && typeof item.st.mtime.getTime === 'function' ? item.st.mtime.getTime() : item.st.mtime)
            const mtimeMs = Number(rawMtime) || Date.now()
            const size = Number(item.st.size) || 0
            const sig = `${size}-${mtimeMs}`
            out.set(rel, { size, mtimeMs, sig })
          }
        }
      }
    }
  }

  await Promise.all(Array.from({ length: WALKERS }, () => worker()))
}

// Check if rel is exactly in dirtySet or under a dirty directory.
function isDirtyOrUnder(dirtySet, rel) {
  if (!dirtySet) return true
  if (dirtySet.has(rel)) return true
  const parts = rel.split('/')
  for (let i = 1; i < parts.length; i++) {
    if (dirtySet.has(parts.slice(0, i).join('/'))) return true
  }
  return false
}

// Incremental scan of dirty paths when the watcher provides them.
async function statDirtyPaths(fsp, baseDir, dirtyPaths) {
  const out = new Map()
  let sawDir = false
  const { path: p } = require('../../compat.js')
  for (const dirty of dirtyPaths) {
    if (!dirty || out.size >= MAX_LIBRARY_FILES) continue
    const abs = p.join(baseDir, ...dirty.split('/'))
    let st = null
    try {
      st = await fsp.stat(abs)
    } catch {
      out.set(dirty, { size: 0, mtimeMs: Date.now(), sig: '', hash: '', deleted: true })
      continue
    }
    const isDir = typeof st.isDirectory === 'function' ? st.isDirectory() : false
    if (isDir) {
      sawDir = true
      continue
    }
    const rel = safeRelPath(dirty)
    if (!rel) continue
    const rawMtime =
      st.mtimeMs || (st.mtime && typeof st.mtime.getTime === 'function' ? st.mtime.getTime() : st.mtime)
    const mtimeMs = Number(rawMtime) || Date.now()
    const size = Number(st.size) || 0
    const sig = `${size}-${mtimeMs}`
    out.set(rel, { size, mtimeMs, sig, hash: '' })
  }
  out._sawDir = sawDir
  return out
}

// Streaming content hash of a file for divergence disambiguation.
async function hashFileFast(fsp, abs) {
  try {
    const fd = await fsp.open(abs, 'r')
    try {
      const chunkHashes = []
      const buf = Buffer.alloc(256 * 1024)
      let pos = 0
      for (;;) {
        const { bytesRead } = await fd.read(buf, 0, buf.length, pos)
        if (bytesRead <= 0) break
        chunkHashes.push(sha256(buf.subarray(0, bytesRead)))
        pos += bytesRead
      }
      if (chunkHashes.length === 0) return ''
      // Issue 6 fix: hash the concatenated chunk-hashes into a single digest
      // so the result is the same regardless of how the file was chunked.
      const combined = Buffer.concat(chunkHashes.map((h) => (Buffer.isBuffer(h) ? h : Buffer.from(h))))
      const final = sha256(combined)
      return Buffer.isBuffer(final) ? final.toString('hex') : ''
    } finally {
      await fd.close().catch(() => {})
    }
  } catch {
    return ''
  }
}

module.exports = {
  scanFolder,
  isDirtyOrUnder,
  statDirtyPaths,
  hashFileFast
}
