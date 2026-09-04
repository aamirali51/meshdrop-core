'use strict'

const path = require('path')

const SITE_MIME = {
  '.html': 'text/html; charset=utf-8',
  '.htm': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.avif': 'image/avif',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.otf': 'font/otf',
  '.eot': 'application/vnd.ms-fontobject',
  '.mp4': 'video/mp4',
  '.webm': 'video/webm',
  '.mp3': 'audio/mpeg',
  '.ogg': 'audio/ogg',
  '.wav': 'audio/wav',
  '.pdf': 'application/pdf',
  '.txt': 'text/plain; charset=utf-8',
  '.md': 'text/markdown; charset=utf-8',
  '.xml': 'application/xml; charset=utf-8',
  '.wasm': 'application/wasm'
}

function getSiteMimeType(filePath) {
  const ext = path.extname(String(filePath || '')).toLowerCase()
  return SITE_MIME[ext] || 'application/octet-stream'
}

function resolveSitePath(siteRoot, reqPath) {
  const root = path.resolve(siteRoot)
  const clean = String(reqPath || '')
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
    .split('/')
    .filter((seg) => seg && seg !== '.' && seg !== '..')
  const abs = path.resolve(root, clean.join(path.sep))
  const rel = path.relative(root, abs)
  if (rel === '..' || rel.startsWith('..' + path.sep) || path.isAbsolute(rel)) return null
  return abs
}

function parseRange(rangeHeader, size) {
  if (!rangeHeader) return null
  const m = /^bytes=(\d*)-(\d*)$/.exec(String(rangeHeader).trim())
  if (!m) return null
  let start = m[1] === '' ? null : parseInt(m[1], 10)
  let end = m[2] === '' ? null : parseInt(m[2], 10)
  if (start === null && end === null) return null
  if (start === null) {
    const suffix = end
    if (suffix <= 0) return null
    start = Math.max(size - suffix, 0)
    end = size - 1
  } else {
    if (start >= size) return null
    if (end === null || end >= size) end = size - 1
    if (end < start) return null
  }
  return { start, end }
}

async function listDir(fsp, absDir, root) {
  const out = []
  const entries = await fsp.readdir(absDir, { withFileTypes: true })
  for (const ent of entries) {
    if (ent.name.startsWith('.')) continue
    const abs = path.join(absDir, ent.name)
    const isDir = ent.isDirectory()
    const isFile = ent.isFile()
    if (!isDir && !isFile) continue
    const rel = path.relative(root, abs).split(path.sep).join('/')
    const item = {
      name: ent.name,
      path: '/' + rel,
      type: isDir ? 'dir' : 'file'
    }
    if (isFile) {
      try {
        const st = await fsp.stat(abs)
        item.size = st.size
        item.mtimeMs = st.mtimeMs
      } catch {}
    }
    out.push(item)
  }
  out.sort((a, b) => {
    if (a.type !== b.type) return a.type === 'dir' ? -1 : 1
    return a.name.localeCompare(b.name)
  })
  return out
}

async function resolveSiteFile(fsp, siteRoot, reqPath, opts = {}) {
  const spa = !!opts.spa
  const primaryRoot = path.resolve(siteRoot)
  const childRoots = ['html', 'public', 'dist', 'build', 'out', 'site'].map((c) => path.join(primaryRoot, c))
  const parentRoot = path.dirname(primaryRoot)
  const rootsToTry = [primaryRoot, ...childRoots, parentRoot]
  const tried = new Set()

  for (const root of rootsToTry) {
    if (!root || tried.has(root)) continue
    tried.add(root)
    const abs = resolveSitePath(root, reqPath)
    if (!abs) continue
    try {
      const st = await fsp.stat(abs)
      if (st.isFile()) return { kind: 'file', abs, stat: st, triedRoot: root }
      if (st.isDirectory()) {
        const indexAbs = path.join(abs, 'index.html')
        try {
          const ist = await fsp.stat(indexAbs)
          if (ist.isFile()) return { kind: 'file', abs: indexAbs, stat: ist, triedRoot: root }
        } catch {}
        return { kind: 'dir', abs, stat: st, triedRoot: root }
      }
    } catch {}
  }
  if (spa) {
    for (const root of rootsToTry) {
      if (!root || tried.has(root + ':spa')) continue
      const fallback = path.join(root, 'index.html')
      try {
        const fst = await fsp.stat(fallback)
        if (fst.isFile()) return { kind: 'file', abs: fallback, stat: fst, spaFallback: true, triedRoot: root }
      } catch {}
    }
    for (const root of rootsToTry) {
      const notFound = path.join(root, '404.html')
      try {
        const nst = await fsp.stat(notFound)
        if (nst.isFile()) return { kind: 'file', abs: notFound, stat: nst, spaFallback: true, notFound: true, triedRoot: root }
      } catch {}
    }
  }
  const abs0 = resolveSitePath(primaryRoot, reqPath)
  return abs0 ? { kind: 'not-found', abs: abs0 } : { kind: 'invalid' }
}

function matchHeaderPattern(pattern, reqPath) {
  const esc = String(pattern || '').replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*')
  return new RegExp('^' + esc + '$').test(String(reqPath || ''))
}

function resolveSiteHeaders(manifest, reqPath) {
  const extra = {}
  const rules = manifest && manifest.headers && typeof manifest.headers === 'object' ? manifest.headers : null
  if (!rules) return extra
  for (const [pattern, value] of Object.entries(rules)) {
    if (!matchHeaderPattern(pattern, reqPath)) continue
    if (typeof value === 'string') {
      extra['Cache-Control'] = value
    } else if (value && typeof value === 'object') {
      for (const [k, v] of Object.entries(value)) extra[k] = String(v)
    }
  }
  return extra
}

async function loadSiteManifest(fsp, siteRoot) {
  const candidates = ['site.json', '.meshdrop.json', 'meshdrop.json']
  for (const name of candidates) {
    const p = path.join(path.resolve(siteRoot), name)
    try {
      const raw = await fsp.readFile(p, 'utf8')
      const parsed = JSON.parse(raw)
      if (parsed && typeof parsed === 'object') return parsed
    } catch {}
  }
  return null
}

module.exports = { resolveSitePath, parseRange, listDir, getSiteMimeType, resolveSiteFile, loadSiteManifest, resolveSiteHeaders }
