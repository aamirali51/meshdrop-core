'use strict'

const crypto = require('crypto')
const { generateSiteCode, normalizeSiteCode } = require('./sites-codes.js')

function normalizeRole(role) {
  return role === 'editor' ? 'editor' : 'viewer'
}

function createSitesStore({ getBee }) {
  const byId = new Map()
  let hydratedAt = 0
  let hydratePromise = null

  async function hydrate({ force = false } = {}) {
    if (!force && Date.now() - hydratedAt < 2000 && byId.size > 0) return
    if (hydratePromise) return hydratePromise
    hydratePromise = (async () => {
      const bee = await getBee('sites')
      byId.clear()
      for await (const node of bee.createReadStream()) {
        const s = node.value
        if (s && s.siteId) {
          if (!s.allowlist) s.allowlist = []
          if (Array.isArray(s.allowlist) && s.allowlist.length > 0 && typeof s.allowlist[0] === 'string') {
            s.allowlist = s.allowlist.map((k) => ({ key: k, role: 'viewer' }))
          } else if (Array.isArray(s.allowlist)) {
            s.allowlist = s.allowlist.map((e) => ({ key: e.key, role: normalizeRole(e.role) }))
          }
          if (typeof s.writeMode !== 'string') s.writeMode = 'read-only'
          if (s.writeMode !== 'collab') s.writeMode = 'read-only'
          if (typeof s.spa !== 'boolean') s.spa = false
          if (typeof s.expiresAt !== 'number') s.expiresAt = 0
          if (typeof s.expirationPreset !== 'string') s.expirationPreset = s.expiresAt > 0 ? 'custom' : 'never'
          byId.set(s.siteId, s)
        }
      }
      hydratedAt = Date.now()
    })()
    try { await hydratePromise } finally { hydratePromise = null }
  }

  async function listSites() {
    await hydrate()
    return Array.from(byId.values())
  }

  async function getSite(siteId) {
    await hydrate()
    return byId.get(siteId) || null
  }

  function getDurationMs(preset) {
    const map = { '30m': 30*60*1000, '1h': 60*60*1000, '6h': 6*60*60*1000, '24h': 24*60*60*1000, '7d': 7*24*60*60*1000, never: 0 }
    return map[preset] ?? 0
  }

  async function createSite({ name, folderPath, writeMode, spa, expirationPreset }) {
    const bee = await getBee('sites')
    await hydrate({ force: true })
    const siteId = crypto.randomUUID()
    const code = generateSiteCode()
    const preset = expirationPreset || 'never'
    const duration = getDurationMs(preset)
    const createdAt = Date.now()
    const site = {
      id: siteId,
      siteId,
      code,
      name: name || 'My Drive',
      folderPath: folderPath || '',
      createdAt,
      expiresAt: duration > 0 ? createdAt + duration : 0,
      expirationPreset: preset,
      allowlist: [],
      writeMode: writeMode === 'collab' ? 'collab' : 'read-only',
      spa: !!spa
    }
    byId.set(siteId, site)
    await bee.put(siteId, site)
    hydratedAt = Date.now()
    return site
  }

  async function updateSite(siteId, patch) {
    const bee = await getBee('sites')
    const site = await getSite(siteId)
    if (!site) throw new Error('unknown site')
    if (typeof patch.name === 'string' && patch.name.trim()) site.name = patch.name.trim()
    if (typeof patch.folderPath === 'string' && patch.folderPath.trim()) site.folderPath = patch.folderPath.trim()
    if (typeof patch.writeMode === 'string') site.writeMode = patch.writeMode === 'collab' ? 'collab' : 'read-only'
    if (typeof patch.spa === 'boolean') site.spa = patch.spa
    if (typeof patch.expirationPreset === 'string') {
      site.expirationPreset = patch.expirationPreset
      const d = getDurationMs(patch.expirationPreset)
      site.expiresAt = d > 0 ? Date.now() + d : 0
    }
    if (typeof patch.expiresAt === 'number') site.expiresAt = patch.expiresAt
    await bee.put(siteId, site)
    hydratedAt = Date.now()
    return site
  }

  async function isExpired(siteId) {
    const site = await getSite(siteId)
    if (!site || !site.expiresAt || site.expiresAt === 0) return false
    return Date.now() >= site.expiresAt
  }

  async function removeSite(siteId) {
    const bee = await getBee('sites')
    byId.delete(siteId)
    await bee.del(siteId)
    hydratedAt = Date.now()
  }

  async function addToAllowlist(siteId, publicKeyHex, role = 'viewer') {
    if (typeof publicKeyHex !== 'string' || publicKeyHex.length !== 64) {
      throw new Error('site allowlist entries must be 64-hex noise public keys')
    }
    const r = normalizeRole(role)
    const site = await getSite(siteId)
    if (!site) throw new Error('unknown site')
    const idx = site.allowlist.findIndex((e) => e.key === publicKeyHex)
    if (idx >= 0) site.allowlist[idx].role = r
    else site.allowlist.push({ key: publicKeyHex, role: r })
    await getBee('sites').then((bee) => bee.put(siteId, site))
    hydratedAt = Date.now()
    return site
  }

  async function removeFromAllowlist(siteId, publicKeyHex) {
    const site = await getSite(siteId)
    if (!site) return
    site.allowlist = site.allowlist.filter((e) => e.key !== publicKeyHex)
    await getBee('sites').then((bee) => bee.put(siteId, site))
    hydratedAt = Date.now()
    return site
  }

  async function isAllowed(siteId, publicKeyHex) {
    const site = await getSite(siteId)
    if (!site) return false
    if (site.expiresAt > 0 && Date.now() >= site.expiresAt) return false
    return site.allowlist.some((e) => e.key === publicKeyHex)
  }

  async function getRole(siteId, publicKeyHex) {
    const site = await getSite(siteId)
    if (!site) return null
    const e = site.allowlist.find((x) => x.key === publicKeyHex)
    return e ? e.role : null
  }

  async function canWrite(siteId, publicKeyHex) {
    const site = await getSite(siteId)
    if (!site || site.writeMode !== 'collab') return false
    const e = site.allowlist.find((x) => x.key === publicKeyHex)
    return !!e && e.role === 'editor'
  }

  async function getSiteByCode(code) {
    const clean = normalizeSiteCode(code)
    if (!clean) return null
    await hydrate()
    for (const s of byId.values()) {
      if (s.code === clean) return s
    }
    return null
  }

  return {
    hydrate,
    listSites,
    getSite,
    createSite,
    updateSite,
    removeSite,
    isExpired,
    addToAllowlist,
    removeFromAllowlist,
    isAllowed,
    getRole,
    canWrite,
    getSiteByCode
  }
}

module.exports = { createSitesStore }
