'use strict'

// SITE code family for MeshDrop Sites — the house base32 format from crypto.js
// (alphabet minus I/O/0/1, grouped with dashes), prefix SITE-XXXX-XXXX.
// Standalone so the codec can be shared by the site registry (host) and the
// visitor entry flow without importing the full engine.

const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
const SITE_GROUPS = 2
const SITE_GROUP_SIZE = 4
const SITE_LENGTH = SITE_GROUPS * SITE_GROUP_SIZE // 8 chars * 5 bits = 40 bits

function randomBytes(n) {
  const nodeCrypto = require('crypto')
  return nodeCrypto.randomBytes(n)
}

function generateSiteCode() {
  const bytes = randomBytes(SITE_LENGTH)
  let code = ''
  for (let i = 0; i < SITE_LENGTH; i++) {
    code += ALPHABET[bytes[i] % 32]
  }
  const body = code.slice(0, SITE_GROUP_SIZE) + '-' + code.slice(SITE_GROUP_SIZE)
  return `SITE-${body}`
}

// Returns the canonical 'SITE-XXXX-XXXX' or null if invalid.
function normalizeSiteCode(raw) {
  if (typeof raw !== 'string') return null
  let clean = raw.trim().toUpperCase().replace(/[^A-Z0-9]/g, '')
  if (clean.startsWith('SITE')) clean = clean.slice(4)
  if (clean.length !== SITE_LENGTH) return null
  const body = clean.slice(0, SITE_GROUP_SIZE) + '-' + clean.slice(SITE_GROUP_SIZE)
  return `SITE-${body}`
}

module.exports = { generateSiteCode, normalizeSiteCode, SITE_LENGTH }
