'use strict'

// SiteManager — MeshDrop Sites host-side access control (slices 1-2; see
// meshdrop-app/docs/SITES.md).
//
// Owns: the `sites` Hyperbee store (site records + per-site allowlists of
// VERIFIED visitor Noise keys) and the challenge state for adding a visitor by
// code.
//
// Wire protocol: the allowlist challenge uses DEDICATED SITE_VERIFY_CHALLENGE
// / SITE_VERIFY_RESP messages — NOT PAIRING_CHALLENGE/PAIRING_RESP — so it can
// never collide with device pairing. The host joins the visitor's pairing
// topic purely as the rendezvous (every device announces permanently on its
// own host-code topic) and sends SITE_VERIFY_CHALLENGE over the direct
// signaling channel AND the relay. The visitor answers SITE_VERIFY_RESP with
// mac(itsCode, nonce). The host verifies the MAC and allowlists the visitor's
// Noise key — WITHOUT granting device trust (TrustManager.handleResponse is
// never involved; KB invariant 3 is preserved).
//
// This is a HOST-side module. The visitor answers SITE_VERIFY_CHALLENGE in
// TrustManager.handleSiteVerifyChallenge (it MACs with whatever MD- code it
// holds that matches the codeId) — no SiteManager needed on the visitor.

const { createSitesStore } = require('../sites.js')
const { normalizePairingCode, codeId, mac } = require('../crypto.js')
const b4a = require('b4a')
const crypto = require('crypto')

class SiteManager {
  constructor({ engine, getBee }) {
    this.engine = engine
    this.getBee = getBee || ((name) => engine.getBee(name))
    this.store = null
    this.pendingVisitors = new Map() // codeId -> { code, siteId, nonceHex, sentAt, resolve, reject, timer }
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────────────

  async init() {
    this.store = createSitesStore({ getBee: this.getBee })
    await this.store.hydrate()
  }

  // Hook the engine's inbound signal router via the first-chance interceptor
  // (signaling.js consults ctx.beforePeerMessage before the generic router).
  // A SITE_VERIFY_RESP that answers one of OUR site challenges is consumed
  // here — MAC-verified against the visitor code — and never reaches the
  // device-pairing path. Returns true only when the message was genuinely a
  // site-verification response.
  installSignalHook() {
    const conn = this.engine.connections
    if (!conn || typeof conn.setBeforePeerMessage !== 'function') return
    const self = this
    conn.setBeforePeerMessage((peerId, msg) => {
      if (msg && msg.type === 'SITE_VERIFY_RESP' && self._isPendingVisitorCodeId(msg)) {
        self._handleVisitorResponse(peerId, msg).catch((err) => {
          console.error('[MeshEngine] Site visitor response failed:', err.message)
        })
        return true // consumed — never reaches the device-trust grant path
      }
      return false
    })
  }

  _isPendingVisitorCodeId(msg) {
    return this.pendingVisitors.has(msg && msg.codeId)
  }

  // Relay path: a visitor's SITE_VERIFY_RESP arrived via the Cloudflare relay
  // (no direct signaling peerId). The visitor includes its noise publicKey in
  // the response; verify the MAC by codeId and allowlist that key.
  async handleRelayVerifyResponse(fromPeerId, msg) {
    if (!msg || msg.type !== 'SITE_VERIFY_RESP') return
    // The relay `fromPeerId` is unreliable — the visitor's real key is in the
    // response body (added by handleSiteVerifyChallenge's relay send).
    const peerKey = (typeof msg.publicKey === 'string' && msg.publicKey.length === 64)
      ? msg.publicKey
      : (typeof fromPeerId === 'string' && fromPeerId.length === 64 ? fromPeerId : null)
    if (!peerKey) {
      console.warn('[MeshEngine] SITE_VERIFY_RESP via relay without a peer key; ignored')
      return
    }
    await this._handleVisitorResponse(peerKey, msg)
  }

  // ─── Site store passthroughs ──────────────────────────────────────────────

  listSites() {
    return this.store ? this.store.listSites() : Promise.resolve([])
  }

  getSite(siteId) {
    return this.store ? this.store.getSite(siteId) : Promise.resolve(null)
  }

  async createSite({ name, folderPath, writeMode, spa } = {}) {
    if (!this.store) throw new Error('SiteManager not initialized')
    return this.store.createSite({ name, folderPath, writeMode, spa })
  }

  async updateSite(siteId, patch) {
    if (!this.store) throw new Error('SiteManager not initialized')
    return this.store.updateSite(siteId, patch)
  }

  async removeSite(siteId) {
    if (!this.store) throw new Error('SiteManager not initialized')
    await this.store.removeSite(siteId)
  }

  // ─── Visitor allowlist (the v1 product promise) ───────────────────────────

  // The sharing host's own identity, attached to SITE_INVITE so the receiving
  // device can show "Shared by <device>" on the folder card.
  _hostIdentity() {
    const id = this.engine && this.engine.deviceIdentity
    return {
      hostName: (id && id.name) || '',
      hostDeviceId: (id && id.id) || ''
    }
  }

  _invitePayload(siteId) {
    return new Promise((resolve) => {
      this.store.getSite(siteId).then((site) => {
        resolve({
          type: 'SITE_INVITE',
          siteId,
          code: site?.code || '',
          name: site?.name || 'Shared Folder',
          expiresAt: site?.expiresAt || 0,
          ...this._hostIdentity()
        })
      }).catch(() => {
        resolve({ type: 'SITE_INVITE', siteId, code: '', name: 'Shared Folder', expiresAt: 0, ...this._hostIdentity() })
      })
    })
  }

  async updateAllowlistRole(siteId, publicKeyHex, role) {
    if (!this.store) throw new Error('SiteManager not initialized')
    return this.store.addToAllowlist(siteId, publicKeyHex, role)
  }

  // Host pastes a visitor's MD- code. Returns a promise that resolves with the
  // visitor's verified Noise public key once the visitor answers the MAC
  // challenge, or rejects on timeout / wrong device.
  async addSiteVisitor(siteId, visitorCode, { timeoutMs = 30000, role = 'viewer' } = {}) {
    if (!this.store) throw new Error('SiteManager not initialized')
    const site = await this.store.getSite(siteId)
    if (!site) throw new Error('Unknown site: ' + siteId)

    const maybeKey = String(visitorCode || '').trim()
    if (/^[0-9a-fA-F]{64}$/.test(maybeKey)) {
      const lower = maybeKey.toLowerCase()
      await this.store.addToAllowlist(siteId, lower, role === 'editor' ? 'editor' : 'viewer')
      if (this.engine.siteServer && this.engine.siteServer._sites) {
        const live = this.engine.siteServer._sites.get(siteId)
        if (live) { if (!live.allowlist) live.allowlist = []; const idx = live.allowlist.findIndex((e) => (typeof e === 'string' ? e === lower : e.key === lower)); if (idx < 0) live.allowlist.push({ key: lower, role: role === 'editor' ? 'editor' : 'viewer' }) }
      }
      try {
        const peer = this.engine.peers.get(lower)
        if (peer && peer.signaling) {
          const inv = await this._invitePayload(siteId)
          peer.signaling.send(inv)
        }
        // Persist on receiver via signaling SITE_INVITE (handled below + on receiver init)
      } catch {}
      // Receiver will persist visitedSites when it receives SITE_INVITE (see connections/signaling)
      this.engine.emit('site:visitor:added', { siteId, peerId: lower, publicKey: lower })
      return { siteId, publicKey: lower }
    }

    const clean = normalizePairingCode(visitorCode)
    if (!clean) throw new Error('Invalid visitor code (expected MD-XXXX-XXXX-XXXX-XXXX)')

    // Only one in-flight verification per code at a time.
    const codeId = require('../crypto.js').codeId(clean)
    if (this.pendingVisitors.has(codeId)) {
      throw new Error('That visitor code is already being verified — wait for it to finish')
    }

    // Register the code on the HOST side (join the visitor's pairing topic as
    // the rendezvous + bring the relay up). registerHostVerificationCode
    // deliberately does NOT auto-answer PAIRING challenges for this code (no
    // impersonation, no accidental pairing).
    const reg = this.engine.trustManager.registerHostVerificationCode(clean)
    if (!reg) throw new Error('Failed to register visitor code')

    return new Promise((resolve, reject) => {
      const entry = {
        code: clean,
        codeId,
        siteId,
        role: role === 'editor' ? 'editor' : 'viewer',
        challenge: reg.challenge,
        resolve,
        reject
      }
      this.pendingVisitors.set(codeId, entry)
      entry.timer = setTimeout(() => {
        this.pendingVisitors.delete(codeId)
        // Leave the visitor's pairing topic once verification settles.
        try {
          this.engine.trustManager.leaveHostVerificationCode(clean)
        } catch {}
        reject(new Error('Timed out waiting for the visitor to prove they hold that code'))
      }, timeoutMs)
      if (entry.timer.unref) entry.timer.unref()

      // Challenge the visitor with a DEDICATED SITE_VERIFY_CHALLENGE over the
      // direct signaling channel AND the relay. This never collides with
      // device pairing (PAIRING_CHALLENGE/RESP) — see module header.
      const challengeMsg = {
        type: 'SITE_VERIFY_CHALLENGE',
        siteId,
        codeId,
        nonce: reg.nonce
      }
      try {
        for (const [, peerObj] of this.engine.peers.entries()) {
          if (peerObj && peerObj.signaling) {
            peerObj.signaling.send(challengeMsg)
          }
        }
      } catch (err) {
        console.warn('[MeshEngine] Failed to send site visitor challenge over DHT:', err.message)
      }
      // Relay broadcast (best-effort; the visitor answers on whichever path it
      // received the challenge on).
      try {
        if (this.engine.relayClient) {
          this.engine.relayClient.start()
          this.engine.relayClient.send(`p2p-pair-${clean}`, challengeMsg)
        }
      } catch (err) {
        console.warn('[MeshEngine] Failed to send site visitor challenge over relay:', err.message)
      }
    })
  }

  // Remove a visitor from a site allowlist by their verified Noise key.
  async removeSiteVisitor(siteId, publicKeyHex) {
    if (!this.store) throw new Error('SiteManager not initialized')
    const site = await this.store.removeFromAllowlist(siteId, publicKeyHex)
    return site || null
  }

  // ─── Response handling (the MAC check that lands the key on the list) ─────

  async _handleVisitorResponse(peerId, msg) {
    if (!msg || typeof msg.nonce !== 'string' || typeof msg.mac !== 'string') return
    const codeId = msg.codeId
    const pending = this.pendingVisitors.get(codeId)
    if (!pending) return
    if (msg.nonce !== pending.challenge.nonce) return // wrong challenge epoch

    // Verify the MAC against the code the HOST was given. This is the ONLY
    // check that matters: if the visitor answers with mac(code, nonce), it
    // holds the code.
    const expected = b4a.toString(mac(pending.code, b4a.from(msg.nonce, 'hex')), 'hex')
    const ok =
      expected.length === msg.mac.length &&
      require('crypto').timingSafeEqual(b4a.from(expected, 'hex'), b4a.from(msg.mac, 'hex'))

    // Clean up the pending entry regardless of outcome.
    this.pendingVisitors.delete(codeId)
    if (pending.timer) clearTimeout(pending.timer)
    try {
      this.engine.trustManager.leaveHostVerificationCode(pending.code)
    } catch {}

    if (!ok) {
      console.warn(`[MeshEngine] Site visitor MAC verification FAILED for ${peerId.slice(0, 12)}...`)
      this.engine.emit('site:visitor:failed', {
        siteId: pending.siteId,
        peerId,
        reason: 'mac_mismatch'
      })
      pending.reject(new Error('The visitor could not prove they hold that code (wrong code or wrong device)'))
      return
    }

    // MAC verified: the responder holds the code. Its Noise public key (from
    // the transport) is what we allowlist — this is the same key the transport
    // authenticated the connection with.
    if (typeof peerId !== 'string' || peerId.length !== 64) {
      console.warn('[MeshEngine] Site visitor verified but peer key is missing; not allowlisting')
      this.engine.emit('site:visitor:failed', {
        siteId: pending.siteId,
        peerId,
        reason: 'no_peer_key'
      })
      pending.reject(new Error('Visitor verified but could not be identified'))
      return
    }

    await this.store.addToAllowlist(pending.siteId, peerId, pending.role || 'viewer')
    if (this.engine.siteServer && this.engine.siteServer._sites) {
      const live = this.engine.siteServer._sites.get(pending.siteId)
      if (live) {
        if (!live.allowlist) live.allowlist = []
        const idx = live.allowlist.findIndex((e) => (typeof e === 'string' ? e === peerId : e.key === peerId))
        if (idx < 0) live.allowlist.push({ key: peerId, role: pending.role || 'viewer' })
        else if (typeof live.allowlist[idx] === 'object') live.allowlist[idx].role = pending.role || 'viewer'
      }
    }
    // The visitor is verified but NOT paired: clear the pairing watchdog so the
    // connection survives while they visit (same contract as claims/watch).
    const peerObj = this.engine.peers && this.engine.peers.get(peerId)
    if (peerObj && peerObj.pairing && peerObj.pairing.timeout) {
      clearTimeout(peerObj.pairing.timeout)
      peerObj.pairing.timeout = null
    }
    console.log(
      `[MeshEngine] Site visitor VERIFIED + allowlisted for ${pending.siteId} (${peerId.slice(0, 12)}...)`
    )
    // Notify the visitor directly so they can auto-add the folder (push)
    try {
      const peer = this.engine.peers.get(peerId)
      if (peer && peer.signaling) {
        const inv = await this._invitePayload(pending.siteId)
        peer.signaling.send(inv)
      }
      // Also via relay so a not-yet-connected visitor gets it
      if (this.engine.relayClient) {
        const inv = await this._invitePayload(pending.siteId)
        this.engine.relayClient.send(`p2p-peer-${peerId}`, inv)
      }
    } catch {}
    if (this.engine.notificationStore) this.engine.notificationStore.addNotification('Shared Folder', `Someone shared "${(await this.store.getSite(pending.siteId))?.name || 'a folder'}" with you`, 'info')
    // Persist on the visitor side will happen when they receive SITE_INVITE (handled in connections)
    this.engine.emit('site:visitor:added', {
      siteId: pending.siteId,
      peerId,
      publicKey: peerId
    })
    pending.resolve({ siteId: pending.siteId, publicKey: peerId })
  }

  // ─── Cleanup ──────────────────────────────────────────────────────────────

  async stop() {
    for (const [, pending] of this.pendingVisitors.entries()) {
      if (pending.timer) clearTimeout(pending.timer)
      try {
        this.engine.trustManager.leaveHostVerificationCode(pending.code)
      } catch {}
      pending.reject(new Error('Engine stopped'))
    }
    this.pendingVisitors.clear()
  }
}

module.exports = { SiteManager }
