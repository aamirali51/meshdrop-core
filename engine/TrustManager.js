'use strict'

// TrustManager owns the pairing-code lifecycle and the challenge-response
// handshake. Trust is granted ONLY after a peer proves knowledge of the code
// (keyed MAC over our random nonce). Nothing here fabricates trust.
//
// Deleting a device revokes its key until it completes a FRESH pairing: the
// host code is rotated so its memorized secret dies, and the key is refused
// from the auto-trust paths (stale records, LAN auto-trust). Pairing with the
// current code again re-admits it (unrevokeKey in handleResponse).

const b4a = require('b4a')
const { randomBytes, generatePairingCode, normalizePairingCode, mac, codeId } = require('../crypto.js')
const { MESSAGES } = require('../protocol.js')

const PAIRING_TTL = 15 * 60 * 1000 // pairing code lifetime (15 minutes)
// Watchdog for the challenge-response phase. It is armed ONLY when a
// PAIRING_CHALLENGE is actually sent or received (_armPairingTimeout), never
// on raw connection open — so a user typing a pairing code is not racing a
// timer that started when the connection formed. It is cleared on successful
// verification (handleResponse) so a verified connection is never killed by
// a leftover timer while the HANDSHAKE is still in flight.
const PAIRING_TIMEOUT = 30 * 1000 // max time between challenge activity and verification

class TrustManager {
  constructor({
    getBee,
    computeTopicHash,
    swarm,
    topicRegistry,
    relayClient,
    getPeers,
    sendHandshake,
    emit,
    isRefreshing,
    onTrustGranted,
    getDeviceIdentity,
    getPeerId,
    isSiteSessionPeer
  }) {
    this.getBee = getBee
    this.computeTopicHash = computeTopicHash
    this.swarm = swarm
    this.topicRegistry = topicRegistry
    this.relayClient = relayClient || null
    this.getPeers = getPeers // () => Map<peerId, peerObj>
    this.sendHandshake = sendHandshake // (peerId) => void
    this.emit = emit || (() => {}) // (event, data) => void — engine EventEmitter
    this.isRefreshing = isRefreshing || (() => false) // () => bool — swarm rebuild in flight
    this.onTrustGranted = onTrustGranted || (() => {}) // (peerId, code) => void
    this.getDeviceIdentity = getDeviceIdentity || (() => ({}))
    this.getPeerId = getPeerId || (() => '')
    this.engine = null // wired to the owning MeshEngine after construction
    this.isSiteSessionPeer = isSiteSessionPeer || (() => false) // () => bool
    this.pairingSecrets = new Map() // codeId -> { code, role, createdAt, expiresAt, codeId }
    this.trustedPeerKeys = new Set() // hex noise public keys currently trusted
    this.revokedKeys = new Map() // hex noise public key -> revokedAt ms; refused until a fresh pairing
    this.pairingCodePromise = null // single-flight guard for concurrent code fetches
    this.relayOutstanding = [] // { nonce, code, codeId, sentAt }
    // Failed-MAC lockout: per-key consecutive bad-MAC counter. Repeated wrong
    // answers (brute-force attempt, stale code) push the peer into an
    // exponential backoff instead of letting it reconnect and retry forever.
    // The counter resets on a successful verification (handleResponse).
    this.failedPairings = new Map() // hex noise public key -> { count, lockedUntil }
    // Unanswered-pairing backoff: peers that repeatedly let OUR challenge time
    // out (the recurring 'challenge never verified' connect/destroy loop) stop
    // receiving automatic challenges — nobody is pairing, so retrying on every
    // reconnect only burns battery and spams logs. Suppression clears on any
    // user intent (a code entered) or incoming challenge we can actually
    // answer, so explicit pairing is never blocked.
    this.unansweredPairings = new Map() // hex noise public key -> { count, suppressUntil }

    if (this.relayClient) {
      this.relayClient.onMessage = (topic, msg, fromPeerId) => {
        this.handleRelayMessage(topic, msg, fromPeerId)
      }
    }
  }

  // Hydrate the revoked-key set (deleted devices awaiting re-pairing). Must
  // run before loadTrustedPeerKeys so a stale device record can never re-admit
  // a key that was revoked by deletion.
  async loadRevokedKeys() {
    try {
      const bee = await this.getBee('revokedPeers')
      for await (const node of bee.createReadStream()) {
        const v = node.value
        if (v && typeof v.publicKey === 'string' && v.publicKey.length === 64) {
          // Persisted revokedAt is an ISO string; normalize to epoch ms so the
          // freshness comparison in handleResponse stays numeric.
          const ts = typeof v.revokedAt === 'number' ? v.revokedAt : Date.parse(v.revokedAt || '')
          this.revokedKeys.set(v.publicKey, Number.isFinite(ts) ? ts : Date.now())
        }
      }
      if (this.revokedKeys.size > 0) {
        console.log(
          `[MeshEngine] Loaded ${this.revokedKeys.size} revoked peer key(s): ${Array.from(this.revokedKeys)
            .map((k) => k[0].slice(0, 12))
            .join(', ')}`
        )
      }
    } catch (err) {
      console.warn('[MeshEngine] loadRevokedKeys failed:', err.message)
    }
  }

  async loadTrustedPeerKeys() {
    try {
      const bee = await this.getBee('devices')
      for await (const node of bee.createReadStream()) {
        const dev = node.value
        if (
          dev &&
          dev.isTrusted === true &&
          dev.trustedAt &&
          dev.publicKey &&
          dev.publicKey.length === 64 &&
          !this.revokedKeys.has(dev.publicKey) // a revoked key is never trusted
        ) {
          this.trustedPeerKeys.add(dev.publicKey)
        }
      }
      console.log(`[MeshEngine] Loaded ${this.trustedPeerKeys.size} trusted peer key(s)`)
      if (this.trustedPeerKeys.size > 0) {
        console.log(
          `[MeshEngine] Trusted keys: ${Array.from(this.trustedPeerKeys)
            .map((k) => k.slice(0, 12))
            .join(', ')}`
        )
      }
    } catch (err) {
      console.warn('[MeshEngine] loadTrustedPeerKeys failed:', err.message)
    }
  }

  isTrustedPublicKey(pubKeyHex) {
    return (
      typeof pubKeyHex === 'string' &&
      pubKeyHex.length === 64 &&
      !this.revokedKeys.has(pubKeyHex) &&
      this.trustedPeerKeys.has(pubKeyHex)
    )
  }

  isRevoked(pubKeyHex) {
    return typeof pubKeyHex === 'string' && this.revokedKeys.has(pubKeyHex)
  }

  addTrustedKey(pubKeyHex) {
    if (typeof pubKeyHex !== 'string' || pubKeyHex.length !== 64) return
    // A revoked key is not trusted until it is explicitly re-paired
    // (handleResponse un-revokes it on a successful fresh challenge).
    if (this.revokedKeys.has(pubKeyHex)) return
    this.trustedPeerKeys.add(pubKeyHex)
  }

  removeTrustedKey(pubKeyHex) {
    this.trustedPeerKeys.delete(pubKeyHex)
  }

  // Refuse a peer key until it completes a fresh pairing. Called when a device
  // is deleted: deletion breaks the CURRENT trust, so the key cannot sneak back
  // via a stale record, LAN auto-trust, or the memorized old pairing code — but
  // pairing with a code registered AFTER this moment re-admits it
  // (handleResponse -> unrevokeKey).
  async revokeKey(pubKeyHex) {
    if (typeof pubKeyHex !== 'string' || pubKeyHex.length !== 64) return
    const revokedAt = Date.now()
    this.revokedKeys.set(pubKeyHex, revokedAt)
    this.trustedPeerKeys.delete(pubKeyHex)
    try {
      const bee = await this.getBee('revokedPeers')
      await bee.put(pubKeyHex, {
        publicKey: pubKeyHex,
        revokedAt: new Date(revokedAt).toISOString()
      })
      console.log(
        `[MeshEngine] Revoked peer key ${pubKeyHex.slice(0, 12)}... (deleted device; re-pairing required)`
      )
    } catch (err) {
      console.warn('[MeshEngine] Failed to persist revoked key:', err.message)
    }
  }

  // Re-admit a key after it completes a fresh explicit pairing. The in-memory
  // removal is synchronous because handleResponse grants trust immediately
  // after this; persistence is best-effort fire-and-forget.
  unrevokeKey(pubKeyHex) {
    if (typeof pubKeyHex !== 'string') return
    this.revokedKeys.delete(pubKeyHex)
    this.getBee('revokedPeers')
      .then((bee) => bee.del(pubKeyHex))
      .catch((err) => console.warn('[MeshEngine] Failed to clear revoked key:', err.message))
  }

  // Delete rotates the pairing secret(s) so the deleted peer's memorized code
  // becomes useless: a fresh host code is generated and every joiner code is
  // dropped (the deleted peer could answer challenges for the codes it took
  // part in). This is what makes deletion permanent WITHOUT a blacklist — the
  // deleted peer can only come back by pairing with the CURRENT code again.
  async rotateHostPairingCode() {
    for (const [cid, secret] of this.pairingSecrets.entries()) {
      this.pairingSecrets.delete(cid)
      if (this.topicRegistry) this.topicRegistry.leave(`p2p-pair-${secret.code}`)
    }
    try {
      const bee = await this.getBee('pairingCodes')
      await bee.del('active')
    } catch (err) {
      console.warn('[MeshEngine] Failed to clear persisted pairing code:', err.message)
    }
    // Force a fresh code: getOrCreatePairingCode reuses the active secret and
    // single-flights through pairingCodePromise, so both must be reset.
    this.pairingCodePromise = null
    const fresh = await this.getOrCreatePairingCode()
    console.log(`[MeshEngine] Rotated pairing code after device deletion: ${fresh}`)
    return fresh
  }

  // Reuse active in-memory host code (permanent per device)
  getActiveHostCode() {
    const now = Date.now()
    for (const [, secret] of this.pairingSecrets.entries()) {
      if (secret.role === 'host' && (secret.expiresAt === 0 || now < secret.expiresAt)) {
        return secret.code
      }
    }
    return null
  }

  async getOrCreatePairingCode() {
    const active = this.getActiveHostCode()
    if (active) return active
    // Single-flight: concurrent callers must all see the SAME code. Without
    // this each call generated its own code and every extra secret was
    // orphaned.
    if (this.pairingCodePromise) return this.pairingCodePromise
    this.pairingCodePromise = this._generatePairingCode().finally(() => {
      this.pairingCodePromise = null
    })
    return this.pairingCodePromise
  }

  async _generatePairingCode() {
    const now = Date.now()

    // Reuse a persisted host code across restarts. The code lives in its OWN
    // bee ('pairingCodes'). Host pairing codes are permanent per device (expiresAt = 0).
    try {
      const bee = await this.getBee('pairingCodes')
      const entry = await bee.get('active')
      if (entry && entry.value && entry.value.code) {
        const p = entry.value
        const cid = codeId(p.code)
        this.pairingSecrets.set(cid, {
          code: p.code,
          role: 'host',
          createdAt: p.createdAt,
          expiresAt: 0,
          codeId: cid
        })
        this._joinPairingTopic(p.code)
        console.log(`[MeshEngine] Restored permanent pairing code: ${p.code}`)
        return p.code
      }
    } catch (err) {
      console.warn(
        '[MeshEngine] Persisted pairing code unavailable; generating a fresh code:',
        err.message
      )
    }

    // Generate a fresh random 80-bit code
    const code = generatePairingCode()
    const secret = {
      code,
      role: 'host',
      createdAt: now,
      expiresAt: 0, // Permanent host code until user explicitly rotates
      codeId: codeId(code)
    }
    this.pairingSecrets.set(secret.codeId, secret)
    try {
      const bee = await this.getBee('pairingCodes')
      await bee.put('active', {
        code,
        codeId: secret.codeId,
        createdAt: secret.createdAt,
        expiresAt: 0
      })
    } catch (err) {
      console.warn('[MeshEngine] Failed to persist pairing code:', err.message)
    }
    try {
      this._joinPairingTopic(code)
      this.sendChallengesToAll()
    } catch (err) {
      // Never let topic join/sync break code delivery: callers must always
      // receive a code.
      console.warn('[MeshEngine] Pairing topic join failed:', err.message)
    }
    console.log(`[MeshEngine] Permanent pairing code generated: ${code}`)
    return code
  }

  // Drop a joiner (ephemeral) pairing secret and leave its DHT topic. Called
  // when pairWithCode settles (success or failure) so a failed/abandoned
  // pairing never keeps the code registered for its full TTL or the topic
  // announced. Never touches host secrets (role: 'host').
  dropJoinerCode(code) {
    if (!code) return
    const cid = codeId(code)
    const secret = this.pairingSecrets.get(cid)
    if (!secret || secret.role !== 'joiner') return
    this.pairingSecrets.delete(cid)
    this.relayOutstanding = this.relayOutstanding.filter((o) => o.codeId !== cid)
    try {
      if (this.relayClient) this.relayClient.leave(`p2p-pair-${code}`)
      if (this.topicRegistry) this.topicRegistry.leave(`p2p-pair-${code}`)
    } catch {}
  }

  // Register a code the user is pairing with (joiner side) and join its topic.
  // Returns the canonical code, or null if the format is invalid.
  registerJoinerCode(rawCode) {
    const cleanCode = normalizePairingCode(rawCode)
    if (!cleanCode) return null
    const now = Date.now()
    const cid = codeId(cleanCode)
    this.pairingSecrets.set(cid, {
      code: cleanCode,
      role: 'joiner',
      createdAt: now,
      expiresAt: now + PAIRING_TTL,
      codeId: cid
    })
    // Snapshot peers that were already trusted BEFORE this registration:
    // sendChallengesToAll may complete a fresh pairing for an untrusted peer (which
    // emits its own events), so only pre-existing trusted peers need probing.
    const trustedIds = new Set()
    for (const [pId, peerObj] of this.getPeers().entries()) {
      if (peerObj.pairing && peerObj.pairing.trusted && !this.isRevoked(pId)) trustedIds.add(pId)
    }
    // The user just entered a code: explicit pairing intent. Give every peer a
    // fresh chance regardless of any unanswered-pairing backoff.
    this.unansweredPairings.clear()
    this._joinPairingTopic(cleanCode)
    this.sendChallengesToAll({ force: true })
    // Untrusted peers were just challenged by sendChallengesToAll. Trusted peers are
    // skipped there, but the code's host may already be connected and trusted
    // (LAN auto-trust, or a pairing completed before the code was entered) —
    // probe them so the host can identify itself without a fresh handshake.
    this._probeTrustedPeers(cleanCode, cid, trustedIds)

    // Broadcast challenge via Cloudflare WSS Relay immediately for Port 443 fallback.
    // Joiner intent: the user just entered a code — bring the relay up right
    // away so the challenge reaches the host even in lazy-'auto' mode.
    const relayNonce = randomBytes(16)
    this.relayOutstanding.push({ nonce: relayNonce, code: cleanCode, codeId: cid, sentAt: now })
    if (this.relayClient) {
      this.relayClient.start()
      // NOTE: no device identity here on purpose — the relay is metadata
      // untrusted. The host answers with its identity only after proving it
      // holds the code, and the code topic itself is the capability.
      this.relayClient.send(`p2p-pair-${cleanCode}`, {
        type: MESSAGES.PAIRING_CHALLENGE,
        codeId: cid,
        nonce: b4a.toString(relayNonce, 'hex')
      })
      // Send again shortly in case the remote host's WebSocket connection just opened
      setTimeout(() => {
        if (this.pairingSecrets.has(cid) && this.relayClient) {
          this.relayClient.send(`p2p-pair-${cleanCode}`, {
            type: MESSAGES.PAIRING_CHALLENGE,
            codeId: cid,
            nonce: b4a.toString(relayNonce, 'hex')
          })
        }
      }, 600)
    }

    return cleanCode
  }

  // MeshDrop Sites — register a VISITOR's pairing code on the HOST side so the
  // host can verify that the visitor holds it (the allowlist flow). The host
  // joins the visitor's own pairing topic (which every device announces
  // permanently) and challenges; the visitor answers with mac(code, nonce); a
  // match proves code ownership and the visitor's key is allowlisted — WITHOUT
  // granting device trust.
  //
  // CRITICAL SCOPE BOUNDARY: this must NOT auto-answer challenges for the code
  // (unlike registerJoinerCode's reciprocal handleChallenge path). If the host
  // answered a challenge on the visitor's topic with this code, it would
  // impersonate the visitor and could complete a pairing that grants the host
  // trust on the visitor's side. Site verification is one-way: the HOST
  // challenges, the VISITOR answers.
  //
  // Returns { code, codeId, topic, nonceHex, macHex? } for the caller to send,
  // or null if the format is invalid.
  registerHostVerificationCode(rawCode) {
    const cleanCode = normalizePairingCode(rawCode)
    if (!cleanCode) return null
    const cid = codeId(cleanCode)
    const topic = `p2p-pair-${cleanCode}`
    this._joinPairingTopic(cleanCode)
    // Host-side verification intent: bring the relay up right away so the
    // challenge reaches the visitor even in lazy-'auto' mode (same as pairing
    // intent does for a joiner).
    if (this.relayClient) this.relayClient.start()
    const nonce = randomBytes(16)
    const challenge = { codeId: cid, nonce: b4a.toString(nonce, 'hex') }
    return { code: cleanCode, codeId: cid, topic, challenge, nonce: challenge.nonce }
  }

  // Leave a visitor's pairing topic after a host-side site verification settles
  // (success, failure, or timeout). Mirrors dropJoinerCode: never touches host
  // secrets (role: 'host'). Harmless no-op if the topic was never joined.
  leaveHostVerificationCode(code) {
    if (!code) return
    try {
      if (this.relayClient) this.relayClient.leave(`p2p-pair-${code}`)
      if (this.topicRegistry) this.topicRegistry.leave(`p2p-pair-${code}`)
    } catch {}
  }

  // VISITOR side of a site allowlist challenge (SITE_VERIFY_CHALLENGE from a
  // host that was given one of OUR MD- codes). We answer with mac(code, nonce)
  // over the same channel the challenge arrived on. This proves we hold the
  // code WITHOUT any device pairing/trust — it is a pure capability proof for
  // the host's site allowlist. The host's PAIRING challenge/response machinery
  // is never involved, so this cannot grant the host trust on our side.
  // When `viaRelay` is true the response also carries our noise publicKey
  // (the relay has no authenticated peer identity, so the host needs it to
  // know which key to allowlist).
  handleSiteVerifyChallenge(sendFn, msg, viaRelay) {
    if (!msg || typeof msg.nonce !== 'string' || typeof msg.codeId !== 'string') return
    const secret = this.pairingSecrets.get(msg.codeId)
    // Only answer for a code we actually hold (our permanent host code, or a
    // joiner code we entered). Never answer for a code we don't have.
    if (!secret || (secret.expiresAt > 0 && Date.now() >= secret.expiresAt)) return
    const nonceBuf = b4a.from(msg.nonce, 'hex')
    if (nonceBuf.length !== 16) return
    const sig = mac(secret.code, nonceBuf)
    const resp = {
      type: 'SITE_VERIFY_RESP',
      siteId: msg.siteId,
      codeId: msg.codeId,
      nonce: msg.nonce,
      mac: b4a.toString(sig, 'hex')
    }
    if (viaRelay && this.getPeerId) {
      const myKey = this.getPeerId()
      if (myKey) resp.publicKey = myKey
    }
    try {
      sendFn(resp)
    } catch (err) {
      console.warn('[MeshEngine] Failed to send SITE_VERIFY_RESP:', err.message)
    }
  }

  // MeshDrop Sites — verify a visitor's PAIRING_RESP against a code the host
  // registered via registerHostVerificationCode. Confirms the MAC ONLY; never
  // grants device trust (does not touch trustedPeerKeys / onTrustGranted —
  // KB invariant 3). On success the caller may add the visitor's Noise public
  // key (from the transport) to the site allowlist. Returns the matched
  // { code } or null.
  verifyHostVerificationResponse(visitorCode, challenge, answerMac) {
    if (!challenge || typeof answerMac !== 'string' || typeof visitorCode !== 'string') return null
    const expected = b4a.toString(mac(visitorCode, b4a.from(String(challenge.nonce), 'hex')), 'hex')
    // Constant-time compare — the MAC is a capability proof.
    if (expected.length !== answerMac.length) return null
    const a = b4a.from(expected, 'hex')
    const b = b4a.from(answerMac, 'hex')
    if (!a.equals(b)) return null
    return { code: visitorCode }
  }

  /**
   * Broadcast pairing challenges to all connected peers that require verification,
   * and answer any pending challenges that arrived before the secret was registered.
   */
  sendChallengesToAll({ force = false } = {}) {
    for (const [peerId, peerObj] of this.getPeers().entries()) {
      if (!peerObj || !peerObj.signaling || !peerObj.pairing) continue
      // Flush any pending challenges that arrived before we registered a matching code
      if (peerObj.pairing.pendingChallenges && peerObj.pairing.pendingChallenges.length > 0) {
        const pending = peerObj.pairing.pendingChallenges
        peerObj.pairing.pendingChallenges = []
        for (const ch of pending) {
          this.handleChallenge(peerId, ch)
        }
      }
      if (!peerObj.pairing.trusted || this.isRevoked(peerId)) {
        this.sendChallenges(peerId, { force })
      }
    }
  }

  // Ask already-connected trusted peers whether they hold the freshly
  // registered code. Only the real host can answer (keyed MAC over the nonce),
  // which lets the caller complete the pairing instantly instead of waiting
  // for a handshake that will never come on an already-established connection.
  _probeTrustedPeers(code, cid, trustedIds) {
    for (const pId of trustedIds) {
      const peerObj = this.getPeers().get(pId)
      if (!peerObj || !peerObj.signaling || !peerObj.pairing || !peerObj.pairing.trusted) continue
      if (this.isRevoked(pId)) continue // revoked peers are never probed
      if (peerObj.pairing.outstanding.some((o) => o.codeId === cid)) continue
      const nonce = randomBytes(16)
      peerObj.pairing.outstanding.push({ nonce, code, codeId: cid })
      try {
        peerObj.signaling.send({
          type: MESSAGES.PAIRING_CHALLENGE,
          codeId: cid,
          nonce: b4a.toString(nonce, 'hex')
        })
      } catch (err) {
        console.error('[MeshEngine] Failed to probe trusted peer:', err.message)
      }
    }
  }

  _joinPairingTopic(code) {
    const label = `p2p-pair-${code}`
    if (this.relayClient) this.relayClient.join(label)
    if (this.topicRegistry) this.topicRegistry.ensure(label, { client: true, server: true })
    else {
      const topicHash = this.computeTopicHash(label)
      this.swarm.join(topicHash, { client: true, server: true })
      this.swarm.flush().catch(() => {})
    }
  }

  // Handle messages received over the Cloudflare Port 443 WSS Relay
  handleRelayMessage(topic, msg, fromPeerId) {
    if (!msg || typeof msg !== 'object') return

    // MeshDrop Sites allowlist verification over the relay:
    //  - A host's SITE_VERIFY_CHALLENGE reaches the visitor here; answer it.
    //  - A visitor's SITE_VERIFY_RESP reaches the host here; hand it to the
    //    SiteManager hook (the same MAC verification as the direct path).
    if (msg.type === 'SITE_VERIFY_CHALLENGE') {
      if (this.relayClient) {
        this.handleSiteVerifyChallenge((resp) => this.relayClient.send(topic, resp), msg, true)
      }
      return
    }
    if (msg.type === 'SITE_VERIFY_RESP') {
      // The host's SiteManager verifies + allowlists (same MAC check as the
      // direct path). The engine wires this.siteManager.
      if (this.siteManager && typeof this.siteManager.handleRelayVerifyResponse === 'function') {
        this.siteManager.handleRelayVerifyResponse(fromPeerId, msg).catch((err) => {
          console.warn('[MeshEngine] Relay SITE_VERIFY_RESP handling failed:', err.message)
        })
      }
      return
    }

    // 1. Inbound Pairing Challenge from Joiner via WSS relay
    if (msg.type === MESSAGES.PAIRING_CHALLENGE) {
      if (typeof msg.codeId !== 'string' || typeof msg.nonce !== 'string') return
      const secret = this.pairingSecrets.get(msg.codeId)
      if (!secret || (secret.expiresAt > 0 && Date.now() >= secret.expiresAt)) return

      const nonceBuf = b4a.from(msg.nonce, 'hex')
      if (nonceBuf.length !== 16) return
      const signature = mac(secret.code, nonceBuf)
      const myIdentity = this.getDeviceIdentity ? this.getDeviceIdentity() : {}

      console.log(`[MeshEngine] Answering PAIRING_CHALLENGE via Cloudflare WSS Relay for topic: ${topic}`)
      if (this.relayClient) {
        this.relayClient.send(topic, {
          type: MESSAGES.PAIRING_RESP,
          nonce: msg.nonce,
          mac: b4a.toString(signature, 'hex'),
          identity: {
            ...myIdentity,
            publicKey: this.getPeerId ? this.getPeerId() : ''
          }
        })

        // Reciprocate over the relay so trust completes on BOTH sides: our
        // answer alone only satisfies the challenger. The peer here proved
        // knowledge of the code topic — clear any unanswered-pairing
        // suppression (this is pairing activity we can complete) and challenge
        // them back; only a peer that genuinely holds the code can answer.
        // Skipped once the peer is already trusted (no ping-pong).
        const peerPubKey =
          typeof fromPeerId === 'string' && fromPeerId.length === 64 ? fromPeerId : ''
        this.unansweredPairings.delete(peerPubKey)
        if (peerPubKey && !this.isTrustedPublicKey(peerPubKey)) {
          const myNonce = randomBytes(16)
          this.relayOutstanding.push({
            nonce: myNonce,
            code: secret.code,
            codeId: secret.codeId,
            sentAt: Date.now()
          })
          this.relayClient.send(topic, {
            type: MESSAGES.PAIRING_CHALLENGE,
            codeId: secret.codeId,
            nonce: b4a.toString(myNonce, 'hex')
          })
        }
      }
      return
    }

    // 2. Inbound Pairing Response from Host via WSS relay
    if (msg.type === MESSAGES.PAIRING_RESP) {
      if (typeof msg.nonce !== 'string' || typeof msg.mac !== 'string') return
      const idx = this.relayOutstanding.findIndex((o) => b4a.toString(o.nonce, 'hex') === msg.nonce)
      if (idx === -1) return
      const outstanding = this.relayOutstanding[idx]
      const expected = b4a.toString(mac(outstanding.code, outstanding.nonce), 'hex')
      if (msg.mac !== expected) {
        console.warn('[MeshEngine] Relay pairing challenge FAILED: MAC mismatch')
        this.emit('pairing:failed', { peerId: fromPeerId || 'relay', reason: 'mac_mismatch', codeId: outstanding.codeId })
        return
      }

      this.relayOutstanding.splice(idx, 1)
      console.log(`[MeshEngine] Pairing challenge VERIFIED via Cloudflare WSS Relay (${outstanding.code})`)

      const peerIdentity = msg.identity || {}
      const targetPublicKey = peerIdentity.publicKey || fromPeerId || `relay-${Date.now()}`
      const deviceId = peerIdentity.id || (targetPublicKey ? targetPublicKey.slice(0, 16) : 'remote-device')
      const nowIso = new Date().toISOString()

      // A relay pairing is the mobile app's fallback path when direct DHT
      // connectivity fails, so it is exercised on first pairing and on every
      // reconnect after a phone app update. Presence: the peer has no socket
      // and no close event — isOnline is derived from relay-liveness freshness,
      // so the persisted row must never carry a stale "true" (which would keep
      // the device green in the desktop list forever).
      const remoteDevice = {
        id: deviceId,
        publicKey: targetPublicKey,
        name: peerIdentity.name || 'Remote Peer',
        os: peerIdentity.os || 'Unknown',
        osVersion: peerIdentity.osVersion || '',
        avatar: peerIdentity.avatar || '',
        isTrusted: true,
        isEncrypted: true,
        isOnline: false,
        lastSeen: nowIso,
        relayLastSeen: nowIso,
        transferMethod: 'relay',
        relayed: true,
        pairedVia: (this.getDeviceIdentity ? this.getDeviceIdentity().id : null) || null,
        trustedAt: nowIso
      }

      if (targetPublicKey && targetPublicKey.length === 64) {
        this.addTrustedKey(targetPublicKey)
        this.unrevokeKey(targetPublicKey)
      }

      // Persist in device bee
      this.getBee('devices')
        .then(async (bee) => {
          // Preserve a user's custom rename across relay reconnects (the
          // "renamed phone resets to 'MeshDrop Mobile'" bug).
          const existing = await bee.get(deviceId).catch(() => null)
          const prev = (existing && existing.value) || null
          if (prev) {
            if (prev.customName) remoteDevice.name = prev.customName
            remoteDevice.customName = prev.customName || ''
            if (!prev.customName) remoteDevice.lastReportedName = peerIdentity.name || ''
            remoteDevice.avatar = prev.avatar || remoteDevice.avatar
            remoteDevice.trustedAt = prev.trustedAt || remoteDevice.trustedAt
            remoteDevice.pairedVia = prev.pairedVia || remoteDevice.pairedVia
          } else {
            remoteDevice.customName = ''
            remoteDevice.lastReportedName = remoteDevice.name || ''
          }
          await bee.put(deviceId, remoteDevice)
        })
        .catch(() => {})

      // Send reciprocal handshake info back via relay
      if (this.relayClient) {
        const myIdentity = this.getDeviceIdentity ? this.getDeviceIdentity() : {}
        this.relayClient.send(topic, {
          type: MESSAGES.HANDSHAKE,
          identity: {
            ...myIdentity,
            publicKey: this.getPeerId ? this.getPeerId() : ''
          }
        })
      }

      this.onTrustGranted(targetPublicKey, outstanding.code)
      this.emit(EVENTS.TRUST_PAIRED, { peer: remoteDevice, code: outstanding.code })
      // The relay pair is verified and live right now — nudge the engine's
      // relay-presence clock so a just-paired device can never read as stale.
      try {
        const dev = this.getPeers().get(targetPublicKey)?.device
        if (dev && typeof dev.isOnline === 'boolean' && dev.relayed) {
          if (this.engine && typeof this.engine.touchPresence === 'function') this.engine.touchPresence(dev)
        }
      } catch {}
      this.emit(EVENTS.PEER_CONNECTED, remoteDevice)
      return
    }

    // 3. Reciprocal Handshake via WSS relay
    if (msg.type === MESSAGES.HANDSHAKE && msg.identity) {
      const peerIdentity = msg.identity
      const targetPublicKey = peerIdentity.publicKey || fromPeerId
      if (targetPublicKey && this.isTrustedPublicKey(targetPublicKey)) {
        const deviceId = peerIdentity.id || targetPublicKey.slice(0, 16)
        const nowIso = new Date().toISOString()
        // This path re-fires on every relay reconnect (e.g. after a phone app
        // update). Presence is derived from relay-liveness freshness, never
        // from the persisted row — the row must not carry stale "true".
        const remoteDevice = {
          id: deviceId,
          publicKey: targetPublicKey,
          name: peerIdentity.name || 'Remote Peer',
          os: peerIdentity.os || 'Unknown',
          osVersion: peerIdentity.osVersion || '',
          avatar: peerIdentity.avatar || '',
          isTrusted: true,
          isOnline: false,
          lastSeen: nowIso,
          relayLastSeen: nowIso,
          transferMethod: 'relay',
          relayed: true,
          trustedAt: new Date().toISOString()
        }
        // Preserve a user's custom rename across relay reconnects, and carry
        // the earlier pairing metadata (pairedVia, avatar) forward.
        this.getBee('devices')
          .then(async (bee) => {
            const existing = await bee.get(deviceId).catch(() => null)
            const prev = (existing && existing.value) || null
            if (prev) {
              if (prev.customName) remoteDevice.name = prev.customName
              remoteDevice.customName = prev.customName || ''
              if (!prev.customName) remoteDevice.lastReportedName = peerIdentity.name || ''
              remoteDevice.pairedVia = prev.pairedVia || remoteDevice.pairedVia
              remoteDevice.avatar = prev.avatar || remoteDevice.avatar
              remoteDevice.trustedAt = prev.trustedAt || remoteDevice.trustedAt
            } else {
              remoteDevice.customName = ''
              remoteDevice.lastReportedName = remoteDevice.name || ''
            }
            await bee.put(deviceId, remoteDevice)
          })
          .catch(() => {})
        // The relay peer is verified and live right now — nudge relay-presence
        // so the desktop list does not flip it to offline on the next refresh.
        try {
          if (this.engine && typeof this.engine.touchPresence === 'function') {
            this.engine.touchPresence(remoteDevice)
          }
        } catch {}
        this.emit(EVENTS.PEER_CONNECTED, remoteDevice)
      }
    }
  }

  // Record a failed MAC verification for a key and apply exponential backoff.
  // Returns true when the peer is currently locked out (must not be sent a new
  // challenge). A successful verification (handleResponse) clears the entry.
  _recordPairingFailure(peerId) {
    const entry = this.failedPairings.get(peerId) || { count: 0, lockedUntil: 0 }
    entry.count += 1
    // Backoff ladder: 1st fail → 5s, 2nd → 10s, 3rd → 20s, ... capped at 5min.
    const backoffMs = Math.min(5000 * Math.pow(2, entry.count - 1), 5 * 60 * 1000)
    entry.lockedUntil = Date.now() + backoffMs
    this.failedPairings.set(peerId, entry)
    return entry.lockedUntil > Date.now()
  }

  _isPairingLocked(peerId) {
    const entry = this.failedPairings.get(peerId)
    if (!entry) return false
    if (entry.lockedUntil <= Date.now()) {
      this.failedPairings.delete(peerId)
      return false
    }
    return true
  }

  _clearPairingFailures(peerId) {
    this.failedPairings.delete(peerId)
  }

  // Arm (or re-arm) the pairing watchdog for a peer. The timer is tied to
  // challenge activity, not connection open, and only runs while the peer is
  // still in an unverified pairing state. On fire it destroys the connection
  // so an abandoned pairing never lingers forever.
  _armPairingTimeout(peerId) {
    const peerObj = this.getPeers().get(peerId)
    if (!peerObj || !peerObj.pairing) return
    if (peerObj.pairing.trusted || peerObj.pairing.complete) return
    if (peerObj.pairing.timeout) clearTimeout(peerObj.pairing.timeout)
    // Network-transition awareness: when the swarm is being rebuilt (Wi-Fi →
    // cellular, router swap), the challenge/response legitimately takes longer
    // than PAIRING_TIMEOUT — the connection is destroyed and re-established on
    // the new interface. Extend the watchdog by the refresh window so a slow
    // relay or rebuild never kills a legitimate in-flight pairing.
    const graceMs = this.isRefreshing() ? PAIRING_TIMEOUT : 0
    peerObj.pairing.timeout = setTimeout(() => {
      peerObj.pairing.timeout = null
      const p = this.getPeers().get(peerId)
      if (!p || !p.pairing) return
      if (p.pairing.trusted || p.pairing.complete) return
      // Site-session peers (allowlist-authenticated, never paired) must not be
      // killed by the pairing watchdog — their connection is the site session.
      if (this.isSiteSessionPeer && this.isSiteSessionPeer(peerId)) return
      console.warn(
        `[MeshEngine] Pairing timed out for ${peerId.slice(0, 12)}... (challenge never verified)`
      )
      // Nobody is pairing: remember the unanswered cycle so automatic
      // challenges to this peer back off instead of looping forever.
      this._recordUnansweredPairing(peerId)
      try {
        p.connection.destroy()
      } catch {}
    }, PAIRING_TIMEOUT + graceMs)
    if (peerObj.pairing.timeout.unref) peerObj.pairing.timeout.unref()
  }

  // Record a 'challenge never verified' cycle for a peer and apply an
  // exponential suppression window to AUTOMATIC challenges. Deliberate pairing
  // (a code entered on either side) is never affected: forced challenges and
  // any incoming challenge we can answer reset the suppression.
  _recordUnansweredPairing(peerId) {
    const entry = this.unansweredPairings.get(peerId) || { count: 0, suppressUntil: 0 }
    entry.count += 1
    // First window is short (15s): a peer that registers a code mid-backoff
    // (the re-pair race) must recover within a normal pairing timeout. The
    // ladder still decays to a 30min cap, which is what breaks the loop.
    const backoffMs = Math.min(15 * 1000 * Math.pow(2, entry.count - 1), 30 * 60 * 1000)
    entry.suppressUntil = Date.now() + backoffMs
    this.unansweredPairings.set(peerId, entry)
    console.log(
      `[MeshEngine] No pairing response from ${peerId.slice(0, 12)}... after ${entry.count} attempt(s) — automatic challenges paused for ${Math.round(backoffMs / 1000)}s (enter a code to pair immediately)`
    )
  }

  _isPairingSuppressed(peerId) {
    const entry = this.unansweredPairings.get(peerId)
    if (!entry) return false
    if (entry.suppressUntil <= Date.now()) {
      this.unansweredPairings.delete(peerId)
      return false
    }
    return true
  }

  // Clear the watchdog for a peer (called on successful verification and on
  // failure paths where the connection is being torn down anyway).
  _clearPairingTimeout(peerId) {
    const peerObj = this.getPeers().get(peerId)
    if (!peerObj || !peerObj.pairing) return
    if (peerObj.pairing.timeout) {
      clearTimeout(peerObj.pairing.timeout)
      peerObj.pairing.timeout = null
    }
  }

  // Send a pairing challenge for every active pairing secret, one outstanding
  // challenge per (peer, secret) so MACs tie to a single nonce. Revoked peers
  // are challenged too: they can only answer with a CURRENT code (the old ones
  // were rotated away on deletion), which is exactly what a fresh pairing needs.
  sendChallenges(peerId, { force = false } = {}) {
    const peerObj = this.getPeers().get(peerId)
    if (!peerObj || !peerObj.signaling || !peerObj.pairing) return
    if (peerObj.pairing.trusted && !this.isRevoked(peerId)) return
    // A peer that just failed a MAC verification is in exponential backoff —
    // do not re-challenge it until the lockout expires.
    if (this._isPairingLocked(peerId)) return
    // Nobody has ever answered our challenges for this peer and no user intent
    // is in play: leave them alone instead of looping connect/challenge/destroy.
    if (!force) {
      if (this._isPairingSuppressed(peerId)) return
    } else {
      this.unansweredPairings.delete(peerId)
    }
    const sentCodeIds = new Set(peerObj.pairing.outstanding.map((o) => o.codeId))
    let sentAny = false
    for (const [, secret] of this.pairingSecrets.entries()) {
      if (sentCodeIds.has(secret.codeId)) continue
      if (secret.expiresAt > 0 && Date.now() >= secret.expiresAt) continue
      const nonce = randomBytes(16)
      peerObj.pairing.outstanding.push({ nonce, code: secret.code, codeId: secret.codeId })
      try {
        peerObj.signaling.send({
          type: MESSAGES.PAIRING_CHALLENGE,
          codeId: secret.codeId,
          nonce: b4a.toString(nonce, 'hex')
        })
        sentAny = true
      } catch (err) {
        console.error('[MeshEngine] Failed to send PAIRING_CHALLENGE:', err.message)
      }
    }
    // A challenge went out: start the watchdog from actual pairing activity.
    if (sentAny) this._armPairingTimeout(peerId)
  }

  // Respond to a peer's challenge using the secret matching its codeId.
  // Trust is NEVER granted here; we only prove knowledge of the code. We answer
  // even if the peer is already trusted, otherwise a peer whose challenge
  // arrives after we verified its response could never verify ours (deadlock).
  handleChallenge(peerId, msg) {
    const peerObj = this.getPeers().get(peerId)
    if (!peerObj || !peerObj.pairing) {
      return
    }
    // A revoked (deleted) peer's challenge is left unanswered UNLESS we have an
    // active matching secret registered (e.g. user explicitly entered the code to
    // re-pair). Re-admission happens on OUR challenge to it (handleResponse).
    if (this.isRevoked(peerId)) {
      const secret = typeof msg.codeId === 'string' ? this.pairingSecrets.get(msg.codeId) : null
      if (!secret || (secret.expiresAt > 0 && Date.now() >= secret.expiresAt)) {
        return
      }
    }
    if (typeof msg.codeId !== 'string' || typeof msg.nonce !== 'string') return

    const secret = this.pairingSecrets.get(msg.codeId)
    const secretUsable = secret && (secret.expiresAt === 0 || Date.now() < secret.expiresAt)

    // Receiving a challenge means pairing activity is underway: start (or
    // reset) the watchdog now, not at connection open. No-op for trusted peers
    // (the watchdog guard below) and harmless for direct-mode ones.
    // A challenge we CAN answer proves the other side is actively pairing with
    // a code we hold: clear any suppression and reciprocate below so trust
    // completes on BOTH sides. A challenge we cannot answer (neither side
    // knows the other's code) keeps its suppression — answering is impossible
    // and re-arming would only continue the connect/destroy loop.
    // Site-session peers (allowlist-authenticated, not pairing) never arm the
    // watchdog: their connection must survive for browsing, and an unanswered
    // cross-code challenge is expected, not a failure.
    const isSitePeer =
      this.isSiteSessionPeer && typeof this.isSiteSessionPeer === 'function'
        ? this.isSiteSessionPeer(peerId)
        : false
    if (!isSitePeer) {
      if (secretUsable) {
        this.unansweredPairings.delete(peerId)
        this._armPairingTimeout(peerId)
      } else if (!this._isPairingSuppressed(peerId)) {
        this._armPairingTimeout(peerId)
      }
    }

    if (!secret || (secret.expiresAt > 0 && Date.now() >= secret.expiresAt)) {
      // We don't know this code (yet). Remember it so we can answer once a
      // matching secret is registered (e.g. code entered after connection).
      // This applies to TRUSTED peers too: if the challenger's stored trust
      // for us is stale (noise key changed before persistence), answering its
      // challenge once the code is entered is the only way it can verify us
      // and complete its side — dropping the challenge here deadlocks it in
      // a one-way trust state (it keeps challenging, we keep not answering).
      if (!peerObj.pairing.pendingChallenges) peerObj.pairing.pendingChallenges = []
      peerObj.pairing.pendingChallenges.push({ codeId: msg.codeId, nonce: msg.nonce })
      if (peerObj.pairing.pendingChallenges.length > 16) peerObj.pairing.pendingChallenges.shift()
      return
    }

    const nonceBuf = b4a.from(msg.nonce, 'hex')
    if (nonceBuf.length !== 16) return
    const signature = mac(secret.code, nonceBuf)
    try {
      peerObj.signaling.send({
        type: MESSAGES.PAIRING_RESP,
        nonce: msg.nonce,
        mac: b4a.toString(signature, 'hex')
      })
    } catch (err) {
      console.error('[MeshEngine] Failed to send PAIRING_RESP:', err.message)
    }

    // Reciprocate so trust becomes MUTUAL: our answer alone only completes the
    // CHALLENGER's side. If we were not already challenging them (no outstanding
    // nonce), challenge them back with the code we hold — they can only answer
    // it if they genuinely know it. Guarded by hadOutstanding so two actively
    // pairing peers cannot ping-pong challenges forever. Site-session peers are
    // never reciprocated (they are not pairing).
    const hadOutstanding = peerObj.pairing.outstanding.length > 0
    if (!hadOutstanding && !peerObj.pairing.trusted && !isSitePeer) {
      this.sendChallenges(peerId, { force: true })
    }
  }

  // Verify the peer's response to OUR challenge. Only here do we grant trust.
  // On failure the connection is destroyed (untrusted peers only).
  handleResponse(peerId, msg) {
    const peerObj = this.getPeers().get(peerId)
    if (!peerObj || !peerObj.pairing) {
      return
    }
    const alreadyTrusted = peerObj.pairing.trusted
    if (!alreadyTrusted && peerObj.pairing.mode !== 'pairing') {
      return
    }
    if (typeof msg.nonce !== 'string' || typeof msg.mac !== 'string') return

    const idx = peerObj.pairing.outstanding.findIndex((o) => b4a.toString(o.nonce, 'hex') === msg.nonce)
    if (idx === -1) {
      if (alreadyTrusted) return // stale/duplicate response — never kill a live peer
      console.warn(`[MeshEngine] Pairing response nonce mismatch from ${peerId}, disconnecting`)
      this._recordPairingFailure(peerId)
      this._clearPairingTimeout(peerId)
      try {
        peerObj.connection.destroy()
      } catch {}
      this.emit('pairing:failed', { peerId, reason: 'nonce_mismatch' })
      return
    }
    const outstanding = peerObj.pairing.outstanding[idx]
    const expected = b4a.toString(mac(outstanding.code, outstanding.nonce), 'hex')
    if (msg.mac !== expected) {
      if (alreadyTrusted) return // probe mismatch — ignore rather than kill a live peer
      console.warn(`[MeshEngine] Pairing challenge FAILED from ${peerId}, disconnecting`)
      this._recordPairingFailure(peerId)
      this._clearPairingTimeout(peerId)
      try {
        peerObj.connection.destroy()
      } catch {}
      this.emit('pairing:failed', { peerId, reason: 'mac_mismatch', codeId: outstanding.codeId })
      return
    }

    peerObj.pairing.outstanding.splice(idx, 1)
    if (alreadyTrusted) {
      // The peer proved knowledge of a registered code over an already-trusted
      // connection: it is the code's host (LAN auto-trust, or a pairing that
      // completed before the code was entered). Re-confirm so the caller gets
      // a fresh completion signal without re-running the handshake.
      console.log(`[MeshEngine] Pairing re-confirmed for ${peerId} (${outstanding.code})`)
      this._clearPairingTimeout(peerId)
      this.onTrustGranted(peerId, outstanding.code)
      return
    }

    // A deleted (revoked) peer is re-admitted ONLY if it answers with a code
    // registered AFTER the revocation (the pairing code was rotated on
    // deletion, so such a code can only be known through an explicit fresh
    // pairing). An answer with a stale, memorized code is refused — this holds
    // even if the code rotation write above ever failed.
    if (this.isRevoked(peerId)) {
      const revokedAt = this.revokedKeys.get(peerId)
      const secret = this.pairingSecrets.get(outstanding.codeId)
      const secretIsFresh = secret && typeof secret.createdAt === 'number' && secret.createdAt >= revokedAt
      if (!secretIsFresh) {
        console.warn(
          `[MeshEngine] Refusing stale-code pairing from revoked peer ${peerId.slice(0, 12)}... (re-pairing required)`
        )
        this._clearPairingTimeout(peerId)
        try {
          peerObj.connection.destroy()
        } catch {}
        return
      }
      console.log(
        `[MeshEngine] Pairing re-admitted previously revoked peer ${peerId.slice(0, 12)}... (${outstanding.code})`
      )
      this.unrevokeKey(peerId)
    }

    peerObj.pairing.trusted = true
    peerObj.device.isTrusted = true
    peerObj.device.trustedAt = peerObj.device.trustedAt || new Date().toISOString()
    // Challenge VERIFIED: drop the watchdog so the connection is never killed
    // while the reciprocal HANDSHAKE is still in flight, and reset the
    // failed-MAC lockout and the unanswered-pairing backoff for this key.
    this._clearPairingTimeout(peerId)
    this._clearPairingFailures(peerId)
    this.unansweredPairings.delete(peerId)
    console.log(`[MeshEngine] Pairing challenge VERIFIED for ${peerId} (${outstanding.code})`)
    this.getPeers().set(peerId, peerObj)
    this.onTrustGranted(peerId, outstanding.code)
    this.sendHandshake(peerId)
  }

  // Called whenever a new pairing secret is registered: answer any previously
  // unanswered challenges and send fresh challenges to still-untrusted peers.
  syncToPeers() {
    for (const [pId, peerObj] of this.getPeers().entries()) {
      if (!peerObj.pairing) continue
      // Answer challenges we could not answer before — for ANY peer, including
      // trusted ones: a challenger whose stored trust for us is stale can only
      // complete its side if we answer its challenge once the code is entered.
      if (Array.isArray(peerObj.pairing.pendingChallenges)) {
        const still = []
        for (const pc of peerObj.pairing.pendingChallenges) {
          const secret = this.pairingSecrets.get(pc.codeId)
          if (secret && (secret.expiresAt === 0 || Date.now() < secret.expiresAt)) {
            const signature = mac(secret.code, b4a.from(pc.nonce, 'hex'))
            try {
              peerObj.signaling.send({
                type: MESSAGES.PAIRING_RESP,
                nonce: pc.nonce,
                mac: b4a.toString(signature, 'hex')
              })
            } catch {}
          } else {
            still.push(pc)
          }
        }
        peerObj.pairing.pendingChallenges = still
      }
      // Fresh challenges only go to untrusted peers still in the pairing phase.
      if (peerObj.pairing.mode !== 'pairing' || peerObj.pairing.trusted) continue
      this.sendChallenges(pId)
    }
  }

  // Drop expired pairing secrets so stale codes cannot be used indefinitely.
  expireSecrets() {
    const now = Date.now()
    for (const [cid, secret] of this.pairingSecrets.entries()) {
      if (secret.expiresAt > 0 && now >= secret.expiresAt) {
        this.pairingSecrets.delete(cid)
        if (this.topicRegistry) this.topicRegistry.leave(`p2p-pair-${secret.code}`)
      }
    }
  }
}

module.exports = { TrustManager, PAIRING_TTL, PAIRING_TIMEOUT }
