'use strict'

// One-time DROP claim flow (WeTransfer-style, no pairing): the host serves its
// drop cores to a claimer that proves knowledge of the code, and the claimer
// pulls each file over per-core replication — never the whole exchange store.
//
// Cross-module references (peer cleanup on disconnect, signaling send) are
// reached through ctx.refs so this module stays self-contained.

const { EVENTS, MESSAGES, dropTopic } = require('../protocol.js')

function createClaims(ctx) {
  const { engine, peers, activeClaims } = ctx

  // Swarm distribution registry: code -> { shareId, files } for drops this
  // peer has fully claimed and hash-verified. A registered peer answers
  // CLAIM_FILE_REQ as a seeder — it serves the verified blocks to new
  // claimers so a popular drop stops hammering the original host. In-memory
  // by design: seeding is best-effort and dies with the app.
  const completedClaims = new Map()

  async function handleClaimFileReq(peerId, msg) {
    const code = (msg.code || '').trim().toUpperCase()
    console.log(
      `[MeshEngine] Received CLAIM_FILE_REQ for code ${code} from ${peerId.slice(0, 12)}...`
    )

    const bee = await engine.getBee('pendingShares')
    let foundShare = null
    const now = Date.now()

    // Multi-download: a live share keeps serving every claimer until it
    // expires, is revoked, or hits maxDownloads. 'claimed' only means "at
    // least one download started" — the code stays valid.
    for await (const node of bee.createReadStream()) {
      const s = node.value
      if (!s || s.code !== code || s.isHost !== true) continue
      const claimable = s.status === 'waiting' || s.status === 'claimed'
      if (!claimable) continue
      if (s.expiresAt > 0 && now >= s.expiresAt) continue
      if (s.maxDownloads > 0 && (s.downloadCount || 0) >= s.maxDownloads) continue
      foundShare = s
      break
    }

    const peerObj = peers.get(peerId)
    if (!peerObj || !peerObj.signaling) return

    // Swarm distribution: a peer that completed this claim serves the blocks
    // to new claimers. Checked BEFORE the host share scan — a seeder is not
    // the share's authority, so it never emits the failure response (only
    // the host judges expiry/download caps); a seeder that cannot serve
    // stays silent and the host's answer (if any) rules.
    const seeded = completedClaims.get(code)
    if (seeded && seeded.files.length > 0) {
      await serveClaimFromSeeder(peerId, peerObj, code, seeded)
      return
    }

    if (!foundShare) {
      console.log(`[MeshEngine] CLAIM_FILE_REQ for ${code} not found or expired`)
      peerObj.signaling.send({
        type: MESSAGES.CLAIM_FILE_RES,
        code,
        success: false,
        error: 'Share expired or invalid code'
      })
      return
    }

    foundShare.downloadCount = (foundShare.downloadCount || 0) + 1
    if (foundShare.status === 'waiting') foundShare.status = 'claimed'
    await bee.put(foundShare.id, foundShare)

    // A claim connection never verifies a pairing challenge: drop the
    // watchdog so long downloads are not killed mid-transfer.
    if (peerObj.pairing && peerObj.pairing.timeout) {
      clearTimeout(peerObj.pairing.timeout)
      peerObj.pairing.timeout = null
    }

    // Serve ONLY this share's drop cores to the claimer — never the whole
    // exchange store. Authorization was the code check above; the claimer
    // opens the same cores by key (from the offer) so the hypercores handshake
    // over the shared protomux and blocks flow. The store-wide ReplicationScope
    // is gated on pairing trust, which claims never have (by design).
    const files =
      foundShare.files && foundShare.files.length > 0
        ? foundShare.files
        : [
            // Legacy single-file shares stored the fields at the top level.
            {
              filename: foundShare.filename,
              fileSize: foundShare.fileSize,
              fileType: foundShare.fileType || '',
              coreName: `file-drop-${foundShare.id}`,
              coreKey: foundShare.coreKey,
              manifestHash: foundShare.manifestHash || '',
              checksum: foundShare.checksum || ''
            }
          ]

    peerObj.dropStreams = peerObj.dropStreams || []
    for (const f of files) {
      try {
        const core = engine.storage.exchangeStore.get({ name: f.coreName })
        await core.ready()
        const stream = core.replicate(peerObj.connection, { live: true })
        stream.on('error', () => {})
        peerObj.dropStreams.push(stream)
        console.log(
          `[MeshEngine] Serving drop core for ${f.filename} to ${peerId.slice(0, 12)}... (downloadCount: ${foundShare.downloadCount})`
        )
      } catch (err) {
        console.error('[MeshEngine] Failed to open drop core replication:', err.message)
        peerObj.signaling.send({
          type: MESSAGES.CLAIM_FILE_RES,
          code,
          success: false,
          error: 'Share unavailable'
        })
        return
      }
    }

    peerObj.signaling.send({
      type: MESSAGES.CLAIM_FILE_RES,
      code,
      success: true,
      offer: {
        transferId: `claim-${foundShare.id}-${Date.now().toString(36)}`,
        filename: foundShare.filename,
        fileSize: foundShare.fileSize,
        fileType: foundShare.fileType,
        files: files.map((f) => ({
          filename: f.filename,
          fileSize: f.fileSize,
          fileType: f.fileType || '',
          coreKey: f.coreKey,
          manifestHash: f.manifestHash || '',
          checksum: f.checksum || ''
        })),
        senderIdentity: engine.deviceIdentity,
        transferMethod: 'internet',
        shareId: foundShare.id
      }
    })
  }

  const pendingClaimOffers = new Map()
  // Multi-seeder roster: shareId -> Set<peerKey>. Every peer that answers a
  // CLAIM_FILE_REQ for the share (the host + any completedClaims seeder) is
  // recorded here so the claimer can open replication streams to SEVERAL
  // seeders at once and pull each block from whichever delivers first
  // (latency-weighted multi-peer swarming). Populated opportunistically as
  // duplicate descriptors arrive for an already-started download; the primary
  // transfer still binds to the first responder, and the extras become
  // secondary replication sources on the SAME core instance.
  const claimRoster = new Map()

  // Seeder serve: mirror of the host serve step (watchdog drop, per-core
  // replication toward the requester, descriptor response) minus the share
  // authority — the blocks this peer serves were already hash-verified on
  // download, so a seeder cannot poison anything.
  async function serveClaimFromSeeder(peerId, peerObj, code, seeded) {
    try {
      // A seeding connection never verifies a pairing challenge: drop the
      // watchdog so long downloads are not killed mid-transfer.
      if (peerObj.pairing && peerObj.pairing.timeout) {
        clearTimeout(peerObj.pairing.timeout)
        peerObj.pairing.timeout = null
      }

      peerObj.dropStreams = peerObj.dropStreams || []
      for (const f of seeded.files) {
        if (typeof f.coreKey !== 'string' || f.coreKey.length !== 64) continue
        const core = engine.storage.exchangeStore.get(Buffer.from(f.coreKey, 'hex'))
        await core.ready()
        const stream = core.replicate(peerObj.connection, { live: true })
        stream.on('error', () => {})
        peerObj.dropStreams.push(stream)
      }

      console.log(
        `[MeshEngine] Seeder serving drop ${code} to ${peerId.slice(0, 12)}... (${seeded.files.length} file(s))`
      )
      peerObj.signaling.send({
        type: MESSAGES.CLAIM_FILE_RES,
        code,
        success: true,
        offer: {
          transferId: `claim-${seeded.shareId}`,
          filename: seeded.files[0].filename,
          fileSize: seeded.files.reduce((sum, f) => sum + (f.fileSize || 0), 0),
          fileType: seeded.files[0].fileType || '',
          files: seeded.files.map((f) => ({
            filename: f.filename,
            fileSize: f.fileSize,
            fileType: f.fileType || '',
            coreKey: f.coreKey,
            manifestHash: f.manifestHash || '',
            checksum: f.checksum || ''
          })),
          senderIdentity: engine.deviceIdentity,
          transferMethod: 'internet',
          shareId: seeded.shareId
        }
      })
    } catch (err) {
      // Seeder serve failed: stay SILENT — the real host (if reachable)
      // answers, and a failure response here would wrongly kill the claim.
      console.error('[MeshEngine] Seeder serve failed:', err.message)
    }
  }

  // Helper to start the actual download for chosen files
  async function startClaimDownload(peerId, offer, offerFiles, code) {
    const peerObj = peers.get(peerId)
    if (!peerObj) throw new Error('Sender is no longer connected')

    const isGroup = offer.isGroupDrop === true || (code && code.includes('GRP'))

    if (code) {
      activeClaims.delete(code)
      // NOTE: the drop topic stays joined — once this claim completes, this
      // peer seeds the share for the next claimer (swarm distribution).
      if (engine.transferEngine) {
        await engine.transferEngine.clearWaitingClaims({ code }).catch(() => {})
      }
    }

    // A claim connection never verifies a pairing challenge: drop the
    // watchdog so long downloads are not killed mid-transfer.
    if (peerObj.pairing && peerObj.pairing.timeout) {
      clearTimeout(peerObj.pairing.timeout)
      peerObj.pairing.timeout = null
    }

    // Multi-seeder swarming: open per-core replication to the PRIMARY peer
    // (the first responder) AND every secondary seeder that answered the same
    // CLAIM_FILE_REQ (host + completedClaims peers on the drop topic). All
    // streams attach to the SAME core instances, so hypercore's internal block
    // exchange can serve a missing block from whichever peer has it — the
    // scheduler's completed/inflight dedupe means one block is requested once
    // even though N seeders could serve it.
    const primaryPeerId = peerId
    const shareKey = offer.shareId || code || ''
    const roster = new Set([primaryPeerId])
    const known = shareKey ? (claimRoster.get(shareKey) || new Set()) : new Set()
    for (const k of known) roster.add(k)
    if (shareKey) claimRoster.set(shareKey, roster)

    const openDropReplication = async (targetPeerId, files) => {
      const p = peers.get(targetPeerId)
      if (!p || !p.connection) return
      if (p.pairing && p.pairing.timeout) {
        clearTimeout(p.pairing.timeout)
        p.pairing.timeout = null
      }
      p.dropStreams = p.dropStreams || []
      for (const f of files) {
        if (typeof f.coreKey !== 'string' || f.coreKey.length !== 64) continue
        try {
          const core = engine.storage.exchangeStore.get(Buffer.from(f.coreKey, 'hex'))
          await core.ready()
          const stream = core.replicate(p.connection, { live: true })
          stream.on('error', () => {})
          p.dropStreams.push(stream)
          console.log(
            `[MeshEngine] Claimed drop core opened for ${f.filename} via ${targetPeerId.slice(0, 12)}...`
          )
        } catch (err) {
          console.error('[MeshEngine] Failed to open claimed drop core:', err.message)
        }
      }
    }

    await openDropReplication(primaryPeerId, offerFiles)
    // Secondary seeders attach too — but ONLY up to the host's configured fan-in
    // cap so a phone on cellular isn't replicating from 20 peers at once. The
    // transfer record's seederPeerIds lets _runReceive attach late joiners on
    // resume. (Cap read from the engine's network profile when available.)
    const profile = engine.transferEngine && typeof engine.transferEngine.getNetworkProfile === 'function'
      ? engine.transferEngine.getNetworkProfile()
      : null
    const maxFanIn = profile && Number.isFinite(profile.maxConcurrentPeers)
      ? Math.max(1, Math.floor(profile.maxConcurrentPeers))
      : 3
    const extraSeeders = Array.from(roster).filter((id) => id !== primaryPeerId).slice(0, Math.max(0, maxFanIn - 1))
    for (const seederId of extraSeeders) {
      await openDropReplication(seederId, offerFiles)
    }

    // Auto-accept and start one transfer per file. Transfer ids are
    // DETERMINISTIC (claim-<shareId>-<i>): a second descriptor for the same
    // share (host + seeder both answering) dedupes in receiveOffer instead
    // of downloading everything twice. Cores stay open after completion —
    // this peer seeds the share until the app restarts.
    const records = []
    for (let i = 0; i < offerFiles.length; i++) {
      const f = offerFiles[i]
      if (typeof f.fileSize !== 'number' || f.fileSize <= 0) continue
      const record = await engine.transferEngine.receiveOffer(
        {
          ...f,
          transferId: `claim-${offer.shareId || code}-${i}`,
          transferMethod: 'internet',
          isClaim: true,
          // Secondary seeder peer keys ride on the record so _runReceive can
          // re-attach replication to any that are still connected on resume.
          seederPeerIds: Array.from(roster),
          peerKey: peerId,
          shareId: offer.shareId
        },
        { autoAccept: true, isClaim: true, keepCoreOpen: true }
      )
      if (record) records.push(record.id)
    }

    if (records.length > 0) {
      const pending = new Set(records)
      let allCompleted = true
      const cleanup = () => {
        engine.removeListener(EVENTS.TRANSFER_COMPLETED, onTerminal)
        engine.removeListener(EVENTS.TRANSFER_FAILED, onTerminal)
        engine.removeListener(EVENTS.TRANSFER_CANCELLED, onTerminal)
      }
      const onTerminal = (rec) => {
        if (!rec || !pending.has(rec.id)) return
        pending.delete(rec.id)
        if (rec.status !== 'completed') allCompleted = false
        if (pending.size === 0) {
          cleanup()
          // Drop the multi-seeder roster entry (the transfer is terminal).
          if (shareKey) claimRoster.delete(shareKey)
          // Every file hash-verified on disk: register as a seeder so the
          // next claimer pulls from us instead of hammering the host.
          if (allCompleted && code && offer.shareId) {
            completedClaims.set(code, { shareId: offer.shareId, files: offerFiles })
          }
          const p = peers.get(peerId)
          if (p && p.signaling) {
            p.signaling.send({ type: MESSAGES.CLAIM_FILE_DONE, shareId: offer.shareId })
          }
        }
      }
      peerObj.claimCleanups = peerObj.claimCleanups || []
      peerObj.claimCleanups.push(cleanup)
      engine.on(EVENTS.TRANSFER_COMPLETED, onTerminal)
      engine.on(EVENTS.TRANSFER_FAILED, onTerminal)
      engine.on(EVENTS.TRANSFER_CANCELLED, onTerminal)
    } else if (offerFiles.length > 0 && code && offer.shareId) {
      // Every file was a duplicate (this peer already holds the share).
      completedClaims.set(code, { shareId: offer.shareId, files: offerFiles })
    }
    return records
  }

  async function handleClaimFileRes(peerId, msg) {
    const code = (msg.code || '').trim().toUpperCase()
    console.log(`[MeshEngine] Received CLAIM_FILE_RES for ${code}: success=${msg.success}`)

    if (msg.success && msg.offer) {
      // Normalize to a file list (single-file offers carry the fields at the
      // top level; multi-file offers carry files[]).
      const offerFiles =
        Array.isArray(msg.offer.files) && msg.offer.files.length > 0
          ? msg.offer.files
          : [
              {
                filename: msg.offer.filename,
                fileSize: msg.offer.fileSize,
                fileType: msg.offer.fileType,
                coreKey: msg.offer.coreKey,
                manifestHash: msg.offer.manifestHash || '',
                checksum: msg.offer.checksum || ''
              }
            ]

      const claimOpts = (engine.activeClaimOptions && engine.activeClaimOptions.get(code)) || {}
      const isInteractive = claimOpts.interactive === true || engine.interactiveClaims === true
      const isFolderOrMulti = offerFiles.length > 1 || !!msg.offer.folderName

      // Multi-seeder: a SECOND responder for the same share (host + a
      // completedClaims seeder both answering the claim) must not start a
      // duplicate download — instead it joins the roster and its connection is
      // attached to the already-open cores so the active transfer can pull
      // blocks from it. Detect "already downloading" by checking whether the
      // deterministic claim transfer ids already exist.
      const shareKey = msg.offer.shareId || code || ''
      if (shareKey && engine.transferEngine) {
        const probeId = `claim-${shareKey}-0`
        const bee = await engine.getBee('transfers')
        const existing = await bee.get(probeId)
        const alreadyActive = existing && existing.value && existing.value.status !== 'completed'
        if (alreadyActive) {
          console.log(
            `[MeshEngine] Duplicate CLAIM_FILE_RES for ${code}: adding ${peerId.slice(0, 12)}... as secondary seeder`
          )
          const roster = claimRoster.get(shareKey) || new Set()
          roster.add(peerId)
          claimRoster.set(shareKey, roster)
          // Attach this seeder's connection to the claimed cores (same keys as
          // the primary offer) so the running receive can fetch from it.
          const p = peers.get(peerId)
          if (p && p.connection && engine.storage && engine.storage.exchangeStore) {
            p.dropStreams = p.dropStreams || []
            for (const f of offerFiles) {
              if (typeof f.coreKey !== 'string' || f.coreKey.length !== 64) continue
              try {
                const core = engine.storage.exchangeStore.get(Buffer.from(f.coreKey, 'hex'))
                await core.ready()
                const stream = core.replicate(p.connection, { live: true })
                stream.on('error', () => {})
                p.dropStreams.push(stream)
              } catch (err) {
                console.error('[MeshEngine] Secondary seeder attach failed:', err.message)
              }
            }
          }
          return
        }
      }

      if (isInteractive && isFolderOrMulti) {
        console.log(`[MeshEngine] Emitting CLAIM_PREVIEW for ${code} (${offerFiles.length} files)`)
        pendingClaimOffers.set(msg.offer.shareId, {
          peerId,
          code,
          offer: msg.offer,
          offerFiles,
          createdAt: Date.now()
        })
        engine.emit(EVENTS.CLAIM_PREVIEW, {
          code,
          shareId: msg.offer.shareId,
          folderName: msg.offer.folderName || (offerFiles.length > 1 ? msg.offer.filename : null),
          totalSize: msg.offer.fileSize,
          totalFiles: offerFiles.length,
          files: offerFiles.map((f, i) => ({
            index: i,
            filename: f.filename,
            fileSize: f.fileSize,
            fileType: f.fileType || ''
          }))
        })
        return
      }

      await startClaimDownload(peerId, msg.offer, offerFiles, code)
    } else {
      // The host rejected the claim (expired / already used): stop advertising
      // the code so it is not re-sent on every future connection, and drop the
      // placeholder row so the user is not left waiting on a dead claim.
      activeClaims.delete(code)
      try {
        engine.topicRegistry.leave(dropTopic(code))
      } catch {}
      if (engine.transferEngine) {
        await engine.transferEngine.clearWaitingClaims({ code }).catch(() => {})
      }
      engine.emit(EVENTS.ERROR, {
        code,
        error: msg.error || 'Share expired or invalid code'
      })
    }
  }

  async function confirmClaimDownload({ shareId, selectedIndices }) {
    if (!shareId) throw new Error('Missing shareId')
    const pending = pendingClaimOffers.get(shareId)
    if (!pending) throw new Error('No pending claim offer found for ' + shareId)

    let chosenFiles = pending.offerFiles
    if (Array.isArray(selectedIndices) && selectedIndices.length > 0) {
      const set = new Set(selectedIndices)
      chosenFiles = pending.offerFiles.filter((_, idx) => set.has(idx))
    }
    if (chosenFiles.length === 0) throw new Error('No files selected for download')

    pendingClaimOffers.delete(shareId)
    const records = await startClaimDownload(pending.peerId, pending.offer, chosenFiles, pending.code)
    return { success: true, count: records.length }
  }

  async function cancelClaimDownload({ shareId, code }) {
    const pending = shareId ? pendingClaimOffers.get(shareId) : null
    const c = code || (pending && pending.code)
    if (shareId) pendingClaimOffers.delete(shareId)

    if (c) {
      activeClaims.delete(c)
      try {
        engine.topicRegistry.leave(dropTopic(c))
      } catch {}
      if (engine.transferEngine) {
        await engine.transferEngine.clearWaitingClaims({ code: c }).catch(() => {})
      }
    }
    if (pending && pending.peerId) {
      const p = peers.get(pending.peerId)
      if (p && p.signaling) {
        p.signaling.send({ type: MESSAGES.CLAIM_FILE_DONE, shareId: pending.offer.shareId })
      }
    }
    return { success: true }
  }

  // The claimer finished (or abandoned) a one-time download. With multi-
  // download semantics the share stays live until expiry or maxDownloads;
  // only a share that reached its download cap is torn down.
  async function handleClaimDone(peerId, msg) {
    if (!msg || !msg.shareId) return
    const bee = await engine.getBee('pendingShares')
    const entry = await bee.get(msg.shareId)
    if (!entry || !entry.value || entry.value.isHost !== true) return
    const share = entry.value
    if (share.maxDownloads > 0 && (share.downloadCount || 0) >= share.maxDownloads) {
      console.log(`[MeshEngine] Claimed share ${share.code} download finished (cap reached); cleaning up`)
      await engine.cleanupPendingShare(msg.shareId, 'completed')
    } else {
      console.log(`[MeshEngine] Claimed share ${share.code} download finished; share stays live`)
    }
  }

  return { handleClaimFileReq, handleClaimFileRes, handleClaimDone, confirmClaimDownload, cancelClaimDownload }
}

module.exports = { createClaims }
