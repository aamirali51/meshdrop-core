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

  // Helper to start the actual download for chosen files
  async function startClaimDownload(peerId, offer, offerFiles, code) {
    const peerObj = peers.get(peerId)
    if (!peerObj) throw new Error('Sender is no longer connected')

    const isGroup = offer.isGroupDrop === true || (code && code.includes('GRP'))

    if (code) {
      activeClaims.delete(code)
      if (!isGroup) {
        try {
          engine.topicRegistry.leave(dropTopic(code))
        } catch {}
      }
      // The host answered: the placeholder "waiting for sender" row (if any)
      // is superseded by the real offer(s) below.
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

    // Open per-core replication for every claimed file — the same keys as
    // the host's file-drop-* cores — so block fetch works WITHOUT opening
    // the whole exchange store. Claims never have pairing trust, so the
    // store-wide ReplicationScope gate would (correctly) refuse them.
    peerObj.dropStreams = peerObj.dropStreams || []
    for (const f of offerFiles) {
      if (typeof f.coreKey !== 'string' || f.coreKey.length !== 64) continue
      try {
        const core = engine.storage.exchangeStore.get(Buffer.from(f.coreKey, 'hex'))
        await core.ready()
        const stream = core.replicate(peerObj.connection, { live: true })
        stream.on('error', () => {})
        peerObj.dropStreams.push(stream)
        console.log(`[MeshEngine] Claimed drop core opened for ${f.filename}`)
      } catch (err) {
        console.error('[MeshEngine] Failed to open claimed drop core:', err.message)
      }
    }

    // Auto-accept and start one transfer per file. When every transfer of
    // this claim bundle reaches a terminal state, tell the host ONCE so it
    // can tear the share down (cap reached) or keep it live (multi-download).
    const records = []
    for (let i = 0; i < offerFiles.length; i++) {
      const f = offerFiles[i]
      if (typeof f.fileSize !== 'number' || f.fileSize <= 0) continue
      const record = await engine.transferEngine.receiveOffer(
        {
          ...f,
          transferId: `claim-${offer.shareId}-${i}-${Date.now().toString(36)}`,
          transferMethod: 'internet',
          isClaim: true,
          peerKey: peerId,
          shareId: offer.shareId
        },
        { autoAccept: true, isClaim: true }
      )
      if (record) records.push(record.id)
    }

    if (records.length > 0) {
      const pending = new Set(records)
      const cleanup = () => {
        engine.removeListener(EVENTS.TRANSFER_COMPLETED, onTerminal)
        engine.removeListener(EVENTS.TRANSFER_FAILED, onTerminal)
        engine.removeListener(EVENTS.TRANSFER_CANCELLED, onTerminal)
      }
      const onTerminal = (rec) => {
        if (!rec || !pending.has(rec.id)) return
        pending.delete(rec.id)
        if (pending.size === 0) {
          cleanup()
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
