'use strict'

// Signaling: the framed p2p-signal-v1 protomux channel plus the inbound message
// dispatcher. Handlers live in their own modules (claims.js, keepalive.js,
// devices.js) and are reached through ctx.refs so this module stays a pure
// transport + router. Pairing challenge wrappers delegate to TrustManager,
// which owns the actual challenge-response state machine.

const Protomux = require('protomux')
const c = require('compact-encoding')
const { MESSAGES, EVENTS, PROTOCOL_VERSION } = require('../protocol.js')
const { getTransferMethod } = require('./util.js')

function createSignaling(ctx) {
  const { engine, peers, activeClaims } = ctx

  // Send our identity to a peer we already trust. Never called for untrusted peers.
  function sendHandshake(peerId) {
    const peerObj = peers.get(peerId)
    if (!peerObj || !peerObj.signaling || peerObj.handshakeSent) return
    if (!engine.deviceIdentity || !peerObj.pairing || !peerObj.pairing.trusted) return
    peerObj.handshakeSent = true
    try {
      peerObj.signaling.send({
        type: MESSAGES.HANDSHAKE,
        protocolVersion: PROTOCOL_VERSION,
        // The peer already knows our noise key from the transport (peerInfo /
        // connection), so the identity carries only the stable device identity.
        identity: { ...engine.deviceIdentity }
      })
    } catch (err) {
      console.error('[MeshEngine] Failed to send HANDSHAKE:', err.message)
    }
  }

  // Send a pairing challenge for every active pairing secret this device knows,
  // one outstanding challenge per (peer, secret) to keep MACs unambiguously tied
  // to a single nonce.
  function sendPairingChallenges(peerId) {
    if (engine.trustManager) engine.trustManager.sendChallenges(peerId)
  }

  // Respond to a peer's challenge using the pairing secret matching its codeId.
  // Trust is NEVER granted here; we only prove our own knowledge of the code.
  function handlePairingChallenge(peerId, msg) {
    if (engine.trustManager) engine.trustManager.handleChallenge(peerId, msg)
  }

  // Verify the peer's response to OUR challenge. Only here do we grant trust.
  // On failure the connection is destroyed.
  function handlePairingResponse(peerId, msg) {
    if (engine.trustManager) engine.trustManager.handleResponse(peerId, msg)
  }

  function setupPeerSignaling(connection, peerId, { directTrusted = false } = {}) {
    const mux = Protomux.isProtomux(connection) ? connection : Protomux.from(connection)
    const pendingQueue = []
    let signalMessage = null

    const channel = mux.createChannel({
      protocol: 'p2p-signal-v1',
      id: null,
      async onopen() {
        console.log(`[MeshEngine] Signaling channel opened with ${peerId.slice(0, 12)}...`)
        const peerObj = peers.get(peerId)
        if (peerObj && peerObj.pairing) {
          if (peerObj.pairing.trusted) {
            sendHandshake(peerId)
          } else {
            sendPairingChallenges(peerId)
          }
        }
        if (activeClaims.size > 0) {
          for (const code of activeClaims) {
            console.log(
              `[MeshEngine] Sending queued CLAIM_FILE_REQ for code ${code} to ${peerId.slice(0, 12)}...`
            )
            try {
              if (signalMessage) {
                signalMessage.send(JSON.stringify({ type: MESSAGES.CLAIM_FILE_REQ, code }))
              }
            } catch (err) {
              console.error('[MeshEngine] Failed to send queued CLAIM_FILE_REQ:', err)
            }
          }
        }
        while (pendingQueue.length > 0) {
          const item = pendingQueue.shift()
          try {
            if (signalMessage) signalMessage.send(JSON.stringify(item))
          } catch (err) {
            console.error('[MeshEngine] Failed to send queued signal message:', err)
          }
        }
      },
      onclose() {
        console.log(`[MeshEngine] Signaling channel closed with ${peerId.slice(0, 12)}...`)
      }
    })

    signalMessage = channel.addMessage({
      encoding: c.string,
      onmessage(raw) {
        try {
          const msg = JSON.parse(raw)
          handlePeerMessage(peerId, msg)
        } catch (err) {
          console.error('[MeshEngine] Error parsing peer signal message:', err)
        }
      }
    })

    channel.open()

    // Retry sending handshake/challenges after open tick to handle async channel ready states
    setTimeout(() => {
      const peerObj = peers.get(peerId)
      if (!peerObj || !peerObj.pairing) return
      if (peerObj.pairing.trusted) sendHandshake(peerId)
      else sendPairingChallenges(peerId)
    }, 150)
    setTimeout(() => {
      const peerObj = peers.get(peerId)
      if (!peerObj || !peerObj.pairing) return
      if (peerObj.pairing.trusted) sendHandshake(peerId)
      else sendPairingChallenges(peerId)
    }, 600)

    return {
      send(obj) {
        if (channel.opened && signalMessage) {
          try {
            signalMessage.send(JSON.stringify(obj))
          } catch (err) {
            console.error('[MeshEngine] Failed to send signal message:', err)
          }
        } else {
          pendingQueue.push(obj)
        }
      }
    }
  }

  // Inbound message router. All message types are validated and dispatched to
  // the owning module; unknown peers only ever get pairing challenges.
  function handlePeerMessage(peerId, msg) {
    if (!msg || typeof msg.type !== 'string') return
    if (msg.type === MESSAGES.HANDSHAKE) {
      const peerObj = peers.get(peerId)
      if (!peerObj || !peerObj.pairing) {
        console.warn(`[MeshEngine] Ignoring HANDSHAKE from unknown peer ${peerId.slice(0, 12)}...`)
        return
      }
      if (!peerObj.pairing.trusted || engine.trustManager.isRevoked(peerId)) {
        // The peer may have verified OUR pairing response before we verified
        // theirs, or it sent a handshake because it still holds old trust while
        // we revoked it. Buffer it: the handshake is applied the moment our
        // challenge-response verification grants trust (flushPendingHandshake).
        peerObj.pairing.pendingHandshake = msg
        return
      }
      ctx.refs.applyHandshake(peerId, msg)
    } else if (msg.type === MESSAGES.PAIRING_CHALLENGE) {
      handlePairingChallenge(peerId, msg)
    } else if (msg.type === MESSAGES.PAIRING_RESP) {
      handlePairingResponse(peerId, msg)
    } else if (msg.type === MESSAGES.DEVICE_REMOVED) {
      // The remote host deleted this device. React immediately so the UI can
      // show "you were removed" and the local trust state is torn down —
      // otherwise the peer only discovers the deletion on its next reconnect
      // (revoked → refused auto-trust → must re-pair).
      const peerObj = peers.get(peerId)
      if (peerObj) {
        try {
          if (engine.trustManager) {
            // The host's key is no longer trusted locally. The connection is
            // destroyed so any in-flight transfers/sync abort and park.
            engine.trustManager.removeTrustedKey(peerId)
            engine.trustManager.revokeKey(peerId).catch(() => {})
          }
          peerObj.connection.destroy()
        } catch {}
      }
      engine.emit(EVENTS.TRUST_REVOKED, { publicKey: peerId })
      engine.emit(EVENTS.DEVICE_REMOVED, { peerId })
    } else if (msg.type === MESSAGES.TRANSFER_OFFER) {
      const peerObj = peers.get(peerId)
      // Only accept file offers from authenticated (trusted) peers. One-time
      // shares go through the CLAIM_FILE_REQ flow instead, which requires
      // knowledge of the drop code.
      if (!peerObj || !peerObj.pairing || !peerObj.pairing.trusted) {
        console.warn(
          `[MeshEngine] Ignoring TRANSFER_OFFER from unauthenticated peer ${peerId.slice(0, 12)}...`
        )
        return
      }
      const transferMethod =
        msg.transferMethod ||
        peerObj?.transferMethod ||
        getTransferMethod(msg.senderIdentity?.ipAddress || peerObj?.device?.ipAddress || '')
      if (engine.transferEngine) {
        engine.transferEngine
          .receiveOffer(
            { ...msg, transferMethod, senderPeerId: peerId },
            { autoAccept: engine.autoAcceptOffers !== false }
          )
          .catch((err) => {
            console.error('[MeshEngine] receiveOffer failed:', err)
          })
      }
    } else if (msg.type === MESSAGES.CLAIM_FILE_REQ) {
      ctx.refs.handleClaimFileReq(peerId, msg).catch((err) => {
        console.error('[MeshEngine] handleClaimFileReq failed:', err)
      })
    } else if (msg.type === MESSAGES.CLAIM_FILE_RES) {
      ctx.refs.handleClaimFileRes(peerId, msg).catch((err) => {
        console.error('[MeshEngine] handleClaimFileRes failed:', err)
      })
    } else if (msg.type === MESSAGES.CLAIM_FILE_DONE) {
      ctx.refs.handleClaimDone(peerId, msg).catch((err) => {
        console.error('[MeshEngine] handleClaimDone failed:', err)
      })
    } else if (msg.type === MESSAGES.PING) {
      ctx.refs.handlePing(peerId, msg)
    } else if (msg.type === MESSAGES.PONG) {
      ctx.refs.handlePong(peerId, msg)
    } else if (msg.type === MESSAGES.SYNC_INDEX) {
      if (ctx.refs.handleSyncIndex) {
        ctx.refs.handleSyncIndex(peerId, msg).catch((err) => {
          console.warn('[MeshEngine] handleSyncIndex failed:', err?.message)
        })
      }
    } else if (msg.type === MESSAGES.SYNC_DELETE) {
      if (ctx.refs.handleSyncDelete) {
        ctx.refs.handleSyncDelete(peerId, msg).catch((err) => {
          console.warn('[MeshEngine] handleSyncDelete failed:', err?.message)
        })
      }
    } else if (msg.type === MESSAGES.SYNC_INVITE) {
      if (ctx.refs.handleSyncInvite) {
        ctx.refs.handleSyncInvite(peerId, msg).catch((err) => {
          console.warn('[MeshEngine] handleSyncInvite failed:', err?.message)
        })
      }
    } else if (msg.type === MESSAGES.SYNC_INVITE_ACCEPT) {
      if (ctx.refs.handleSyncInviteAccept) {
        ctx.refs.handleSyncInviteAccept(peerId, msg).catch((err) => {
          console.warn('[MeshEngine] handleSyncInviteAccept failed:', err?.message)
        })
      }
    } else if (msg.type === MESSAGES.SYNC_INVITE_DECLINE) {
      if (ctx.refs.handleSyncInviteDecline) {
        ctx.refs.handleSyncInviteDecline(peerId, msg).catch((err) => {
          console.warn('[MeshEngine] handleSyncInviteDecline failed:', err?.message)
        })
      }
    } else if (msg.type === MESSAGES.SYNC_REMOVE) {
      if (ctx.refs.handleSyncRemove) {
        ctx.refs.handleSyncRemove(peerId, msg).catch((err) => {
          console.warn('[MeshEngine] handleSyncRemove failed:', err?.message)
        })
      }
    } else if (msg.type === MESSAGES.SYNC_VERIFY) {
      if (ctx.refs.handleSyncVerify) {
        ctx.refs.handleSyncVerify(peerId, msg).catch((err) => {
          console.warn('[MeshEngine] handleSyncVerify failed:', err?.message)
        })
      }
    } else if (msg.type === MESSAGES.SYNC_VERIFY_RESULT) {
      if (ctx.refs.handleSyncVerifyResult) {
        ctx.refs.handleSyncVerifyResult(peerId, msg).catch((err) => {
          console.warn('[MeshEngine] handleSyncVerifyResult failed:', err?.message)
        })
      }
    } else if (msg.type?.startsWith?.('WATCH_')) {
      if (ctx.refs.handleWatchMessage) {
        ctx.refs.handleWatchMessage(peerId, msg)
      }
      if (msg.type === MESSAGES.WATCH_STATE_SYNC) {
        engine.emit(EVENTS.WATCH_STATE_UPDATED, {
          peerId,
          action: msg.action,
          positionSec: msg.positionSec,
          playbackRate: msg.playbackRate,
          timestampMs: msg.timestampMs,
          senderDevice: msg.senderDevice || msg.sender || null,
          roomCode: msg.roomCode,
          roomMeta: msg.roomMeta || null
        })
      } else if (msg.type === MESSAGES.WATCH_PEER_STATUS) {
        engine.emit(EVENTS.WATCH_STATE_UPDATED, {
          peerId,
          buffering: msg.buffering,
          positionSec: msg.positionSec,
          roomCode: msg.roomCode
        })
      }
    }
  }

  return {
    setupPeerSignaling,
    sendHandshake,
    sendPairingChallenges,
    handlePairingChallenge,
    handlePairingResponse,
    handlePeerMessage
  }
}

module.exports = { createSignaling }
