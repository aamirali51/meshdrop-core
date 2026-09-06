'use strict'

// Reconciler — Pure 3-way synchronization diff and deterministic action planner

const { CONFLICT_TOLERANCE_MS } = require('./SyncConstants.js')

class Reconciler {
  /**
   * Pure reconciliation function
   * @param {Object} params
   * @param {Object} params.localIndex - Map of rel -> { size, mtimeMs, sig, deleted, authorKey }
   * @param {Object} params.baseline - Map of rel -> { size, mtimeMs, sig, deleted, authorKey }
   * @param {Object} params.remoteIndex - Map of rel -> { size, mtimeMs, sig, deleted, authorKey }
   * @param {string} params.mode - 'push' | 'receive_only' | 'two-way'
   * @returns {Object} { toPush: string[], toPull: string[], toDeleteLocal: string[], toDeleteRemote: string[], conflicts: string[] }
   */
  static reconcile({ localIndex = {}, baseline = {}, remoteIndex = {}, mode = 'two-way' }) {
    const toPush = []
    const toPull = []      // Fix #1: remote-new files the local side has never seen
    const toDeleteLocal = []
    const toDeleteRemote = []
    const conflicts = []

    if (mode === 'receive_only') {
      return { toPush, toPull, toDeleteLocal, toDeleteRemote, conflicts }
    }

    if (mode === 'push') {
      // Single-owner push: owner diffs local folder against its sentIndex
      for (const [rel, localEntry] of Object.entries(localIndex)) {
        if (!localEntry || localEntry.deleted) continue
        const known = baseline[rel]
        if (!known) {
          toPush.push(rel)
        } else if (known.sig !== localEntry.sig) {
          toPush.push(rel)
        }
      }
      return { toPush, toPull, toDeleteLocal, toDeleteRemote, conflicts }
    }

    // Two-Way Sync (Desktop ↔ Desktop)
    for (const [rel, localEntry] of Object.entries(localIndex)) {
      if (!localEntry) continue
      const baseEntry = baseline[rel]
      const remoteEntry = remoteIndex[rel]

      if (!localEntry.deleted) {
        // File is present on local disk
        if (!remoteEntry) {
          // Missing remotely -> push
          toPush.push(rel)
        } else if (!remoteEntry.deleted) {
          // Present on both sides
          if (localEntry.mtimeMs > remoteEntry.mtimeMs + CONFLICT_TOLERANCE_MS) {
            // Local is newer
            toPush.push(rel)
          } else if (remoteEntry.mtimeMs > localEntry.mtimeMs + CONFLICT_TOLERANCE_MS) {
            // Remote is newer -> receiver pulls or waits for remote push
          } else if ((localEntry.sig || '') === (remoteEntry.sig || '')) {
            // Same version on both sides (mtime within tolerance but identical
            // content) — nothing to do.
          } else {
            // Audit fix #2: concurrent edit within the mtime tolerance. The old
            // code resolved to NOTHING here — both devices kept their own
            // version silently, forever (empirically reproduced: two edits 1ms
            // apart diverged permanently with no event). Resolve so both sides
            // converge on the SAME winner: later mtime wins; a tie inside the
            // tolerance is broken by the lexicographically greater authorKey —
            // a pure function of the two index entries, so each side computes
            // the same verdict independently. The loser's copy is displaced to
            // .meshdrop-trash/conflict-… by the receiver's two-way receive
            // path (TransferEngine._displaceForSync — recoverable).
            const localTiebreak = `${localEntry.mtimeMs}|${localEntry.authorKey || ''}`
            const remoteTiebreak = `${remoteEntry.mtimeMs}|${remoteEntry.authorKey || ''}`
            const localWins = localTiebreak >= remoteTiebreak
            if (localWins) toPush.push(rel)
            conflicts.push({
              rel,
              winner: localWins ? 'local' : 'remote',
              localSig: localEntry.sig || `${localEntry.size || 0}-${localEntry.mtimeMs || 0}`,
              remoteSig: remoteEntry.sig || `${remoteEntry.size || 0}-${remoteEntry.mtimeMs || 0}`,
              ts: Date.now()
            })
          }
        } else if (remoteEntry.deleted) {
          // Remote marked deleted. Audit fix #5: a tombstone may only destroy
          // the local copy when the local file is UNCHANGED since the last
          // sync (sig still matches the baseline's pre-delete sig). Mtime
          // comparison cannot prove that — an edit made before the peer's
          // delete but learned about later must survive and be pushed back.
          if (baseEntry && baseEntry.sig === localEntry.sig) {
            // Local unchanged since last sync -> pure remote-intent delete.
            toDeleteLocal.push(rel)
          } else {
            // Local differs from baseline (or no baseline) -> local edit wins.
            toPush.push(rel)
          }
        }
      } else {
        // File is deleted on local disk
        if (remoteEntry && !remoteEntry.deleted) {
          // Remote still has file -> tell remote to delete
          toDeleteRemote.push(rel)
        }
      }
    }

    // Fix #1 — Second pass: find files that exist remotely but are completely absent
    // from localIndex (never seen — not just locally deleted). These are genuine remote
    // additions. Signal the caller to nudge the remote into pushing them.
    for (const [rel, remoteEntry] of Object.entries(remoteIndex)) {
      if (!remoteEntry || remoteEntry.deleted) continue
      if (localIndex[rel] === undefined) {
        toPull.push(rel)
      }
    }

    return { toPush, toPull, toDeleteLocal, toDeleteRemote, conflicts }
  }
}

module.exports = { Reconciler }
