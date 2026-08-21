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
   * @returns {Object} { toPush: string[], toDeleteLocal: string[], toDeleteRemote: string[], conflicts: string[] }
   */
  static reconcile({ localIndex = {}, baseline = {}, remoteIndex = {}, mode = 'two-way' }) {
    const toPush = []
    const toDeleteLocal = []
    const toDeleteRemote = []
    const conflicts = []

    if (mode === 'receive_only') {
      return { toPush, toDeleteLocal, toDeleteRemote, conflicts }
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
      return { toPush, toDeleteLocal, toDeleteRemote, conflicts }
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
          }
        } else if (remoteEntry.deleted) {
          // Remote marked deleted
          if (remoteEntry.mtimeMs >= localEntry.mtimeMs - CONFLICT_TOLERANCE_MS || (baseEntry && baseEntry.sig === localEntry.sig)) {
            // Remote deletion is current -> apply delete locally
            toDeleteLocal.push(rel)
          } else {
            // Local was modified strictly after remote deletion -> local edit wins
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

    return { toPush, toDeleteLocal, toDeleteRemote, conflicts }
  }
}

module.exports = { Reconciler }
