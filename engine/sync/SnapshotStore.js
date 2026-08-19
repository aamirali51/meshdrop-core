'use strict'

// SnapshotStore — Hyperbee/RocksDB storage for SyncEngine manifests & baseline snapshots

const { VERIFY_TOLERANCE_MS, safeRelPath } = require('./SyncConstants.js')

class SnapshotStore {
  static rowRange(kind, libId) {
    const prefix = `${kind}/${libId}/`
    return { gte: prefix, lte: `${prefix}\uffff` }
  }

  static async iterRows(getBee, kind, libId) {
    if (!getBee || !libId) return []
    try {
      const bee = await getBee('sync')
      const out = []
      for await (const node of bee.createReadStream(SnapshotStore.rowRange(kind, libId))) {
        const rel = node.key.slice(kind.length + libId.length + 2)
        out.push([rel, node.value])
      }
      return out
    } catch (err) {
      console.warn(`[SnapshotStore] iterRows(${kind}) warning:`, err.message)
      return []
    }
  }

  static async writeRows(getBee, kind, libId, entries) {
    if (!getBee || !libId || !entries || entries.length === 0) return
    try {
      const bee = await getBee('sync')
      const batch = bee.batch()
      try {
        for (const [rel, value] of entries) {
          await batch.put(`${kind}/${libId}/${rel}`, value)
        }
        await batch.flush()
      } catch (err) {
        try { await batch.close() } catch {}
        throw err
      }
    } catch (err) {
      console.warn(`[SnapshotStore] writeRows(${kind}) error:`, err.message)
    }
  }

  static async delRows(getBee, kind, libId, rels) {
    if (!getBee || !libId || !rels || rels.length === 0) return
    try {
      const bee = await getBee('sync')
      const batch = bee.batch()
      try {
        for (const rel of rels) {
          await batch.del(`${kind}/${libId}/${rel}`)
        }
        await batch.flush()
      } catch (err) {
        try { await batch.close() } catch {}
        throw err
      }
    } catch (err) {
      console.warn(`[SnapshotStore] delRows(${kind}) error:`, err.message)
    }
  }

  static async persistConfig(getBee, lib) {
    if (!getBee || !lib || !lib.id) return
    try {
      const bee = await getBee('sync')
      await bee.put(lib.id, {
        id: lib.id,
        name: lib.name,
        localPath: lib.localPath,
        peerId: lib.peerId || '',
        mode: lib.mode || 'two-way',
        paused: !!lib.paused,
        accepted: typeof lib.accepted === 'boolean' ? lib.accepted : true,
        lastScanAt: lib.lastScanAt || 0,
        lastSyncAt: lib.lastSyncAt || 0
      })
    } catch (err) {
      console.warn('[SnapshotStore] persistConfig error:', err.message)
    }
  }

  static async removeLibrary(getBee, id) {
    if (!getBee || !id) return
    try {
      const bee = await getBee('sync')
      const batch = bee.batch()
      try {
        await batch.del(id)
        for (const kind of ['index', 'sent', 'remote', 'delivered', 'snapshot']) {
          for await (const node of bee.createReadStream(SnapshotStore.rowRange(kind, id))) {
            await batch.del(node.key)
          }
        }
        await batch.flush()
      } catch (err) {
        try { await batch.close() } catch {}
        throw err
      }
    } catch (err) {
      console.warn('[SnapshotStore] removeLibrary error:', err.message)
    }
  }

  static async recordDelivered(getBee, libId, rel, size, mtimeMs) {
    const safe = safeRelPath(rel)
    if (!safe || !libId || !getBee) return
    try {
      const s = Number(size) || 0
      const m = Number(mtimeMs) || 0
      if (s > 0 && m > 0) {
        await SnapshotStore.writeRows(getBee, 'delivered', libId, [[safe, { size: s, mtimeMs: m, sig: `${s}-${m}` }]])
      }
    } catch {}
  }

  static async isDelivered(getBee, libId, rel, mtimeMs) {
    const safe = safeRelPath(rel)
    if (!safe || !libId || !getBee) return false
    try {
      const bee = await getBee('sync')
      const node = await bee.get(`delivered/${libId}/${safe}`).catch(() => null)
      const del = node && node.value
      return !!(del && Math.abs(Number(del.mtimeMs || 0) - Number(mtimeMs || 0)) < VERIFY_TOLERANCE_MS)
    } catch {
      return false
    }
  }
}

module.exports = { SnapshotStore }
