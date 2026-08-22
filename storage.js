'use strict'

// Engine storage: the private metadata Corestore, the exchange (file-only)
// Corestore, Hyperbee factories, topic hashing, and identity management.
//
// Storage roots come from the engine config (storageDir) — never from an
// Electron app.getPath() default.

const { path, fsp, isBare } = require('./compat.js')

// Android FUSE storage (Android 11+; CalyxOS, stock) does not support flock —
// fd-lock's tryLock returns false and hypercore-storage throws
// "File descriptor could not be locked" on every core open. The flock is a
// single-process safety (two processes sharing a corestore); the engine is
// single-process, so on Bare/Android we patch fd-lock and fs-native-extensions directly
// to skip the native lock entirely. Desktop keeps the lock.
try {
  if (isBare || (typeof process !== 'undefined' && process.platform === 'android')) {
    try {
      const fsx = require('fs-native-extensions')
      if (fsx) {
        fsx.tryLock = function tryLock() { return true }
        fsx.waitForLock = async function waitForLock() { return true }
        fsx.waitForLockSync = function waitForLockSync() { return true }
        fsx.tryDowngradeLock = function tryDowngradeLock() { return true }
        fsx.waitForDowngradeLock = async function waitForDowngradeLock() { return true }
        fsx.waitForDowngradeLockSync = function waitForDowngradeLockSync() { return true }
        fsx.tryUpgradeLock = function tryUpgradeLock() { return true }
        fsx.waitForUpgradeLock = async function waitForUpgradeLock() { return true }
        fsx.waitForUpgradeLockSync = function waitForUpgradeLockSync() { return true }
        fsx.unlock = function unlock() {}
      }
    } catch {}

    try {
      const FDLock = require('fd-lock')
      if (FDLock && FDLock.prototype) {
        FDLock.prototype._resume = async function _resume() {
          this._locked = true
        }
        FDLock.prototype._suspend = async function _suspend() {
          this._locked = false
        }
      }
    } catch {}

    try {
      const DeviceFile = require('device-file')
      if (typeof DeviceFile === 'function') {
        class NoLockDeviceFile extends DeviceFile {
          constructor(filename, opts = {}) {
            super(filename, { ...opts, lock: false })
          }
        }
        for (const k of Object.keys(DeviceFile)) {
          if (typeof DeviceFile[k] === 'function') NoLockDeviceFile[k] = DeviceFile[k]
        }
        if (typeof require.cache !== 'undefined' && require.resolve) {
          delete require.cache[require.resolve('device-file')]
          require.cache[require.resolve('device-file')] = {
            id: require.resolve('device-file'),
            filename: require.resolve('device-file'),
            loaded: true,
            exports: NoLockDeviceFile
          }
        }
      }
    } catch {}

    console.log('[Storage] Bare/Android platform: flock disabled (bypassing Android FUSE lock error)')
  }
} catch (err) {
  console.warn('[Storage] device-file lock patch skipped:', err.message)
}

const Corestore = require('corestore')
const Hyperbee = require('hyperbee')
const hcrypto = require('hypercore-crypto')
const { deriveDeviceId } = require('./crypto.js')

function createStorage({ storageDir, downloadsDir, deviceName }) {
  // Private metadata store (identity, devices, history, settings). NEVER
  // replicated. Only the exchange store is exposed to authenticated peers.
  let store = new Corestore(path.join(storageDir, 'corestore'))
  let exchangeStore = new Corestore(path.join(storageDir, 'exchange'))

  // Cache Hyperbee instances — one per named bee, not one per call
  const beeCache = new Map()

  async function _healCorestore(targetDir, isExchange = false) {
    console.warn(`[Storage] Corrupt device file detected in ${targetDir}. Self-healing reset...`)
    try {
      if (isExchange) await exchangeStore.close()
      else await store.close()
    } catch {}
    try {
      const fs = require('fs')
      fs.rmSync(targetDir, { recursive: true, force: true })
    } catch {}
    if (isExchange) {
      exchangeStore = new Corestore(targetDir)
    } else {
      store = new Corestore(targetDir)
      beeCache.clear()
    }
  }

  async function getBee(name) {
    if (beeCache.has(name)) return beeCache.get(name)
    let core = store.get({ name })
    try {
      await core.ready()
    } catch (err) {
      if (
        err?.code === 'DEVICE_FILE' ||
        err?.message?.includes('device file') ||
        err?.message?.includes('could not be locked')
      ) {
        await _healCorestore(path.join(storageDir, 'corestore'), false)
        core = store.get({ name })
        await core.ready()
      } else {
        throw err
      }
    }
    const bee = new Hyperbee(core, { keyEncoding: 'utf-8', valueEncoding: 'json' })
    beeCache.set(name, bee)
    return bee
  }

  // Compute a 32-byte DHT topic hash for a given label string (BLAKE2b-256,
  // identical to the old worker's hypercore-crypto topic hashing).
  function computeTopicHash(label) {
    const buf = Buffer.from(String(label))
    if (typeof hcrypto.data === 'function') return hcrypto.data(buf)
    if (typeof hcrypto.hash === 'function') return hcrypto.hash([buf])
    try {
      const mod = 'crypto'
      return require(mod).createHash('sha256').update(buf).digest()
    } catch {
      return buf
    }
  }

  let deviceIdentity = null
  let info = { name: 'Unknown Device', os: 'Unknown' }

  function setDeviceInfo(next) {
    info = next
  }

  async function initIdentity() {
    // Reuse the cached 'identity' bee. A second Hyperbee instance over the
    // same core (as done before) corrupted the store: two instances append
    // their own operations and reads fail with DECODING_ERROR.
    let identityCore = store.get({ name: 'identity' })
    try {
      await identityCore.ready()
    } catch (err) {
      if (
        err?.code === 'DEVICE_FILE' ||
        err?.message?.includes('device file') ||
        err?.message?.includes('could not be locked')
      ) {
        await _healCorestore(path.join(storageDir, 'corestore'), false)
        identityCore = store.get({ name: 'identity' })
        await identityCore.ready()
      } else {
        throw err
      }
    }
    const bee = await getBee('identity')

    const b4a = require('b4a')
    const keyHex = b4a.toString(identityCore.key, 'hex')
    const derivedId = deriveDeviceId(keyHex)
    let identity = await bee.get('device')
    if (!identity) {
      identity = {
        id: derivedId,
        publicKey: keyHex,
        name: info.name,
        os: info.os,
        createdAt: Date.now()
      }
      await bee.put('device', identity)
    } else {
      // Keep device name updated with the configured name
      identity = identity.value || identity
      // Migrate legacy id (identity core key slice) to the non-derivable derived id
      if (identity.id !== derivedId) {
        identity.id = derivedId
        await bee.put('device', identity)
      }
      if (info.name && identity.name !== info.name) {
        identity.name = info.name
        await bee.put('device', identity)
      }
    }

    deviceIdentity = identity.value || identity
    return deviceIdentity
  }

  async function storeReady() {
    await store.ready()
    await exchangeStore.ready()
  }

  // Rebuild the metadata Corestore to reclaim RocksDB blob garbage. The sync
  // library records embed full file indexes; repeated rewrites of a large
  // index leave multi-hundred-MB blob versions behind that RocksDB does not
  // GC promptly. All metadata is small and re-writable, so the rebuild reads
  // every row, wipes the store, and rewrites the current state.
  // NOTE: the identity core key changes, so the derived device id changes —
  // the noise keypair (mesh_store/noise-keypair.json) and the swarm identity
  // are preserved, so connections, trust and transfers keep working.
  async function compactStore() {
    const names = ['identity', 'devices', 'settings', 'sync', 'transfers', 'history', 'shared']
    const rows = new Map()
    for (const name of names) {
      try {
        const bee = await getBee(name)
        const list = []
        for await (const node of bee.createReadStream()) {
          list.push({ key: node.key, value: node.value })
        }
        rows.set(name, list)
      } catch (err) {
        console.warn(`[Storage] compactStore read ${name} failed:`, err.message)
      }
    }
    try {
      await store.close()
    } catch {}
    try {
      const fs = require('fs')
      fs.rmSync(path.join(storageDir, 'corestore'), { recursive: true, force: true })
    } catch (err) {
      console.warn('[Storage] compactStore wipe failed:', err.message)
    }
    store = new Corestore(path.join(storageDir, 'corestore'))
    beeCache.clear()
    await store.ready()
    for (const [name, list] of rows) {
      if (list.length === 0) continue
      const bee = await getBee(name)
      for (const { key, value } of list) {
        await bee.put(key, value).catch(() => {})
      }
    }
    console.log('[Storage] metadata store rebuilt')
  }

  return {
    store,
    exchangeStore,
    getBee,
    computeTopicHash,
    initIdentity,
    getDeviceIdentity: () => deviceIdentity,
    setDeviceIdentity: (v) => {
      deviceIdentity = v
    },
    setDeviceInfo,
    storeReady,
    compactStore,
    downloadsDir
  }
}

module.exports = { createStorage }
