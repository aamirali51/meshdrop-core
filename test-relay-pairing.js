'use strict'

// Repro: relay-mode pairing between two real engines. Forces relay-primary on
// both sides so ALL connectivity goes through the Cloudflare WSS relay — the
// same condition as the phone<->desktop relay pairing. Verifies:
//  1. trust:paired fires on BOTH engines
//  2. the swarm connection stays up (no 30s pairing-watchdog destroy loop)
//  3. both sides list the other as a trusted device

const os = require('os')
const path = require('path')
const fs = require('fs')
const { MeshEngine } = require('./index.js')

function mkEngine(name) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `md-relay-${name}-`))
  const engine = new MeshEngine({
    storageDir: path.join(dir, 'storage'),
    downloadsDir: path.join(dir, 'downloads'),
    deviceName: name,
    autoAcceptOffers: false,
    relayMode: 'relay-primary'
  })
  return engine
}

async function main() {
  const A = mkEngine('HostA')
  const B = mkEngine('JoinerB')

  const events = { A: [], B: [] }
  for (const [eng, key] of [[A, 'A'], [B, 'B']]) {
    eng.on('trust:paired', (d) => {
      events[key].push(`trust:paired code=${d.code} peer=${d.peer && d.peer.publicKey && d.peer.publicKey.slice(0, 12)}`)
      console.log(`[${key}] trust:paired`, JSON.stringify({ code: d.code, pk: d.peer && d.peer.publicKey && d.peer.publicKey.slice(0, 12) }))
    })
    eng.on('peer:connected', (d) => {
      events[key].push(`peer:connected ${d.id && d.id.slice(0, 12)}`)
      console.log(`[${key}] peer:connected ${d.id && d.id.slice(0, 12)}`)
    })
    eng.on('peer:disconnected', (d) => {
      events[key].push(`peer:disconnected ${d.id && d.id.slice(0, 12)}`)
      console.log(`[${key}] peer:disconnected ${d.id && d.id.slice(0, 12)}`)
    })
    eng.on('pairing:failed', (d) => {
      console.log(`[${key}] pairing:failed`, JSON.stringify(d))
    })
  }

  console.log('starting engines...')
  await A.start()
  await B.start()

  const codeRes = await A.trustManager.getOrCreatePairingCode()
  const code = codeRes.code || codeRes
  console.log('host code:', code)

  console.log('B pairing with code...')
  const pairPromise = B.pairWithCode(code).then(
    (peer) => {
      console.log('[B] pairWithCode RESOLVED:', JSON.stringify({ id: peer && peer.id, name: peer && peer.name }))
      return true
    },
    (err) => {
      console.log('[B] pairWithCode REJECTED:', err.message)
      return false
    }
  )

  const pairedB = await Promise.race([pairPromise, new Promise((r) => setTimeout(() => r('TIMEOUT-90s'), 90000))])
  console.log('pair outcome:', pairedB)

  // Watch the connection stability for 45 more seconds: count flaps.
  const startDisc = events.A.filter((e) => e.startsWith('peer:disconnected')).length
  await new Promise((r) => setTimeout(r, 45000))
  const discA = events.A.filter((e) => e.startsWith('peer:disconnected')).length - startDisc
  const discB = events.B.filter((e) => e.startsWith('peer:disconnected')).length - startDisc

  const statusA = A.getStatus()
  const statusB = B.getStatus()
  console.log('--- RESULTS ---')
  console.log('A status:', JSON.stringify(statusA))
  console.log('B status:', JSON.stringify(statusB))
  console.log('flaps in 45s watch window: A=' + discA, 'B=' + discB)
  console.log('A devices:', JSON.stringify((await A.listDevices() || []).map((d) => ({ id: d.id && d.id.slice(0, 10), name: d.name, trusted: d.isTrusted, online: d.isOnline, relayed: d.relayed }))))
  console.log('B devices:', JSON.stringify((await B.listDevices() || []).map((d) => ({ id: d.id && d.id.slice(0, 10), name: d.name, trusted: d.isTrusted, online: d.isOnline, relayed: d.relayed }))))

  await A.stop().catch(() => {})
  await B.stop().catch(() => {})
  process.exit(0)
}

main().catch((err) => {
  console.error('repro crashed:', err)
  process.exit(1)
})
