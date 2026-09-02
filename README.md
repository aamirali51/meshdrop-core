# @mesh/core

Zero-cloud, P2P messenger + file-transfer engine extracted from the MeshDrop
app. Pure JavaScript, platform-agnostic (Node >= 18; runs on desktop
and mobile Node threads). **No Electron, no IPC, no cloud.**

Part of the [MeshDrop](https://github.com/aamirali51/meshdrop-app) ecosystem.

| Repository | Visibility | Contents |
|------------|-----------|----------|
| [meshdrop-app](https://github.com/aamirali51/meshdrop-app) | Private | Desktop + mobile clients |
| **meshdrop-core** (this repo) | Public | P2P engine — `@mesh/core` |
| [meshdrop-releases](https://github.com/aamirali51/meshdrop-releases) | Public | Release artifacts for the auto-updater |

## Install

```sh
npm install @mesh/core
```

Dependencies: `hyperswarm`, `hypercore-crypto`, `corestore`, `hyperbee`,
`protomux`, `compact-encoding` — the standard Holepunch P2P stack. No
Electron/pear/bare-\* modules.

## Usage

```js
const { MeshEngine } = require('@mesh/core')

const engine = new MeshEngine({
  storageDir: '/path/to/engine-data', // required
  downloadsDir: '/path/to/downloads', // optional, defaults to <storageDir>/downloads
  deviceName: 'My Laptop', // optional, defaults to hostname
  autoAcceptOffers: true, // optional, default true (headless-friendly)
  autoTrustLAN: false, // optional, default FALSE — devices must pair via the code handshake
  lanDiscovery: true // optional, default true (UDP multicast)
})

engine.on('peer:connected', (device) => {})
engine.on('peer:disconnected', ({ id }) => {})
engine.on('trust:paired', ({ peer, code }) => {})
engine.on('transfer:offer', (offer) => {})
engine.on('transfer:progress', (p) => {})
engine.on('transfer:completed', (record) => {})
engine.on('transfer:failed', (record) => {})
engine.on('error', (err) => {})

await engine.start()

// Host side: show the pairing code
const { deviceId, publicKey, pairingCode } = engine.getIdentity()

// Joiner side: pair with the code (resolves on verified mutual trust)
const peer = await engine.pairWithCode('MD-XXXX-XXXX-XXXX-XXXX')

// Send a file to the paired peer (peerId = peer.publicKey from trust:paired)
await engine.offerFile(peer.publicKey, '/absolute/path/to/file.bin')
```

## API

| Method                                             | Description                                                                                    |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `constructor(config)`                              | `{ storageDir, downloadsDir, deviceName, autoAcceptOffers, autoTrustLAN, lanDiscovery }`       |
| `await start()`                                    | Boot the swarm, storage, engines; join the DHT; prime the pairing code                         |
| `await stop()`                                     | Tear down swarm, engines, stores                                                               |
| `getIdentity()`                                    | `{ deviceId, publicKey, pairingCode }` — `publicKey` is the noise peer key used in `offerFile` |
| `await pairWithCode(code, { timeoutMs })`          | Challenge-response pairing; resolves with the paired device record                             |
| `await offerFile(peerId, filePath)`                | Offer a local file; returns the send-transfer record                                           |
| `await acceptTransfer(id)` / `declineTransfer(id)` | Approve / decline a pending incoming offer                                                     |
| `await listTransfers()`                            | Persisted transfer records                                                                     |
| `getPeers()`                                       | Connected, handshake-complete peer device records                                              |
| `getStatus()`                                      | `{ connected, peerCount, relayedPeerCount, directPeerCount }`                                  |

## Events

`peer:connected`, `peer:disconnected`, `trust:paired`, `transfer:offer`,
`transfer:progress`, `transfer:completed`, `transfer:failed`, `error`.

## How it works

- **Identity**: a persistent noise keypair (`<storageDir>/noise-keypair.json`)
  doubles as the peer id. Trust, device records and reconnects key on it.
- **Discovery**: each engine announces on its identity topic and on the
  pairing topic of its active code; optional UDP multicast LAN discovery
  (`239.255.255.250:39001`) auto-trusts local peers.
- **Pairing**: `MD-XXXX-XXXX-XXXX-XXXX` codes. Both peers join the code's DHT
  topic; each proves knowledge of the code with a keyed HMAC-SHA256 over a
  random nonce (challenge-response). Trust is only granted on verification.
- **Transfers**: SHA-256 block manifests (64 KiB blocks) + whole-file checksum
  over a Corestore-backed exchange store. Files land in a per-transfer staging
  dir and are atomically renamed after checksum verification. Resume and
  cancellation are supported (same core, stored byte offset).

📖 **[Internals deep-dive](docs/INTERNALS.md)** — how pairing, the relay
fallback, the three transfer paths, and watch party media actually work,
with an honest security-model section.

## Test (no Electron required)

```sh
node test.js
```

Spawns two real `MeshEngine` instances in separate processes and storage
directories, pairs them over the public DHT, transfers a file, and verifies
the received bytes against the source hash.

## License

MIT — see [LICENSE](LICENSE).
