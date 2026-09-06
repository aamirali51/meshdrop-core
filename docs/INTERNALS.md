# MeshDrop Internals: how a zero-cloud file transfer actually works

*A tour of @mesh/core — the P2P engine behind MeshDrop. Written for the
Holepunch community after the "explain the internals" ask. Everything here
is in the open: [meshdrop-core](https://github.com/aamirali51/meshdrop-core)
is public, and this post links the exact modules.*

MeshDrop has no accounts, no cloud storage, and no signaling server you can
point at a username. Two devices find each other over the DHT, prove to each
other that a human exchanged a code, and move bytes directly. When direct
connectivity fails, HyperDHT's own relaying steps in — no separate relay
component, nothing but the DHT (see §3).

---

## 1. The stack in one paragraph

Everything runs on the standard Holepunch stack: **Hyperswarm + HyperDHT**
for discovery and transport, **hypercore** (append-only, replicated logs)
for bulk data, **protomux** for multiplexing channels over one connection,
and **noise** encryption at the transport layer. The engine (`MeshEngine`)
composes four subsystems:

```
MeshEngine
├── connections/   swarm setup, signaling router, pairing, claims, keepalive
├── engine/TrustManager       pairing codes, challenge-response, revocation
├── engine/TransferEngine     send/receive state machine, integrity, resume
├── engine/SyncEngine         folder sync (push / two-way / receive-only)
└── engine/WatchPartyManager  media staging + playback sync
```

Every device generates a **noise keypair on first boot**. The public key *is*
the device identity — there is no server-issued account. Each device listens
on a DHT topic derived from its own public key (`p2p-peer-<key>`), which is
how "find my device" works without a registry.

## 2. Pairing: a code is a capability, not a password

Devices pair with an 80-bit code shown as `MD-XXXX-XXXX-XXXX-XXXX`.

- Every device generates a **permanent host code** at first boot.
- The device that *enters* a code becomes the **joiner**: it registers the
  code as an ephemeral secret (15-minute TTL) and both sides prove knowledge
  of it to each other.
- Proof is a **keyed MAC**: the challenger sends `nonce = random(16)`, the
  prover answers with `MAC(code, nonce)`. Only a correct MAC — computable
  only by holding the code — grants trust. No code ever crosses the wire.
- Trust is **mutual and symmetric**: because both sides hold the same code
  during pairing, both challenge and both verify. One code entry pairs two
  devices.
- Deleting a device **rotates the host code**, so a deleted peer's memorized
  code is dead. A deleted peer can only come back by pairing with the
  *current* code — that makes deletion permanent without maintaining a
  blacklist.

Trusted keys are persisted; connections between trusted peers skip pairing
and exchange device identities directly. LAN auto-trust is **off by default**.

One design note we're proud of: pairing **actively drives itself**. While
`pairWithCode` is pending, the joiner re-announces challenges and re-attempts
direct connectivity every few seconds — so one missed DHT connect or a slow
relayed hop can't stall the handshake.

## 3. Relaying is HyperDHT-native — there is no hosted relay

An earlier design ran pairing/signaling through a hosted Cloudflare WSS/KV
relay (`connections/relayClient.js`). That component is **gone** (removed in
v1.0.59): there is no hosted relay, no custom relay URL, no `relayMode`.
Everything is HyperDHT.

**Discovery + signaling:** pairing and all device discovery run **only** over
HyperDHT topics (`p2p-pair-<code>` and the other `TopicRegistry` topics).
`test-cross-network-dht-pairing.js` proves it: two full `MeshEngine` instances
in separate processes — `lanDiscovery: false`, `autoTrustLAN: false` — pair
across networks over the DHT alone, and it asserts the relay module no longer
exists.

**Connectivity fallback:** when direct UDP holepunching fails (symmetric NAT,
CGNAT, TCP-only VPNs), hyperswarm reconnects through HyperDHT's own
`relayThrough` hook (`meshdrop-core/index.js:261`), which picks a relay **in
this order** (`pickOwnPeerRelay` → `pickRelayNode`):

1. **Your own trusted desktop peer** (`preferOwnRelay`, the default): an
   online, paired, desktop-class device acts as your relay — private to your
   mesh, ciphertext only.
2. **Public Holepunch bootstrap nodes** (`88.99.3.86`, `142.93.90.113`,
   `138.68.147.8`), falling back to the longest-resident routing-table node.

The relay just tunnels the noise stream over TCP — it cannot decrypt anything
or forge a MAC, and file payloads are end-to-end encrypted regardless.

Pairing backs off automatically: if a peer never answers pairing challenges
(nobody is pairing), automatic challenges pause (15s → 30min exponential
ladder) instead of looping connect/destroy forever — battery and log friendly.

## 4. File transfer: three paths, one integrity model

All transfers share the integrity scheme — a **manifest** (block 0 of a
hypercore) holding a SHA-256 per 64 KiB block plus a whole-file checksum.
Receivers verify every block against the manifest and the file against the
checksum before reporting `completed`. Interrupted transfers park as
resumable: a `.part` file in a per-transfer staging dir + a persisted byte
offset.

**a) 1:1 sends — `offerFile()`, `source: 'stream'`.** Blocks stream straight
from the source file over a dedicated protomux channel
(`meshdrop-sync-v1`): manifest first, then blocks with per-block hashes,
windowed flow control (32 blocks in flight, ACK every 8), and a
receiver-verified `done` handshake. **Nothing is duplicated on the sender** —
no staging copy, ever. Resume is byte-exact from a partial `.part`.

**b) Drop codes — the WeTransfer flow.** `files.createCode` stages the file
into a per-share hypercore, and a one-time `DROP-XXXX` code is the claim
capability. Claimers prove the code, get per-core replication (never the
whole store), and pull with the same integrity pipeline. Multi-file and
folder shares stage one core per file, which lets one staged core serve many
claimers without re-reading the source.

**c) Folder sync — `SyncEngine`.** Libraries are one-way `push`
(phone → backup), `receive-only` (pure sink), or `two-way` (bidirectional
mirror). The sender announces a compact index; the receiver diffs against
what it already holds and requests only differences. Push mode stamps the
sender's original mtime onto received files, re-syncs skip identical files
via a batch pre-verify (zero transfers for unchanged folders), and
self-heals: delete a file on the receiver and the next round re-pushes it.
Two-way conflicts are preserved in `.meshdrop-trash`, never silently
overwritten.

## 5. Watch Party: media as a capability

Creating a room stages the media into a hypercore named after a
deterministic `watch-<room-code>` id. The room code — the same capability
model as drop codes — is what authorizes joining. On join, the host opens
**per-core replication** for that one core (never the whole exchange store)
and hands the guest the media descriptor. Guests stream progressively off
the growing staging file, and play/pause/seek syncs over the same signaling
channel. Announcements carry the room code (the join capability) but keep
host name and room title among paired peers only.

## 6. The security model, honestly

- **Content**: always end-to-end. Transport noise encryption + hypercore
  payloads; even relayed links only carry ciphertext (HyperDHT `relayThrough`).
- **Capabilities**: codes are the permission. Drop codes are one-time with
  TTL and download caps; room codes are live while the party runs; pairing
  codes are long-lived by design (that's what "pair my devices" means) and
  rotate on deletion.
- **Trust gates**: file offers are only accepted from authenticated (paired)
  peers. Claims, watch party, and pairing itself are the code-capability
  paths.
- **Known limits we'll keep working**: the macOS builds are unsigned (you
  need an Apple Developer account to fix properly). Relaying depends on
  HyperDHT `relayThrough` — via your own paired desktop peer or the public
  Holepunch bootstrap DHT nodes — carrying ciphertext only; there is no
  hosted or centralized relay component anymore.

## 7. Try it

Pair two devices, or run the engine headless:

```sh
git clone https://github.com/aamirali51/meshdrop-core
cd meshdrop-core && npm install && npm test   # 60/60 live checks, two real engines
```

The test suite pairs two real engines over the wire, re-pairs after device
deletion, claims drops, syncs folders, and resumes interrupted transfers.

---

*Questions, holes you want poked at, or internals you want expanded —
[open an issue](https://github.com/aamirali51/meshdrop-core/issues) or find
us in the Holepunch Discord.*
