# MeshDrop Internals: how a zero-cloud file transfer actually works

*A tour of @mesh/core — the P2P engine behind MeshDrop. Written for the
Holepunch community after the "explain the internals" ask. Everything here
is in the open: [meshdrop-core](https://github.com/aamirali51/meshdrop-core)
is public, and this post links the exact modules.*

MeshDrop has no accounts, no cloud storage, and no signaling server you can
point at a username. Two devices find each other over the DHT, prove to each
other that a human exchanged a code, and move bytes directly. When direct
connectivity fails, a relay fallback steps in — and we'll be precise about
what that relay can and cannot see, because we got (fair) criticism about it.

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
relay hop can't stall the handshake.

## 3. The relay fallback: lazy, minimized, honest

This is the part that got (justified) pushback, so here's the full picture.

**When it's used:** the relay exists for networks where UDP is blocked or
NAT holepunching fails — CGNAT, some hotspots, hotspot+VPN combos. In `auto`
mode (the default) the relay is **not** contacted at boot. It engages when:

1. the user shows pairing intent (pairing screen opened, or a code entered),
   or
2. the swarm has been up with **zero peers** for 30 seconds — the signature
   of a challenged network.

`direct-only` mode never starts it; you can also point it at **your own**
relay URL in settings.

**What it sees:** pairing challenges sent over the relay carry only a code id
and a nonce — **no device identity** (we stripped it after the community
review). File bytes *never* touch the relay: transfers are direct hypercore
replication over HyperDHT. The relay sees opaque transport frames plus
pairing metadata (code usage, IPs, timing) — and the code topics self-expire.

**What it is:** a stateless Cloudflare Worker + KV. `POST /poll?topic=`
appends a message to a per-topic list (120s TTL); `GET /poll?topic=` returns
it. Clients poll, dedupe by message id, and verify MACs locally. The relay
cannot decrypt anything or forge a MAC.

We also back off automatically: if a peer never answers pairing challenges
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
  payloads; the relay never sees a byte of file data.
- **Capabilities**: codes are the permission. Drop codes are one-time with
  TTL and download caps; room codes are live while the party runs; pairing
  codes are long-lived by design (that's what "pair my devices" means) and
  rotate on deletion.
- **Trust gates**: file offers are only accepted from authenticated (paired)
  peers. Claims, watch party, and pairing itself are the code-capability
  paths.
- **Known limits we'll keep working**: the macOS builds are unsigned (you
  need an Apple Developer account to fix properly); the fallback relay is a
  centralized component — it's metadata-minimized and lazy, and self-hosted
  / community relays are supported via a settings URL, but federation is
  future work; KV-based relays are eventually consistent across regions.

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
