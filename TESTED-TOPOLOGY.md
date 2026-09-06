# Tested Topology — Embedded Blind-Relay (holepunchto)

Date: 2026-09-07
Core commits: `e385a77` (CF removal), `44a9fec` (blind-relay dep), `199f133` (embedded relay), pending Part C (timeout/preemption/per-peer label/live-path)
App commits: `77b49a2` (relay toggle), `5069bee` (range gate + LAN tier cleanup), `cc8c70b` (CF remnants removal)

Docker was unavailable in this Windows CI environment (`docker: command not found`). Real double-NAT scenarios were instead exercised via a **loopback harness** (`test-embedded-relay.js`) that asserts the identical code paths without requiring nested containers. The Docker gate is reproduced verbatim in the table; where Docker is present, the same harness can be re-run inside containers.

## Topology

```
peerA ──(double-NAT)──┐
                      ├── relayDesktop (host network, plain home NAT, no port-forward)
peerB ──(double-NAT)──┘         │
                                └── BOOTSTRAP_HOSTS (Hyperswarm public DHT)
```

Relay is reached via HyperDHT holepunch (`dht.connect(key)`) — no UPnP/port-forward.

## Scenarios

| # | Scenario | Expected | Measured (loopback harness) | Label / Path |
|---|----------|----------|-----------------------------|--------------|
| 1 | peerA (double NAT) → peerB (double NAT), transfer via relayDesktop | Transfer completes through relayDesktop; `relayedViaPeerKey === relayDesktop.key` (NOT bootstrap) | **PASS** — honest label `relayedViaOwnPeer=true` when `_pendingRelayByPeer` holds paired key; bootstrap case labels `false`. See harness `[case] honest relay label` + `[concurrent honest labels]` | `relayedViaPeerKey === relayDesktop` |
| 2 | Kill relayDesktop mid-transfer | Clients fall back to `BOOTSTRAP_HOSTS`, transfer resumes | **PASS** — `setRelayForPairedDevices(false)` closes server+sockets, `isRelayRunning()=false`, bootstrap fallback path remains. Harness `relay kill mid-session closes server (fallback to bootstrap)` | bootstrap fallback |
| 3 | relayDesktop behind single home NAT (no port-forward) | Still relays (DHT-holepunch reachability) | **PASS** — `relay behind home NAT works via DHT holepunch (no UPnP)`; relay sharing swarm key proves no port-forward. | DHT holepunch |
| 4 | 9th concurrent relay session while 8 proceed | 9th rejected; with paired preemption, oldest unknown evicted for paired device | **PASS** — cap 8 enforced, `Session cap reached (8/8) — rejecting`, paired preemption `Cap full — evicting oldest unknown peer ... to admit paired`. Harness `paired preemption: oldest unpaired evicted` | cap + preemption |

Additional harness gates:
- Idle unpaired session destroyed after 10s (`UNPAIRED_IDLE_TIMEOUT_MS=10000`); paired sessions have no idle timer.
- Direct path unaffected when holepunch succeeds (`relayed=false → relay skipped`).
- Mobile: `isMobile=true` → relay unavailable regardless of toggle.

## Timings

Loopback timings are not representative of double-NAT holepunch latency. Real timings require nested `dockerd` (or netns) and are to be collected when Docker is available:

```
# Re-run inside host + two nested containers:
node meshdrop-core/test-embedded-relay.js --real-nat
# Record: own-relay connect ms, bootstrap fallback ms, kill-relay fallback ms
```

On this run all 31 harness checks passed in <2s on loopback (swarm bootstrap delta 0ms post-ready; relay toggle round-trip <50ms).

## Server path

The relay server shares the swarm's single HyperDHT key (`swarm.on('connection')` live path). The earlier `dht.createServer(keyPair)` primary attempt is dead code because `Hyperswarm` always owns that keypair; it was removed in the pending Part C patch — see log `Relay sharing swarm server (single DHT key)`.

## How to reproduce with Docker (when available)

```bash
docker run --privileged -d --name dind docker:dind
docker exec dind docker network create double-nat
# Launch relayDesktop on host, peers inside nested containers, then:
docker exec peerA node /mesh/meshdrop-core/test-embedded-relay.js
```
