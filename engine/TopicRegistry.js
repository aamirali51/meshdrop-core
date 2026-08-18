'use strict'

// TopicRegistry tracks every topic join/leave with refcounting so cleanup is
// predictable: peers join the same topic from multiple flows (pairing, drops,
// reconnect), and `leave` only detaches when the last reference is gone.

class TopicRegistry {
  constructor({ computeTopicHash, swarm }) {
    this.computeTopicHash = computeTopicHash
    this.swarm = swarm
    this.counts = new Map() // label -> { count, opts }
  }

  join(label, opts) {
    const topicHash = this.computeTopicHash(label)
    const entry = this.counts.get(label)
    const count = entry ? entry.count + 1 : 1
    this.counts.set(label, { count, opts })
    if (count === 1) {
      this.swarm.join(topicHash, opts)
      this.swarm.flush().catch(() => {})
    }
    return topicHash
  }

  // Join a long-lived topic once. Reconnect timers and repeated UI requests
  // should not create unbounded references that can never be released.
  ensure(label, opts) {
    if (this.counts.has(label)) return this.computeTopicHash(label)
    return this.join(label, opts)
  }

  leave(label) {
    const entry = this.counts.get(label)
    if (!entry) return
    if (entry.count > 1) {
      this.counts.set(label, { count: entry.count - 1, opts: entry.opts })
      return
    }
    this.counts.delete(label)
    const topicHash = this.computeTopicHash(label)
    this.swarm.leave(topicHash)
    this.swarm.flush().catch(() => {})
  }

  // Re-join every tracked label on a (possibly new) swarm. Called after
  // refreshNetwork() rebuilds the Hyperswarm: the old node's announcements
  // died with its sockets, and only re-announcing on the fresh DHT node makes
  // this device findable again. Refcounts are preserved untouched.
  reattach(swarm) {
    this.swarm = swarm
    for (const [label, entry] of this.counts.entries()) {
      if (entry && entry.count > 0) {
        this.swarm.join(this.computeTopicHash(label), entry.opts)
      }
    }
    this.swarm.flush().catch(() => {})
  }

  // Drop all references (used on shutdown / share cleanup).
  leaveAll() {
    for (const label of this.counts.keys()) this.leave(label)
  }

  count(label) {
    const entry = this.counts.get(label)
    return entry ? entry.count : 0
  }

  activeLabels() {
    return Array.from(this.counts.keys())
  }
}

module.exports = TopicRegistry
