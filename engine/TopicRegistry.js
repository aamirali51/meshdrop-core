'use strict'

// TopicRegistry tracks every topic join/leave with refcounting so cleanup is
// predictable: peers join the same topic from multiple flows (pairing, drops,
// reconnect), and `leave` only detaches when the last reference is gone.

class TopicRegistry {
  constructor({ computeTopicHash, swarm }) {
    this.computeTopicHash = computeTopicHash
    this.swarm = swarm
    this.counts = new Map() // label -> refcount
  }

  join(label, opts) {
    const topicHash = this.computeTopicHash(label)
    const count = (this.counts.get(label) || 0) + 1
    this.counts.set(label, count)
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
    const count = this.counts.get(label)
    if (!count) return
    if (count > 1) {
      this.counts.set(label, count - 1)
      return
    }
    this.counts.delete(label)
    const topicHash = this.computeTopicHash(label)
    this.swarm.leave(topicHash)
    this.swarm.flush().catch(() => {})
  }

  // Drop all references (used on shutdown / share cleanup).
  leaveAll() {
    for (const label of this.counts.keys()) this.leave(label)
  }

  count(label) {
    return this.counts.get(label) || 0
  }

  activeLabels() {
    return Array.from(this.counts.keys())
  }
}

module.exports = TopicRegistry
