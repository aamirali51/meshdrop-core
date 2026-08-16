'use strict'

// Shared pure helpers for the connections layer: classifying a socket address
// and detecting DHT-relayed connections. No engine state — import anywhere.

// Best-effort per-connection relay detection. hyperdht computes `relayed`
// internally (lib/connect.js stores it on the connect session) but does not
// expose it on the stream it hands to hyperswarm, so we derive it here from
// the signals the library does give us:
//   1. peerInfo.forceRelaying — hyperswarm sets this when a direct punch
//      failed and the connection was retried through a DHT relay.
//   2. Routing-table match — a relayed connection's socket talks to the DHT
//      relay node we dialed by key, which is a node in our routing table.
function isRelayedConnection(peerInfo, connection, dht) {
  if (peerInfo && peerInfo.forceRelaying) return true
  try {
    const remote =
      (connection &&
        (connection.remoteAddress ||
          (connection.rawStream && connection.rawStream.remoteAddress) ||
          (connection._socket && connection._socket.remoteAddress))) ||
      ''
    if (remote && dht && dht.nodes) {
      const host = remote.split(':')[0]
      // Relays are public DHT nodes; a private-range remote is a direct LAN
      // connection, never a relay.
      if (
        /^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|169\.254\.|127\.|::1|::ffff:127\.)/.test(
          host
        )
      ) {
        return false
      }
      for (let node = dht.nodes.latest; node; node = node.prev) {
        if (!node.host || !node.port) continue
        if (remote === node.host || remote === node.host + ':' + node.port) return true
      }
    }
  } catch {}
  return false
}

// Classify a socket address as LAN (private range / loopback) or internet.
function getTransferMethod(ipAddress, isClaim = false) {
  if (isClaim) return 'internet'
  if (!ipAddress) return 'internet'
  let ip = String(ipAddress)
    .trim()
    .replace(/^::ffff:/, '')
  if (ip.startsWith('[')) {
    const end = ip.indexOf(']')
    if (end !== -1) ip = ip.slice(1, end)
  } else if (ip.indexOf(':') === ip.lastIndexOf(':')) {
    ip = ip.replace(/:\d+$/, '')
  }
  if (ip === '127.0.0.1' || ip === 'localhost' || ip === '::1' || ip === '0.0.0.0') return 'lan'
  if (/^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|169\.254\.)/.test(ip)) return 'lan'
  return 'internet'
}

module.exports = { isRelayedConnection, getTransferMethod }
