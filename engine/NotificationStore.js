'use strict'

// Simple in-memory notification store (capped). Every added notification is
// pushed to the renderer over the engine event bus (notification:received),
// so the bell/drawer updates live.

const { EVENTS } = require('../protocol.js')

class NotificationStore {
  constructor({ emit }) {
    this.emit = emit
    this.notifications = [
      {
        id: 'notif-init-1',
        title: 'Mesh P2P Engine Active',
        description: 'Bound to Hyperswarm DHT with secure challenge pairing.',
        type: 'info',
        timestamp: new Date().toISOString(),
        read: false
      }
    ]
  }

  getNotifications() {
    return this.notifications
  }

  addNotification(title, description, type = 'info') {
    const item = {
      id: `notif-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      title,
      description,
      type,
      timestamp: new Date().toISOString(),
      read: false
    }
    this.notifications.unshift(item)
    this.notifications = this.notifications.slice(0, 50)
    try {
      this.emit(EVENTS.NOTIFICATION_RECEIVED, item)
    } catch {}
    return item
  }

  markAllRead() {
    this.notifications.forEach((n) => (n.read = true))
    return this.notifications
  }

  clear() {
    this.notifications = []
    return []
  }
}

module.exports = NotificationStore
