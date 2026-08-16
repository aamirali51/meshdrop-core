'use strict'

// Cross-runtime compatibility layer for Node.js (desktop) and Bare (mobile).
const isBare = typeof Bare !== 'undefined'

let path, fs, fsp, os, events, EventEmitter

if (isBare) {
  path = require('bare-path')
  fs = require('bare-fs')
  fsp = require('bare-fs/promises')
  os = require('bare-os')
  events = require('bare-events')
  EventEmitter = events.EventEmitter || events

  const origMkdir = fsp.mkdir.bind(fsp)
  fsp.mkdir = async function mkdir(dirPath, opts) {
    if (opts && opts.recursive) {
      if (fs.existsSync(dirPath)) return
      const parent = path.dirname(dirPath)
      if (parent && parent !== dirPath && parent !== '/' && parent !== '.') {
        await mkdir(parent, opts)
      }
      try {
        await origMkdir(dirPath)
      } catch (err) {
        if (!fs.existsSync(dirPath)) throw err
      }
      return
    }
    return origMkdir(dirPath)
  }

  if (!fsp.copyFile) {
    fsp.copyFile = async function (src, dest) {
      const data = await fsp.readFile(src)
      await fsp.writeFile(dest, data)
    }
  }
} else {
  path = eval("require('path')")
  fs = eval("require('fs')")
  fsp = eval("require('fs/promises')")
  os = eval("require('os')")
  events = eval("require('events')")
  EventEmitter = events.EventEmitter || events
}

module.exports = {
  isBare,
  path,
  fs,
  fsp,
  os,
  events,
  EventEmitter
}
