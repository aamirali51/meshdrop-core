'use strict'

// Per-device video capability negotiation for Watch Party (direct -> remux ->
// refuse). Pure and dependency-free so it can be unit-tested without a DHT or
// an engine instance, and shared by the core engine, the desktop app and any
// future consumer.
//
// Model (PearCinema-inspired): each viewer device declares what it can actually
// play; the host decides the cheapest honest route and serves the right bytes.
// Remux here is always a container/audio rebuild with the video stream COPIED
// (-c:v copy) — no re-encoding, so no GPU and near-zero CPU.

const VIDEO_CONTAINERS = {
  mp4: 'mp4',
  m4v: 'mp4',
  mov: 'mov',
  mkv: 'mkv',
  webm: 'webm',
  avi: 'avi',
  ts: 'ts',
  m2ts: 'ts',
  mts: 'ts',
  flv: 'flv',
  wmv: 'wmv'
}

// Canonical video codec names (ffprobe's `codec_name` and MSE's codec strings
// both map onto these). `wmv3`/`vc1` are listed separately so a device that
// says it can play vc1 still matches a file ffprobe reports as wmv3.
const VIDEO_CODECS = {
  h264: 'h264',
  avc1: 'h264',
  hevc: 'hevc',
  h265: 'hevc',
  hev1: 'hevc',
  hvc1: 'hevc',
  vp9: 'vp9',
  vp8: 'vp8',
  av1: 'av1',
  mpeg4: 'mpeg4',
  msmpeg4v3: 'mpeg4',
  mpeg2video: 'mpeg2',
  theora: 'theora',
  wmv3: 'vc1',
  vc1: 'vc1'
}

const AUDIO_CODECS = {
  aac: 'aac',
  mp3: 'mp3',
  ac3: 'ac3',
  eac3: 'eac3',
  dts: 'dts',
  opus: 'opus',
  vorbis: 'vorbis',
  flac: 'flac',
  pcm_s16le: 'pcm',
  pcm_s24le: 'pcm'
}

// Streaming transports a viewer may support. `mpegts` covers mpegts.js in the
// browser and native TS demuxing on Android; `hls` covers Hls.js / native HLS;
// `mse` covers MediaSource Extensions.
const PROTOCOLS = {
  mpegts: 'mpegts',
  hls: 'hls',
  mse: 'mse'
}

// Map an ffprobe codec_name onto the canonical vocabulary. `input` may be a
// string or undefined; returns null for unknown.
function normalizeVideoCodec(input) {
  if (typeof input !== 'string') return null
  return VIDEO_CODECS[input.trim().toLowerCase()] || null
}

function normalizeAudioCodec(input) {
  if (typeof input !== 'string') return null
  return AUDIO_CODECS[input.trim().toLowerCase()] || null
}

// MIME subtype -> canonical container (the browser's mpegts.js demuxes TS/FLV,
// so a device that declares the mpegts protocol can also declare those
// containers).
const MIME_CONTAINERS = {
  mp4: 'mp4',
  'x-m4v': 'mp4',
  quicktime: 'mov',
  'x-matroska': 'mkv',
  webm: 'webm',
  'x-msvideo': 'avi',
  mp2t: 'ts',
  'x-flv': 'flv',
  'x-ms-wmv': 'wmv'
}

// Strip a container string (a filename extension or MIME subtype) to its
// canonical form.
function normalizeContainer(input) {
  if (typeof input !== 'string') return null
  const s = input.trim().toLowerCase()
  const last = s.slice(s.lastIndexOf('/') + 1)
  return VIDEO_CONTAINERS[last] || MIME_CONTAINERS[last] || null
}

// Whitelist/validate an incoming capability object. Anything a viewer sends
// over the wire is untrusted; we keep only the fields we understand and only
// the values in our vocabulary. Unknown values are dropped rather than
// forwarded.
function normalizeCapabilities(raw) {
  const out = {
    videoCodecs: [],
    audioCodecs: [],
    containers: [],
    protocols: []
  }
  if (!raw || typeof raw !== 'object') return out
  for (const c of Array.isArray(raw.videoCodecs) ? raw.videoCodecs : []) {
    const n = normalizeVideoCodec(c)
    if (n && !out.videoCodecs.includes(n)) out.videoCodecs.push(n)
  }
  for (const c of Array.isArray(raw.audioCodecs) ? raw.audioCodecs : []) {
    const n = normalizeAudioCodec(c)
    if (n && !out.audioCodecs.includes(n)) out.audioCodecs.push(n)
  }
  for (const c of Array.isArray(raw.containers) ? raw.containers : []) {
    const n = normalizeContainer(c)
    if (n && !out.containers.includes(n)) out.containers.push(n)
  }
  for (const c of Array.isArray(raw.protocols) ? raw.protocols : []) {
    if (PROTOCOLS[c] && !out.protocols.includes(c)) out.protocols.push(c)
  }
  return out
}

// Build a fileMeta object from a filename (extension -> container) and optional
// ffprobe results. `probe` is `{ videoCodec, audioCodec }` where the codec
// fields are already canonical (see probeFile).
function fileMetaFrom({ filename = '', container = null, videoCodec = null, audioCodec = null } = {}) {
  let ext = null
  if (typeof filename === 'string') {
    const m = /\.([a-z0-9]{1,5})$/i.exec(filename)
    if (m) ext = m[1].toLowerCase()
  }
  const resolvedContainer = container || (ext ? VIDEO_CONTAINERS[ext] : null) || 'unknown'
  return {
    filename: String(filename || ''),
    container: resolvedContainer,
    videoCodec: normalizeVideoCodec(videoCodec),
    audioCodec: normalizeAudioCodec(audioCodec)
  }
}

// Direct play requires the file's container AND its video codec to be
// supported. A device that declares a container can demux it (a browser with
// mpegts.js declares `ts`/`flv`; Android declares what its demuxer handles), so
// the container check is the whole gate. No audio check: an unsupported audio
// track is tolerated by every player (silent fallback) and remux can fix it.
function canDirectPlay(caps, meta) {
  const c = normalizeCapabilities(caps)
  if (!meta || meta.container === 'unknown' || !meta.videoCodec) return false
  if (!c.containers.includes(meta.container)) return false
  if (!c.videoCodecs.includes(meta.videoCodec)) return false
  return true
}

// The decision ladder. Pure: given a viewer's capabilities and the file's
// metadata (plus whether the host can remux), return the mode.
//
//   direct  -> serve the raw bytes (Range 206)
//   remux   -> container/audio rebuilt to MP4, video copied (host ffmpeg)
//   refuse  -> nothing we can serve; the viewer sees `reason`
function decide(caps, meta, { remuxAvailable = false } = {}) {
  const c = normalizeCapabilities(caps)

  if (!meta || meta.container === 'unknown' || !meta.videoCodec) {
    return { mode: 'refuse', reason: 'The host could not identify the video in this file.' }
  }
  if (canDirectPlay(c, meta)) {
    return { mode: 'direct' }
  }
  if (remuxAvailable && c.videoCodecs.includes(meta.videoCodec)) {
    return { mode: 'remux', container: 'mp4' }
  }
  if (remuxAvailable) {
    return {
      mode: 'refuse',
      reason:
        `This device cannot play ${meta.videoCodec.toUpperCase()} video and the host cannot ` +
        'convert it. The file needs transcoding, which MeshDrop does not do yet.'
    }
  }
  return {
    mode: 'refuse',
    reason: `This device cannot play ${meta.videoCodec.toUpperCase()} in a ${meta.container} container.`
  }
}

// Read a file's codecs with ffprobe. `ffprobeBin` is the resolved path (or
// 'ffprobe' for PATH). Invoked only when remux is available; on any failure
// returns null so callers degrade to direct/refuse rather than throwing.
// `child_process` is loaded via eval so bare-pack does not try to bundle it
// for the mobile worklet (same pattern as compat.js) — probeFile is a Node-only
// capability and the mobile app never calls it.
function probeFile(ffprobeBin, filePath) {
  return new Promise((resolve) => {
    if (!ffprobeBin || !filePath) return resolve(null)
    const { spawn } = eval("require('child_process')")
    let stdout = ''
    let settled = false
    const child = spawn(ffprobeBin, [
      '-v', 'error',
      '-show_entries', 'stream=codec_type,codec_name',
      '-of', 'json',
      filePath
    ], { windowsHide: true })
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true
        try { child.kill() } catch {}
        resolve(null)
      }
    }, 15000)
    child.stdout.on('data', (d) => { stdout += d })
    child.stderr.on('data', () => {})
    child.on('error', () => {
      if (!settled) { settled = true; clearTimeout(timer); resolve(null) }
    })
    child.on('close', (code) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      if (code !== 0) return resolve(null)
      try {
        const parsed = JSON.parse(stdout)
        const video = (parsed.streams || []).find((s) => s.codec_type === 'video')
        const audio = (parsed.streams || []).find((s) => s.codec_type === 'audio')
        resolve({
          videoCodec: normalizeVideoCodec(video?.codec_name),
          audioCodec: normalizeAudioCodec(audio?.codec_name)
        })
      } catch {
        resolve(null)
      }
    })
  })
}

module.exports = {
  VIDEO_CONTAINERS,
  VIDEO_CODECS,
  AUDIO_CODECS,
  PROTOCOLS,
  normalizeVideoCodec,
  normalizeAudioCodec,
  normalizeContainer,
  normalizeCapabilities,
  fileMetaFrom,
  canDirectPlay,
  decide,
  probeFile
}
