'use strict'

// Unit tests for the pure capability-negotiation module.
//
//   node test-watch-capabilities.js
//
// Exit code 0 on success, 1 on any failure.

const {
  normalizeCapabilities,
  normalizeContainer,
  normalizeVideoCodec,
  fileMetaFrom,
  canDirectPlay,
  decide
} = require('./engine/watchCapabilities.js')

let checks = 0
let failures = 0

function check(name, cond, extra) {
  checks++
  if (cond) {
    console.log('PASS  ' + name + (extra ? ' — ' + extra : ''))
  } else {
    failures++
    console.error('FAIL  ' + name + (extra ? ' — ' + extra : ''))
  }
}

// ─── normalizeCapabilities: whitelist + canonicalize ─────────────────────────

const clean = normalizeCapabilities({
  videoCodecs: ['avc1', 'hev1', 'h265', 'vp9', 'bogus-codec', 42, ''],
  audioCodecs: ['AAC', 'ac3', null],
  containers: ['mp4', 'video/x-matroska', 'MKV', 'flv'],
  protocols: ['mpegts', 'hls', 'mse', 'garbage'],
  unknownField: 'dropped'
})
// avc1->h264, hev1+h265->hevc, vp9 => 3 canonical entries.
check('normalize: canonical video codecs', clean.videoCodecs.length === 3 && clean.videoCodecs.includes('h264'), JSON.stringify(clean.videoCodecs))
check('normalize: hev1/h265 -> hevc once', clean.videoCodecs.includes('hevc') && clean.videoCodecs.filter((c) => c === 'hevc').length === 1)
check('normalize: drops unknown video codecs', !clean.videoCodecs.includes('bogus-codec'))
check('normalize: audio codecs canonicalized', clean.audioCodecs.length === 2 && clean.audioCodecs.includes('aac') && clean.audioCodecs.includes('ac3'))
check('normalize: containers canonicalized', clean.containers.length === 3 && clean.containers.includes('mkv'))
check('normalize: protocols whitelisted', clean.protocols.length === 3 && !clean.protocols.includes('garbage'))
check('normalize: strips unknown fields', !('unknownField' in clean))
check('normalize: null input is empty', JSON.stringify(normalizeCapabilities(null)) === '{"videoCodecs":[],"audioCodecs":[],"containers":[],"protocols":[]}')
check('normalize: non-object input is empty', JSON.stringify(normalizeCapabilities('nope')) === '{"videoCodecs":[],"audioCodecs":[],"containers":[],"protocols":[]}')

// ─── normalizers ─────────────────────────────────────────────────────────────

check('normalizeContainer: mp4', normalizeContainer('mp4') === 'mp4')
check('normalizeContainer: mime subtype', normalizeContainer('video/mp4') === 'mp4')
check('normalizeContainer: matroska mime', normalizeContainer('video/x-matroska') === 'mkv')
check('normalizeContainer: mp2t mime', normalizeContainer('video/mp2t') === 'ts')
check('normalizeContainer: unknown', normalizeContainer('exe') === null)
check('normalizeVideoCodec: h265 alias', normalizeVideoCodec('h265') === 'hevc')
check('normalizeVideoCodec: wmv3 -> vc1', normalizeVideoCodec('wmv3') === 'vc1')
check('normalizeVideoCodec: unknown', normalizeVideoCodec('mpeg1video') === null)

// ─── fileMetaFrom: container from filename extension ─────────────────────────

const mkvMeta = fileMetaFrom({ filename: 'movie.mkv', videoCodec: 'h264', audioCodec: 'aac' })
check('fileMetaFrom: mkv container from ext', mkvMeta.container === 'mkv')
check('fileMetaFrom: videoCodec canonicalized', mkvMeta.videoCodec === 'h264')
check('fileMetaFrom: audioCodec canonicalized', mkvMeta.audioCodec === 'aac')
check('fileMetaFrom: HEVC filename', fileMetaFrom({ filename: 'x265.mkv', videoCodec: 'hevc' }).videoCodec === 'hevc')
check('fileMetaFrom: explicit container wins', fileMetaFrom({ filename: 'x.mkv', container: 'mp4' }).container === 'mp4')
check('fileMetaFrom: unknown ext + no container', fileMetaFrom({ filename: 'x.bin', videoCodec: 'h264' }).container === 'unknown')
check('fileMetaFrom: missing video codec', fileMetaFrom({ filename: 'x.mp4' }).videoCodec === null)

// ─── canDirectPlay ───────────────────────────────────────────────────────────

const android = normalizeCapabilities({
  videoCodecs: ['h264', 'hevc'],
  audioCodecs: ['aac'],
  containers: ['mp4', 'mkv'],
  protocols: []
})
const chrome = normalizeCapabilities({
  videoCodecs: ['h264', 'vp9'],
  audioCodecs: ['aac', 'opus'],
  containers: ['mp4', 'webm', 'ts'], // ts via mpegts.js
  protocols: ['mpegts', 'hls', 'mse']
})
check('direct: mp4/h264 android', canDirectPlay(android, fileMetaFrom({ filename: 'a.mp4', videoCodec: 'h264' })))
check('direct: mkv/hevc android (can demux mkv, decodes hevc)', canDirectPlay(android, fileMetaFrom({ filename: 'a.mkv', videoCodec: 'hevc' })))
check('direct: refused when container unsupported', !canDirectPlay(android, fileMetaFrom({ filename: 'a.webm', videoCodec: 'vp9' })))
check('direct: refused when codec unsupported', !canDirectPlay(android, fileMetaFrom({ filename: 'a.mp4', videoCodec: 'vp9' })))
check('direct: chrome mp4/h264', canDirectPlay(chrome, fileMetaFrom({ filename: 'a.mp4', videoCodec: 'h264' })))
check('direct: chrome refuses mkv (no mkv demuxer)', !canDirectPlay(chrome, fileMetaFrom({ filename: 'a.mkv', videoCodec: 'h264' })))
check('direct: TS direct-plays when container declared', canDirectPlay(chrome, fileMetaFrom({ filename: 'a.ts', videoCodec: 'h264' })))
check('direct: unknown container refused', !canDirectPlay(chrome, fileMetaFrom({ filename: 'a.bin', videoCodec: 'h264' })))
check('direct: missing video codec refused', !canDirectPlay(chrome, fileMetaFrom({ filename: 'a.mp4' })))

// ─── decide ladder ───────────────────────────────────────────────────────────

// Direct when it can direct-play.
check('decide: direct wins', decide(android, fileMetaFrom({ filename: 'a.mp4', videoCodec: 'h264' }), { remuxAvailable: true }).mode === 'direct')
// Remux when only the container is wrong but the video codec is playable.
const remuxVerdict = decide(chrome, fileMetaFrom({ filename: 'a.mkv', videoCodec: 'h264' }), { remuxAvailable: true })
check('decide: remux when container wrong, codec ok', remuxVerdict.mode === 'remux' && remuxVerdict.container === 'mp4', JSON.stringify(remuxVerdict))
// Refuse (with transcode reason) when the video codec is unplayable and remux available.
const refuseCodec = decide(chrome, fileMetaFrom({ filename: 'a.mkv', videoCodec: 'hevc' }), { remuxAvailable: true })
check('decide: refuse with transcode reason when codec unplayable', refuseCodec.mode === 'refuse' && /cannot play HEVC/.test(refuseCodec.reason))
// Refuse with container reason when no remux available at all.
const noRemux = decide(chrome, fileMetaFrom({ filename: 'a.mkv', videoCodec: 'h264' }), { remuxAvailable: false })
check('decide: refuse container reason when no remux', noRemux.mode === 'refuse' && /H264.*mkv/.test(noRemux.reason))
// Refuse on unidentifiable file.
check('decide: refuse unknown file', decide(chrome, fileMetaFrom({ filename: 'a.bin', videoCodec: 'h264' }), { remuxAvailable: true }).mode === 'refuse')
// TS: a browser with ts container direct-plays; one without it (no mpegts.js)
// needs a remux.
check('decide: TS + ts container direct', decide(chrome, fileMetaFrom({ filename: 'a.ts', videoCodec: 'h264' }), { remuxAvailable: true }).mode === 'direct')
check('decide: TS without ts container remuxes', decide({ ...chrome, containers: ['mp4', 'webm'] }, fileMetaFrom({ filename: 'a.ts', videoCodec: 'h264' }), { remuxAvailable: true }).mode === 'remux')

console.log(`\n${checks} checks, ${failures} failures`)
process.exit(failures ? 1 : 0)
