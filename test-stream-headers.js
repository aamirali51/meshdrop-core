'use strict'
// Acceptance: .part staging file serves the ORIGINAL extension's MIME + sniff
//   movie.mp4.part -> video/mp4 + X-MeshDrop-Container, clip.mkv.part -> video/x-matroska, notes.txt -> octet-stream/no header
const fs = require('fs')
const fsp = fs.promises
const path = require('path')
const os = require('os')
const http = require('http')
const webdav = require('../meshdrop-app/electron/webdav.js')

let passed=0, failed=0
function ok(name, cond, detail){
  if(cond){ passed++; console.log(`PASS  ${name}`)}
  else { failed++; console.error(`FAIL  ${name}${detail?' — '+detail:''}`); process.exitCode=1 }
}

function request(port, id){
  return new Promise((res, rej)=>{
    const req=http.get({host:'127.0.0.1',port,path:`/stream/transfer?id=${encodeURIComponent(id)}`,headers:{Range:'bytes=0-1023','X-MeshDrop-Token':webdav.getWebDAVToken()}}, r=>{
      const chunks=[]; r.on('data',c=>chunks.push(c)); r.on('end',()=>res({status:r.statusCode, headers:r.headers, body:Buffer.concat(chunks)}))
    }); req.on('error',rej)
  })
}
function requestNoRange(port, id){
  return new Promise((res, rej)=>{
    const req=http.get({host:'127.0.0.1',port,path:`/stream/transfer?id=${encodeURIComponent(id)}`,headers:{'X-MeshDrop-Token':webdav.getWebDAVToken()}}, r=>{
      const chunks=[]; r.on('data',c=>chunks.push(c)); r.on('end',()=>res({status:r.statusCode, headers:r.headers, body:Buffer.concat(chunks)}))
    }); req.on('error',rej)
  })
}

async function main(){
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(),'meshdrop-headers-'))
  const { TransferEngine } = require('./engine/TransferEngine.js')
  const engine = Object.create(TransferEngine.prototype)
  engine.runs = new Map()
  const { CHUNK_SIZE } = require('./engine/transfer/constants.js')

  async function stageCase(transferId, filename, fileSize, bytes){
    const safe = filename.replace(/[^a-zA-Z0-9._-]/g,'_')
    const dir = path.join(tmp, '.p2p-staging', transferId)
    await fsp.mkdir(dir,{recursive:true})
    const partPath = path.join(dir, safe + '.part')
    await fsp.writeFile(partPath, bytes)
    const rec = { id: transferId, filename, fileSize, fileType: '', stagingPath: partPath, destPath: '', status:'downloading', playable:true, byteOffset:0 }
    engine.runs.set(transferId, { record: rec, core: { has: async()=>true }, blockSize: CHUNK_SIZE, blockCount: Math.ceil(fileSize/CHUNK_SIZE) })
    return partPath
  }

  const store = new Map()
  engine.getBee = async (name)=>{
    if(name!=='transfers') return null
    return { get: async(id)=>{ const v=store.get(id); return v?{value:v}:null } }
  }
  function putRec(transferId, filename, fileSize, stagingPath){
    store.set(transferId, { id: transferId, filename, filePath:'', fileSize, fileType:'', stagingPath, destPath:'', status:'downloading', playable:true })
  }

  const mp4Head = Buffer.alloc(64)
  mp4Head.writeUInt32BE(24,0); mp4Head.write('ftyp',4); mp4Head.write('isom',8); mp4Head.writeUInt32BE(0,12); mp4Head.write('isom',16)
  mp4Head.writeUInt32BE(16,24); mp4Head.write('moov',28)
  const mkvHead = Buffer.from([0x1A,0x45,0xDF,0xA3, 0x01,0x00,0x00,0x00, 0x42,0x86,0x81,0x01])
  const mkvPadded = Buffer.concat([mkvHead, Buffer.alloc(64,0)])
  const txtBytes = Buffer.from('hello world '.repeat(200))

  const idMp4 = 'watch-test-mp4'
  const idMkv = 'watch-test-mkv'
  const idTxt = 'watch-test-txt'

  const partMp4 = await stageCase(idMp4, 'movie.mp4', 8*1024*1024, Buffer.concat([mp4Head, Buffer.alloc(64*1024, 0xAB)]))
  putRec(idMp4, 'movie.mp4', 8*1024*1024, partMp4)
  const partMkv = await stageCase(idMkv, 'clip.mkv', 5*1024*1024, Buffer.concat([mkvPadded, Buffer.alloc(64*1024, 0xCD)]))
  putRec(idMkv, 'clip.mkv', 5*1024*1024, partMkv)
  const partTxt = await stageCase(idTxt, 'notes.txt', 10240, txtBytes)
  store.set(idTxt, { id: idTxt, filename: 'notes.txt', fileSize: 10240, fileType:'', stagingPath: partTxt, destPath:'', status:'downloading', playable:true })

  webdav.setWebDAVEngine(engine)
  const port = await webdav.startWebDAVServer({ port: 41988 })
  console.log(`# webdav headers test on :${port}`)

  engine.coveredThrough = async (id, start, end)=>{ const s=store.get(id); return s ? (s.fileSize-1) : null }
  engine.waitForRange = async (id, start)=> engine.coveredThrough(id, start)
  engine.prioritizeRange = async ()=>0
  engine.setPlayheadByte = ()=>true
  engine.noteMediaRead = ()=>true

  const rMp4 = await request(port, idMp4)
  ok('mp4 .part -> 206', rMp4.status===206, `${rMp4.status}`)
  ok('mp4 Content-Type video/mp4', (rMp4.headers['content-type']||'').includes('video/mp4'), rMp4.headers['content-type'])
  ok('mp4 X-MeshDrop-Container present (mp4)', !!rMp4.headers['x-meshdrop-container'], String(rMp4.headers['x-meshdrop-container']))

  const rMkv = await request(port, idMkv)
  ok('mkv .part -> 206', rMkv.status===206, `${rMkv.status}`)
  ok('mkv Content-Type video/x-matroska', (rMkv.headers['content-type']||'').includes('video/x-matroska'), rMkv.headers['content-type'])
  ok('mkv X-MeshDrop-Container present', !!rMkv.headers['x-meshdrop-container'], String(rMkv.headers['x-meshdrop-container']))

  const rTxt = await request(port, idTxt)
  ok('non-media .txt -> 206, no container', rTxt.status===206, `${rTxt.status}`)
  ok('txt has no X-MeshDrop-Container', !rTxt.headers['x-meshdrop-container'], String(rTxt.headers['x-meshdrop-container']))

  const rMp4200 = await requestNoRange(port, idMp4)
  ok('mp4 200 Content-Type video/mp4', (rMp4200.headers['content-type']||'').includes('video/mp4'), rMp4200.headers['content-type'])
  ok('mp4 200 has container header', !!rMp4200.headers['x-meshdrop-container'], String(rMp4200.headers['x-meshdrop-container']))

  webdav.stopWebDAVServer()
  fs.rmSync(tmp,{recursive:true,force:true})
  console.log(`\n${passed} passed, ${failed} failed`)
  if(failed) process.exitCode=1
}
main().catch(e=>{ console.error('FATAL',e); process.exitCode=1 })
