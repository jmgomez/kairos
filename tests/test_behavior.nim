## Behavioral gap-fill tests:
##  - oversized chunked request → 413, handler NOT invoked
##  - idleTimeout closes idle keep-alive
##  - forget() + async background send keeps transport alive
##  - writeTimeout with a slow reader aborts streaming write mid-stream
##  - FD-pressure: server survives a burst that holds many partial sockets
##  - writeTimeout-driven close also returns the server to serving state
##  - forget()/WebSocket connections do not bypass maxConnections

import std/[nativesockets, net, options, os, osproc, posix, strutils,
            monotimes, atomics, unittest]
import ../kairos

# ---- Tracked state shared with handlers ------------------------------

var handlerHits: Atomic[int]
handlerHits.store(0)
var writeAborted: Atomic[bool]
writeAborted.store(false)

# ---- Handlers --------------------------------------------------------

proc oversizedGuard(req: Request): Future[void] {.gcsafe.} =
  handlerHits.atomicInc(1)
  req.send(Http200, "should-not-happen")
  var f = newFuture[void]("h"); f.complete(); return f

proc slowStreamHandler(req: Request): Future[void] {.gcsafe.} =
  return (proc(): Future[void] {.async, gcsafe.} =
    await req.respond(Http200)
    let chunk = repeat('Z', 65_536)
    for i in 0 ..< 300:
      await req.sendChunk(chunk)
      if req.finished:
        writeAborted.store(true)
        return
    await req.finishResponse()
  )()

proc forgetHandler(req: Request): Future[void] {.gcsafe.} =
  req.forget()
  asyncSpawn (proc(): Future[void] {.async, gcsafe.} =
    await sleepAsync(chronos.milliseconds(150))
    try:
      await req.respond(Http200)
      await req.sendChunk("late-")
      await req.sendChunk("response")
      await req.finishResponse()
    finally:
      try: await req.transp.closeWait()
      except CatchableError: discard
  )()
  var f = newFuture[void]("h"); f.complete(); return f

proc quickHandler(req: Request): Future[void] {.gcsafe.} =
  req.send(Http200, "ok")
  var f = newFuture[void]("h"); f.complete(); return f

proc holdHandler(req: Request): Future[void] {.gcsafe.} =
  req.forget()
  asyncSpawn (proc(): Future[void] {.async, gcsafe.} =
    var buf: array[64, byte]
    try:
      while not req.transp.closed:
        let n = await req.transp.readOnce(addr buf[0], 64)
        if n == 0: break
    except CatchableError: discard
    try: await req.transp.closeWait()
    except CatchableError: discard
  )()
  var f = newFuture[void]("h"); f.complete(); return f

# ---- Server topology -------------------------------------------------

const
  PortChunk    = 20201
  PortIdle     = 20202
  PortForget   = 20203
  PortWriteTO  = 20204
  PortFdPress  = 20205
  PortHoldCap  = 20206
  PortRecov    = 20207

var tChunk, tIdle, tForget, tWrite, tFd, tHold, tRecov: Thread[void]

proc startChunk()  {.thread.} = run(oversizedGuard,     initSettings(port = Port(PortChunk),   numThreads = 1))
proc startIdle()   {.thread.} = run(quickHandler,       initSettings(port = Port(PortIdle),    numThreads = 1, idleTimeout = chronos.milliseconds(300)))
proc startForget() {.thread.} = run(forgetHandler,      initSettings(port = Port(PortForget),  numThreads = 1))
proc startWrite()  {.thread.} = run(slowStreamHandler,  initSettings(port = Port(PortWriteTO), numThreads = 1, writeTimeout = chronos.milliseconds(200)))
proc startFd()     {.thread.} = run(quickHandler,       initSettings(port = Port(PortFdPress), numThreads = 1))
proc startHold()   {.thread.} = run(holdHandler,        initSettings(port = Port(PortHoldCap), numThreads = 1, maxConnections = 1, acceptWaitTimeout = chronos.milliseconds(200)))
proc startRecov()  {.thread.} = run(slowStreamHandler,  initSettings(port = Port(PortRecov),   numThreads = 1, writeTimeout = chronos.milliseconds(200)))

createThread(tChunk,  startChunk)
createThread(tIdle,   startIdle)
createThread(tForget, startForget)
createThread(tWrite,  startWrite)
createThread(tFd,     startFd)
createThread(tHold,   startHold)
createThread(tRecov,  startRecov)
sleep(600)

# ---- Helpers ---------------------------------------------------------

proc nowMs(): int = (getMonoTime().ticks div 1_000_000).int

proc setRecvTimeoutMs(sock: Socket, ms: int) =
  var tv: Timeval
  tv.tv_sec = posix.Time(ms div 1000)
  tv.tv_usec = Suseconds((ms mod 1000) * 1000)
  discard setsockopt(sock.getFd(), SOL_SOCKET, SO_RCVTIMEO, addr tv, SockLen(sizeof(tv)))

proc openTo(port: int, rcvBufBytes = 0): Socket =
  result = newSocket(buffered = false)
  if rcvBufBytes > 0:
    var rb: cint = rcvBufBytes.cint
    discard setsockopt(result.getFd(), SOL_SOCKET, SO_RCVBUF, addr rb, SockLen(sizeof(rb)))
  result.connect("127.0.0.1", Port(port), timeout = 2000)
  result.setRecvTimeoutMs(2500)

proc shortRecv(sock: Socket, maxBytes = 8192): string =
  var buf = newString(maxBytes)
  let n = recv(sock.getFd(), addr buf[0], buf.len.cint, 0)
  if n <= 0: return ""
  buf.setLen(n)
  buf

# ---- Tests -----------------------------------------------------------

suite "Behavior":
  test "oversized chunked → 413 AND handler not invoked":
    let hitsBefore = handlerHits.load()
    let sock = openTo(PortChunk)
    defer: sock.close()
    # Chunk size 0xA00001 ≈ 10 MB + 1, above MaxBodySize.
    sock.send("POST /x HTTP/1.1\c\LHost: l\c\LTransfer-Encoding: chunked\c\L\c\LA00001\c\L")
    let resp = shortRecv(sock)
    check "413" in resp
    check handlerHits.load() == hitsBefore
    let after =
      try: shortRecv(sock)
      except OSError, IOError: ""
    check after.len == 0

  test "idleTimeout closes idle keep-alive":
    let sock = openTo(PortIdle)
    defer: sock.close()
    sock.send("GET / HTTP/1.1\c\LHost: l\c\L\c\L")
    let r1 = shortRecv(sock)
    check "200" in r1
    let t0 = nowMs()
    setRecvTimeoutMs(sock, 1500)
    let r2 =
      try: shortRecv(sock)
      except OSError, IOError: ""
    let elapsed = nowMs() - t0
    check r2.len == 0
    check elapsed >= 250 and elapsed < 1500

  test "forget() + async background send delivers full body":
    let (output, _) = execCmdEx(
      "curl -s --max-time 5 -w '\\n%{http_code}' http://127.0.0.1:" & $PortForget & "/"
    )
    let lines = output.strip().splitLines()
    check lines[^1] == "200"
    let body = lines[0 ..< ^1].join("\n")
    check body == "late-response"

  test "writeTimeout aborts streaming write under slow reader":
    writeAborted.store(false)
    let sock = openTo(PortWriteTO, rcvBufBytes = 1024)
    sock.send("GET / HTTP/1.1\c\LHost: l\c\L\c\L")
    let t0 = nowMs()
    while nowMs() - t0 < 3000:
      if writeAborted.load(): break
      sleep(50)
    check writeAborted.load()
    sock.close()

  test "FD-pressure burst leaves server serving":
    const N = 400
    var holds = newSeq[Socket](N)
    var opened = 0
    for i in 0 ..< N:
      try:
        let s = newSocket(buffered = false)
        s.connect("127.0.0.1", Port(PortFdPress), timeout = 1000)
        try: s.send("GET / HTTP/1.1\c\L")
        except OSError: discard
        holds[i] = s
        inc opened
      except CatchableError:
        break
    let (output, _) = execCmdEx(
      "curl -s --max-time 3 -o /dev/null -w '%{http_code}' http://127.0.0.1:" & $PortFdPress & "/"
    )
    for i in 0 ..< opened:
      try: holds[i].close()
      except OSError: discard
    check output.strip() == "200"

  test "writeTimeout closes connection AND server keeps serving":
    let sock = openTo(PortRecov, rcvBufBytes = 1024)
    sock.send("GET / HTTP/1.1\c\LHost: l\c\L\c\L")
    sleep(900)
    setRecvTimeoutMs(sock, 1500)
    var sawEof = false
    for _ in 0 ..< 200:
      var b = newString(4096)
      let n = recv(sock.getFd(), addr b[0], b.len.cint, 0)
      if n == 0: sawEof = true; break
      if n < 0: break
    sock.close()
    check sawEof
    let (output, _) = execCmdEx(
      "curl -s --max-time 3 -o /dev/null -w '%{http_code}' http://127.0.0.1:" & $PortRecov & "/"
    )
    check output.strip() == "200"

  test "forget() holds maxConnections slot until transport closes":
    let s1 = openTo(PortHoldCap)
    s1.send("GET / HTTP/1.1\c\LHost: l\c\L\c\L")
    sleep(150)  # let server accept + handler call forget()
    let t0 = nowMs()
    let (out503, _) = execCmdEx(
      "curl -s --max-time 3 -o /dev/null -w '%{http_code}' http://127.0.0.1:" & $PortHoldCap & "/"
    )
    let elapsed503 = nowMs() - t0
    check out503.strip() == "503"
    check elapsed503 < 1000
    s1.close()
    sleep(200)
    let (outOk, _) = execCmdEx(
      "curl -s --max-time 3 -o /dev/null -w '%{http_code}' http://127.0.0.1:" & $PortHoldCap & "/"
    )
    check outOk.strip() != "503"
