## Overload / backpressure tests — semaphore wait, acceptWaitTimeout 503,
## slowloris, body/chunked timeouts, KairosDisabled sentinel.
##
## Each scenario spawns its own server thread on a distinct port so the
## settings don't interfere.

import std/[nativesockets, net, options, os, osproc, posix, strutils,
            monotimes, unittest]
import ../kairos

proc slowHandler(req: Request): Future[void] {.gcsafe.} =
  return (proc(): Future[void] {.async.} =
    await sleepAsync(chronos.milliseconds(400))
    req.send(Http200, "done")
  )()

proc quickHandler(req: Request): Future[void] {.gcsafe.} =
  req.send(Http200, "ok")
  var f = newFuture[void]("q"); f.complete(); return f

const
  PortQueue     = 20081
  PortTimeout   = 20082
  PortSlow      = 20083
  PortUnlimited = 20084

var t1, t2, t3, t4: Thread[void]

proc startQueue() {.thread.} =
  run(slowHandler, initSettings(port = Port(PortQueue), numThreads = 1, maxConnections = 1))

proc startTimeout() {.thread.} =
  run(slowHandler, initSettings(
    port = Port(PortTimeout), numThreads = 1, maxConnections = 1,
    acceptWaitTimeout = chronos.milliseconds(200)
  ))

proc startSlow() {.thread.} =
  run(quickHandler, initSettings(
    port = Port(PortSlow), numThreads = 1,
    headerTimeout = chronos.milliseconds(300),
    bodyTimeout = chronos.milliseconds(300)
  ))

proc startUnlimited() {.thread.} =
  run(quickHandler, initSettings(
    port = Port(PortUnlimited), numThreads = 1, maxConnections = 0,
    acceptWaitTimeout = chronos.milliseconds(100)  # ignored when cap=0
  ))

createThread(t1, startQueue)
createThread(t2, startTimeout)
createThread(t3, startSlow)
createThread(t4, startUnlimited)
sleep(700)

# -- Helpers ----------------------------------------------------------

proc nowMs(): int = (getMonoTime().ticks div 1_000_000).int

# -- Tests ------------------------------------------------------------

suite "Overload":
  test "maxConnections=1 second client waits and succeeds":
    let cmd =
      "curl -s --max-time 5 -o /dev/null -w '%{http_code} %{time_total}\\n' http://127.0.0.1:" &
      $PortQueue & "/ & " &
      "sleep 0.1; curl -s --max-time 5 -o /dev/null -w '%{http_code} %{time_total}\\n' http://127.0.0.1:" &
      $PortQueue & "/ & wait"
    let t0 = nowMs()
    let (output, _) = execCmdEx("bash -c \"" & cmd & "\"")
    let elapsed = nowMs() - t0
    var ok = 0
    for line in output.splitLines:
      if line.startsWith("200 "): inc ok
    check ok == 2
    check elapsed >= 700   # serialized → ~800ms

  test "acceptWaitTimeout=200ms → 503 for overflow request":
    let cmd =
      "curl -s --max-time 5 -o /dev/null -w '%{http_code}\\n' http://127.0.0.1:" &
      $PortTimeout & "/ & " &
      "sleep 0.05; curl -s --max-time 5 -o /dev/null -w '%{http_code}\\n' http://127.0.0.1:" &
      $PortTimeout & "/ & wait"
    let (output, _) = execCmdEx("bash -c \"" & cmd & "\"")
    var seen200 = 0
    var seen503 = 0
    for line in output.splitLines:
      if line.strip() == "200": inc seen200
      elif line.strip() == "503": inc seen503
    check seen200 == 1
    check seen503 == 1

  test "cap=1 with no wait-timeout never returns 503":
    var cmd = ""
    for i in 0 ..< 5:
      if cmd.len > 0: cmd.add(" & ")
      cmd.add("curl -s --max-time 10 -o /dev/null -w '%{http_code}\\n' http://127.0.0.1:" & $PortQueue & "/")
    cmd.add(" & wait")
    let (output, _) = execCmdEx("bash -c \"" & cmd & "\"")
    var ok = 0
    var bad = 0
    for line in output.splitLines:
      if line.strip() == "200": inc ok
      elif line.strip() == "503": inc bad
    check bad == 0
    check ok == 5

  test "header slowloris cut off near headerTimeout":
    let sock = newSocket(buffered = false)
    sock.connect("127.0.0.1", Port(PortSlow), timeout = 2000)
    defer: sock.close()
    var tv: Timeval
    tv.tv_sec = posix.Time(1); tv.tv_usec = Suseconds(500_000)
    discard setsockopt(sock.getFd(), SOL_SOCKET, SO_RCVTIMEO, addr tv, SockLen(sizeof(tv)))

    let t0 = nowMs()
    try: sock.send("GET / HTTP/1.1\c\LHost: localhost\c\L")  # no final \c\L\c\L
    except OSError: discard

    var buf = newString(4096)
    let n = recv(sock.getFd(), addr buf[0], buf.len.cint, 0)
    let elapsed = nowMs() - t0
    if n > 0:
      buf.setLen(n)
      check "408" in buf
    check elapsed < 1500

  test "bodyTimeout (Content-Length) → 408":
    let sock = newSocket(buffered = false)
    sock.connect("127.0.0.1", Port(PortSlow), timeout = 2000)
    defer: sock.close()
    var tv: Timeval
    tv.tv_sec = posix.Time(1); tv.tv_usec = Suseconds(500_000)
    discard setsockopt(sock.getFd(), SOL_SOCKET, SO_RCVTIMEO, addr tv, SockLen(sizeof(tv)))

    let t0 = nowMs()
    try: sock.send("POST /x HTTP/1.1\c\LHost: l\c\LContent-Length: 100\c\L\c\Lpartial...")
    except OSError: discard
    var buf = newString(4096)
    let n = recv(sock.getFd(), addr buf[0], buf.len.cint, 0)
    let elapsed = nowMs() - t0
    check n > 0
    buf.setLen(n)
    check "408" in buf
    check elapsed < 1500

  test "bodyTimeout (chunked) → 408":
    let sock = newSocket(buffered = false)
    sock.connect("127.0.0.1", Port(PortSlow), timeout = 2000)
    defer: sock.close()
    var tv: Timeval
    tv.tv_sec = posix.Time(1); tv.tv_usec = Suseconds(500_000)
    discard setsockopt(sock.getFd(), SOL_SOCKET, SO_RCVTIMEO, addr tv, SockLen(sizeof(tv)))

    let t0 = nowMs()
    try: sock.send("POST /x HTTP/1.1\c\LHost: l\c\LTransfer-Encoding: chunked\c\L\c\L100\c\L")
    except OSError: discard
    var buf = newString(4096)
    let n = recv(sock.getFd(), addr buf[0], buf.len.cint, 0)
    let elapsed = nowMs() - t0
    check n > 0
    buf.setLen(n)
    check "408" in buf
    check elapsed < 1500

  test "maxConnections=0 = unlimited (100 concurrent, 0×503)":
    var cmd = "ulimit -n 8192; "
    const N = 100
    for i in 0 ..< N:
      if i > 0: cmd.add(" & ")
      cmd.add("curl -s --max-time 5 -o /dev/null -w '%{http_code}\\n' http://127.0.0.1:" & $PortUnlimited & "/")
    cmd.add(" & wait")
    let (output, _) = execCmdEx("bash -c \"" & cmd & "\"")
    var ok = 0; var bad = 0
    for line in output.splitLines:
      if line.strip() == "200": inc ok
      elif line.strip() == "503": inc bad
    check bad == 0
    check ok == N

  test "KairosDisabled overrides every timeout":
    let s = initSettings(
      port = Port(0),
      headerTimeout = KairosDisabled,
      bodyTimeout = KairosDisabled,
      writeTimeout = KairosDisabled,
      idleTimeout = KairosDisabled,
      acceptWaitTimeout = KairosDisabled
    )
    check s.headerTimeout == Duration.default
    check s.bodyTimeout == Duration.default
    check s.writeTimeout == Duration.default
    check s.idleTimeout == Duration.default
    check s.acceptWaitTimeout == Duration.default
