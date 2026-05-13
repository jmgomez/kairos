## Stress / overload tests — concurrent connections, keep-alive reuse cap,
## oversized response, streaming chunked response.

import std/[nativesockets, net, options, os, osproc, posix, strutils, unittest]
import ../kairos

const TestPort = 19080

proc onRequest(req: Request): Future[void] {.gcsafe.} =
  let p = req.path.get("/")
  case p
  of "/":
    req.send(Http200, "ok")
  of "/big":
    req.send(Http200, repeat('x', 1_000_000))
  of "/oversized":
    # Push past MaxResponseSize (16 MB) — must trigger 500 + close.
    req.send(Http200, repeat('x', 17_000_000))
  of "/stream":
    return (proc(): Future[void] {.async.} =
      await req.respond(Http200, "Content-Type: text/plain")
      await req.sendChunk("chunk1 ")
      await req.sendChunk("chunk2 ")
      await req.sendChunk("chunk3")
      await req.finishResponse()
    )()
  else:
    req.send(Http404, "no")
  var fut = newFuture[void]("h")
  fut.complete()
  return fut

var serverThread: Thread[void]
proc startServer() {.thread.} =
  run(onRequest, initSettings(
    port = Port(TestPort),
    numThreads = 1,
    maxRequestsPerConnection = 3
  ))
createThread(serverThread, startServer)
sleep(500)

# -- Helpers ----------------------------------------------------------

proc setRecvTimeout(sock: Socket, ms: int) =
  var tv: Timeval
  tv.tv_sec = posix.Time(ms div 1000)
  tv.tv_usec = Suseconds((ms mod 1000) * 1000)
  discard setsockopt(sock.getFd(), SOL_SOCKET, SO_RCVTIMEO, addr tv, SockLen(sizeof(tv)))

proc sendRecv(sock: Socket, req: string): string =
  sock.send(req)
  var buf = newString(8192)
  let n = recv(sock.getFd(), addr buf[0], buf.len.cint, 0)
  if n <= 0: return ""
  buf.setLen(n)
  buf

proc openSock(): Socket =
  result = newSocket(buffered = false)
  result.connect("127.0.0.1", Port(TestPort), timeout = 2000)
  result.setRecvTimeout(2000)

# -- Tests ------------------------------------------------------------

suite "Stress":
  test "maxRequestsPerConnection cap closes after limit":
    let s = openSock()
    defer: s.close()
    for i in 1 .. 3:
      let resp = sendRecv(s, "GET / HTTP/1.1\c\LHost: localhost\c\L\c\L")
      check "200 OK" in resp
      check "ok" in resp
    let final =
      try: sendRecv(s, "GET / HTTP/1.1\c\LHost: localhost\c\L\c\L")
      except OSError, IOError: ""
    check final.len == 0

  test "2000 concurrent Connection: close GETs":
    const N = 2000
    var cmd = "ulimit -n 8192; "
    for i in 0 ..< N:
      if i > 0: cmd.add(" & ")
      cmd.add("curl -s -o /dev/null -w '%{http_code}\\n' -H 'Connection: close' http://127.0.0.1:" & $TestPort & "/")
    cmd.add(" & wait")
    let (output, _) = execCmdEx("bash -c \"" & cmd & "\"")
    var ok = 0
    for line in output.splitLines:
      if line.strip() == "200": inc ok
    check ok == N

  test "50 concurrent keep-alive clients, 2 reqs each":
    var cmd = ""
    for i in 0 ..< 50:
      if cmd.len > 0: cmd.add(" & ")
      cmd.add("curl -s -o /dev/null -o /dev/null -w '%{http_code}\\n' http://127.0.0.1:" & $TestPort & "/ http://127.0.0.1:" & $TestPort & "/")
    cmd.add(" & wait")
    let (output, _) = execCmdEx("bash -c \"" & cmd & "\"")
    var ok = 0
    for line in output.splitLines:
      if line.strip() == "200": inc ok
    check ok == 100

  test "1 MB buffered response":
    let (output, _) = execCmdEx(
      "curl -s -o /dev/null -w '%{http_code} %{size_download}' http://127.0.0.1:" &
      $TestPort & "/big"
    )
    let parts = output.strip().split(' ')
    check parts.len >= 2
    check parts[0] == "200"
    check parseInt(parts[1]) == 1_000_000

  test "oversized response → 500 AND TCP close":
    let sock = openSock()
    defer: sock.close()
    let resp = sendRecv(sock, "GET /oversized HTTP/1.1\c\LHost: l\c\L\c\L")
    check "500" in resp
    let after =
      try: sendRecv(sock, "GET / HTTP/1.1\c\LHost: l\c\L\c\L")
      except OSError, IOError: ""
    check after.len == 0

  test "streaming chunked response reassembles":
    let (output, _) = execCmdEx(
      "curl -s -w '\\n%{http_code}' http://127.0.0.1:" & $TestPort & "/stream"
    )
    let lines = output.strip().splitLines()
    check lines[^1] == "200"
    let body = lines[0 ..< ^1].join("\n")
    check body == "chunk1 chunk2 chunk3"
