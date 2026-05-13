## Server integration tests — uses raw posix exec to avoid asyncdispatch.

import std/[options, net, os, strutils, tempfiles, unittest]
import ../kairos

proc curl(url: string, extraArgs = ""): string =
  let (f, tmpPath) = createTempFile("kairos_", ".txt")
  f.close()
  discard os.execShellCmd("curl -s " & extraArgs & " " & url & " > " & tmpPath & " 2>/dev/null")
  result = readFile(tmpPath)
  removeFile(tmpPath)

proc curlStatus(url: string, extraArgs = ""): int =
  let resp = curl(url, "-o /dev/null -w '%{http_code}' " & extraArgs)
  try: parseInt(resp.strip().replace("'", "")) except ValueError: 0

proc onRequest(req: Request): Future[void] {.gcsafe.} =
  let p = req.path.get("/")
  case p
  of "/":       req.send(Http200, "hello kairos")
  of "/method": req.send(Http200, $req.httpMethod.get())
  of "/echo":   req.send(Http200, req.body.get(""))
  of "/ip":     req.send(Http200, req.ip)
  else:         req.send(Http404, "not found")
  var fut = newFuture[void]("handler")
  fut.complete()
  return fut

var serverThread: Thread[void]
proc startServer() {.thread.} =
  run(onRequest, initSettings(port = Port(18080), numThreads = 1))

createThread(serverThread, startServer)
sleep(500)

suite "Server integration":
  test "basic hello response":
    check curl("http://127.0.0.1:18080/") == "hello kairos"

  test "method detection":
    check curl("http://127.0.0.1:18080/method") == "GET"

  test "POST body reading":
    check curl("http://127.0.0.1:18080/echo", "-X POST -d 'test body data'") ==
      "test body data"

  test "IP address":
    let resp = curl("http://127.0.0.1:18080/ip")
    check resp.len > 0
    check "127.0.0.1" in resp

  test "404 response":
    check curlStatus("http://127.0.0.1:18080/nonexistent") == 404

  test "chunked transfer encoding":
    check curl(
      "http://127.0.0.1:18080/echo",
      "-H 'Transfer-Encoding: chunked' -d 'chunked body test'"
    ) == "chunked body test"

  test "10 sequential requests":
    for _ in 0 ..< 10:
      check curl("http://127.0.0.1:18080/") == "hello kairos"
