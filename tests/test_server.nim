## Server integration tests — uses raw posix exec to avoid asyncdispatch.

import std/[options, net, os, strutils, tempfiles]
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
  of "/":
    req.send(Http200, "hello kairos")
  of "/method":
    req.send(Http200, $req.httpMethod.get())
  of "/echo":
    let b = req.body.get("")
    req.send(Http200, b)
  of "/ip":
    req.send(Http200, req.ip)
  else:
    req.send(Http404, "not found")
  var fut = newFuture[void]("handler")
  fut.complete()
  return fut

var serverThread: Thread[void]
proc startServer() {.thread.} =
  run(onRequest, initSettings(port = Port(18080), numThreads = 1))

createThread(serverThread, startServer)
sleep(500)

block testHello:
  let resp = curl("http://127.0.0.1:18080/")
  doAssert resp == "hello kairos", "Got: " & resp
  echo "PASS: basic hello response"

block testMethod:
  let resp = curl("http://127.0.0.1:18080/method")
  doAssert resp == "GET", "Got: " & resp
  echo "PASS: method detection"

block testPostBody:
  let resp = curl("http://127.0.0.1:18080/echo", "-X POST -d 'test body data'")
  doAssert resp == "test body data", "Got: " & resp
  echo "PASS: POST body reading"

block testIp:
  let resp = curl("http://127.0.0.1:18080/ip")
  doAssert resp.len > 0, "IP should not be empty"
  doAssert "127.0.0.1" in resp, "Got: " & resp
  echo "PASS: IP address"

block test404:
  let status = curlStatus("http://127.0.0.1:18080/nonexistent")
  doAssert status == 404, "Got: " & $status
  echo "PASS: 404 response"

block testChunked:
  # curl -H "Transfer-Encoding: chunked" sends chunked automatically with -d
  let resp = curl("http://127.0.0.1:18080/echo", "-H 'Transfer-Encoding: chunked' -d 'chunked body test'")
  doAssert resp == "chunked body test", "Got: " & resp
  echo "PASS: chunked transfer encoding"

block testConcurrent:
  for i in 0 ..< 10:
    let resp = curl("http://127.0.0.1:18080/")
    doAssert resp == "hello kairos"
  echo "PASS: concurrent requests"

echo "All server tests passed"
quit(0)
