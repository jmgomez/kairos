## Proves runAsync(numThreads > 1) resolves on readiness (workers listening)
## rather than on shutdown. A regression hangs the chronos loop; the
## elapsed-time check inside the suite catches that.

import std/[options, os, osproc, monotimes, strutils, unittest]
import ../kairos

proc handler(req: Request): Future[void] {.gcsafe.} =
  req.send(Http200, "ready")
  var f = newFuture[void]("h"); f.complete(); return f

proc nowMs(): int = (getMonoTime().ticks div 1_000_000).int

suite "runAsync readiness":
  test "runAsync(numThreads=2) resolves on readiness and server is serving":
    let t0 = nowMs()
    proc startup() {.async.} =
      await runAsync(handler, initSettings(port = Port(20100), numThreads = 2))
    waitFor startup()
    let readyMs = nowMs() - t0
    check readyMs < 2000

    let (output, _) = execCmdEx(
      "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:20100/"
    )
    check output.strip() == "200"
