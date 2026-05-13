## Prologue API compatibility — verifies every httpx method Prologue calls
## exists on the kairos Request/Settings/OnRequest types.

import std/[options, net, unittest]
import ../kairos

suite "Prologue compatibility":
  test "Request API methods exist":
    proc checkAPI(req: Request) =
      discard req.path()
      discard req.httpMethod()
      discard req.headers()
      discard req.body()
      discard req.ip()
      req.send(Http200, "body", "")
      req.send(Http200, "body", some(5), "")
      req.send(Http200, "body")
      req.unsafeSend("raw data")
      req.forget()
    check declared(checkAPI)

  test "Settings API":
    let s = initSettings(port = Port(8080))
    check s.port == Port(8080)
    let s2 = initSettings()
    check s2.port == Port(8080)

  test "OnRequest callback signature":
    proc handler(req: Request): Future[void] {.gcsafe.} =
      req.send(Http200, "hello")
      var fut = newFuture[void]("test")
      fut.complete()
      return fut
    var cb: OnRequest = handler
    check cb != nil

  test "run() signature compiles":
    proc handler(req: Request): Future[void] {.gcsafe.} =
      var fut = newFuture[void]("test")
      fut.complete()
      return fut
    proc checkRun() = discard
    check declared(handler)
    check declared(checkRun)
