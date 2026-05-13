import std/[options, net, unittest]
import ../kairos

suite "API surface":
  test "initSettings honors explicit port":
    let settings = initSettings(port = Port(8080))
    check settings.port == Port(8080)

  test "default initSettings":
    let settings = initSettings()
    check settings.port == Port(8080)
    check settings.bindAddr == ""

  test "OnRequest callback signature compiles":
    proc handler(req: Request): Future[void] {.gcsafe.} =
      req.send(Http200, "hello")
      var fut = newFuture[void]("test")
      fut.complete()
      return fut
    var cb: OnRequest = handler
    check cb != nil

  test "Request accessor signatures compile":
    proc checkSignatures(req: Request) =
      discard req.closed()
      discard req.httpMethod()
      discard req.path()
      discard req.headers()
      discard req.body()
      discard req.ip()
      req.send(Http200, "body", "")
      req.send(Http200, "body")
      req.send(Http200)
      req.send("body")
      req.unsafeSend("data")
      req.forget()
    check declared(checkSignatures)
