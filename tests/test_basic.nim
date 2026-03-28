import std/[options, net]
import ../kairos

# Test that types exist and constructors work
block testTypes:
  let settings = initSettings(port = Port(8080))
  doAssert settings.port == Port(8080)
  echo "PASS: initSettings"

block testDefaultSettings:
  let settings = initSettings()
  doAssert settings.port == Port(8080)
  doAssert settings.bindAddr == ""
  echo "PASS: default initSettings"

# Test that the OnRequest callback signature compiles
block testCallbackSignature:
  proc handler(req: Request): Future[void] {.gcsafe.} =
    req.send(Http200, "hello")
    var fut = newFuture[void]("test")
    fut.complete()
    return fut
  var cb: OnRequest = handler
  echo "PASS: OnRequest callback signature"

# Test that all Request method signatures compile
block testRequestMethodSignatures:
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
  echo "PASS: Request method signatures compile"

echo "All tests passed"
