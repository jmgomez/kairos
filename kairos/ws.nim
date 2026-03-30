## Kairos WebSocket adapter — bridges kairos Request to websock's HttpRequest.
## Usage:
##   import kairos/ws
##   proc handler(req: Request): Future[void] {.async.} =
##     let wsSession = await req.upgradeToWebSocket()
##     await wsSession.send("hello")
##     let msg = await wsSession.recvMsg()

import std/uri
import chronos
import chronos/streams/asyncstream
import chronos/apps/http/httptable
import pkg/websock/websock
import httpcore
import ../kairos

export websock

# httpx/websocketx compat shim
proc receiveStrPacket*(ws: WSSession): Future[string] {.async.} =
  ## Compat with websocketx API — receives a message as string.
  let data = await ws.recvMsg()
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc toWsHttpTable(headers: HttpHeaders): HttpTable =
  var res = HttpTable.init()
  for key, val in headers:
    res.add(key, val)
  res

proc upgradeToWebSocket*(
  req: Request,
  protos: seq[string] = @[""],
  version: uint = WSDefaultVersion
): Future[WSSession] {.async.} =
  ## Upgrade a kairos Request to a WebSocket session.
  ## Calls req.forget() to detach from the keep-alive loop,
  ## wraps the transport in AsyncStream for websock.
  req.forget()

  let stream = AsyncStream(
    reader: newAsyncStreamReader(req.transp),
    writer: newAsyncStreamWriter(req.transp))

  # Build websock HttpRequest from kairos data
  let hdrs = req.headers.get(newHttpHeaders())
  let path = req.path.get("/")

  let httpReq = websock.HttpRequest(
    headers: hdrs.toWsHttpTable(),
    stream: stream,
    uri: parseUri(path))

  let server = WSServer.new(protos = protos)
  return await server.handleRequest(httpReq, version)
