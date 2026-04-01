## Kairos — Multi-threaded chronos HTTP server with httpx-compatible API
##
## Uses chronos's raw transport layer (StreamServer/StreamTransport) with
## minimal HTTP parsing and direct socket writes for maximum performance.
## Each thread runs its own chronos event loop with SO_REUSEPORT.

import std/[net, options, strutils]
import chronos
import chronos/transports/[common, stream]
import chronos/threadsync
import httpcore
export httpcore, options, chronos

import kairos/parser
export parser

proc statusLine(code: HttpCode): string {.inline.} =
  "HTTP/1.1 " & $code & "\c\LContent-Length:"

when defined(posix):
  from cpuinfo import countProcessors

const
  MaxHeaderSize = 8192
  MaxBodySize = 10_485_760  # 10 MB
  HeaderSep = @[byte('\c'), byte('\L'), byte('\c'), byte('\L')]
  ChunkSep = @[byte('\c'), byte('\L')]

type
  OnRequest* = proc(req: Request): Future[void] {.gcsafe.}

  Startup = proc() {.closure, gcsafe.}

  Settings* = object
    port*: Port
    bindAddr*: string
    numThreads: int
    startup: Startup
    listener*: Socket
    headerTimeout*: Duration
    maxConnections*: int

  Request* = object
    transp*: StreamTransport
    data: string
    cachedBody: string
    sendQueue: ptr string
    forgotten: ptr bool

func initSettings*(port = Port(8080),
                   bindAddr = "",
                   numThreads = 0,
                   startup: Startup = nil,
                   listener: Socket = nil,
                   headerTimeout = Duration.default,
                   maxConnections = 0): Settings =
  Settings(
    port: port,
    bindAddr: bindAddr,
    numThreads: numThreads,
    startup: startup,
    listener: listener,
    headerTimeout: headerTimeout,
    maxConnections: maxConnections
  )

# -- Request accessors (httpx-compatible API) --

func closed*(req: Request): bool {.inline.} =
  req.transp.isNil or req.transp.closed

func httpMethod*(req: Request): Option[HttpMethod] {.inline.} =
  parseHttpMethod(req.data)

func path*(req: Request): Option[string] {.inline.} =
  parsePath(req.data)

func headers*(req: Request): Option[HttpHeaders] =
  parseHeaders(req.data)

func body*(req: Request): Option[string] =
  if req.cachedBody.len == 0: none(string) else: some(req.cachedBody)

proc ip*(req: Request): string =
  if req.closed: return ""
  try:
    $req.transp.remoteAddress()
  except TransportOsError:
    ""

# -- Response: build raw HTTP bytes into send queue --

proc send*(req: Request, code: HttpCode, body: string,
           contentLength: Option[int], headers = "") {.inline.} =
  if req.sendQueue.isNil: return
  let sq = req.sendQueue
  let sl = statusLine(code)  # pre-computed, no allocation for common codes
  let cl = if contentLength.isSome: contentLength.get() else: body.len
  if sq[].len == 0:
    sq[] = newStringOfCap(sl.len + 10 + 4 + body.len + headers.len + 4)
  sq[].add(sl)
  sq[].addInt(cl)
  if headers.len > 0:
    sq[].add("\c\L")
    sq[].add(headers)
  sq[].add("\c\L\c\L")
  sq[].add(body)

template send*(req: Request, code: HttpCode, body: string,
               headers = "") =
  req.send(code, body, some(body.len), headers)

proc send*(req: Request, code: HttpCode) =
  if req.sendQueue.isNil: return
  let sq = req.sendQueue
  sq[].add(statusLine(code))
  sq[].add("0\c\L\c\L")

proc send*(req: Request, body: string, code = Http200) {.inline.} =
  req.send(code, body)

proc unsafeSend*(req: Request, data: string) {.inline.} =
  if req.sendQueue.isNil: return
  req.sendQueue[].add(data)

proc forget*(req: Request) =
  if not req.forgotten.isNil:
    req.forgotten[] = true

# -- Raw header scanning: find specific headers without full parse --


# -- Connection handler --

proc invokeHandler(onRequest: OnRequest, req: Request): Future[void] {.raises: [].} =
  type Cb = proc(req: Request): Future[void] {.gcsafe, raises: [].}
  cast[Cb](onRequest)(req)

var gSettings {.threadvar.}: Settings

proc processClient(onRequest: OnRequest, transp: StreamTransport) {.async.} =
  var buf: array[MaxHeaderSize, byte]
  var sendBuf = newStringOfCap(512)
  var keepGoing = true
  let timeout = gSettings.headerTimeout

  while keepGoing and not transp.closed:
    var headerLen: int
    try:
      if timeout != Duration.default:
        headerLen = await transp.readUntil(addr buf[0], MaxHeaderSize, HeaderSep).wait(timeout)
      else:
        headerLen = await transp.readUntil(addr buf[0], MaxHeaderSize, HeaderSep)
    except TransportIncompleteError, TransportLimitError, TransportError:
      break
    except AsyncTimeoutError:
      try:
        discard await transp.write("HTTP/1.1 408 Request Timeout\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
      except TransportError: discard
      break

    if headerLen == 0:
      break

    # Validate method — scan first bytes directly, no string alloc
    if buf[0] notin {byte('G'), byte('H'), byte('P'), byte('D'), byte('O')}:
      try:
        discard await transp.write("HTTP/1.1 400 Bad Request\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
      except TransportError: discard
      break

    # Parse headers to check Content-Length, Transfer-Encoding, Connection
    var data = newString(headerLen)
    copyMem(addr data[0], addr buf[0], headerLen)
    let hdrs = parseHeaders(data)
    var contentLen = -1
    var chunked = false
    var connectionClose = false
    if hdrs.isSome:
      let h = hdrs.get()
      if h.hasKey("Content-Length"):
        try: contentLen = parseInt(h["Content-Length"])
        except ValueError: discard
      if h.hasKey("Transfer-Encoding"):
        chunked = cmpIgnoreCase(h["Transfer-Encoding"], "chunked") == 0
      if h.hasKey("Connection"):
        connectionClose = cmpIgnoreCase(h["Connection"], "close") == 0

    # Read body
    var bodyStr = ""
    if chunked:
      var chunkBuf: array[32, byte]
      var totalBody = newStringOfCap(4096)
      while true:
        var chunkLineLen: int
        try:
          chunkLineLen = await transp.readUntil(addr chunkBuf[0], 32, ChunkSep)
        except TransportIncompleteError, TransportLimitError, TransportError:
          break
        let hexStr = newString(chunkLineLen - 2)
        if hexStr.len > 0:
          copyMem(addr hexStr[0], addr chunkBuf[0], hexStr.len)
        var chunkSize: int
        try: chunkSize = fromHex[int](hexStr)
        except ValueError: break
        if chunkSize == 0:
          try: discard await transp.readUntil(addr chunkBuf[0], 32, ChunkSep)
          except TransportIncompleteError, TransportLimitError, TransportError: discard
          break
        if totalBody.len + chunkSize > MaxBodySize:
          break
        try:
          let chunkData = await transp.read(chunkSize)
          if chunkData.len > 0:
            let pos = totalBody.len
            totalBody.setLen(pos + chunkData.len)
            copyMem(addr totalBody[pos], unsafeAddr chunkData[0], chunkData.len)
        except TransportError:
          break
        try: discard await transp.readUntil(addr chunkBuf[0], 32, ChunkSep)
        except TransportIncompleteError, TransportLimitError, TransportError: break
      bodyStr = totalBody
    elif contentLen > 0:
      if contentLen > MaxBodySize:
        try:
          discard await transp.write("HTTP/1.1 413 Payload Too Large\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
        except TransportError: discard
        break
      try:
        let bodyBytes = await transp.read(contentLen)
        if bodyBytes.len > 0:
          bodyStr = newString(bodyBytes.len)
          copyMem(addr bodyStr[0], unsafeAddr bodyBytes[0], bodyBytes.len)
      except TransportError:
        break

    sendBuf.setLen(0)
    var forgotten = false

    let req = Request(
      transp: transp,
      data: data,
      cachedBody: bodyStr,
      sendQueue: addr sendBuf,
      forgotten: addr forgotten
    )

    let fut = invokeHandler(onRequest, req)

    # Fast path: if handler completed synchronously, skip await entirely.
    # This avoids a Future allocation from join() on every sync request.
    if not fut.finished():
      await noCancel(fut.join())

    if forgotten:
      return

    if fut.failed():
      if sendBuf.len == 0:
        sendBuf.add("HTTP/1.1 500 Internal Server Error\c\LContent-Length:0\c\L\c\L")

    if sendBuf.len == 0:
      sendBuf.add("HTTP/1.1 200 OK\c\LContent-Length:0\c\L\c\L")

    try:
      discard await transp.write(sendBuf)
    except TransportError:
      break

    if connectionClose:
      keepGoing = false

  await transp.closeWait()

# -- Event loop + threading --

var gOnRequest {.threadvar.}: OnRequest
var gServer {.threadvar.}: StreamServer
var gActiveConns {.threadvar.}: int

proc acceptLoop(server: StreamServer) {.async.} =
  let maxConns = gSettings.maxConnections
  while true:
    if maxConns > 0 and gActiveConns >= maxConns:
      await sleepAsync(1.milliseconds)
      continue

    let transp =
      try:
        await server.accept()
      except TransportTooManyError:
        continue
      except TransportAbortedError:
        continue
      except TransportUseClosedError:
        break
      except TransportOsError:
        break
      except CancelledError:
        break

    inc gActiveConns
    proc wrappedClient(onReq: OnRequest, t: StreamTransport) {.async.} =
      try:
        await processClient(onReq, t)
      finally:
        dec gActiveConns
    discard wrappedClient(gOnRequest, transp)

proc setupServer(onRequest: OnRequest, settings: Settings) =
  gOnRequest = onRequest
  gSettings = settings

  let address = initTAddress(
    if settings.bindAddr.len > 0: settings.bindAddr else: "0.0.0.0",
    settings.port
  )

  let socketFlags =
    when defined(posix):
      {ServerFlags.ReuseAddr, ServerFlags.ReusePort, ServerFlags.TcpNoDelay}
    else:
      {ServerFlags.ReuseAddr, ServerFlags.TcpNoDelay}

  gServer =
    try:
      createStreamServer(address, flags = socketFlags)
    except TransportOsError as exc:
      raise newException(IOError, "Failed to create server: " & exc.msg)

  if settings.startup != nil:
    settings.startup()

proc eventLoop(args: (OnRequest, Settings, ThreadSignalPtr)) {.thread.} =
  setupServer(args[0], args[1])
  waitFor acceptLoop(gServer)
  if not args[2].isNil:
    discard args[2].fireSync()

proc runAsync*(onRequest: OnRequest, settings: Settings): Future[void] {.async.} =
  let numThreads =
    when compileOption("threads"):
      if settings.numThreads == 0:
        when defined(posix):
          countProcessors()
        else:
          1
      else:
        settings.numThreads
    else:
      1

  if numThreads > 1:
    when compileOption("threads"):
      var signals = newSeq[ThreadSignalPtr](numThreads)
      var threads = newSeq[Thread[(OnRequest, Settings, ThreadSignalPtr)]](numThreads)
      for i in 0 ..< numThreads:
        signals[i] = ThreadSignalPtr.new().value
        createThread(threads[i], eventLoop, (onRequest, settings, signals[i]))
      for sig in signals:
        await sig.wait()
      for sig in signals:
        discard sig.close()
    else:
      {.cast(raises: []).}: setupServer(onRequest, settings)
      await acceptLoop(gServer)
  else:
    {.cast(raises: []).}: setupServer(onRequest, settings)
    await acceptLoop(gServer)

proc runAsync*(onRequest: OnRequest): Future[void] {.inline.} =
  runAsync(onRequest, initSettings())

proc run*(onRequest: OnRequest, settings: Settings) =
  let numThreads =
    when compileOption("threads"):
      if settings.numThreads == 0:
        when defined(posix):
          countProcessors()
        else:
          1
      else:
        settings.numThreads
    else:
      1

  if numThreads > 1:
    when compileOption("threads"):
      var threads = newSeq[Thread[(OnRequest, Settings, ThreadSignalPtr)]](numThreads)
      for i in 0 ..< numThreads:
        createThread(threads[i], eventLoop, (onRequest, settings, nil))
      joinThreads(threads)
    else:
      eventLoop((onRequest, settings, nil))
  else:
    eventLoop((onRequest, settings, nil))

proc run*(onRequest: OnRequest) {.inline.} =
  run(onRequest, initSettings())
