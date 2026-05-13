## Kairos — Multi-threaded chronos HTTP server with httpx-compatible API
##
## Uses chronos's raw transport layer (StreamServer/StreamTransport) with
## minimal HTTP parsing and direct socket writes for maximum performance.
## Each thread runs its own chronos event loop with SO_REUSEPORT.

import std/[hashes, net, options, sets, strutils]
import chronos
import chronos/asyncsync
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

# Compile-time tunables (override with `-d:kairos<name>=<int>`).
# Convention: 0 means "unlimited / disabled" for every limit/timeout.
# Runtime values passed to `initSettings` override these defaults.
const
  MaxHeaderSize = 8192
  MaxBodySize = 10_485_760  # 10 MB

  KairosMaxResponseSize* {.intdefine.} = 16_777_216    # 16 MB build-in-RAM cap
  KairosAcceptBackoffMs* {.intdefine.} = 5             # ms sleep on TransportTooManyError
  KairosMaxConnections* {.intdefine.} = 0              # per-thread cap; 0 = unlimited
  KairosMaxRequestsPerConnection* {.intdefine.} = 0    # keep-alive reuse cap; 0 = unlimited
  KairosHeaderTimeoutMs* {.intdefine.} = 0             # 0 = no timeout
  KairosBodyTimeoutMs* {.intdefine.} = 0
  KairosWriteTimeoutMs* {.intdefine.} = 0
  KairosIdleTimeoutMs* {.intdefine.} = 0
  KairosAcceptWaitTimeoutMs* {.intdefine.} = 0         # 0 = wait forever for a slot

  # Back-compat alias (still referenced by code below)
  MaxResponseSize* = KairosMaxResponseSize
  AcceptBackoffMs = KairosAcceptBackoffMs

  HeaderSep = @[byte('\c'), byte('\L'), byte('\c'), byte('\L')]
  ChunkSep = @[byte('\c'), byte('\L')]

let KairosDisabled* = chronos.milliseconds(-1)
  ## Pass this to `initSettings(headerTimeout = KairosDisabled, ...)` to
  ## explicitly disable a timeout that would otherwise pick up a non-zero
  ## compile-time default (`-d:kairosXxxMs=...`).

func msToDur(ms: int): Duration {.inline.} =
  if ms <= 0: Duration.default else: ms.milliseconds

func isUnlimited*(sz: int): bool {.inline.} = sz <= 0
  ## A `Max*Size` of 0 (or negative) means "no cap".

type
  OnRequest* = proc(req: Request): Future[void] {.gcsafe.}

  Startup = proc() {.closure, gcsafe.}

  Settings* = object
    port*: Port
    bindAddr*: string
    numThreads: int
    startup: Startup
    listener*: Socket
    headerTimeout*: Duration       ## 0 = compile default (KairosHeaderTimeoutMs)
    bodyTimeout*: Duration
    writeTimeout*: Duration
    idleTimeout*: Duration         ## between keep-alive requests on one connection
    acceptWaitTimeout*: Duration   ## how long to wait for a connection slot when at cap
    maxConnections*: int           ## per-thread cap; 0 = compile default; -1 = explicitly unlimited
    maxRequestsPerConnection*: int

  ResponseState = ref object
    sendQueue: string
    forgotten: bool
    oversized: bool
    streaming: bool       ## headers already flushed via respond() — chunked mode
    finished: bool        ## response terminated (clean OR aborted)
    writeFailed: bool     ## a streaming write hit writeTimeout/transport error;
                          ## connection must be closed, not returned to keep-alive

  Request* = object
    transp*: StreamTransport
    data: string
    cachedBody: string
    state: ResponseState

func resolveInt(runtime, compileDef: int): int {.inline.} =
  ## runtime value overrides; 0 falls back to compile-time default; -1 = explicit unlimited.
  if runtime == -1: 0
  elif runtime != 0: runtime
  else: compileDef

func resolveDur(runtime: Duration, compileMs: int): Duration {.inline.} =
  ## runtime overrides; `Duration.default` (zero) falls back to compile-time;
  ## any negative Duration (e.g. `KairosDisabled`) explicitly disables.
  if runtime == Duration.default:
    msToDur(compileMs)
  elif runtime.milliseconds < 0:
    Duration.default
  else:
    runtime

func initSettings*(port = Port(8080),
                   bindAddr = "",
                   numThreads = 0,
                   startup: Startup = nil,
                   listener: Socket = nil,
                   headerTimeout = Duration.default,
                   bodyTimeout = Duration.default,
                   writeTimeout = Duration.default,
                   idleTimeout = Duration.default,
                   acceptWaitTimeout = Duration.default,
                   maxConnections = 0,
                   maxRequestsPerConnection = 0): Settings =
  Settings(
    port: port,
    bindAddr: bindAddr,
    numThreads: numThreads,
    startup: startup,
    listener: listener,
    headerTimeout: resolveDur(headerTimeout, KairosHeaderTimeoutMs),
    bodyTimeout: resolveDur(bodyTimeout, KairosBodyTimeoutMs),
    writeTimeout: resolveDur(writeTimeout, KairosWriteTimeoutMs),
    idleTimeout: resolveDur(idleTimeout, KairosIdleTimeoutMs),
    acceptWaitTimeout: resolveDur(acceptWaitTimeout, KairosAcceptWaitTimeoutMs),
    maxConnections: resolveInt(maxConnections, KairosMaxConnections),
    maxRequestsPerConnection: resolveInt(maxRequestsPerConnection, KairosMaxRequestsPerConnection)
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
  if req.state.isNil or req.state.oversized: return
  let st = req.state
  let sl = statusLine(code)
  let cl = if contentLength.isSome: contentLength.get() else: body.len
  if not MaxResponseSize.isUnlimited:
    let needed = st.sendQueue.len + sl.len + 12 + headers.len + 4 + body.len
    if needed > MaxResponseSize:
      st.oversized = true
      return
  if st.sendQueue.len == 0:
    st.sendQueue = newStringOfCap(sl.len + 10 + 4 + body.len + headers.len + 4)
  st.sendQueue.add(sl)
  st.sendQueue.addInt(cl)
  if headers.len > 0:
    st.sendQueue.add("\c\L")
    st.sendQueue.add(headers)
  st.sendQueue.add("\c\L\c\L")
  st.sendQueue.add(body)

template send*(req: Request, code: HttpCode, body: string,
               headers = "") =
  req.send(code, body, some(body.len), headers)

proc send*(req: Request, code: HttpCode) =
  if req.state.isNil or req.state.oversized: return
  req.state.sendQueue.add(statusLine(code))
  req.state.sendQueue.add("0\c\L\c\L")

proc send*(req: Request, body: string, code = Http200) {.inline.} =
  req.send(code, body)

proc unsafeSend*(req: Request, data: string) {.inline.} =
  if req.state.isNil or req.state.oversized: return
  if not MaxResponseSize.isUnlimited and
      req.state.sendQueue.len + data.len > MaxResponseSize:
    req.state.oversized = true
    return
  req.state.sendQueue.add(data)

proc forget*(req: Request) =
  if not req.state.isNil:
    req.state.forgotten = true

func finished*(req: Request): bool {.inline.} =
  ## True once the response has been terminated — either explicitly via
  ## `finishResponse()` or implicitly because a streaming write hit an
  ## error / `writeTimeout`. Handlers running long streaming loops can
  ## use this to abort cleanly instead of blindly calling `sendChunk`.
  not req.state.isNil and req.state.finished

# -- Streaming response API (Transfer-Encoding: chunked) --
#
# Use these when the response body is large or produced incrementally.
# Each await provides natural backpressure: the next sendChunk only
# resolves once chronos has written (or queued) the previous chunk.

var gSettings {.threadvar.}: Settings

proc writeWithTimeout(transp: StreamTransport, data: string): Future[int] {.async.} =
  let w = transp.write(data)
  if gSettings.writeTimeout != Duration.default:
    return await w.wait(gSettings.writeTimeout)
  else:
    return await w

proc respond*(req: Request, code: HttpCode, headers = "") {.async.} =
  ## Begin a streaming response. Flushes the status line + headers + a
  ## `Transfer-Encoding: chunked` marker immediately. Follow with one or
  ## more `sendChunk` calls, then `finishResponse`.
  if req.state.isNil or req.state.streaming or req.state.finished: return
  var head = "HTTP/1.1 " & $code & "\c\L"
  if headers.len > 0:
    head.add(headers)
    head.add("\c\L")
  head.add("Transfer-Encoding: chunked\c\L\c\L")
  req.state.streaming = true
  try:
    discard await writeWithTimeout(req.transp, head)
  except CatchableError:
    req.state.finished = true
    req.state.writeFailed = true

proc sendChunk*(req: Request, data: string) {.async.} =
  ## Send one chunk of a streaming response. Skips zero-length input
  ## (zero is reserved as the terminator written by `finishResponse`).
  if req.state.isNil or req.state.finished or not req.state.streaming: return
  if data.len == 0: return
  var frame = newStringOfCap(data.len + 16)
  frame.add(toHex(data.len, sizeof(int) * 2).strip(leading = true, chars = {'0'}))
  if frame.len == 0: frame.add('0')
  frame.add("\c\L")
  frame.add(data)
  frame.add("\c\L")
  try:
    discard await writeWithTimeout(req.transp, frame)
  except CatchableError:
    req.state.finished = true
    req.state.writeFailed = true

proc finishResponse*(req: Request) {.async.} =
  ## Terminate a streaming response with the 0-length chunk.
  if req.state.isNil or req.state.finished or not req.state.streaming: return
  req.state.finished = true
  try:
    discard await writeWithTimeout(req.transp, "0\c\L\c\L")
  except CatchableError:
    req.state.writeFailed = true

# -- Connection handler --

proc invokeHandler(onRequest: OnRequest, req: Request): Future[void] {.raises: [].} =
  type Cb = proc(req: Request): Future[void] {.gcsafe, raises: [].}
  cast[Cb](onRequest)(req)

proc processClient(onRequest: OnRequest, transp: StreamTransport): Future[bool] {.async.} =
  ## Returns `true` if a handler called `forget()` — the caller (wrappedClient)
  ## must then *not* close the transport, because ownership has been handed off
  ## to the handler for background work.
  var buf: array[MaxHeaderSize, byte]
  var keepGoing = true
  let headerTimeout = gSettings.headerTimeout
  let bodyTimeout = gSettings.bodyTimeout
  let idleTimeout = gSettings.idleTimeout
  let maxReuse = gSettings.maxRequestsPerConnection
  var served = 0

  while keepGoing and not transp.closed:
    var headerLen: int
    # On a fresh connection, use headerTimeout. On a reused (keep-alive)
    # connection, use idleTimeout if set, otherwise headerTimeout.
    let activeTimeout =
      if served > 0 and idleTimeout != Duration.default: idleTimeout
      else: headerTimeout
    try:
      if activeTimeout != Duration.default:
        headerLen = await transp.readUntil(addr buf[0], MaxHeaderSize, HeaderSep).wait(activeTimeout)
      else:
        headerLen = await transp.readUntil(addr buf[0], MaxHeaderSize, HeaderSep)
    except TransportIncompleteError, TransportLimitError, TransportError:
      break
    except AsyncTimeoutError:
      # Don't bother replying on idle timeout — peer likely went away.
      if served == 0:
        try:
          discard await writeWithTimeout(transp, "HTTP/1.1 408 Request Timeout\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
        except CatchableError: discard
      break

    if headerLen == 0:
      break

    # Validate method — scan first bytes directly, no string alloc
    if buf[0] notin {byte('G'), byte('H'), byte('P'), byte('D'), byte('O')}:
      try:
        discard await writeWithTimeout(transp, "HTTP/1.1 400 Bad Request\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
      except CatchableError: discard
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
      var bodyAbort = false        # chunked: payload exceeded MaxBodySize → 413 + close
      var bodyTimedOut = false     # chunked: read stalled past bodyTimeout → 408 + close
      var chunkedDone = false
      while not chunkedDone:
        var chunkLineLen: int
        try:
          let fut = transp.readUntil(addr chunkBuf[0], 32, ChunkSep)
          chunkLineLen =
            if bodyTimeout != Duration.default: await fut.wait(bodyTimeout)
            else: await fut
        except AsyncTimeoutError:
          bodyTimedOut = true; break
        except TransportIncompleteError, TransportLimitError, TransportError:
          break
        let hexStr = newString(chunkLineLen - 2)
        if hexStr.len > 0:
          copyMem(addr hexStr[0], addr chunkBuf[0], hexStr.len)
        var chunkSize: int
        try: chunkSize = fromHex[int](hexStr)
        except ValueError: break
        if chunkSize == 0:
          try:
            let fut = transp.readUntil(addr chunkBuf[0], 32, ChunkSep)
            discard (if bodyTimeout != Duration.default: await fut.wait(bodyTimeout) else: await fut)
          except AsyncTimeoutError: bodyTimedOut = true
          except TransportIncompleteError, TransportLimitError, TransportError: discard
          chunkedDone = true
          break
        if totalBody.len + chunkSize > MaxBodySize:
          bodyAbort = true
          break
        try:
          let fut = transp.read(chunkSize)
          let chunkData =
            if bodyTimeout != Duration.default: await fut.wait(bodyTimeout)
            else: await fut
          if chunkData.len > 0:
            let pos = totalBody.len
            totalBody.setLen(pos + chunkData.len)
            copyMem(addr totalBody[pos], unsafeAddr chunkData[0], chunkData.len)
        except AsyncTimeoutError:
          bodyTimedOut = true; break
        except TransportError:
          break
        try:
          let fut = transp.readUntil(addr chunkBuf[0], 32, ChunkSep)
          discard (if bodyTimeout != Duration.default: await fut.wait(bodyTimeout) else: await fut)
        except AsyncTimeoutError:
          bodyTimedOut = true; break
        except TransportIncompleteError, TransportLimitError, TransportError: break
      if bodyAbort:
        try:
          discard await writeWithTimeout(transp, "HTTP/1.1 413 Payload Too Large\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
        except CatchableError: discard
        break
      if bodyTimedOut:
        try:
          discard await writeWithTimeout(transp, "HTTP/1.1 408 Request Timeout\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
        except CatchableError: discard
        break
      bodyStr = totalBody
    elif contentLen > 0:
      if contentLen > MaxBodySize:
        try:
          discard await writeWithTimeout(transp, "HTTP/1.1 413 Payload Too Large\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
        except CatchableError: discard
        break
      try:
        let bodyFut = transp.read(contentLen)
        let bodyBytes =
          if bodyTimeout != Duration.default:
            await bodyFut.wait(bodyTimeout)
          else:
            await bodyFut
        if bodyBytes.len > 0:
          bodyStr = newString(bodyBytes.len)
          copyMem(addr bodyStr[0], unsafeAddr bodyBytes[0], bodyBytes.len)
      except AsyncTimeoutError:
        try:
          discard await writeWithTimeout(transp, "HTTP/1.1 408 Request Timeout\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
        except CatchableError: discard
        break
      except TransportError:
        break

    let state = ResponseState()
    let req = Request(
      transp: transp,
      data: data,
      cachedBody: bodyStr,
      state: state
    )

    let fut = invokeHandler(onRequest, req)

    # Fast path: if handler completed synchronously, skip await entirely.
    if not fut.finished():
      await noCancel(fut.join())

    if state.forgotten:
      return true

    if state.streaming:
      # Handler wrote its own response via respond()/sendChunk(). Make sure
      # it terminated; if not, send the 0-chunk for it.
      if not state.finished:
        try: await finishResponse(req)
        except CatchableError: discard
      # If any streaming write failed (writeTimeout / transport error), the
      # connection is no longer trustworthy: don't go back to readUntil and
      # park in keep-alive — close it.
      if state.writeFailed:
        break
      inc served
      if connectionClose or (maxReuse > 0 and served >= maxReuse):
        keepGoing = false
      continue

    if state.oversized:
      try:
        discard await writeWithTimeout(transp, "HTTP/1.1 500 Internal Server Error\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
      except CatchableError: discard
      break

    if fut.failed():
      if state.sendQueue.len == 0:
        state.sendQueue.add("HTTP/1.1 500 Internal Server Error\c\LContent-Length:0\c\L\c\L")

    if state.sendQueue.len == 0:
      state.sendQueue.add("HTTP/1.1 200 OK\c\LContent-Length:0\c\L\c\L")

    try:
      discard await writeWithTimeout(transp, state.sendQueue)
    except CatchableError:
      break

    inc served
    if connectionClose:
      keepGoing = false
    elif maxReuse > 0 and served >= maxReuse:
      keepGoing = false

  await transp.closeWait()
  return false

# -- Event loop + threading --

proc hash(f: FutureBase): Hash {.inline.} = hash(cast[pointer](f))

var gOnRequest {.threadvar.}: OnRequest
var gServer {.threadvar.}: StreamServer
var gActiveConns {.threadvar.}: int
var gActiveFutures {.threadvar.}: HashSet[FutureBase]
var gConnSlots {.threadvar.}: AsyncSemaphore   ## nil when maxConnections == 0

proc wrappedClient(onReq: OnRequest, t: StreamTransport, holdsSlot: bool) {.async.} =
  var forgotten = false
  try:
    forgotten = await processClient(onReq, t)
  except CatchableError:
    discard

  # A forgotten transport (e.g. a WebSocket upgrade) still counts as an
  # active connection for as long as it stays open. Hold the slot until
  # the handler — or the peer — actually closes the transport. This stops
  # long-lived upgraded sockets from silently bypassing `maxConnections`.
  if forgotten and not t.closed:
    try: await t.join()
    except CatchableError: discard

  dec gActiveConns
  if holdsSlot and not gConnSlots.isNil:
    try: gConnSlots.release()
    except AsyncSemaphoreError: discard

  if not forgotten:
    try: await t.closeWait()
    except CatchableError: discard

proc rejectOverflow(t: StreamTransport) {.async.} =
  try:
    discard await writeWithTimeout(t, "HTTP/1.1 503 Service Unavailable\c\LContent-Length:0\c\LConnection: close\c\L\c\L")
  except CatchableError: discard
  try: await t.closeWait()
  except CatchableError: discard

proc acceptLoop(server: StreamServer) {.async.} =
  let maxConns = gSettings.maxConnections
  let waitTimeout = gSettings.acceptWaitTimeout
  while true:
    let transp =
      try:
        await server.accept()
      except TransportTooManyError:
        await sleepAsync(AcceptBackoffMs.milliseconds)
        continue
      except TransportAbortedError:
        continue
      except TransportUseClosedError:
        break
      except TransportOsError:
        break
      except CancelledError:
        break

    var holdsSlot = false
    if maxConns > 0 and not gConnSlots.isNil:
      if gConnSlots.tryAcquire():
        holdsSlot = true
      else:
        # At cap. Either wait for a slot, or (if a wait timeout is set and
        # we exceed it) reject with 503 and close.
        if waitTimeout != Duration.default:
          var acquired = false
          try:
            await gConnSlots.acquire().wait(waitTimeout)
            acquired = true
          except AsyncTimeoutError: discard
          except CancelledError: discard
          if not acquired:
            asyncSpawn rejectOverflow(transp)
            continue
          holdsSlot = true
        else:
          try: await gConnSlots.acquire()
          except CancelledError:
            try: await transp.closeWait()
            except CatchableError: discard
            break
          holdsSlot = true

    inc gActiveConns
    let fut = wrappedClient(gOnRequest, transp, holdsSlot)
    let base = FutureBase(fut)
    gActiveFutures.incl(base)
    fut.addCallback proc(_: pointer) {.gcsafe.} =
      gActiveFutures.excl(base)

proc setupServer(onRequest: OnRequest, settings: Settings) =
  gOnRequest = onRequest
  gSettings = settings
  gConnSlots =
    if settings.maxConnections > 0: newAsyncSemaphore(settings.maxConnections)
    else: nil

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

proc logEffectiveSettings(s: Settings, numThreads: int) =
  proc dur(d: Duration): string =
    if d == Duration.default: "off" else: $d.milliseconds & "ms"
  proc lim(n: int): string =
    if n <= 0: "unlimited" else: $n
  let host = if s.bindAddr.len > 0: s.bindAddr else: "0.0.0.0"
  echo "kairos: listen=", host, ":", s.port.int,
       " threads=", numThreads,
       " maxConns=", lim(s.maxConnections),
       " maxReqs/conn=", lim(s.maxRequestsPerConnection),
       " headerTO=", dur(s.headerTimeout),
       " bodyTO=", dur(s.bodyTimeout),
       " writeTO=", dur(s.writeTimeout),
       " idleTO=", dur(s.idleTimeout),
       " acceptWaitTO=", dur(s.acceptWaitTimeout),
       " maxRespBytes=", MaxResponseSize

proc eventLoop(args: (OnRequest, Settings, ThreadSignalPtr)) {.thread.} =
  setupServer(args[0], args[1])
  # Fire readiness signal AFTER the listening socket is bound but BEFORE
  # entering the accept loop. This lets `runAsync` resolve as soon as every
  # worker thread is accepting connections.
  if not args[2].isNil:
    discard args[2].fireSync()
  waitFor acceptLoop(gServer)

proc runAsync*(onRequest: OnRequest, settings: Settings): Future[void] {.async.} =
  ## Start the server asynchronously.
  ##
  ## **Multi-thread (`numThreads > 1`):** spawns worker threads, awaits each
  ## thread's readiness signal (fired as soon as its listening socket is
  ## bound), then resolves. The server keeps running in the worker threads;
  ## the caller's chronos loop is free to continue with other work. Use
  ## `joinThreads` / signal handlers for shutdown if you need to block.
  ##
  ## **Single-thread (`numThreads == 1`):** there is no other thread to run
  ## the accept loop, so this future blocks the calling chronos loop until
  ## the server stops. Use `asyncSpawn runAsync(...)` if you also want to
  ## do other async work concurrently in the same thread.
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

  logEffectiveSettings(settings, numThreads)

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

  logEffectiveSettings(settings, numThreads)

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
