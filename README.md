# Kairos

**Kairos** (καιρός) — *the opportune moment*. A chronos HTTP backend for [Prologue](https://github.com/planety/Prologue) with an httpx-compatible API.

## Quick Start

```nim
import kairos

proc onRequest(req: Request): Future[void] {.gcsafe, async.} =
  req.send(Http200, "Hello from Kairos!")

run(onRequest, initSettings(port = Port(8080), numThreads = 4))
```

## Install

```
nimble install kairos
```

Requires Nim >= 2.2.0, chronos >= 4.2.0

## License

MIT
