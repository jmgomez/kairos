# Package
version       = "0.1.1"
author        = "jmgomez"
description   = "Multi-threaded chronos HTTP server with httpx-compatible API"
license       = "MIT"
srcDir        = "."
skipDirs      = @["tests", "benchmarks", "docs"]

# Dependencies
requires "nim >= 2.2.0"
requires "chronos >= 4.0.0"

feature "ws":
  requires "websock >= 0.2.1"
