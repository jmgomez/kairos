# Package
version       = "0.1.0"
author        = "jmgomez"
description   = "Multi-threaded chronos HTTP server with httpx-compatible API"
license       = "MIT"
srcDir        = "."
skipDirs      = @["tests", "benchmarks", "docs"]

# Dependencies
requires "nim >= 2.2.0"
requires "chronos >= 4.2.0"
requires "websock#42c37b4"
