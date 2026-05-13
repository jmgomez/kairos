## Compile-time -d:kairos* defines and runtime override semantics.
##
## Compile with all knobs set to exercise every branch:
##   nim c \
##     -d:kairosMaxConnections=7 \
##     -d:kairosMaxRequestsPerConnection=11 \
##     -d:kairosHeaderTimeoutMs=1100 \
##     -d:kairosBodyTimeoutMs=1200 \
##     -d:kairosWriteTimeoutMs=1300 \
##     -d:kairosIdleTimeoutMs=1400 \
##     -d:kairosAcceptWaitTimeoutMs=1500 \
##     -d:kairosMaxResponseSize=2000000 \
##     -d:kairosAcceptBackoffMs=9 \
##     tests/test_defines.nim

import std/[options, unittest]
import ../kairos

suite "Compile-time defines":
  test "every -d:kairos<Name>= knob is exposed":
    check KairosMaxConnections >= 0
    check KairosMaxRequestsPerConnection >= 0
    check KairosHeaderTimeoutMs >= 0
    check KairosBodyTimeoutMs >= 0
    check KairosWriteTimeoutMs >= 0
    check KairosIdleTimeoutMs >= 0
    check KairosAcceptWaitTimeoutMs >= 0
    check KairosMaxResponseSize >= 0
    check KairosAcceptBackoffMs >= 0

  test "bare initSettings picks up every compile-time default":
    let s = initSettings(port = Port(0))
    check s.maxConnections == KairosMaxConnections
    check s.maxRequestsPerConnection == KairosMaxRequestsPerConnection
    when KairosHeaderTimeoutMs > 0:
      check s.headerTimeout == chronos.milliseconds(KairosHeaderTimeoutMs)
    when KairosBodyTimeoutMs > 0:
      check s.bodyTimeout == chronos.milliseconds(KairosBodyTimeoutMs)
    when KairosWriteTimeoutMs > 0:
      check s.writeTimeout == chronos.milliseconds(KairosWriteTimeoutMs)
    when KairosIdleTimeoutMs > 0:
      check s.idleTimeout == chronos.milliseconds(KairosIdleTimeoutMs)
    when KairosAcceptWaitTimeoutMs > 0:
      check s.acceptWaitTimeout == chronos.milliseconds(KairosAcceptWaitTimeoutMs)

  test "runtime initSettings overrides every define":
    let s = initSettings(
      port = Port(0),
      maxConnections = 3,
      maxRequestsPerConnection = 5,
      headerTimeout = chronos.milliseconds(50),
      bodyTimeout = chronos.milliseconds(60),
      writeTimeout = chronos.milliseconds(70),
      idleTimeout = chronos.milliseconds(80),
      acceptWaitTimeout = chronos.milliseconds(90)
    )
    check s.maxConnections == 3
    check s.maxRequestsPerConnection == 5
    check s.headerTimeout == chronos.milliseconds(50)
    check s.bodyTimeout == chronos.milliseconds(60)
    check s.writeTimeout == chronos.milliseconds(70)
    check s.idleTimeout == chronos.milliseconds(80)
    check s.acceptWaitTimeout == chronos.milliseconds(90)

  test "-1 explicitly unlimits every limit (overrides compile defaults)":
    let s = initSettings(
      port = Port(0),
      maxConnections = -1,
      maxRequestsPerConnection = -1
    )
    check s.maxConnections == 0
    check s.maxRequestsPerConnection == 0

  test "KairosDisabled overrides every nonzero compile-time duration":
    let s = initSettings(
      port = Port(0),
      headerTimeout = KairosDisabled,
      bodyTimeout = KairosDisabled,
      writeTimeout = KairosDisabled,
      idleTimeout = KairosDisabled,
      acceptWaitTimeout = KairosDisabled
    )
    check s.headerTimeout == Duration.default
    check s.bodyTimeout == Duration.default
    check s.writeTimeout == Duration.default
    check s.idleTimeout == Duration.default
    check s.acceptWaitTimeout == Duration.default

  test "isUnlimited helper":
    check isUnlimited(0)
    check isUnlimited(-1)
    check not isUnlimited(1)
