## Aggregator — imports every test module so a single `nim c -r tests/tall.nim`
## runs all 8 suites in one binary.
##
## Each `import` runs the corresponding module's top-level code (spawning
## its server threads, sleeping for readiness, then running its unittest
## suite). Ports and global names are disjoint across the modules so there
## are no collisions.

import test_basic
import test_prologue_compat
import test_defines
import test_server
import test_stress
import test_overload
import test_runasync
import test_behavior
