#!/usr/bin/env bash
#
# Determinism gate (P0 Phase 5): no escape hatches on the single-node sim path.
#
# The simulation runtime (feature = "sim") can only be deterministic if the
# local actor path never reaches around the `Runtime` seam to touch Tokio's
# scheduler/clock or the global RNG directly. This script fails CI if a banned
# token appears, unmarked, in the routed core modules.
#
# A banned token is allowed when the line is a comment, or carries an inline
#   // determinism-gate: allow — <reason>
# marker (used for monitor-only instrumentation and consciously-deferred paths).
#
# NOT scanned (tracked follow-ups, deliberately out of scope for single-node P0):
#   router.rs, app/*, client.rs, node.rs, monitor/*, cluster/*  — these still
#   spawn on Tokio directly and are not sim-ready yet. As each is routed through
#   the runtime seam, move it into CORE below.
# Seam implementations (allowed to touch Tokio/RNG by definition): runtime.rs, sim.rs.

set -uo pipefail
cd "$(dirname "$0")/.."

SRC="murmer/src"
CORE=(
  actor.rs
  supervisor.rs
  receptionist.rs
  ready.rs
  system.rs
  endpoint.rs
  wire.rs
  listing.rs
  lifecycle.rs
  oplog.rs
)

# Tokens that reintroduce nondeterminism if used outside the Runtime seam:
#   spawn  -> uncontrolled task scheduling
#   time   -> wall-clock timers
#   now    -> wall-clock reads that can feed decisions
#   rand   -> unseeded global RNG
BANNED='tokio::spawn|tokio::time::|Instant::now|SystemTime::now|rand::(rng|random|thread_rng)'

violations=0
for f in "${CORE[@]}"; do
  path="$SRC/$f"
  [ -f "$path" ] || continue
  while IFS= read -r match; do
    num="${match%%:*}"
    content="${match#*:}"
    trimmed="${content#"${content%%[![:space:]]*}"}" # left-trim
    case "$trimmed" in
      //*) continue ;;                               # comment line
    esac
    case "$content" in
      *"determinism-gate: allow"*) continue ;;       # explicitly allowed
    esac
    if [ "$violations" -eq 0 ]; then
      echo "✗ determinism gate: escape hatch(es) on the sim path:"
      echo "  (route through the Runtime seam, or mark with '// determinism-gate: allow — <reason>')"
    fi
    echo "  $f:$num: $trimmed"
    violations=$((violations + 1))
  done < <(rg -n -e "$BANNED" "$path" 2>/dev/null || true)
done

if [ "$violations" -gt 0 ]; then
  echo
  echo "FAILED: $violations escape hatch(es) found."
  exit 1
fi

echo "✓ determinism gate: clean across ${#CORE[@]} core modules."

# Informational: the not-yet-routed surface, so it stays visible and shrinks.
deferred=$(rg -n -e "$BANNED" "$SRC/router.rs" "$SRC/app" 2>/dev/null \
  | rg -v 'determinism-gate: allow' | rg -v '^\s*//' | wc -l | tr -d ' ')
echo "  ($deferred Tokio/RNG sites remain in deferred modules: router.rs, app/* — tracked follow-ups)"
