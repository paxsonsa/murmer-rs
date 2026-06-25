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
#   client.rs, node.rs, monitor/*, and the rest of cluster/* — these still spawn
#   on Tokio directly and are not sim-ready yet. As each is routed through the
#   runtime seam, move it into CORE below. (router.rs and the cluster app path —
#   app/bridge.rs, app/spawn_sender.rs, app/placement.rs — have already made the
#   move and now live in CORE.)
# Seam implementations (allowed to touch Tokio/RNG by definition): runtime.rs, sim.rs.
#
# SCOPE: this is the MECHANICAL half of the determinism contract — it catches
# Tokio/clock/RNG escape hatches by token. It does NOT catch new decision-bearing
# HashMap/HashSet iteration (the class Phase 4 fixed): a blanket map ban has too
# many false positives (by-key maps are fine). That half is review-only; the
# `listing_backfill_order_is_deterministic` sim test is the one automated guard.
# Keep decision-path registries as BTreeMap (see receptionist `entries`).
#
# This is a check script, not yet auto-wired into CI — run it from a pre-commit
# hook or a CI step. It is intended to gate, but only gates where it is invoked.

set -uo pipefail
cd "$(dirname "$0")/.."

command -v rg >/dev/null 2>&1 || {
  echo "check-determinism: ripgrep (rg) is required but not found" >&2
  exit 2
}

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
  router.rs
  app/coordinator.rs
  # Cluster app path — routed through the Runtime seam (bridge spawns via
  # `runtime.spawn`) and seeded (RandomPlacement uses a derived StdRng). The
  # only escape hatches left are monitor-instrumentation `Instant::now()` reads,
  # each carrying an explicit `determinism-gate: allow` marker.
  app/bridge.rs
  app/spawn_sender.rs
  app/placement.rs
)

# Tokens that reintroduce nondeterminism if used outside the Runtime seam:
#   spawn  -> uncontrolled task scheduling
#   time   -> wall-clock timers
#   now    -> wall-clock reads that can feed decisions
#   rand   -> unseeded global RNG, OR a generator seeded from OS entropy
#
# The rand alternatives are word-bounded so the *module path* `rand::rngs::…`
# does not match the global-fn form `rand::rng()` (that overlap used to flag the
# legitimately-seeded `rand::rngs::StdRng`). The entropy entry points
# (`OsRng`, `from_entropy`, `from_os_rng`) are listed explicitly so a generator
# seeded from the OS still trips while a seeded `StdRng::seed_from_u64` passes.
BANNED='tokio::spawn|tokio::time::|Instant::now|SystemTime::now|rand::(rng|random|thread_rng)\b|OsRng|from_entropy|from_os_rng'

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

# The cluster app path (bridge/spawn_sender/placement) is now part of CORE above
# — seam-routed and seeded. What this gate still does NOT cover is the rest of
# cluster/* (the foca membership + failure detector + control streams), which is
# guarded empirically instead, by the `seed_sweep_*` sim tests. Keep those green.
