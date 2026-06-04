#!/usr/bin/env bash
#
# CI diagnostic: capture a gdb backtrace for a PHP segfault (signal 11 / exit 139).
#
# Primary path: analyse a core dump produced by the crashing run (most faithful — it is
# the actual crash, not a re-run). Fallback: re-run the given command under gdb and trap
# the SIGSEGV live (used when no core was written).
#
# Usage:
#   tools/ci/capture-segfault.sh <php-script> [args...]
#
# Env:
#   CI_DEBUG_DIR  directory holding core dumps / output (default: var/ci-debug)
#   CORE_ONLY     if "1", never re-run; only analyse an existing core dump
#
set -uo pipefail

DEBUG_DIR="${CI_DEBUG_DIR:-var/ci-debug}"
mkdir -p "$DEBUG_DIR"
BT="$DEBUG_DIR/gdb-backtrace.txt"
PHP_BIN="$(command -v php)"

sudo apt-get update -qq && sudo apt-get install -y -qq gdb

{
  echo "===== ENVIRONMENT ====="
  "$PHP_BIN" -v | head -1
  echo "Loaded extensions: $("$PHP_BIN" -m | tr '\n' ' ')"
  echo
} | tee "$BT"

core="$(ls -1t "$DEBUG_DIR"/core.* 2>/dev/null | head -1 || true)"

GDB_INSPECT=(
  -ex 'set pagination off'
  -ex 'echo \n===== FAULTING THREAD =====\n'
  -ex 'bt'
  -ex 'echo \n===== FAULTING THREAD (full) =====\n'
  -ex 'bt full'
  -ex 'echo \n===== ALL THREADS =====\n'
  -ex 'thread apply all bt'
  -ex 'echo \n===== SHARED LIBRARIES =====\n'
  -ex 'info sharedlibrary'
)

if [ -n "$core" ] && [ -f "$core" ]; then
  echo "Analysing core dump: $core" | tee -a "$BT"
  gdb -batch -nx "${GDB_INSPECT[@]}" "$PHP_BIN" "$core" 2>&1 | tee -a "$BT"
  # Compress the core so the uploaded artifact stays small; the backtrace above is the primary signal.
  gzip -f "$core" || true
elif [ "${CORE_ONLY:-0}" = "1" ]; then
  echo "No core dump found and CORE_ONLY=1; nothing to analyse." | tee -a "$BT"
else
  echo "No core dump found; re-running command under gdb to reproduce..." | tee -a "$BT"
  gdb -batch -nx \
    -ex 'set confirm off' \
    -ex 'run' \
    "${GDB_INSPECT[@]}" \
    --args "$PHP_BIN" "$@" 2>&1 | tee -a "$BT"
fi

echo "Backtrace written to $BT"
