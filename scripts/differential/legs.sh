#!/usr/bin/env bash
# Joins the records of the CI legs that each ran part of run.sh (LAYERS /
# SHARD, see run.sh). <dir> holds one directory per leg with that leg's
# RUNS_DIR: `planned` (every run the whole harness makes), `ran` (the runs the
# leg made) and `coverage.jsonl` (its COVERAGE_FILE).
#
#   ./legs.sh inventory <dir>   every leg planned the same runs, and the legs
#                               together made each planned run exactly once
#   ./legs.sh coverage <dir>    coverage.mjs --check over the union of the legs'
#                               coverage records, as run.sh does for a full run
set -euo pipefail
mode="${1:-}"
dir="${2:-}"
if [ -z "$mode" ] || [ -z "$dir" ]; then
  echo "usage: ./legs.sh inventory|coverage <dir>"; exit 2
fi
here="$(cd "$(dirname "$0")" && pwd)"
legs=$(find "$dir" -mindepth 1 -maxdepth 1 -type d | sort)
if [ -z "$legs" ]; then
  echo "no leg records in $dir"; exit 1
fi

case "$mode" in
inventory)
  failed=0
  first=""
  for leg in $legs; do
    if [ ! -f "$leg/planned" ]; then
      echo "MISSING: $(basename "$leg") wrote no plan"; failed=1; continue
    fi
    if [ -z "$first" ]; then
      first="$leg"
    elif ! cmp -s "$first/planned" "$leg/planned"; then
      echo "PLAN MISMATCH: $(basename "$leg") planned other runs than $(basename "$first"):"
      diff "$first/planned" "$leg/planned" || true
      failed=1
    fi
  done
  [ -n "$first" ] || exit 1

  echo "== runs per leg =="
  for leg in $legs; do
    # one line per layer: "fuzz: 42 3 6 9"
    runs=$(awk -F: '{ s[$1] = s[$1] (NF > 1 ? " " $2 : "") } !($1 in o) { o[$1] = n++; k[n - 1] = $1 }
      END { for (i = 0; i < n; i++) printf "%s%s%s", (i ? "; " : ""), k[i], s[k[i]] ? ":" s[k[i]] : "" }' "$leg/ran" 2>/dev/null)
    printf '  %-10s %s\n' "$(basename "$leg")" "${runs:-(none)}"
  done

  planned=$(sort "$first/planned")
  ran=$(cat $(printf '%s/ran ' $legs) 2>/dev/null | sort || true)
  dup=$(printf '%s\n' "$ran" | uniq -d | grep . || true)
  missing=$(comm -23 <(printf '%s\n' "$planned" | sort -u) <(printf '%s\n' "$ran" | sort -u) | grep . || true)
  extra=$(comm -13 <(printf '%s\n' "$planned" | sort -u) <(printf '%s\n' "$ran" | sort -u) | grep . || true)
  if [ -n "$(printf '%s\n' "$planned" | uniq -d)" ]; then
    echo "DUPLICATE PLAN ENTRIES (a seed listed twice):"; printf '%s\n' "$planned" | uniq -d; failed=1
  fi
  if [ -n "$missing" ]; then echo "RAN IN NO LEG (add the layer to a leg's LAYERS in ci.yml):"; echo "$missing"; failed=1; fi
  if [ -n "$dup" ]; then echo "RAN IN MORE THAN ONE LEG:"; echo "$dup"; failed=1; fi
  if [ -n "$extra" ]; then echo "RAN BUT NOT PLANNED:"; echo "$extra"; failed=1; fi
  if [ "$failed" != 0 ]; then
    echo "LEG INVENTORY FAILED"; exit 1
  fi
  echo "LEG INVENTORY OK: $(printf '%s\n' "$planned" | wc -l) planned runs, each made by exactly one of $(echo "$legs" | wc -l) legs"
  ;;
coverage)
  merged="$dir/coverage.jsonl"
  rm -f "$merged"
  for leg in $legs; do
    if [ ! -s "$leg/coverage.jsonl" ]; then
      echo "MISSING: $(basename "$leg") wrote no coverage records"; exit 1
    fi
    cat "$leg/coverage.jsonl" >> "$merged"
  done
  echo "== legacy surface coverage (union of $(echo "$legs" | wc -l) legs) =="
  node "$here/coverage.mjs" --check "$merged"
  ;;
*)
  echo "usage: ./legs.sh inventory|coverage <dir>"; exit 2
  ;;
esac
