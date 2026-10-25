#!/usr/bin/env bash
# Count source lines: non-test Rust in src/ (everything before an inline `#[cfg(test)]`),
# test code (inline test modules + tests/), and docs.
set -euo pipefail
cd "$(dirname "$0")/.."
src=0; inline=0
for f in $(find src -name '*.rs' | sort); do
  total=$(wc -l < "$f")
  cut=$(grep -n '^#\[cfg(test)\]' "$f" | head -1 | cut -d: -f1 || true)
  if [ -n "$cut" ]; then
    src=$((src + cut - 1)); inline=$((inline + total - cut + 1))
  else
    src=$((src + total))
  fi
done
tests=$(cat tests/*.rs tests/common/*.rs | wc -l | tr -d ' ')
docs=$( (cat README.md docs/*.md DECISIONS.md DATA.md 2>/dev/null || true) | wc -l | tr -d ' ')
echo "non-test source (src/, excluding inline tests): $src"
echo "inline unit tests (src/):                        $inline"
echo "integration/property tests (tests/):             $tests"
echo "docs (README, docs/, DECISIONS, DATA):           $docs"
