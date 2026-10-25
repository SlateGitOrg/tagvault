# tagvault — one command gates everything: `make check`.
CARGO   ?= cargo
COV_MIN ?= 90
TARGET  := $(or $(CARGO_TARGET_DIR),target)
BIN     := $(TARGET)/release/tagvault

.PHONY: check fmt fmt-check lint test coverage coverage-html demo bench integration loc

## Format check + clippy (zero warnings; also the type-check) + full test suite under coverage.
check: fmt-check lint coverage

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

lint:
	$(CARGO) clippy --all-targets --locked -- -D warnings

test:
	$(CARGO) test --locked

## Runs every unit, integration, property and crash test with line coverage; fails below COV_MIN %.
## The CLI entry point (src/main.rs) is excluded from the percentage; it is exercised by tests/cli.rs.
coverage:
	$(CARGO) llvm-cov --locked --ignore-filename-regex 'src/main\.rs' --fail-under-lines $(COV_MIN) --summary-only

coverage-html:
	$(CARGO) llvm-cov --locked --ignore-filename-regex 'src/main\.rs' --html

## Offline end-to-end demo against a generated plant scenario.
demo:
	$(CARGO) build --release --locked --quiet
	TAGVAULT_BIN=$(BIN) ./scripts/demo.sh

## Compression + query benchmarks (tuning/held-out split); writes out/bench/bench.{md,json}.
bench:
	$(CARGO) build --release --locked --quiet
	$(BIN) bench --out out/bench

## Optional: real MQTT broker (Eclipse Mosquitto in Docker); skips when Docker is absent.
integration:
	$(CARGO) build --release --locked --quiet
	TAGVAULT_BIN=$(BIN) ./scripts/integration.sh

## Non-test source lines (inline #[cfg(test)] modules excluded).
loc:
	./scripts/loc.sh
