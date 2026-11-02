default:
    @just --list

test:
    cargo nextest run --workspace --exclude integration

test-integration:
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'docker compose down --volumes --remove-orphans' EXIT
    docker compose up --detach --build
    cargo nextest run -p integration --test-threads 1

lint:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- --deny warnings

fmt:
    cargo fmt --all

dev:
    RUST_LOG=info TERN_DATA_DIR=data cargo run -p server --release -- serve

rebuild:
    TERN_DATA_DIR=data cargo run -p server --release -- rebuild
