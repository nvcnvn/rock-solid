#!/bin/bash
# Runs every Rust lint: formatting, clippy, dependency licenses and advisories, sans-IO purity.
# A crate is sans-IO unless its directory has a clippy.toml (the I/O opt-out, see clippy-io.toml).
set -euo pipefail
cd "$(dirname "$0")/.."

IO_DEPS='tokio|mio|libc|nix|rustix|io-uring|socket2|async-std|smol|getrandom'

cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings

if command -v cargo-deny >/dev/null; then
	cargo deny --log-level error check
else
	echo "cargo-deny not installed (cargo install --locked cargo-deny); skipping license and advisory checks" >&2
fi

fail=0
for manifest in crates/*/Cargo.toml; do
	dir=$(dirname "$manifest")
	[ -e "$dir/clippy.toml" ] && continue
	pkg=$(sed -n 's/^name = "\(.*\)"/\1/p' "$manifest" | head -1)
	deps=$(cargo tree -p "$pkg" -e normal --target all --prefix none)
	if bad=$(sed 's/ .*//' <<<"$deps" | sort -u | grep -Ex "$IO_DEPS"); then
		echo "sans-IO crate $pkg depends on I/O crates: $bad" >&2
		fail=1
	fi
done
exit $fail
