# Tech debts

## Version pin: Rust toolchain 1.99.1
- **Where:** `rust-toolchain.toml`, `rust-version` in `Cargo.toml`
- **Why:** each stable release adds clippy lints, and `scripts/lint.sh` treats warnings as errors, so an unpinned toolchain would break the build without any code change.
- **Remove when:** never removed, only bumped. Bump both together when a newer stable is needed, and fix the new lints in the same change.
