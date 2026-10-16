## Rust
- Run `scripts/lint.sh` before every commit: rustfmt, clippy with warnings as errors, cargo-deny,
  and the sans-IO dependency check. Fix the code rather than adding `#[allow]`; when an allow is
  right, give it a `reason = "..."`.
- Crates are sans-IO by default (see `.claude/skills/sans-io`). An I/O crate opts out with a
  `clippy.toml` symlink to `clippy-io.toml`.

## Comments
- Default to no comments. Write code whose names and structure make intent clear.
- Only comment a constraint the code cannot express: a workaround for an external bug,
  a non-obvious invariant, a reason something that looks wrong is intentional.
- Never comment what the next line does, never leave "changed X to Y" or "added for task Z"
  notes. That history goes in the commit message.
- Public items get a one-line doc comment (Rust `missing_docs`, Go golint convention); nothing more.
- When editing existing code, don't add comments to lines you didn't need to change.

## Documents
- Docs and code state current facts, arranged so they are easiest to read and understand.
  Git history keeps the past; don't preserve it in the text. ADRs in `docs/decisions/` are
  the exception: an accepted ADR is never rewritten, only superseded by a new one.
- When a doc needs updating, consider rewriting the whole doc or section instead of patching
  it or appending a block. Appended blocks drift into a changelog.
- Before adding content, check for duplication elsewhere in the doc (and related docs).
  Merge or remove duplicates, and rewrite when that gives a clearer result.

## Tech debt
- `docs/tech-debts.md` lists every workaround, version pin and pre-release dependency,
  each with why it exists and what must be true to remove it.
- When you add one, add its entry in the same change. When you touch code or a dependency
  an entry covers, check whether its exit condition is met.
- When an entry is resolved, remove the workaround and delete the entry. Don't mark it
  done or move it to a "resolved" list; git history keeps that.
