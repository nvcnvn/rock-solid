# Agent Guidelines

All agent interactions in this repository follow the project rules in [CLAUDE.md](./CLAUDE.md). This file provides the primary conventions and links for Antigravity and other AI coding assistants.

Authoritative source: [CLAUDE.md](./CLAUDE.md)

---

## Workspace Skills

Skills are shared across agents via `.agents/skills` (symlinked to `.claude/skills`):
- `sans-io` (`.agents/skills/sans-io/SKILL.md`): Architecture and constraints for deterministic state machines in `crates/core`, `meta`, and `sim`.
- `design-doc` (`.agents/skills/design-doc/SKILL.md`): Rules for editing `docs/design.md`, recording ADRs in `docs/decisions/`, and managing `docs/tech-debts.md`.
- `spike` (`.agents/skills/spike/SKILL.md`): Layout and protocol for throwaway experiments under `spikes/`.

---

## Core Rules Summary

### 1. Rust & Sans-IO Core
- **Lint script**: Always run `scripts/lint.sh` before committing (runs `rustfmt`, `clippy` with warnings as errors, `cargo-deny`, and the sans-IO dependency check).
- **Allows**: Fix code rather than adding `#[allow]`. When an allow is necessary, provide an explicit `reason = "..."`.
- **Sans-IO by default**: Crates are sans-IO by default (`core`, `meta`, `sim`). No async, no threads, no locks, no clock reads (`Instant`, `SystemTime`), and no OS RNG.
- **I/O opt-out**: Only I/O crates (`drivers-*`, `frontend-*`, `daemon`, `csi`) opt out with a `clippy.toml` symlink to `clippy-io.toml`.

### 2. Comments
- **Default to no comments**: Code structure and descriptive names express intent.
- **Constraints only**: Only comment constraints the code cannot express (workarounds for external bugs, non-obvious invariants).
- **No changelogs or line-by-line notes**: Never comment what the next line does, and never leave "changed X to Y" history comments.
- **Public items**: At most a one-line doc comment.

### 3. Documentation
- **State current facts**: Docs describe what is true now. Do not append dated updates or changelogs; rewrite sections so they read cleanly.
- **ADRs**: Accepted ADRs in `docs/decisions/` are immutable. Never rewrite an accepted ADR; supersede it with a new one.

### 4. Tech Debt
- Any workaround, pre-release dependency, or version pin must be recorded in `docs/tech-debts.md` in the same commit.
- Check and remove the entry as soon as its exit condition is met.
