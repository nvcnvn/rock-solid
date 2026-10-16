---
name: design-doc
description: Edit the project's documents - docs/design.md, ADRs in docs/decisions/, docs/tech-debts.md, spike READMEs and the README. Use whenever a change touches a doc, a design decision is made or reversed, an open question is answered, a spike finishes, or a workaround, version pin or pre-release dependency is added or removed.
---

# Design docs

The rules in [CLAUDE.md](../../../CLAUDE.md) ("Documents", "Tech debt") apply. This is how to apply them in this repo.

## Which file

| Change | Goes in |
| --- | --- |
| How the system works or will work | `docs/design.md`, in the section that owns the topic |
| A choice between real alternatives that is now settled | New ADR in `docs/decisions/`, plus updates to `design.md` and its "Alternatives considered" row |
| Workaround, version pin, pre-release dependency | `docs/tech-debts.md` entry, in the same change as the code |
| Experiment results | The spike's README, then a `design.md` appendix (see the `spike` skill) |

## Editing design.md

- **State what's true now.** No "previously", "we changed", "update:" or dated notes; git keeps the history. A reversed decision gets rewritten as the new decision.
- **Rewrite the section, don't append.** Read the whole section and the sections that reference it, then rewrite it so it reads as if it had always said this. A paragraph tacked onto the end is the start of a changelog.
- **One home per fact.** Before adding anything, grep `docs/` and the spike READMEs for the topic. If the fact lives somewhere else, link to it or move it; don't copy it. Tables (performance targets, driver slots, test layers, roadmap) are usually the home.
- **Keep the sections consistent.** A change to a protocol often touches Architecture, the frontend section, Testing strategy (its invariants and faults), Roadmap and Open questions. Check each one.
- **Open questions:** delete a question once it's answered, and put the answer in the section that owns it. Add the new questions an answer raises.
- **Style:** spell out each acronym on first use, as the doc already does. Give targets as round trips or µs with how they were measured. Show the mechanism with mermaid or code blocks. Write for a reader new to the project.
- **Source doc:** the header links a Claude Doc. If the user edits there, ask which one is authoritative before you change either; never let them drift silently.

## ADRs

`docs/decisions/NNNN-kebab-title.md`, numbered in sequence from `0001`:

```markdown
# NNNN. Title as a decision ("Use FUSE for the file client")

Status: Accepted (YYYY-MM-DD) | Superseded by [NNNN](NNNN-...md)

## Context
Forces and constraints, linking spike results.
## Decision
What we do, in a few sentences.
## Consequences
What gets easier, what gets harder, what we accept.
```

Never rewrite an accepted ADR. To reverse one, write a new ADR and change only the old one's `Status` line to `Superseded by`.

## tech-debts.md

One `##` entry per item:

```markdown
## <what>: <short name>
- **Where:** file or crate
- **Why:** the constraint that forces it
- **Remove when:** a checkable condition (an upstream release, a fixed bug, a feature)
```

- Add the entry in the same change that adds the debt.
- When you touch code or a dependency that an entry covers, check its **Remove when** condition.
- Once an entry is resolved, delete both the workaround and the entry. Don't keep a "resolved" section.

## Before finishing

Reread every section you changed, from start to end, as a newcomer would. Check that links resolve (`#anchor` slugs, relative paths).
