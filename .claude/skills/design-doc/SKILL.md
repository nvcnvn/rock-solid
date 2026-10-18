---
name: design-doc
description: Edit the project's documents - docs/design.md, docs/build-checklist.md, docs/test-scenarios.md, docs/tech-debts.md, docs/glossary.md, spike READMEs and the README. Use whenever a change touches a doc, a design decision is made or reversed, an open question is answered, a spike finishes, or a workaround, version pin or pre-release dependency is added or removed.
---

# Design docs

The rules in [CLAUDE.md](../../../CLAUDE.md) ("Documents", "Tech debt") apply. This is how to apply them in this repo.

## Which file

| Change | Goes in |
| --- | --- |
| How the system works or will work | `docs/design.md`, in the section that owns the topic |
| A choice between real alternatives that is now settled | The `design.md` section that owns it, plus its "Alternatives considered" row (see Decisions below) |
| Workaround, version pin, pre-release dependency | `docs/tech-debts.md` entry, in the same change as the code |
| Experiment results | The spike's README, then a `design.md` appendix (see the `spike` skill) |
| A test scenario a component must pass | `docs/test-scenarios.md`, in the section of its build checklist item |
| A buildable piece of phases 1 to 5 | `docs/build-checklist.md` |
| A term a reader new to storage would not know | `docs/glossary.md`: what it means, why systems need it, and a link to the `design.md` section that owns its details |

## Editing design.md

- **State what's true now.** No "previously", "we changed", "update:" or dated notes; git keeps the history. A reversed decision gets rewritten as the new decision.
- **Rewrite the section, don't append.** Read the whole section and the sections that reference it, then rewrite it so it reads as if it had always said this. A paragraph tacked onto the end is the start of a changelog.
- **One home per fact.** Before adding anything, grep `docs/` and the spike READMEs for the topic. If the fact lives somewhere else, link to it or move it; don't copy it. Tables (performance targets, driver slots, test layers, roadmap) are usually the home.
- **Keep the sections consistent.** A change to a protocol often touches Architecture, the frontend section, Testing strategy (its invariants and faults), Roadmap and Open questions. Check each one.
- **Open questions:** delete a question once it's answered, and put the answer in the section that owns it. Add the new questions an answer raises.
- **Style:** spell out each acronym on first use, as the doc already does. A new term of art gets a `glossary.md` entry, and a term whose meaning changes gets its entry rewritten. Give targets as round trips or µs with how they were measured. Show the mechanism with mermaid or code blocks. Write for a reader new to the project.

## Decisions

There are no separate decision records: `design.md` holds every decision where a reader meets it.

- **In the owning section,** state the decision together with what forced it (the constraints, the measurements, how other systems handle it) and what it costs. A reader should not need another document to see why.
- **In "Alternatives considered",** add or rewrite the row: what was chosen, what was weighed, and the reason in a sentence or two, linking the owning section and any spike.
- **A reversed decision is rewritten** in both places as the new decision. The old reasoning lives in git history; keep only the facts that still explain the new choice, such as a measurement of the rejected option.

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
