## Go
- Run `scripts/lint.sh` before every commit: gofmt, go vet, staticcheck, govulncheck and the
  dependency license check. Fix the code rather than suppressing a finding; when a
  `//lint:ignore` is right, give its reason.
- Run `scripts/seeds.sh` before a change goes to review: every seeded test over its per-commit seeds.
  A failure prints the command that replays it.
- Everything but `cmd/` and `tools/` lives under `internal/`. Packages there are sans-IO, except
  `loop`, `driver/…`, `frontend/…` and `csi` (see "Modularity" in `docs/design.md`).
- In a sans-IO package: no goroutine, channel, `select`, `sync` or `sync/atomic`; no clock, `os`,
  `net`, `syscall` or `unsafe`; no `range` over a map. Time comes from `ctx.Now` (`CLOCK_BOOTTIME`)
  and `ctx.Raw` (`CLOCK_MONOTONIC_RAW`), randomness from a seeded `math/rand/v2` source the machine
  owns. A time daemon can slew `ctx.Now` by up to 22.6%, so anything that must hold across nodes,
  such as a lease, is counted on both: a holder stops on whichever clock runs faster, a grantor waits
  for the slower ("Lease time" in `docs/design.md`). A corrupt message or failed disk is an event to
  handle; panic only on a broken invariant of the machine's own state. Protocol logic goes here,
  never in a frontend.
- On the data path, follow "Runtime in Go" in `docs/design.md`: one epoll loop per core on raw
  descriptors, never a goroutine per connection, `net.Conn`, channel, `select` or `time.Timer`; no allocation per operation; bulk data pointer-free and off the Go
  heap; I/O buffers as pool handles; hand-written encodings. Off the data path, write ordinary Go.
- Tests feed events and assert on actions, with no mocks. Fuzz every decoder. Register new
  invariants with the simulator in the same change, and turn a seed that finds a bug into a
  regression test before the fix. A component covers its section of `docs/test-scenarios.md`.
- Hot-path benchmarks call `b.ReportAllocs` and must report zero allocations per operation.

## Comments
- Default to no comments. Write code whose names and structure make intent clear.
- Only comment a constraint the code cannot express: a workaround for an external bug,
  a non-obvious invariant, a reason something that looks wrong is intentional.
- Never comment what the next line does, never leave "changed X to Y" or "added for task Z"
  notes. That history goes in the commit message.
- Exported identifiers get a one-line doc comment that starts with their name; nothing more.
- When editing existing code, don't add comments to lines you didn't need to change.

## Documents
- `docs/design.md` is the design, including every decision and why it was made;
  `docs/build-checklist.md` the reviewable pieces of phases 1 to 5; `docs/test-scenarios.md`
  the scenarios their tests cover; `docs/glossary.md` explains the terms they use. The
  `design-doc` skill says which change goes where.
- Docs and code state current facts, arranged so they are easiest to read and understand.
  Git history keeps the past; don't preserve it in the text. A reversed decision is rewritten
  as the new decision.
- When a doc needs updating, consider rewriting the whole doc or section instead of patching
  it or appending a block. Appended blocks drift into a changelog.
- Before adding content, check for duplication elsewhere in the doc (and related docs).
  Merge or remove duplicates, and rewrite when that gives a clearer result.

## Spikes and studies
- `spikes/` holds throwaway experiments that answered one design question each, and `studies/`
  findings about other projects. Both are kept as records: their code is not part of the module,
  is not linted, and may need the commit it was written against to build. The `spike` skill
  says how to run a new one.

## Tech debt
- `docs/tech-debts.md` lists every workaround, version pin and pre-release dependency,
  each with why it exists and what must be true to remove it.
- When you add one, add its entry in the same change. When you touch code or a dependency
  an entry covers, check whether its exit condition is met.
- When an entry is resolved, remove the workaround and delete the entry. Don't mark it
  done or move it to a "resolved" list; git history keeps that.
