# 0001. Name the project Rock Solid and license it under Apache 2.0

Status: Accepted (2026-10-16)

## Context
Storage software is adopted slowly, and its license is part of the trust decision. The closest systems in the [landscape](../design.md#landscape-and-positioning) use AGPL (MinIO) or a GPL-based network-copyleft license (Vitastor), which keeps companies and homelab users who build on them away. The name should state the project's priority: correctness before features.

## Decision
The project is named Rock Solid and licensed under Apache 2.0.

## Consequences
- Anyone can embed, fork or ship it, including in closed products, with no source-disclosure duty.
- Apache 2.0's patent grant covers contributors' patents, which matters for a storage system.
- Others can build proprietary forks without contributing back. We accept that in exchange for adoption.
