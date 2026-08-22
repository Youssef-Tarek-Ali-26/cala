# Norn Cala fork

Base: Cala `0.25.0`, commit `d0a4be8daf43b19813441cf19a5149604c72f062`.

Purpose: preserve Cala's ledger domain model while making conserved units open and porting the authoritative repository path from Postgres to embedded Turso/SQLite-compatible persistence for Norn.

## Current divergence

- `Currency` accepts bounded caller-defined unit codes in addition to ISO and crypto codes.
- Custom codes are process-interned so the existing `Copy` API and balance-key representation remain compatible.
- Codes are limited to 64 ASCII alphanumeric or `_-.:/` characters and the process registry is bounded to 65,536 distinct custom units.
- JSON serialization/deserialization and CEL coercion continue to use the stable string code.

## Planned Turso port order

1. Core journals, accounts, templates, transactions, entries, and current balances.
2. Atomic posting with duplicate/external-id prevention and deterministic balance-key ordering under one writer.
3. Norn conservation-certificate gate and arbitrary-unit fixtures.
4. Account sets and velocity enforcement.
5. Eventual/effective balance rollups and background job parity.

Postgres advisory locks will not be translated mechanically. The Turso path must establish equivalent safety through ordered authority, versions, uniqueness constraints, and atomic state/event/idempotency commit.

This file records Norn-specific modifications as required by the Apache-2.0 fork policy. It does not claim that the Turso repository port is complete.
