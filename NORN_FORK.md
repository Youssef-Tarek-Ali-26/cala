# Norn Cala fork

Base: Cala `0.25.0`, commit `d0a4be8daf43b19813441cf19a5149604c72f062`.

Purpose: preserve Cala's ledger domain model while making conserved units open and porting the authoritative repository path from Postgres to embedded Turso/SQLite-compatible persistence for Norn.

## Current divergence

- `Currency` accepts bounded caller-defined unit codes in addition to ISO and crypto codes.
- Custom codes are process-interned so the existing `Copy` API and balance-key representation remain compatible.
- Codes are limited to 64 ASCII alphanumeric or `_-.:/` characters and the process registry is bounded to 65,536 distinct custom units.
- JSON serialization/deserialization and CEL coercion continue to use the stable string code.
- The optional `turso-storage` feature exact-pins embedded Turso
  `0.8.0-pre.7` and compiles a private `Db`/`ReadOp`/`WriteOp` seam. Existing
  public CALA repositories still use Postgres and are not yet threaded through
  this seam.
- Authority writes are constructed only with `TransactionBehavior::Immediate`;
  scoped migration/probe failures explicitly roll back before returning.
  Deferred and concurrent writes are outside the admitted Norn profile.
- Reads use a separate connection configured and tested with
  `PRAGMA query_only = 1`. Both connections assert the pinned profile:
  `journal_mode=wal`, `synchronous=FULL`, foreign keys enabled, and a bounded
  50 ms busy timeout. Cross-process ownership/fencing still belongs to Norn.
- `migrations-turso/0001_core.sql` defines the first SQLite-compatible core
  tables. The embedded bytes are SHA-256 checked, and open fails closed if an
  existing migration name, version, or fingerprint is unknown. The schema
  restores CALA's global external-transaction identity and constrains stored
  status/layer/direction values to CALA's lowercase enum encodings.
- The pinned-engine probe covers recursive CTEs, window functions, row-value
  comparison, partial-UNIQUE NULL/duplicate behavior, `RETURNING`, JSON
  functions, and specifically classified composite foreign-key enforcement.
- Stored generated columns are not admitted: pinned Turso requires an
  experimental builder flag for them. The core schema does not depend on that
  feature.

## Planned Turso port order

1. Core journals, accounts, templates, transactions, entries, and current balances.
2. Atomic posting with duplicate/external-id prevention and deterministic balance-key ordering under one writer.
3. Norn conservation-certificate gate and arbitrary-unit fixtures.
4. Account sets and velocity enforcement.
5. Eventual/effective balance rollups and background job parity.

Postgres advisory locks will not be translated mechanically. The Turso path must establish equivalent safety through ordered authority, versions, uniqueness constraints, and atomic state/event/idempotency commit.

This file records Norn-specific modifications as required by the Apache-2.0 fork policy. It does not claim that the Turso repository port is complete.
