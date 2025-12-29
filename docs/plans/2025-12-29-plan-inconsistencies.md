# PyCassandra Plan Inconsistencies

**Date:** 2025-12-29  
**Purpose:** Document inconsistencies between the design doc and Phase 1 implementation plan to guide reconciliation.

**Referenced documents:**
- [2025-12-29-pycassandra-design.md](2025-12-29-pycassandra-design.md) (Design)
- [2025-12-29-phase1-write-operations.md](2025-12-29-phase1-write-operations.md) (Phase 1)

---

## 1. Naming / Scope / Phases

| Inconsistency | Design | Phase 1 | Why it matters |
|---------------|--------|---------|----------------|
| Delete-flag phase assignment | Listed as separate "Phase 2: Delete Flag Support" | Implemented directly in Phase 1 (Tasks 2, 4); "Next Steps" even says Phase 2 is "already included in Phase 1" | Confusing phase numbering; unclear what future "Phase 2" actually contains |

---

## 2. Import Strategy ("imports inside methods")

| Inconsistency | Design | Phase 1 | Why it matters |
|---------------|--------|---------|----------------|
| Top-level vs executor-side imports | "Imports inside methods (`write()` / `read()`) for partition-level execution" | Task 2 imports `Cluster`, `PlainTextAuthProvider`, `ssl` at module top; Task 4 re-imports inside `write()` | Violates stated principle; complicates test patching (two import locations) |

---

## 3. `batch_size` Semantics

| Inconsistency | Design | Phase 1 | Why it matters |
|---------------|--------|---------|----------------|
| Batching vs concurrency | "Collect rows into batches (size = `batch_size`), for each batch execute concurrently" | `write()` buffers *all* partition rows into `insert_params`/`delete_params`, then calls `execute_concurrent_with_args(..., concurrency=self.batch_size)` | Unbounded memory usage; option name misleading (concurrency ≠ batch size) |

---

## 4. Consistency-Level Validation

| Inconsistency | Design | Phase 1 | Why it matters |
|---------------|--------|---------|----------------|
| Invalid level handling | "Consistency level validated against known Cassandra levels" (implies fail if unknown) | `consistency_map.get(..., ConsistencyLevel.LOCAL_QUORUM)` silently falls back on unknown value | Silent fallback vs fail-fast; behavior differs from documented validation rule |

---

## 5. Streaming Correctness

| Inconsistency | Design | Phase 1 | Why it matters |
|---------------|--------|---------|----------------|
| Integration test validity | Implies streaming writes are supported and testable | `test_write_streaming_integration` uses `spark.createDataFrame(...)` then `df.writeStream...`; that DF is not streaming | Test will fail at Spark level before data source is invoked; false confidence |
| Abort/commit error handling | "Fail fast; include partial error context" | `abort()` says "just log the failure" with no logging mechanism defined; no actionable error info | No consistency on what happens when streaming micro-batch fails |

---

## 6. Schema / Type Handling

| Inconsistency | Design | Phase 1 | Why it matters |
|---------------|--------|---------|----------------|
| UUID support status | Reads: UUID/TIMEUUID → `StringType`, labeled "Unsupported, safe fallback"; Writes: String → UUID "automatic conversion with validation" | Implements `String → uuid.UUID` conversion | UUID is called both "unsupported" and "supported" depending on section |
| `schema()` method | "DataSource implements `schema()` — offloads to CassandraReader" | Phase 1 plan does not mention or test `schema()`; reader not implemented | Inconsistent with design's stated DataSource API |
| Validation timing vs import rule | "Fail fast in `__init__`" | Writer init opens temporary Cassandra connection for PK validation (good for fail-fast), but requires driver import at object-creation time | Conflicts with "imports inside methods" rule; tests must mock driver for any writer instantiation |

---

## 7. Test Plan vs Implementation Mechanics

| Inconsistency | Design | Phase 1 | Why it matters |
|---------------|--------|---------|----------------|
| Patch targets | N/A | Tests patch `cassandra_data_source.execute_concurrent_with_args` | `write()` imports from `cassandra.concurrent` inside the method — different symbol; patch won't intercept call |
| Driver return shape | N/A | Mocks return `[MagicMock(success=True)]` | Actual driver behavior depends on `raise_on_first_error`; mock may not match real contract, giving false-positive tests |

---

## 8. Options Validation Rules

| Inconsistency | Design | Phase 1 | Why it matters |
|---------------|--------|---------|----------------|
| SSL option dependency | "If `ssl_ca_cert` provided, `ssl_enabled` must be true" | Code creates SSL context if `ssl_enabled`; loads CA cert if provided; no explicit cross-validation | Missing validation; user can specify cert without enabling SSL |
| Delete flag column existence | Delete flag is a documented option | Validates "both-or-neither" for column/value; does **not** check that `delete_flag_column` exists in DF schema | Runtime `KeyError` instead of clear validation error |

---

## Additional Gaps (not contradictions, but missing from both docs)

These items are absent or underspecified in both documents and should be addressed when reconciling:

| Gap | Impact |
|-----|--------|
| SaveMode handling (`append`, `overwrite`, `errorifexists`, `ignore`) | Users get unexpected behavior or silent failures |
| Null PK values at runtime | Insert/delete will fail with driver error instead of clear message |
| Identifier quoting (case-sensitive / reserved-word columns) | Generated CQL breaks for non-simple identifiers |
| Multiple contact points / local DC / load-balancing | Production clusters often require these |
| Timeouts (connection, request) | No way to tune for slow networks |
| Streaming at-least-once semantics + idempotence guidance | Users may not understand duplicate-row implications |
| Unknown column in DF (not in Cassandra table) | Driver error instead of early validation |

---

## Recommended Next Steps

1. Choose authoritative phase numbering (delete-flag in Phase 1 or Phase 2).
2. Align import strategy: either top-level (simpler) or executor-side (more isolated) — not both.
3. Rename / split `batch_size` into `concurrency` and `rows_per_batch`; implement incremental flush.
4. Add explicit SaveMode validation.
5. Fix streaming integration test to use an actual streaming source.
6. Reconcile UUID support language ("supported" with String↔UUID conversion).
7. Ensure test patch targets match actual import paths.
8. Add validation for SSL option dependency and delete-flag column existence.

