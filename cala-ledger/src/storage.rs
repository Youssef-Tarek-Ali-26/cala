//! Private Turso operation seam for Norn's ordered-authority storage profile.
//!
//! This is deliberately not a generic SQL abstraction. `ReadOp` owns a
//! dedicated engine-enforced query-only connection, while `WriteOp` can only be
//! obtained from [`Db::begin_write`]. That constructor acquires write authority
//! with an explicit `BEGIN IMMEDIATE` transaction before authoritative state is
//! read. The invariant is not valid for Turso's default deferred transaction or
//! its concurrent-write mode.

// The Turso repository port remains deliberately private until its transaction
// graph can replace (rather than mix with) the public Postgres services. It
// does not yet implement outbox publication or durable idempotent retry
// results and is not authority-ready. Its contract tests exercise the
// implementation in the meantime; remove this allowance when the public
// engine consumes the Turso repositories.
#![allow(dead_code)]

mod journal_account;

use std::time::Duration;

use sha2::{Digest, Sha256};
use thiserror::Error;
use turso::transaction::{DropBehavior, TransactionBehavior};

const CORE_SCHEMA_VERSION: i64 = 1;
const CORE_SCHEMA_NAME: &str = "norn-cala-core";
// Updated whenever migrations-turso/0001_core.sql changes. `Db::open` hashes
// the embedded bytes and refuses to open if this constant drifts.
const CORE_SCHEMA_FINGERPRINT: &str =
    "sha256:60949280dc976506914f7872957b883f54e4908fe049bb5626fddf6e059b75fc";
const CORE_SCHEMA: &str = include_str!("../migrations-turso/0001_core.sql");

const AUTHORITY_JOURNAL_MODE: &str = "wal";
const AUTHORITY_SYNCHRONOUS: i64 = 2; // SQLite/Turso FULL.
const AUTHORITY_BUSY_TIMEOUT_MS: u64 = 50;

#[derive(Debug, Error)]
pub(crate) enum StorageError {
    #[error("Turso storage error: {0}")]
    Turso(#[from] turso::Error),
    #[error("incompatible Turso engine: {0}")]
    IncompatibleEngine(String),
    #[error("embedded core schema fingerprint mismatch: expected {expected}, computed {computed}")]
    EmbeddedSchemaFingerprint {
        expected: &'static str,
        computed: String,
    },
    #[error("migration {version} fingerprint mismatch: expected {expected}, found {found}")]
    MigrationFingerprint {
        version: i64,
        expected: &'static str,
        found: String,
    },
    #[error("migration {version} has unexpected name {found:?}; expected {expected:?}")]
    MigrationName {
        version: i64,
        expected: &'static str,
        found: String,
    },
    #[error("database contains unsupported migration version {version}")]
    UnsupportedMigration { version: i64 },
    #[error("storage operation failed ({operation}); explicit rollback also failed: {rollback}")]
    RollbackFailed {
        operation: Box<StorageError>,
        rollback: turso::Error,
    },
    #[error("successful scoped operation could not be explicitly rolled back: {0}")]
    RollbackAfterSuccess(turso::Error),
}

/// One write connection and one dedicated read-only connection for a CALA
/// authority executor.
///
/// `Db` is intentionally not `Clone`: its owner must serialize access. Norn's
/// outer file router and process fencing remain responsible for guaranteeing one
/// authority-writer process per scope-owned database file.
#[derive(Debug)]
pub(crate) struct Db {
    write_connection: turso::Connection,
    read_connection: turso::Connection,
}

/// A query-only snapshot backed by a connection with `PRAGMA query_only = ON`.
///
/// The deferred transaction establishes its snapshot on the first read and
/// keeps identity, projection, and event-stream reads on that same snapshot.
/// Construction is private to [`Db::begin_read`].
pub(crate) struct ReadOp<'connection> {
    transaction: turso::transaction::Transaction<'connection>,
}

/// The only operation allowed to mutate authoritative CALA state.
///
/// Construction is private to [`Db::begin_write`]. Callers must explicitly
/// commit or roll back; migration and probe scopes do not rely on drop cleanup.
#[derive(Debug)]
pub(crate) struct WriteOp<'connection> {
    transaction: turso::transaction::Transaction<'connection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SqlFeatureReport {
    pub(crate) recursive_cte: bool,
    pub(crate) window_function: bool,
    pub(crate) row_value_comparison: bool,
    pub(crate) partial_unique_null_semantics: bool,
    pub(crate) generated_stored_column: bool,
    pub(crate) returning: bool,
    pub(crate) json_functions: bool,
    pub(crate) composite_foreign_key: bool,
}

impl SqlFeatureReport {
    /// Capability result for the exact `0.8.0-pre.7` engine pin.
    ///
    /// Stored generated columns require an experimental builder flag at this
    /// revision and are deliberately outside the admitted core schema.
    const PINNED_ENGINE: Self = Self {
        recursive_cte: true,
        window_function: true,
        row_value_comparison: true,
        partial_unique_null_semantics: true,
        generated_stored_column: false,
        returning: true,
        json_functions: true,
        composite_foreign_key: true,
    };
}

impl Db {
    pub(crate) async fn open(path: &str) -> Result<Self, StorageError> {
        let path = path.trim();
        if path.is_empty() {
            return Err(StorageError::IncompatibleEngine(
                "database path must not be empty".to_owned(),
            ));
        }
        require_embedded_schema_fingerprint()?;

        let database = turso::Builder::new_local(path).build().await?;
        let write_connection = database.connect()?;
        configure_write_connection(&write_connection).await?;

        let read_connection = database.connect()?;
        configure_read_connection(&read_connection).await?;

        let mut db = Self {
            write_connection,
            read_connection,
        };
        db.validate_existing_migrations().await?;
        Ok(db)
    }

    pub(crate) async fn begin_read(&mut self) -> Result<ReadOp<'_>, StorageError> {
        let transaction = self
            .read_connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .await?;
        Ok(ReadOp { transaction })
    }

    /// Acquire authority before reading the state that a mutation will validate.
    pub(crate) async fn begin_write(&mut self) -> Result<WriteOp<'_>, StorageError> {
        let transaction = self
            .write_connection
            .transaction_with_behavior(authority_transaction_behavior())
            .await?;
        Ok(WriteOp { transaction })
    }

    pub(crate) async fn migrate(&mut self, applied_at: &str) -> Result<(), StorageError> {
        let write = self.begin_write().await?;
        let outcome = async {
            write.execute_batch(CORE_SCHEMA).await?;
            write
                .execute(
                    "INSERT INTO cala_schema_migrations \
                     (version, name, source_fingerprint, applied_at) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(version) DO NOTHING",
                    (
                        CORE_SCHEMA_VERSION,
                        CORE_SCHEMA_NAME,
                        CORE_SCHEMA_FINGERPRINT,
                        applied_at,
                    ),
                )
                .await?;

            let mut rows = write
                .query(
                    "SELECT name, source_fingerprint FROM cala_schema_migrations \
                     WHERE version = ?1",
                    (CORE_SCHEMA_VERSION,),
                )
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                StorageError::IncompatibleEngine(
                    "core migration record was not persisted inside its transaction".to_owned(),
                )
            })?;
            let found_name = row.get::<String>(0)?;
            let found_fingerprint = row.get::<String>(1)?;
            drop(rows); // No live statement may cross COMMIT on pinned Turso.

            validate_migration_identity(CORE_SCHEMA_VERSION, found_name, found_fingerprint)?;
            Ok(())
        }
        .await;

        commit_scope(write, outcome).await
    }

    /// Execute the exact SQL feature probe required by the first core port.
    ///
    /// Probe objects and rows are explicitly rolled back on success and error;
    /// a successful report therefore proves execution on the pinned engine
    /// without mutating durable state.
    pub(crate) async fn probe_required_sql(&mut self) -> Result<SqlFeatureReport, StorageError> {
        let write = self.begin_write().await?;
        let outcome = async {
            write
                .execute_batch(
                    "CREATE TABLE norn_cala_probe_parent (\
                         left_id INTEGER NOT NULL,\
                         right_id INTEGER NOT NULL,\
                         PRIMARY KEY (left_id, right_id)\
                     ) STRICT;\
                     CREATE TABLE norn_cala_probe_child (\
                         id INTEGER PRIMARY KEY,\
                         left_id INTEGER NOT NULL,\
                         right_id INTEGER NOT NULL,\
                         payload TEXT NOT NULL CHECK (json_valid(payload)),\
                         payload_kind TEXT,\
                         FOREIGN KEY (left_id, right_id) \
                             REFERENCES norn_cala_probe_parent(left_id, right_id)\
                     ) STRICT;\
                     CREATE UNIQUE INDEX norn_cala_probe_partial_unique \
                         ON norn_cala_probe_child(payload_kind) \
                         WHERE payload_kind IS NOT NULL;",
                )
                .await?;
            write
                .execute(
                    "INSERT INTO norn_cala_probe_parent (left_id, right_id) VALUES (?1, ?2)",
                    (7_i64, 11_i64),
                )
                .await?;

            let mut returned = write
                .query(
                    "INSERT INTO norn_cala_probe_child \
                     (id, left_id, right_id, payload, payload_kind) \
                     VALUES (?1, ?2, ?3, ?4, ?5) \
                     RETURNING payload_kind",
                    (1_i64, 7_i64, 11_i64, r#"{"kind":"core"}"#, "core"),
                )
                .await?;
            let returned_kind = returned
                .next()
                .await?
                .ok_or_else(|| {
                    StorageError::IncompatibleEngine(
                        "INSERT RETURNING produced no probe row".to_owned(),
                    )
                })?
                .get::<String>(0)?;
            drop(returned);
            require_probe(returned_kind == "core", "INSERT RETURNING")?;

            // A partial UNIQUE index excludes NULL rows but rejects duplicate
            // non-NULL identities.
            for id in [2_i64, 3_i64] {
                write
                    .execute(
                        "INSERT INTO norn_cala_probe_child \
                         (id, left_id, right_id, payload, payload_kind) \
                         VALUES (?1, ?2, ?3, ?4, NULL)",
                        (id, 7_i64, 11_i64, r#"{"kind":null}"#),
                    )
                    .await?;
            }
            let duplicate_partial_key = write
                .execute(
                    "INSERT INTO norn_cala_probe_child \
                     (id, left_id, right_id, payload, payload_kind) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    (4_i64, 7_i64, 11_i64, r#"{"kind":"core"}"#, "core"),
                )
                .await;
            require_constraint(
                duplicate_partial_key,
                "unique",
                "partial UNIQUE duplicate enforcement",
            )?;

            let generated_stored_column = match write
                .execute_batch(
                    "CREATE TABLE norn_cala_probe_generated (\
                         source INTEGER NOT NULL,\
                         doubled INTEGER GENERATED ALWAYS AS (source * 2) STORED\
                     ) STRICT;",
                )
                .await
            {
                Ok(()) => true,
                Err(StorageError::Turso(error))
                    if error.to_string().contains("experimental-generated-columns") =>
                {
                    false
                }
                Err(error) => return Err(error),
            };

            let recursive_sum = query_i64(
                &write,
                "WITH RECURSIVE seq(n) AS (\
                     VALUES(1) UNION ALL SELECT n + 1 FROM seq WHERE n < 4\
                 ) SELECT SUM(n) FROM seq",
            )
            .await?;
            require_probe(recursive_sum == 10, "recursive CTE")?;

            let window_max = query_i64(
                &write,
                "WITH values_(n) AS (VALUES(3), (1), (2)) \
                 SELECT MAX(position) FROM (\
                     SELECT ROW_NUMBER() OVER (ORDER BY n) AS position FROM values_\
                 )",
            )
            .await?;
            require_probe(window_max == 3, "window function")?;

            let tuple_order = query_i64(&write, "SELECT (2, 3) > (1, 99)").await?;
            require_probe(tuple_order == 1, "row-value comparison")?;

            let json_ok = query_i64(
                &write,
                r#"SELECT json_extract('{"nested":{"value":42}}', '$.nested.value')"#,
            )
            .await?;
            require_probe(json_ok == 42, "JSON functions")?;

            // The first child is the valid FK control. This second insert must
            // fail specifically as a foreign-key constraint, not merely as any
            // arbitrary SQL error.
            let invalid_fk = write
                .execute(
                    "INSERT INTO norn_cala_probe_child \
                     (id, left_id, right_id, payload) VALUES (?1, ?2, ?3, ?4)",
                    (5_i64, 999_i64, 999_i64, r#"{"kind":"invalid"}"#),
                )
                .await;
            require_constraint(
                invalid_fk,
                "foreign key",
                "composite foreign-key enforcement",
            )?;

            Ok(SqlFeatureReport {
                generated_stored_column,
                ..SqlFeatureReport::PINNED_ENGINE
            })
        }
        .await;

        rollback_scope(write, outcome).await
    }

    async fn validate_existing_migrations(&mut self) -> Result<(), StorageError> {
        let read = self.begin_read().await?;
        let outcome = async {
            let migration_table_exists = query_i64(
                &read,
                "SELECT EXISTS(\
                 SELECT 1 FROM sqlite_schema \
                 WHERE type = 'table' AND name = 'cala_schema_migrations'\
             )",
            )
            .await?;
            if migration_table_exists == 0 {
                let preexisting_cala_objects = query_i64(
                    &read,
                    "SELECT COUNT(*) FROM sqlite_schema \
                     WHERE type IN ('table', 'index', 'view', 'trigger') \
                       AND substr(name, 1, 5) = 'cala_'",
                )
                .await?;
                if preexisting_cala_objects != 0 {
                    return Err(StorageError::IncompatibleEngine(format!(
                        "database contains {preexisting_cala_objects} pre-existing cala_* schema \
                         objects but no cala_schema_migrations metadata"
                    )));
                }
                return Ok(());
            }

            let mut rows = read
                .query(
                    "SELECT version, name, source_fingerprint \
                 FROM cala_schema_migrations ORDER BY version",
                    (),
                )
                .await?;
            let mut saw_migration = false;
            while let Some(row) = rows.next().await? {
                saw_migration = true;
                let version = row.get::<i64>(0)?;
                if version != CORE_SCHEMA_VERSION {
                    return Err(StorageError::UnsupportedMigration { version });
                }
                validate_migration_identity(version, row.get::<String>(1)?, row.get::<String>(2)?)?;
            }
            drop(rows);
            if !saw_migration {
                return Err(StorageError::IncompatibleEngine(
                    "database contains cala_schema_migrations without a recorded migration"
                        .to_owned(),
                ));
            }
            Ok(())
        }
        .await;
        close_read_scope(read, outcome).await
    }

    #[cfg(test)]
    fn writer_is_autocommit(&self) -> Result<bool, StorageError> {
        Ok(self.write_connection.is_autocommit()?)
    }
}

impl ReadOp<'_> {
    pub(crate) async fn query(
        &self,
        sql: &str,
        params: impl turso::IntoParams,
    ) -> Result<turso::Rows, StorageError> {
        Ok(self.transaction.query(sql, params).await?)
    }

    pub(crate) async fn close(self) -> Result<(), StorageError> {
        Ok(self.transaction.rollback().await?)
    }
}

impl WriteOp<'_> {
    pub(crate) async fn execute(
        &self,
        sql: &str,
        params: impl turso::IntoParams,
    ) -> Result<u64, StorageError> {
        Ok(self.transaction.execute(sql, params).await?)
    }

    pub(crate) async fn execute_batch(&self, sql: &str) -> Result<(), StorageError> {
        Ok(self.transaction.execute_batch(sql).await?)
    }

    pub(crate) async fn query(
        &self,
        sql: &str,
        params: impl turso::IntoParams,
    ) -> Result<turso::Rows, StorageError> {
        Ok(self.transaction.query(sql, params).await?)
    }

    pub(crate) async fn commit(mut self) -> Result<(), StorageError> {
        // Turso's consuming `Transaction::commit` marks a failed transaction
        // for rollback on a later connection access. Authority code cannot
        // return while rollback is merely pending, so issue the equivalent SQL
        // directly and explicitly rollback if COMMIT reports an error.
        match self.transaction.execute("COMMIT", ()).await {
            Ok(_) => {
                self.transaction.set_drop_behavior(DropBehavior::Ignore);
                Ok(())
            }
            Err(commit) => match self.transaction.execute("ROLLBACK", ()).await {
                Ok(_) => {
                    self.transaction.set_drop_behavior(DropBehavior::Ignore);
                    Err(StorageError::Turso(commit))
                }
                Err(rollback) => Err(StorageError::RollbackFailed {
                    operation: Box::new(StorageError::Turso(commit)),
                    rollback,
                }),
            },
        }
    }

    pub(crate) async fn rollback(self) -> Result<(), StorageError> {
        Ok(self.transaction.rollback().await?)
    }
}

fn authority_transaction_behavior() -> TransactionBehavior {
    TransactionBehavior::Immediate
}

fn embedded_schema_fingerprint() -> String {
    format!("sha256:{:x}", Sha256::digest(CORE_SCHEMA.as_bytes()))
}

fn require_embedded_schema_fingerprint() -> Result<(), StorageError> {
    let computed = embedded_schema_fingerprint();
    if computed == CORE_SCHEMA_FINGERPRINT {
        Ok(())
    } else {
        Err(StorageError::EmbeddedSchemaFingerprint {
            expected: CORE_SCHEMA_FINGERPRINT,
            computed,
        })
    }
}

fn validate_migration_identity(
    version: i64,
    found_name: String,
    found_fingerprint: String,
) -> Result<(), StorageError> {
    if found_name != CORE_SCHEMA_NAME {
        return Err(StorageError::MigrationName {
            version,
            expected: CORE_SCHEMA_NAME,
            found: found_name,
        });
    }
    if found_fingerprint != CORE_SCHEMA_FINGERPRINT {
        return Err(StorageError::MigrationFingerprint {
            version,
            expected: CORE_SCHEMA_FINGERPRINT,
            found: found_fingerprint,
        });
    }
    Ok(())
}

async fn configure_write_connection(connection: &turso::Connection) -> Result<(), StorageError> {
    connection
        .pragma_update("journal_mode", format!("'{AUTHORITY_JOURNAL_MODE}'"))
        .await?;
    connection.pragma_update("synchronous", "FULL").await?;
    connection.pragma_update("foreign_keys", "ON").await?;
    connection.busy_timeout(authority_busy_timeout())?;
    assert_connection_profile(connection, false).await
}

async fn configure_read_connection(connection: &turso::Connection) -> Result<(), StorageError> {
    connection.pragma_update("synchronous", "FULL").await?;
    connection.pragma_update("foreign_keys", "ON").await?;
    connection.busy_timeout(authority_busy_timeout())?;
    // The pinned parser accepts numeric boolean pragma values here; `ON` is
    // rejected for `query_only` at 0.8.0-pre.7.
    connection.pragma_update("query_only", 1).await?;
    assert_connection_profile(connection, true).await
}

async fn assert_connection_profile(
    connection: &turso::Connection,
    query_only: bool,
) -> Result<(), StorageError> {
    let expected_journal_mode = AUTHORITY_JOURNAL_MODE;
    let actual_journal_mode = query_connection_string(connection, "PRAGMA journal_mode").await?;
    require_probe(
        actual_journal_mode.eq_ignore_ascii_case(expected_journal_mode),
        &format!("journal_mode expected {expected_journal_mode}, found {actual_journal_mode}"),
    )?;
    require_probe(
        query_connection_i64(connection, "PRAGMA synchronous").await? == AUTHORITY_SYNCHRONOUS,
        "PRAGMA synchronous = FULL",
    )?;
    require_probe(
        query_connection_i64(connection, "PRAGMA foreign_keys").await? == 1,
        "PRAGMA foreign_keys = ON",
    )?;
    require_probe(
        query_connection_i64(connection, "PRAGMA busy_timeout").await?
            == i64::try_from(AUTHORITY_BUSY_TIMEOUT_MS).unwrap_or(i64::MAX),
        "bounded PRAGMA busy_timeout",
    )?;
    require_probe(
        query_connection_i64(connection, "PRAGMA query_only").await? == i64::from(query_only),
        if query_only {
            "read connection PRAGMA query_only = ON"
        } else {
            "write connection PRAGMA query_only = OFF"
        },
    )
}

fn authority_busy_timeout() -> Duration {
    Duration::from_millis(AUTHORITY_BUSY_TIMEOUT_MS)
}

async fn commit_scope<T>(
    write: WriteOp<'_>,
    outcome: Result<T, StorageError>,
) -> Result<T, StorageError> {
    match outcome {
        Ok(value) => {
            write.commit().await?;
            Ok(value)
        }
        Err(operation) => rollback_after_error(write, operation).await,
    }
}

async fn rollback_scope<T>(
    write: WriteOp<'_>,
    outcome: Result<T, StorageError>,
) -> Result<T, StorageError> {
    let rollback = write.rollback().await;
    match (outcome, rollback) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(operation), Ok(())) => Err(operation),
        (Err(operation), Err(StorageError::Turso(rollback))) => Err(StorageError::RollbackFailed {
            operation: Box::new(operation),
            rollback,
        }),
        (Ok(_), Err(StorageError::Turso(rollback))) => {
            Err(StorageError::RollbackAfterSuccess(rollback))
        }
        (_, Err(other)) => Err(other),
    }
}

async fn rollback_after_error<T>(
    write: WriteOp<'_>,
    operation: StorageError,
) -> Result<T, StorageError> {
    match write.rollback().await {
        Ok(()) => Err(operation),
        Err(StorageError::Turso(rollback)) => Err(StorageError::RollbackFailed {
            operation: Box::new(operation),
            rollback,
        }),
        Err(other) => Err(other),
    }
}

async fn close_read_scope<T>(
    read: ReadOp<'_>,
    outcome: Result<T, StorageError>,
) -> Result<T, StorageError> {
    let close = read.close().await;
    match (outcome, close) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(operation), Ok(())) => Err(operation),
        (Err(operation), Err(StorageError::Turso(rollback))) => Err(StorageError::RollbackFailed {
            operation: Box::new(operation),
            rollback,
        }),
        (Ok(_), Err(StorageError::Turso(rollback))) => {
            Err(StorageError::RollbackAfterSuccess(rollback))
        }
        (_, Err(other)) => Err(other),
    }
}

async fn query_db_i64(db: &mut Db, sql: &str) -> Result<i64, StorageError> {
    let read = db.begin_read().await?;
    let outcome = query_i64(&read, sql).await;
    close_read_scope(read, outcome).await
}

async fn query_i64(operation: &impl QueryOperation, sql: &str) -> Result<i64, StorageError> {
    let mut rows = operation.query_no_params(sql).await?;
    let value = rows
        .next()
        .await?
        .ok_or_else(|| StorageError::IncompatibleEngine(format!("query returned no row: {sql}")))?
        .get::<i64>(0)?;
    drop(rows);
    Ok(value)
}

async fn query_connection_i64(
    connection: &turso::Connection,
    sql: &str,
) -> Result<i64, StorageError> {
    let mut rows = connection.query(sql, ()).await?;
    let value = rows
        .next()
        .await?
        .ok_or_else(|| StorageError::IncompatibleEngine(format!("query returned no row: {sql}")))?
        .get::<i64>(0)?;
    drop(rows);
    Ok(value)
}

async fn query_connection_string(
    connection: &turso::Connection,
    sql: &str,
) -> Result<String, StorageError> {
    let mut rows = connection.query(sql, ()).await?;
    let value = rows
        .next()
        .await?
        .ok_or_else(|| StorageError::IncompatibleEngine(format!("query returned no row: {sql}")))?
        .get::<String>(0)?;
    drop(rows);
    Ok(value)
}

trait QueryOperation {
    fn query_no_params(
        &self,
        sql: &str,
    ) -> impl std::future::Future<Output = Result<turso::Rows, StorageError>>;
}

impl QueryOperation for ReadOp<'_> {
    async fn query_no_params(&self, sql: &str) -> Result<turso::Rows, StorageError> {
        self.query(sql, ()).await
    }
}

impl QueryOperation for WriteOp<'_> {
    async fn query_no_params(&self, sql: &str) -> Result<turso::Rows, StorageError> {
        self.query(sql, ()).await
    }
}

fn require_probe(condition: bool, feature: &str) -> Result<(), StorageError> {
    if condition {
        Ok(())
    } else {
        Err(StorageError::IncompatibleEngine(format!(
            "required SQL/storage feature failed its semantic probe: {feature}"
        )))
    }
}

fn require_constraint(
    result: Result<u64, StorageError>,
    expected_fragment: &str,
    feature: &str,
) -> Result<(), StorageError> {
    match result {
        Err(StorageError::Turso(turso::Error::Constraint(message)))
            if message.to_ascii_lowercase().contains(expected_fragment) =>
        {
            Ok(())
        }
        Err(error) => Err(StorageError::IncompatibleEngine(format!(
            "{feature} produced the wrong error class: {error}"
        ))),
        Ok(_) => Err(StorageError::IncompatibleEngine(format!(
            "{feature} accepted an invalid row"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_TIME: &str = "2026-08-22T00:00:00.000000Z";

    #[test]
    fn embedded_schema_matches_reviewed_fingerprint() {
        assert_eq!(embedded_schema_fingerprint(), CORE_SCHEMA_FINGERPRINT);
        require_embedded_schema_fingerprint().expect("embedded schema fingerprint must match");
    }

    #[tokio::test]
    async fn authority_write_constructor_is_immediate() {
        assert!(matches!(
            authority_transaction_behavior(),
            TransactionBehavior::Immediate
        ));

        let mut db = Db::open(":memory:").await.expect("open Turso database");
        let write = db.begin_write().await.expect("begin authority write");
        write.rollback().await.expect("rollback authority write");
    }

    #[tokio::test]
    async fn read_operation_uses_dedicated_query_only_connection() {
        let mut db = Db::open(":memory:").await.expect("open Turso database");
        db.migrate(TEST_TIME).await.expect("run migration");

        let read = db.begin_read().await.expect("begin query-only snapshot");
        let attempted_dml = read
            .query(
                "INSERT INTO cala_journals \
                 (id, version, name, status, payload, created_at, modified_at) \
                 VALUES ('read-bypass', 1, 'bad', 'active', '{}', ?1, ?1)",
                (TEST_TIME,),
            )
            .await;
        let rejected = match attempted_dml {
            Err(StorageError::Turso(error)) => error.to_string(),
            Ok(mut rows) => rows
                .next()
                .await
                .expect_err("query-only connection must reject DML")
                .to_string(),
            Err(error) => panic!("unexpected read-only error class: {error}"),
        };
        assert!(
            rejected.to_ascii_lowercase().contains("read-only")
                || rejected.to_ascii_lowercase().contains("query_only"),
            "unexpected query-only rejection: {rejected}"
        );
        read.close().await.expect("close query-only snapshot");
        assert_eq!(
            query_db_i64(&mut db, "SELECT COUNT(*) FROM cala_journals")
                .await
                .expect("count journals"),
            0
        );
    }

    #[tokio::test]
    async fn core_schema_migrates_idempotently_and_has_every_required_table() {
        let mut db = Db::open(":memory:").await.expect("open Turso database");
        db.migrate(TEST_TIME).await.expect("run first migration");
        db.migrate(TEST_TIME).await.expect("rerun first migration");

        let read = db.begin_read().await.expect("begin schema snapshot");
        for table in [
            "cala_schema_migrations",
            "cala_journals",
            "cala_accounts",
            "cala_tx_templates",
            "cala_transactions",
            "cala_entries",
            "cala_current_balances",
            "cala_balance_history",
            "cala_entity_events",
            "cala_outbox_events",
            "cala_idempotency_results",
        ] {
            let mut rows = read
                .query(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                    (table,),
                )
                .await
                .expect("query migrated table");
            let count = rows
                .next()
                .await
                .expect("read migrated table count")
                .expect("table count row")
                .get::<i64>(0)
                .expect("decode table count");
            drop(rows);
            assert_eq!(count, 1, "missing migrated table {table}");
        }
        read.close().await.expect("close schema snapshot");
    }

    #[tokio::test]
    async fn migration_error_explicitly_rolls_back_schema_changes() {
        let mut db = Db::open(":memory:").await.expect("open Turso database");
        let write = db.begin_write().await.expect("begin setup write");
        write
            .execute_batch(
                "CREATE TABLE cala_schema_migrations (\
                     version INTEGER PRIMARY KEY,\
                     name TEXT NOT NULL UNIQUE,\
                     source_fingerprint TEXT NOT NULL,\
                     applied_at TEXT NOT NULL\
                 ) STRICT;",
            )
            .await
            .expect("create migration table");
        write
            .execute(
                "INSERT INTO cala_schema_migrations \
                 (version, name, source_fingerprint, applied_at) \
                 VALUES (1, ?1, 'sha256:wrong', ?2)",
                (CORE_SCHEMA_NAME, TEST_TIME),
            )
            .await
            .expect("insert mismatched migration");
        write.commit().await.expect("commit setup");

        let error = db
            .migrate(TEST_TIME)
            .await
            .expect_err("mismatched migration must fail");
        assert!(matches!(error, StorageError::MigrationFingerprint { .. }));
        assert!(
            db.writer_is_autocommit()
                .expect("inspect transaction state"),
            "migration error must explicitly rollback before return"
        );
        assert_eq!(
            query_db_i64(
                &mut db,
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'cala_journals')"
            )
            .await
            .expect("inspect rolled-back schema"),
            0
        );
    }

    #[tokio::test]
    async fn probe_error_explicitly_rolls_back_before_return() {
        let mut db = Db::open(":memory:").await.expect("open Turso database");
        let write = db.begin_write().await.expect("begin setup write");
        write
            .execute_batch(
                "CREATE TABLE norn_cala_probe_parent (\
                     left_id INTEGER NOT NULL,\
                     right_id INTEGER NOT NULL,\
                     PRIMARY KEY (left_id, right_id)\
                 ) STRICT;",
            )
            .await
            .expect("create conflicting probe table");
        write.commit().await.expect("commit setup");

        db.probe_required_sql()
            .await
            .expect_err("conflicting probe schema must fail");
        assert!(
            db.writer_is_autocommit()
                .expect("inspect transaction state"),
            "probe error must explicitly rollback before return"
        );
    }

    #[tokio::test]
    async fn open_fails_closed_on_existing_migration_fingerprint_mismatch() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let path = directory.path().join("fingerprint.db");
        let path = path.to_str().expect("utf-8 temporary path");

        let mut clean = Db::open(path).await.expect("clean database stays open");
        let write = clean.begin_write().await.expect("begin setup write");
        write
            .execute_batch(
                "CREATE TABLE cala_schema_migrations (\
                     version INTEGER PRIMARY KEY,\
                     name TEXT NOT NULL UNIQUE,\
                     source_fingerprint TEXT NOT NULL,\
                     applied_at TEXT NOT NULL\
                 ) STRICT;",
            )
            .await
            .expect("create migration table");
        write
            .execute(
                "INSERT INTO cala_schema_migrations \
                 (version, name, source_fingerprint, applied_at) \
                 VALUES (1, ?1, 'sha256:tampered', ?2)",
                (CORE_SCHEMA_NAME, TEST_TIME),
            )
            .await
            .expect("insert tampered fingerprint");
        write.commit().await.expect("commit tampered metadata");
        drop(clean);

        let error = Db::open(path)
            .await
            .expect_err("reopen must reject tampered migration metadata");
        assert!(matches!(error, StorageError::MigrationFingerprint { .. }));
    }

    #[tokio::test]
    async fn open_rejects_preexisting_cala_schema_without_migration_metadata() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let path = directory.path().join("unowned-schema.db");
        let path = path.to_str().expect("utf-8 temporary path");

        let database = turso::Builder::new_local(path)
            .build()
            .await
            .expect("open raw Turso database");
        let connection = database.connect().expect("connect raw Turso database");
        connection
            .execute_batch("CREATE TABLE cala_journals (id TEXT PRIMARY KEY) STRICT;")
            .await
            .expect("create unowned CALA table");
        drop(connection);
        drop(database);

        let error = Db::open(path)
            .await
            .expect_err("unversioned CALA schema must not be blessed by migration");
        assert!(matches!(
            error,
            StorageError::IncompatibleEngine(message)
                if message.contains("pre-existing cala_* schema")
        ));
    }

    #[tokio::test]
    async fn open_rejects_empty_migration_metadata() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let path = directory.path().join("empty-metadata.db");
        let path = path.to_str().expect("utf-8 temporary path");

        let database = turso::Builder::new_local(path)
            .build()
            .await
            .expect("open raw Turso database");
        let connection = database.connect().expect("connect raw Turso database");
        connection
            .execute_batch(
                "CREATE TABLE cala_schema_migrations (\
                     version INTEGER PRIMARY KEY,\
                     name TEXT NOT NULL UNIQUE,\
                     source_fingerprint TEXT NOT NULL,\
                     applied_at TEXT NOT NULL\
                 ) STRICT;",
            )
            .await
            .expect("create empty migration metadata");
        drop(connection);
        drop(database);

        let error = Db::open(path)
            .await
            .expect_err("empty migration metadata must fail closed");
        assert!(matches!(
            error,
            StorageError::IncompatibleEngine(message)
                if message.contains("without a recorded migration")
        ));
    }

    #[tokio::test]
    async fn exact_pinned_engine_executes_required_sql_feature_probe() {
        let mut db = Db::open(":memory:").await.expect("open Turso database");
        db.migrate(TEST_TIME).await.expect("run migration");

        let report = db
            .probe_required_sql()
            .await
            .expect("execute required SQL probe");
        assert_eq!(report, SqlFeatureReport::PINNED_ENGINE);
        assert!(
            db.writer_is_autocommit()
                .expect("inspect transaction state"),
            "probe must explicitly rollback before return"
        );
    }

    #[tokio::test]
    async fn rolled_back_write_is_not_visible_to_following_read() {
        let mut db = Db::open(":memory:").await.expect("open Turso database");
        db.migrate(TEST_TIME).await.expect("run migration");

        let write = db.begin_write().await.expect("begin authority write");
        insert_journal(&write, "journal-one", "active")
            .await
            .expect("insert journal inside authority transaction");
        write.rollback().await.expect("rollback authority write");

        let count = query_db_i64(&mut db, "SELECT COUNT(*) FROM cala_journals")
            .await
            .expect("count journals");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn file_profile_allows_reads_but_rejects_second_immediate_writer() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let path = directory.path().join("contention.db");
        let path = path.to_str().expect("utf-8 temporary path");

        let mut first = Db::open(path).await.expect("open first database owner");
        first.migrate(TEST_TIME).await.expect("migrate database");
        let mut second = Db::open(path).await.expect("open second database owner");

        let first_write = first.begin_write().await.expect("begin first writer");
        insert_journal(&first_write, "uncommitted", "active")
            .await
            .expect("write uncommitted journal");

        let second_error = second
            .begin_write()
            .await
            .expect_err("second immediate writer must be rejected after bounded wait");
        assert!(matches!(
            second_error,
            StorageError::Turso(turso::Error::Busy(_))
        ));
        assert_eq!(
            query_db_i64(&mut second, "SELECT COUNT(*) FROM cala_journals")
                .await
                .expect("read committed state while writer is open"),
            0,
            "read connection must not observe uncommitted state"
        );

        first_write.commit().await.expect("commit first writer");
        assert_eq!(
            query_db_i64(&mut second, "SELECT COUNT(*) FROM cala_journals")
                .await
                .expect("read committed state"),
            1
        );
    }

    #[tokio::test]
    async fn schema_enforces_global_external_ids_and_known_enum_encodings() {
        let mut db = Db::open(":memory:").await.expect("open Turso database");
        db.migrate(TEST_TIME).await.expect("run migration");
        let write = db.begin_write().await.expect("begin authority write");

        insert_journal(&write, "journal-one", "active")
            .await
            .expect("insert first journal");
        insert_journal(&write, "journal-two", "locked")
            .await
            .expect("insert second journal");
        write
            .execute(
                "INSERT INTO cala_accounts \
                 (id, version, code, name, status, payload, created_at, modified_at) \
                 VALUES ('account-one', 1, 'account-one', 'Account', 'active', '{}', ?1, ?1)",
                (TEST_TIME,),
            )
            .await
            .expect("insert account");
        insert_transaction(&write, "transaction-one", "journal-one", "external-one")
            .await
            .expect("insert first transaction");

        require_constraint(
            insert_transaction(&write, "transaction-two", "journal-two", "external-one").await,
            "unique",
            "global external transaction identity",
        )
        .expect("duplicate external identity rejected globally");
        require_constraint(
            insert_journal(&write, "bad-status", "ACTIVE").await,
            "check",
            "journal status encoding",
        )
        .expect("uppercase storage status rejected");
        require_constraint(
            write
                .execute(
                    "INSERT INTO cala_entries \
                     (id, version, transaction_id, journal_id, account_id, entry_sequence, \
                      unit, layer, direction, amount, payload, created_at) \
                     VALUES ('bad-entry', 1, 'transaction-one', 'journal-one', 'account-one', \
                             0, 'USD', 'SETTLED', 'DEBIT', '1', '{}', ?1)",
                    (TEST_TIME,),
                )
                .await,
            "check",
            "entry layer/direction encoding",
        )
        .expect("uppercase storage enums rejected");

        write.rollback().await.expect("rollback schema test");
    }

    async fn insert_journal(
        write: &WriteOp<'_>,
        id: &str,
        status: &str,
    ) -> Result<u64, StorageError> {
        write
            .execute(
                "INSERT INTO cala_journals \
                 (id, version, name, status, payload, created_at, modified_at) \
                 VALUES (?1, 1, ?1, ?2, '{}', ?3, ?3)",
                (id, status, TEST_TIME),
            )
            .await
    }

    async fn insert_transaction(
        write: &WriteOp<'_>,
        id: &str,
        journal_id: &str,
        external_id: &str,
    ) -> Result<u64, StorageError> {
        write
            .execute(
                "INSERT INTO cala_transactions \
                 (id, version, journal_id, external_id, effective_date, correlation_id, \
                  payload, created_at, modified_at) \
                 VALUES (?1, 1, ?2, ?3, '2026-08-22', ?1, '{}', ?4, ?4)",
                (id, journal_id, external_id, TEST_TIME),
            )
            .await
    }
}
