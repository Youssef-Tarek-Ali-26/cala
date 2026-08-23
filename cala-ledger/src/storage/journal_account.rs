//! Private journal/account repository slice for the Turso storage profile.
//!
//! This deliberately remains behind the crate-private storage seam. It does
//! not mix a Turso transaction with the public Postgres-backed `CalaLedger`.
//! Durable idempotency results and outbox publication are not implemented in
//! this slice, so it is not yet an authority-ready replacement.

use chrono::{DateTime, SecondsFormat, Utc};
use es_entity::{EntityEvents, EsEntity, EsEvent, GenericEvent, IntoEvents, TryFromEvents};
use serde::Serialize;
use thiserror::Error;

use crate::{
    account::{Account, AccountEvent, NewAccount},
    journal::{Journal, JournalEvent, NewJournal},
    primitives::{AccountId, JournalId, Status},
};

use super::{Db, ReadOp, StorageError, WriteOp};

const JOURNAL_KIND: &str = "journal";
const ACCOUNT_KIND: &str = "account";

#[derive(Debug, Error)]
pub(crate) enum RepositoryError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("{entity_kind} {id} was not found")]
    NotFound {
        entity_kind: &'static str,
        id: String,
    },
    #[error("{entity_kind} identity {field}={value:?} already exists")]
    DuplicateIdentity {
        entity_kind: &'static str,
        field: &'static str,
        value: String,
    },
    #[error("stale {entity_kind} {id}: expected version {expected}, current version {actual}")]
    StaleVersion {
        entity_kind: &'static str,
        id: String,
        expected: u32,
        actual: u32,
    },
    #[error("account-set backing accounts are outside the Day-2 Turso repository slice")]
    AccountSetUnsupported,
    #[error("corrupt {entity_kind} {id}: {reason}")]
    CorruptState {
        entity_kind: &'static str,
        id: String,
        reason: String,
    },
    #[error("codec failure: {0}")]
    Codec(String),
    #[error(
        "repository operation failed ({operation}); explicit rollback also failed: {rollback}"
    )]
    RollbackFailed {
        operation: Box<RepositoryError>,
        rollback: StorageError,
    },
}

impl From<turso::Error> for RepositoryError {
    fn from(error: turso::Error) -> Self {
        StorageError::Turso(error).into()
    }
}

/// The first Turso-backed CALA repository slice.
///
/// `Db` remains non-cloneable and every mutation starts an explicit immediate
/// write transaction before it checks authoritative state.
pub(crate) struct JournalAccountRepo {
    db: Db,
}

impl JournalAccountRepo {
    pub(crate) async fn open(
        path: &str,
        migration_time: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let mut db = Db::open(path).await?;
        db.migrate(&encode_time(migration_time)).await?;
        Ok(Self { db })
    }

    pub(crate) async fn create_journal(
        &mut self,
        new_journal: NewJournal,
        recorded_at: DateTime<Utc>,
    ) -> Result<Journal, RepositoryError> {
        let mut events = new_journal.into_events();
        let values = match &events
            .iter_new_events()
            .next()
            .ok_or_else(|| {
                corrupt(
                    JOURNAL_KIND,
                    events.id().to_string(),
                    "missing initialized event",
                )
            })?
            .event
        {
            JournalEvent::Initialized { values } => values.clone(),
            JournalEvent::Updated { .. } => {
                return Err(corrupt(
                    JOURNAL_KIND,
                    events.id().to_string(),
                    "first event is not initialized",
                ));
            }
        };
        if events.iter_new_events().count() != 1 {
            return Err(corrupt(
                JOURNAL_KIND,
                values.id.to_string(),
                "new journal must contain exactly one event",
            ));
        }

        let payload = encode_json(&values)?;
        let event_payloads = serialize_events(&events)?;
        let event_types = events.new_event_types();
        let timestamp = encode_time(recorded_at);
        let recorded_at = decode_time(timestamp.clone())?;
        let id = values.id.to_string();
        events.mark_new_events_persisted_at(recorded_at);
        let journal = Journal::try_from_events(events)
            .map_err(|error| RepositoryError::Codec(error.to_string()))?;
        let write = self.db.begin_write().await?;
        let outcome = async {
            ensure_json_identity_available(
                &write,
                "cala_journals",
                "code",
                values.code.as_deref(),
                &id,
                JOURNAL_KIND,
            )
            .await?;
            write
                .execute(
                    "INSERT INTO cala_journals \
                     (id, version, name, status, payload, created_at, modified_at) \
                     VALUES (?1, 1, ?2, ?3, ?4, ?5, ?5)",
                    (
                        id.as_str(),
                        values.name.as_str(),
                        encode_status(values.status),
                        payload.as_str(),
                        timestamp.as_str(),
                    ),
                )
                .await
                .map_err(|error| classify_projection_constraint(error, JOURNAL_KIND, "id", &id))?;
            insert_events(
                &write,
                JOURNAL_KIND,
                &id,
                0,
                &event_types,
                &event_payloads,
                &timestamp,
            )
            .await?;
            Ok(())
        }
        .await;
        finish_write(write, outcome).await?;
        Ok(journal)
    }

    pub(crate) async fn find_journal(&mut self, id: JournalId) -> Result<Journal, RepositoryError> {
        let read = self.db.begin_read().await?;
        let outcome = load_journal(&read, id).await;
        finish_read(read, outcome).await
    }

    pub(crate) async fn find_journal_by_code(
        &mut self,
        code: &str,
    ) -> Result<Journal, RepositoryError> {
        let read = self.db.begin_read().await?;
        let outcome = async {
            let id = find_id_by_json_identity(&read, "cala_journals", "code", code)
                .await?
                .ok_or_else(|| RepositoryError::NotFound {
                    entity_kind: JOURNAL_KIND,
                    id: format!("code={code:?}"),
                })?;
            load_journal(&read, parse_id(&id, JOURNAL_KIND)?).await
        }
        .await;
        finish_read(read, outcome).await
    }

    /// Persist the new events already produced by `Journal::update`.
    ///
    /// The expected version is the committed event count, matching CALA's
    /// Postgres event-stream concurrency contract. `JournalValues::version`
    /// is intentionally left untouched because existing CALA folds do not
    /// advance that payload field on update.
    pub(crate) async fn persist_journal(
        &mut self,
        journal: &mut Journal,
        expected_version: u32,
        recorded_at: DateTime<Utc>,
    ) -> Result<u32, RepositoryError> {
        require_entity_version(
            JOURNAL_KIND,
            journal.id.to_string(),
            expected_version,
            journal.events().len_persisted(),
        )?;
        let event_payloads = serialize_events(journal.events())?;
        let event_types = journal.events().new_event_types();
        let new_count = u32::try_from(event_payloads.len())
            .map_err(|_| RepositoryError::Codec("journal event count exceeds u32".to_owned()))?;
        let next_version = expected_version
            .checked_add(new_count)
            .ok_or_else(|| RepositoryError::Codec("journal version overflow".to_owned()))?;
        if new_count == 0 {
            let write = self.db.begin_write().await?;
            let outcome = require_stored_version(
                &write,
                "cala_journals",
                JOURNAL_KIND,
                &journal.id.to_string(),
                expected_version,
            )
            .await;
            finish_write(write, outcome).await?;
            return Ok(expected_version);
        }
        let values = journal.values();
        let id = values.id.to_string();
        let payload = encode_json(values)?;
        let timestamp = encode_time(recorded_at);
        let recorded_at = decode_time(timestamp.clone())?;

        let write = self.db.begin_write().await?;
        let outcome = async {
            require_stored_version(&write, "cala_journals", JOURNAL_KIND, &id, expected_version)
                .await?;
            ensure_json_identity_available(
                &write,
                "cala_journals",
                "code",
                values.code.as_deref(),
                &id,
                JOURNAL_KIND,
            )
            .await?;
            insert_events(
                &write,
                JOURNAL_KIND,
                &id,
                expected_version,
                &event_types,
                &event_payloads,
                &timestamp,
            )
            .await?;
            let changed = write
                .execute(
                    "UPDATE cala_journals SET version = ?1, name = ?2, status = ?3, \
                     payload = ?4, modified_at = ?5 WHERE id = ?6 AND version = ?7",
                    (
                        i64::from(next_version),
                        values.name.as_str(),
                        encode_status(values.status),
                        payload.as_str(),
                        timestamp.as_str(),
                        id.as_str(),
                        i64::from(expected_version),
                    ),
                )
                .await
                .map_err(|error| classify_projection_constraint(error, JOURNAL_KIND, "id", &id))?;
            require_one_projection_change(changed, JOURNAL_KIND, &id)?;
            Ok(())
        }
        .await;
        finish_write(write, outcome).await?;
        let persisted = journal
            .events_mut()
            .mark_new_events_persisted_at(recorded_at);
        debug_assert_eq!(persisted, event_payloads.len());
        Ok(next_version)
    }

    pub(crate) async fn create_account(
        &mut self,
        new_account: NewAccount,
        recorded_at: DateTime<Utc>,
    ) -> Result<Account, RepositoryError> {
        let mut events = new_account.into_events();
        let values = match &events
            .iter_new_events()
            .next()
            .ok_or_else(|| {
                corrupt(
                    ACCOUNT_KIND,
                    events.id().to_string(),
                    "missing initialized event",
                )
            })?
            .event
        {
            AccountEvent::Initialized { values } => values.clone(),
            AccountEvent::Updated { .. } => {
                return Err(corrupt(
                    ACCOUNT_KIND,
                    events.id().to_string(),
                    "first event is not initialized",
                ));
            }
        };
        require_non_account_set(values.config.is_account_set)?;
        if events.iter_new_events().count() != 1 {
            return Err(corrupt(
                ACCOUNT_KIND,
                values.id.to_string(),
                "new account must contain exactly one event",
            ));
        }

        let payload = encode_json(&values)?;
        let event_payloads = serialize_events(&events)?;
        let event_types = events.new_event_types();
        let timestamp = encode_time(recorded_at);
        let recorded_at = decode_time(timestamp.clone())?;
        let id = values.id.to_string();
        events.mark_new_events_persisted_at(recorded_at);
        let account = Account::try_from_events(events)
            .map_err(|error| RepositoryError::Codec(error.to_string()))?;
        let write = self.db.begin_write().await?;
        let outcome = async {
            ensure_column_identity_available(
                &write,
                "cala_accounts",
                "code",
                &values.code,
                &id,
                ACCOUNT_KIND,
            )
            .await?;
            ensure_json_identity_available(
                &write,
                "cala_accounts",
                "external_id",
                values.external_id.as_deref(),
                &id,
                ACCOUNT_KIND,
            )
            .await?;
            write
                .execute(
                    "INSERT INTO cala_accounts \
                     (id, version, code, name, status, payload, created_at, modified_at) \
                     VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?6)",
                    (
                        id.as_str(),
                        values.code.as_str(),
                        values.name.as_str(),
                        encode_status(values.status),
                        payload.as_str(),
                        timestamp.as_str(),
                    ),
                )
                .await
                .map_err(|error| classify_account_constraint(error, &id, &values.code))?;
            insert_events(
                &write,
                ACCOUNT_KIND,
                &id,
                0,
                &event_types,
                &event_payloads,
                &timestamp,
            )
            .await?;
            Ok(())
        }
        .await;
        finish_write(write, outcome).await?;
        Ok(account)
    }

    pub(crate) async fn find_account(&mut self, id: AccountId) -> Result<Account, RepositoryError> {
        let read = self.db.begin_read().await?;
        let outcome = load_account(&read, id).await;
        finish_read(read, outcome).await
    }

    pub(crate) async fn find_account_by_code(
        &mut self,
        code: &str,
    ) -> Result<Account, RepositoryError> {
        let read = self.db.begin_read().await?;
        let outcome = async {
            let id = find_id_by_column(&read, "cala_accounts", "code", code)
                .await?
                .ok_or_else(|| RepositoryError::NotFound {
                    entity_kind: ACCOUNT_KIND,
                    id: format!("code={code:?}"),
                })?;
            load_account(&read, parse_id(&id, ACCOUNT_KIND)?).await
        }
        .await;
        finish_read(read, outcome).await
    }

    pub(crate) async fn find_account_by_external_id(
        &mut self,
        external_id: &str,
    ) -> Result<Account, RepositoryError> {
        let read = self.db.begin_read().await?;
        let outcome = async {
            let id = find_id_by_json_identity(&read, "cala_accounts", "external_id", external_id)
                .await?
                .ok_or_else(|| RepositoryError::NotFound {
                    entity_kind: ACCOUNT_KIND,
                    id: format!("external_id={external_id:?}"),
                })?;
            load_account(&read, parse_id(&id, ACCOUNT_KIND)?).await
        }
        .await;
        finish_read(read, outcome).await
    }

    pub(crate) async fn persist_account(
        &mut self,
        account: &mut Account,
        expected_version: u32,
        recorded_at: DateTime<Utc>,
    ) -> Result<u32, RepositoryError> {
        require_non_account_set(account.values().config.is_account_set)?;
        require_entity_version(
            ACCOUNT_KIND,
            account.id.to_string(),
            expected_version,
            account.events().len_persisted(),
        )?;
        let event_payloads = serialize_events(account.events())?;
        let event_types = account.events().new_event_types();
        let new_count = u32::try_from(event_payloads.len())
            .map_err(|_| RepositoryError::Codec("account event count exceeds u32".to_owned()))?;
        let next_version = expected_version
            .checked_add(new_count)
            .ok_or_else(|| RepositoryError::Codec("account version overflow".to_owned()))?;
        if new_count == 0 {
            let write = self.db.begin_write().await?;
            let outcome = require_stored_version(
                &write,
                "cala_accounts",
                ACCOUNT_KIND,
                &account.id.to_string(),
                expected_version,
            )
            .await;
            finish_write(write, outcome).await?;
            return Ok(expected_version);
        }
        let values = account.values();
        let id = values.id.to_string();
        let payload = encode_json(values)?;
        let timestamp = encode_time(recorded_at);
        let recorded_at = decode_time(timestamp.clone())?;

        let write = self.db.begin_write().await?;
        let outcome = async {
            require_stored_version(&write, "cala_accounts", ACCOUNT_KIND, &id, expected_version)
                .await?;
            ensure_json_identity_available(
                &write,
                "cala_accounts",
                "external_id",
                values.external_id.as_deref(),
                &id,
                ACCOUNT_KIND,
            )
            .await?;
            // Insert events first. If the projection's database-level code
            // uniqueness check fails, the explicit rollback must remove them.
            insert_events(
                &write,
                ACCOUNT_KIND,
                &id,
                expected_version,
                &event_types,
                &event_payloads,
                &timestamp,
            )
            .await?;
            let changed = write
                .execute(
                    "UPDATE cala_accounts SET version = ?1, code = ?2, name = ?3, status = ?4, \
                     payload = ?5, modified_at = ?6 WHERE id = ?7 AND version = ?8",
                    (
                        i64::from(next_version),
                        values.code.as_str(),
                        values.name.as_str(),
                        encode_status(values.status),
                        payload.as_str(),
                        timestamp.as_str(),
                        id.as_str(),
                        i64::from(expected_version),
                    ),
                )
                .await
                .map_err(|error| classify_account_constraint(error, &id, &values.code))?;
            require_one_projection_change(changed, ACCOUNT_KIND, &id)?;
            Ok(())
        }
        .await;
        finish_write(write, outcome).await?;
        let persisted = account
            .events_mut()
            .mark_new_events_persisted_at(recorded_at);
        debug_assert_eq!(persisted, event_payloads.len());
        Ok(next_version)
    }
}

#[derive(Debug)]
struct Projection {
    version: u32,
    name: String,
    code: Option<String>,
    status: String,
    payload: String,
    created_at: DateTime<Utc>,
    modified_at: DateTime<Utc>,
}

#[derive(Debug)]
struct RawEvent {
    sequence: i32,
    event_type: String,
    payload: serde_json::Value,
    recorded_at: DateTime<Utc>,
}

async fn load_journal(read: &ReadOp<'_>, id: JournalId) -> Result<Journal, RepositoryError> {
    let id_text = id.to_string();
    let projection = load_projection(read, "cala_journals", &id_text, JOURNAL_KIND).await?;
    let raw_events = load_raw_events(read, JOURNAL_KIND, &id_text, projection.version).await?;
    let mut generic = Vec::with_capacity(raw_events.len());
    for raw in raw_events {
        let event: JournalEvent = serde_json::from_value(raw.payload.clone())
            .map_err(|error| corrupt(JOURNAL_KIND, &id_text, error.to_string()))?;
        if event.event_type() != raw.event_type {
            return Err(corrupt(
                JOURNAL_KIND,
                &id_text,
                format!(
                    "event {} type column {:?} disagrees with payload {:?}",
                    raw.sequence,
                    raw.event_type,
                    event.event_type()
                ),
            ));
        }
        generic.push(GenericEvent {
            entity_id: id,
            sequence: raw.sequence,
            event: raw.payload,
            context: None,
            recorded_at: raw.recorded_at,
            forgettable_payload: None,
        });
    }
    let journal: Journal = EntityEvents::<JournalEvent>::load_first(generic)
        .map_err(|error| corrupt(JOURNAL_KIND, &id_text, error.to_string()))?
        .ok_or_else(|| corrupt(JOURNAL_KIND, &id_text, "projection has no events"))?;
    validate_journal_projection(&journal, &projection)?;
    Ok(journal)
}

async fn load_account(read: &ReadOp<'_>, id: AccountId) -> Result<Account, RepositoryError> {
    let id_text = id.to_string();
    let projection = load_projection(read, "cala_accounts", &id_text, ACCOUNT_KIND).await?;
    let raw_events = load_raw_events(read, ACCOUNT_KIND, &id_text, projection.version).await?;
    let mut generic = Vec::with_capacity(raw_events.len());
    for raw in raw_events {
        let event: AccountEvent = serde_json::from_value(raw.payload.clone())
            .map_err(|error| corrupt(ACCOUNT_KIND, &id_text, error.to_string()))?;
        if event.event_type() != raw.event_type {
            return Err(corrupt(
                ACCOUNT_KIND,
                &id_text,
                format!(
                    "event {} type column {:?} disagrees with payload {:?}",
                    raw.sequence,
                    raw.event_type,
                    event.event_type()
                ),
            ));
        }
        generic.push(GenericEvent {
            entity_id: id,
            sequence: raw.sequence,
            event: raw.payload,
            context: None,
            recorded_at: raw.recorded_at,
            forgettable_payload: None,
        });
    }
    let account: Account = EntityEvents::<AccountEvent>::load_first(generic)
        .map_err(|error| corrupt(ACCOUNT_KIND, &id_text, error.to_string()))?
        .ok_or_else(|| corrupt(ACCOUNT_KIND, &id_text, "projection has no events"))?;
    require_non_account_set(account.values().config.is_account_set)?;
    validate_account_projection(&account, &projection)?;
    Ok(account)
}

async fn load_projection(
    read: &ReadOp<'_>,
    table: &'static str,
    id: &str,
    entity_kind: &'static str,
) -> Result<Projection, RepositoryError> {
    let sql = match table {
        "cala_journals" => {
            "SELECT version, name, NULL, status, payload, created_at, modified_at \
             FROM cala_journals WHERE id = ?1"
        }
        "cala_accounts" => {
            "SELECT version, name, code, status, payload, created_at, modified_at \
             FROM cala_accounts WHERE id = ?1"
        }
        _ => unreachable!("repository only admits fixed projection tables"),
    };
    let mut rows = read.query(sql, (id,)).await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| RepositoryError::NotFound {
            entity_kind,
            id: id.to_owned(),
        })?;
    let projection = Projection {
        version: decode_version(row.get::<i64>(0)?, entity_kind, id)?,
        name: row.get::<String>(1)?,
        code: row.get::<Option<String>>(2)?,
        status: row.get::<String>(3)?,
        payload: row.get::<String>(4)?,
        created_at: decode_time(row.get::<String>(5)?)?,
        modified_at: decode_time(row.get::<String>(6)?)?,
    };
    drop(rows);
    Ok(projection)
}

async fn load_raw_events(
    read: &ReadOp<'_>,
    entity_kind: &'static str,
    entity_id: &str,
    version: u32,
) -> Result<Vec<RawEvent>, RepositoryError> {
    let mut rows = read
        .query(
            "SELECT entity_sequence, event_type, event_payload, recorded_at \
             FROM cala_entity_events WHERE entity_kind = ?1 AND entity_id = ?2 \
             ORDER BY entity_sequence ASC",
            (entity_kind, entity_id),
        )
        .await?;
    let mut events = Vec::new();
    while let Some(row) = rows.next().await? {
        let sequence_i64 = row.get::<i64>(0)?;
        let sequence = i32::try_from(sequence_i64).map_err(|_| {
            corrupt(
                entity_kind,
                entity_id,
                format!("event sequence {sequence_i64} exceeds i32"),
            )
        })?;
        let expected = i32::try_from(events.len() + 1)
            .map_err(|_| corrupt(entity_kind, entity_id, "event stream exceeds i32"))?;
        if sequence != expected {
            return Err(corrupt(
                entity_kind,
                entity_id,
                format!("expected event sequence {expected}, found {sequence}"),
            ));
        }
        let payload_text = row.get::<String>(2)?;
        events.push(RawEvent {
            sequence,
            event_type: row.get::<String>(1)?,
            payload: serde_json::from_str(&payload_text)
                .map_err(|error| corrupt(entity_kind, entity_id, error.to_string()))?,
            recorded_at: decode_time(row.get::<String>(3)?)?,
        });
    }
    drop(rows);
    if events.len() != usize::try_from(version).unwrap_or(usize::MAX) {
        return Err(corrupt(
            entity_kind,
            entity_id,
            format!(
                "projection version {version} disagrees with {} stored events",
                events.len()
            ),
        ));
    }
    Ok(events)
}

fn validate_journal_projection(
    journal: &Journal,
    projection: &Projection,
) -> Result<(), RepositoryError> {
    let values = journal.values();
    let payload = decode_json_value(&projection.payload)?;
    let folded = serde_json::to_value(values).map_err(codec)?;
    if payload != folded
        || projection.name != values.name
        || projection.status != encode_status(values.status)
        || projection.created_at != journal.created_at()
        || projection.modified_at != journal.modified_at()
    {
        return Err(corrupt(
            JOURNAL_KIND,
            journal.id.to_string(),
            "projection disagrees with folded event stream",
        ));
    }
    Ok(())
}

fn validate_account_projection(
    account: &Account,
    projection: &Projection,
) -> Result<(), RepositoryError> {
    let values = account.values();
    let payload = decode_json_value(&projection.payload)?;
    let folded = serde_json::to_value(values).map_err(codec)?;
    if payload != folded
        || projection.name != values.name
        || projection.code.as_deref() != Some(values.code.as_str())
        || projection.status != encode_status(values.status)
        || projection.created_at != account.created_at()
        || projection.modified_at != account.modified_at()
    {
        return Err(corrupt(
            ACCOUNT_KIND,
            account.id.to_string(),
            "projection disagrees with folded event stream",
        ));
    }
    Ok(())
}

async fn insert_events(
    write: &WriteOp<'_>,
    entity_kind: &'static str,
    entity_id: &str,
    offset: u32,
    event_types: &[String],
    payloads: &[String],
    recorded_at: &str,
) -> Result<(), RepositoryError> {
    if event_types.len() != payloads.len() {
        return Err(corrupt(
            entity_kind,
            entity_id,
            "event type/payload counts differ",
        ));
    }
    for (index, (event_type, payload)) in event_types.iter().zip(payloads).enumerate() {
        let sequence = u32::try_from(index + 1)
            .ok()
            .and_then(|index| offset.checked_add(index))
            .ok_or_else(|| RepositoryError::Codec("event sequence overflow".to_owned()))?;
        let event_id = format!("{entity_kind}:{entity_id}:{sequence}");
        write
            .execute(
                "INSERT INTO cala_entity_events \
                 (event_id, entity_kind, entity_id, entity_sequence, event_type, event_payload, recorded_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (
                    event_id.as_str(),
                    entity_kind,
                    entity_id,
                    i64::from(sequence),
                    event_type.as_str(),
                    payload.as_str(),
                    recorded_at,
                ),
            )
            .await
            .map_err(|error| match error {
                StorageError::Turso(turso::Error::Constraint(message)) => corrupt(
                    entity_kind,
                    entity_id,
                    format!("could not append event {sequence}: {message}"),
                ),
                other => other.into(),
            })?;
    }
    Ok(())
}

async fn require_stored_version(
    write: &WriteOp<'_>,
    table: &'static str,
    entity_kind: &'static str,
    id: &str,
    expected: u32,
) -> Result<(), RepositoryError> {
    let sql = match table {
        "cala_journals" => "SELECT version FROM cala_journals WHERE id = ?1",
        "cala_accounts" => "SELECT version FROM cala_accounts WHERE id = ?1",
        _ => unreachable!("repository only admits fixed projection tables"),
    };
    let mut rows = write.query(sql, (id,)).await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| RepositoryError::NotFound {
            entity_kind,
            id: id.to_owned(),
        })?;
    let actual = decode_version(row.get::<i64>(0)?, entity_kind, id)?;
    drop(rows);
    if actual != expected {
        return Err(RepositoryError::StaleVersion {
            entity_kind,
            id: id.to_owned(),
            expected,
            actual,
        });
    }
    Ok(())
}

async fn ensure_json_identity_available(
    write: &WriteOp<'_>,
    table: &'static str,
    field: &'static str,
    value: Option<&str>,
    except_id: &str,
    entity_kind: &'static str,
) -> Result<(), RepositoryError> {
    let Some(value) = value else { return Ok(()) };
    let sql = match (table, field) {
        ("cala_journals", "code") => {
            "SELECT id FROM cala_journals \
             WHERE id <> ?1 AND json_extract(payload, '$.code') = ?2 LIMIT 1"
        }
        ("cala_accounts", "external_id") => {
            "SELECT id FROM cala_accounts \
             WHERE id <> ?1 AND json_extract(payload, '$.external_id') = ?2 LIMIT 1"
        }
        _ => unreachable!("repository only admits declared JSON identities"),
    };
    let mut rows = write.query(sql, (except_id, value)).await?;
    let duplicate = rows.next().await?.is_some();
    drop(rows);
    if duplicate {
        Err(RepositoryError::DuplicateIdentity {
            entity_kind,
            field,
            value: value.to_owned(),
        })
    } else {
        Ok(())
    }
}

async fn ensure_column_identity_available(
    write: &WriteOp<'_>,
    table: &'static str,
    field: &'static str,
    value: &str,
    except_id: &str,
    entity_kind: &'static str,
) -> Result<(), RepositoryError> {
    let sql = match (table, field) {
        ("cala_accounts", "code") => {
            "SELECT id FROM cala_accounts WHERE id <> ?1 AND code = ?2 LIMIT 1"
        }
        _ => unreachable!("repository only admits declared column identities"),
    };
    let mut rows = write.query(sql, (except_id, value)).await?;
    let duplicate = rows.next().await?.is_some();
    drop(rows);
    if duplicate {
        Err(RepositoryError::DuplicateIdentity {
            entity_kind,
            field,
            value: value.to_owned(),
        })
    } else {
        Ok(())
    }
}

async fn find_id_by_json_identity(
    read: &ReadOp<'_>,
    table: &'static str,
    field: &'static str,
    value: &str,
) -> Result<Option<String>, RepositoryError> {
    let sql = match (table, field) {
        ("cala_journals", "code") => {
            "SELECT id FROM cala_journals WHERE json_extract(payload, '$.code') = ?1 LIMIT 1"
        }
        ("cala_accounts", "external_id") => {
            "SELECT id FROM cala_accounts WHERE json_extract(payload, '$.external_id') = ?1 LIMIT 1"
        }
        _ => unreachable!("repository only admits declared JSON identities"),
    };
    find_one_id(read, sql, value).await
}

async fn find_id_by_column(
    read: &ReadOp<'_>,
    table: &'static str,
    field: &'static str,
    value: &str,
) -> Result<Option<String>, RepositoryError> {
    let sql = match (table, field) {
        ("cala_accounts", "code") => "SELECT id FROM cala_accounts WHERE code = ?1 LIMIT 1",
        _ => unreachable!("repository only admits declared column identities"),
    };
    find_one_id(read, sql, value).await
}

async fn find_one_id(
    read: &ReadOp<'_>,
    sql: &str,
    value: &str,
) -> Result<Option<String>, RepositoryError> {
    let mut rows = read.query(sql, (value,)).await?;
    let id = match rows.next().await? {
        Some(row) => Some(row.get::<String>(0)?),
        None => None,
    };
    drop(rows);
    Ok(id)
}

async fn finish_write<T>(
    write: WriteOp<'_>,
    outcome: Result<T, RepositoryError>,
) -> Result<T, RepositoryError> {
    match outcome {
        Ok(value) => {
            write.commit().await?;
            Ok(value)
        }
        Err(operation) => match write.rollback().await {
            Ok(()) => Err(operation),
            Err(rollback) => Err(RepositoryError::RollbackFailed {
                operation: Box::new(operation),
                rollback,
            }),
        },
    }
}

async fn finish_read<T>(
    read: ReadOp<'_>,
    outcome: Result<T, RepositoryError>,
) -> Result<T, RepositoryError> {
    match (outcome, read.close().await) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(operation), Ok(())) => Err(operation),
        (Err(operation), Err(rollback)) => Err(RepositoryError::RollbackFailed {
            operation: Box::new(operation),
            rollback,
        }),
        (Ok(_), Err(rollback)) => Err(rollback.into()),
    }
}

fn require_entity_version(
    entity_kind: &'static str,
    id: String,
    expected: u32,
    persisted_event_count: usize,
) -> Result<(), RepositoryError> {
    let actual = u32::try_from(persisted_event_count)
        .map_err(|_| corrupt(entity_kind, &id, "persisted event count exceeds u32"))?;
    if actual == expected {
        Ok(())
    } else {
        Err(RepositoryError::StaleVersion {
            entity_kind,
            id,
            expected,
            actual,
        })
    }
}

fn require_one_projection_change(
    changed: u64,
    entity_kind: &'static str,
    id: &str,
) -> Result<(), RepositoryError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(corrupt(
            entity_kind,
            id,
            format!("projection update changed {changed} rows"),
        ))
    }
}

fn require_non_account_set(is_account_set: bool) -> Result<(), RepositoryError> {
    if is_account_set {
        Err(RepositoryError::AccountSetUnsupported)
    } else {
        Ok(())
    }
}

fn serialize_events<E: EsEvent>(events: &EntityEvents<E>) -> Result<Vec<String>, RepositoryError> {
    events
        .serialize_new_events()
        .into_iter()
        .map(|event| serde_json::to_string(&event).map_err(codec))
        .collect()
}

fn encode_json(value: &impl Serialize) -> Result<String, RepositoryError> {
    serde_json::to_string(value).map_err(codec)
}

fn decode_json_value(value: &str) -> Result<serde_json::Value, RepositoryError> {
    serde_json::from_str(value).map_err(codec)
}

fn encode_time(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn decode_time(value: String) -> Result<DateTime<Utc>, RepositoryError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| {
            RepositoryError::Codec(format!("invalid UTC timestamp {value:?}: {error}"))
        })
}

fn encode_status(value: Status) -> &'static str {
    match value {
        Status::Active => "active",
        Status::Locked => "locked",
    }
}

fn decode_version(value: i64, entity_kind: &'static str, id: &str) -> Result<u32, RepositoryError> {
    u32::try_from(value).map_err(|_| {
        corrupt(
            entity_kind,
            id,
            format!("invalid projection version {value}"),
        )
    })
}

fn parse_id<T>(value: &str, entity_kind: &'static str) -> Result<T, RepositoryError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|error| {
        RepositoryError::Codec(format!("invalid {entity_kind} id {value:?}: {error}"))
    })
}

fn classify_projection_constraint(
    error: StorageError,
    entity_kind: &'static str,
    field: &'static str,
    value: &str,
) -> RepositoryError {
    match error {
        StorageError::Turso(turso::Error::Constraint(_)) => RepositoryError::DuplicateIdentity {
            entity_kind,
            field,
            value: value.to_owned(),
        },
        other => other.into(),
    }
}

fn classify_account_constraint(error: StorageError, id: &str, code: &str) -> RepositoryError {
    match error {
        StorageError::Turso(turso::Error::Constraint(message))
            if message.to_ascii_lowercase().contains("cala_accounts.code") =>
        {
            RepositoryError::DuplicateIdentity {
                entity_kind: ACCOUNT_KIND,
                field: "code",
                value: code.to_owned(),
            }
        }
        StorageError::Turso(turso::Error::Constraint(_)) => RepositoryError::DuplicateIdentity {
            entity_kind: ACCOUNT_KIND,
            field: "id",
            value: id.to_owned(),
        },
        other => other.into(),
    }
}

fn codec(error: serde_json::Error) -> RepositoryError {
    RepositoryError::Codec(error.to_string())
}

fn corrupt(
    entity_kind: &'static str,
    id: impl Into<String>,
    reason: impl Into<String>,
) -> RepositoryError {
    RepositoryError::CorruptState {
        entity_kind,
        id: id.into(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Timelike, Utc};

    use crate::{
        account::{AccountUpdate, NewAccount},
        journal::{JournalUpdate, NewJournal},
        primitives::{AccountId, JournalId, Status},
    };

    use super::*;

    fn at(second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 23, 10, 0, second)
            .single()
            .expect("valid test timestamp")
    }

    async fn memory_repo() -> JournalAccountRepo {
        JournalAccountRepo::open(":memory:", at(0))
            .await
            .expect("open migrated repository")
    }

    fn expect_repository_error<T>(
        result: Result<T, RepositoryError>,
        message: &str,
    ) -> RepositoryError {
        match result {
            Ok(_) => panic!("{message}"),
            Err(error) => error,
        }
    }

    fn new_journal(id: JournalId, name: &str, code: &str) -> NewJournal {
        NewJournal::builder()
            .id(id)
            .name(name)
            .code(code)
            .build()
            .expect("build journal")
    }

    fn new_account(id: AccountId, code: &str, external_id: &str) -> NewAccount {
        NewAccount::builder()
            .id(id)
            .code(code)
            .name(code)
            .external_id(external_id)
            .build()
            .expect("build account")
    }

    #[tokio::test]
    async fn journals_round_trip_updates_and_reopen_file() {
        let directory = tempfile::tempdir().expect("create temp directory");
        let path = directory.path().join("journal-reopen.db");
        let path = path.to_str().expect("utf-8 path");
        let id = JournalId::new();
        let account_id = AccountId::new();

        let mut repo = JournalAccountRepo::open(path, at(0))
            .await
            .expect("open repository");
        let created = repo
            .create_journal(new_journal(id, "Operations", "OPS"), at(1))
            .await
            .expect("create journal");
        assert_eq!(created.values().name, "Operations");
        assert_eq!(created.created_at(), at(1));

        let mut journal = repo
            .find_journal_by_code("OPS")
            .await
            .expect("find by code");
        let mut update = JournalUpdate::default();
        update.name("Core operations").status(Status::Locked);
        assert!(journal.update(update).did_execute());
        assert_eq!(
            repo.persist_journal(&mut journal, 1, at(2)).await.unwrap(),
            2
        );
        assert_eq!(journal.modified_at(), at(2));
        repo.create_account(
            new_account(account_id, "operating-cash", "operating-cash-id"),
            at(3),
        )
        .await
        .expect("create account before reopen");
        drop(repo);

        let mut repo = JournalAccountRepo::open(path, at(0))
            .await
            .expect("reopen repository");
        let reopened = repo.find_journal(id).await.expect("find reopened journal");
        assert_eq!(reopened.values().name, "Core operations");
        assert_eq!(reopened.values().status, Status::Locked);
        assert_eq!(reopened.created_at(), at(1));
        assert_eq!(reopened.modified_at(), at(2));
        assert_eq!(reopened.events().len_persisted(), 2);
        let reopened_account = repo
            .find_account_by_code("operating-cash")
            .await
            .expect("find reopened account");
        assert_eq!(reopened_account.id(), account_id);
        assert_eq!(reopened_account.created_at(), at(3));
    }

    #[tokio::test]
    async fn global_journal_and_account_identities_reject_duplicates() {
        let mut repo = memory_repo().await;
        repo.create_journal(new_journal(JournalId::new(), "One", "GLOBAL"), at(1))
            .await
            .expect("create first journal");
        let duplicate_journal = expect_repository_error(
            repo.create_journal(new_journal(JournalId::new(), "Two", "GLOBAL"), at(2))
                .await,
            "duplicate journal code must fail",
        );
        assert!(matches!(
            duplicate_journal,
            RepositoryError::DuplicateIdentity { field: "code", .. }
        ));

        repo.create_account(
            new_account(AccountId::new(), "cash", "external-cash"),
            at(3),
        )
        .await
        .expect("create first account");
        let duplicate_code = expect_repository_error(
            repo.create_account(
                new_account(AccountId::new(), "cash", "other-external"),
                at(4),
            )
            .await,
            "duplicate account code must fail",
        );
        assert!(matches!(
            duplicate_code,
            RepositoryError::DuplicateIdentity { field: "code", .. }
        ));
        let duplicate_external = expect_repository_error(
            repo.create_account(
                new_account(AccountId::new(), "bank", "external-cash"),
                at(5),
            )
            .await,
            "duplicate external id must fail",
        );
        assert!(matches!(
            duplicate_external,
            RepositoryError::DuplicateIdentity {
                field: "external_id",
                ..
            }
        ));
        assert_eq!(
            repo.find_account_by_external_id("external-cash")
                .await
                .expect("find external identity")
                .values()
                .code,
            "cash"
        );
    }

    #[tokio::test]
    async fn stale_loaded_account_cannot_overwrite_a_newer_version() {
        let mut repo = memory_repo().await;
        let id = AccountId::new();
        repo.create_account(new_account(id, "cash", "cash-id"), at(1))
            .await
            .expect("create account");
        let mut first = repo.find_account(id).await.expect("load first copy");
        let mut stale = repo.find_account(id).await.expect("load stale copy");

        let mut rename = AccountUpdate::default();
        rename.name("Cash account");
        assert!(first.update(rename).did_execute());
        assert_eq!(repo.persist_account(&mut first, 1, at(2)).await.unwrap(), 2);

        assert!(stale.update_status(Status::Locked).did_execute());
        let error = repo
            .persist_account(&mut stale, 1, at(3))
            .await
            .expect_err("stale account must fail");
        assert!(matches!(
            error,
            RepositoryError::StaleVersion {
                expected: 1,
                actual: 2,
                ..
            }
        ));
        let current = repo.find_account(id).await.expect("reload current account");
        assert_eq!(current.values().name, "Cash account");
        assert_eq!(current.values().status, Status::Active);
        assert_eq!(current.events().len_persisted(), 2);
    }

    #[tokio::test]
    async fn projection_constraint_rolls_back_already_inserted_account_event() {
        let mut repo = memory_repo().await;
        repo.create_account(
            new_account(AccountId::new(), "reserved", "reserved-id"),
            at(1),
        )
        .await
        .expect("create reserved account");
        let id = AccountId::new();
        repo.create_account(new_account(id, "mutable", "mutable-id"), at(2))
            .await
            .expect("create mutable account");

        let mut account = repo.find_account(id).await.expect("load mutable account");
        let mut update = AccountUpdate::default();
        update.code("reserved").status(Status::Locked);
        assert!(account.update(update).did_execute());
        let error = repo
            .persist_account(&mut account, 1, at(3))
            .await
            .expect_err("duplicate projection must fail");
        assert!(matches!(
            error,
            RepositoryError::DuplicateIdentity { field: "code", .. }
        ));

        let stored = repo
            .find_account(id)
            .await
            .expect("reload rolled-back account");
        assert_eq!(stored.values().code, "mutable");
        assert_eq!(stored.values().status, Status::Active);
        assert_eq!(stored.events().len_persisted(), 1);
    }

    #[tokio::test]
    async fn projection_event_disagreement_fails_closed() {
        let mut repo = memory_repo().await;
        let id = JournalId::new();
        repo.create_journal(new_journal(id, "Correct", "CORRECT"), at(1))
            .await
            .expect("create journal");

        let write = repo.db.begin_write().await.expect("begin corruption write");
        write
            .execute(
                "UPDATE cala_journals SET name = 'tampered' WHERE id = ?1",
                (id.to_string(),),
            )
            .await
            .expect("tamper projection");
        write.commit().await.expect("commit test-only corruption");

        let error = expect_repository_error(
            repo.find_journal(id).await,
            "state/event mismatch must fail closed",
        );
        assert!(matches!(error, RepositoryError::CorruptState { .. }));
    }

    #[tokio::test]
    async fn persist_without_new_events_is_a_true_noop() {
        let mut repo = memory_repo().await;
        let id = JournalId::new();
        let mut journal = repo
            .create_journal(new_journal(id, "No-op", "NOOP"), at(1))
            .await
            .expect("create journal");

        assert_eq!(
            repo.persist_journal(&mut journal, 1, at(2))
                .await
                .expect("persist no-op"),
            1
        );
        let stored = repo.find_journal(id).await.expect("reload journal");
        assert_eq!(stored.modified_at(), at(1));
        assert_eq!(stored.events().len_persisted(), 1);
    }

    #[tokio::test]
    async fn returned_entity_uses_the_same_microsecond_timestamp_as_reopen() {
        let mut repo = memory_repo().await;
        let id = JournalId::new();
        let precise = at(1)
            .with_nanosecond(123_456_789)
            .expect("valid sub-microsecond timestamp");
        let stored_precision = at(1)
            .with_nanosecond(123_456_000)
            .expect("valid microsecond timestamp");

        let created = repo
            .create_journal(new_journal(id, "Precision", "PRECISION"), precise)
            .await
            .expect("create journal");
        assert_eq!(created.modified_at(), stored_precision);
        let reloaded = repo.find_journal(id).await.expect("reload journal");
        assert_eq!(created.modified_at(), reloaded.modified_at());
    }

    #[tokio::test]
    async fn projection_and_events_remain_on_one_snapshot_during_concurrent_commit() {
        let directory = tempfile::tempdir().expect("create temp directory");
        let path = directory.path().join("snapshot-events.db");
        let path = path.to_str().expect("utf-8 path");
        let id = JournalId::new();

        let mut reader = JournalAccountRepo::open(path, at(0))
            .await
            .expect("open reader");
        reader
            .create_journal(new_journal(id, "Before", "SNAPSHOT"), at(1))
            .await
            .expect("create journal");
        let mut writer = JournalAccountRepo::open(path, at(0))
            .await
            .expect("open writer");

        let read = reader.db.begin_read().await.expect("begin read snapshot");
        let projection = load_projection(&read, "cala_journals", &id.to_string(), JOURNAL_KIND)
            .await
            .expect("pin projection snapshot");
        assert_eq!(projection.version, 1);

        let mut journal = writer.find_journal(id).await.expect("load writer journal");
        let mut update = JournalUpdate::default();
        update.name("After");
        assert!(journal.update(update).did_execute());
        writer
            .persist_journal(&mut journal, 1, at(2))
            .await
            .expect("commit concurrent update");

        let snapshot_events = load_raw_events(&read, JOURNAL_KIND, &id.to_string(), 1)
            .await
            .expect("events must match pinned projection");
        assert_eq!(snapshot_events.len(), 1);
        let snapshot_journal = load_journal(&read, id)
            .await
            .expect("hydrate from one snapshot");
        assert_eq!(snapshot_journal.values().name, "Before");
        read.close().await.expect("close read snapshot");

        let latest = reader.find_journal(id).await.expect("load latest journal");
        assert_eq!(latest.values().name, "After");
        assert_eq!(latest.events().len_persisted(), 2);
    }

    #[tokio::test]
    async fn identity_lookup_and_entity_load_share_snapshot_during_rename() {
        let directory = tempfile::tempdir().expect("create temp directory");
        let path = directory.path().join("snapshot-identity.db");
        let path = path.to_str().expect("utf-8 path");
        let id = AccountId::new();

        let mut reader = JournalAccountRepo::open(path, at(0))
            .await
            .expect("open reader");
        reader
            .create_account(new_account(id, "before-code", "snapshot-account"), at(1))
            .await
            .expect("create account");
        let mut writer = JournalAccountRepo::open(path, at(0))
            .await
            .expect("open writer");

        let read = reader.db.begin_read().await.expect("begin read snapshot");
        let found_id = find_id_by_column(&read, "cala_accounts", "code", "before-code")
            .await
            .expect("lookup old identity")
            .expect("old identity exists");

        let mut account = writer.find_account(id).await.expect("load writer account");
        let mut update = AccountUpdate::default();
        update.code("after-code");
        assert!(account.update(update).did_execute());
        writer
            .persist_account(&mut account, 1, at(2))
            .await
            .expect("commit concurrent rename");

        let snapshot_account = load_account(&read, parse_id(&found_id, ACCOUNT_KIND).unwrap())
            .await
            .expect("load identity from the lookup snapshot");
        assert_eq!(snapshot_account.values().code, "before-code");
        read.close().await.expect("close read snapshot");

        let old_identity = expect_repository_error(
            reader.find_account_by_code("before-code").await,
            "old identity must disappear after snapshot closes",
        );
        assert!(matches!(old_identity, RepositoryError::NotFound { .. }));
        assert_eq!(
            reader
                .find_account_by_code("after-code")
                .await
                .expect("new identity exists")
                .id(),
            id
        );
    }

    #[tokio::test]
    async fn account_sets_are_rejected_before_storage() {
        let mut repo = memory_repo().await;
        let mut builder = NewAccount::builder();
        builder
            .id(AccountId::new())
            .code("set")
            .name("Set")
            .is_account_set(true);
        let error = expect_repository_error(
            repo.create_account(builder.build().expect("build account set"), at(1))
                .await,
            "account set must be outside this slice",
        );
        assert!(matches!(error, RepositoryError::AccountSetUnsupported));
    }
}
