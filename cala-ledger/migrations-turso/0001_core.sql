CREATE TABLE IF NOT EXISTS cala_schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    source_fingerprint TEXT NOT NULL,
    applied_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS cala_journals (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL CHECK (version >= 0),
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'locked')),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    created_at TEXT NOT NULL,
    modified_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS cala_accounts (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL CHECK (version >= 0),
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'locked')),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    created_at TEXT NOT NULL,
    modified_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS cala_tx_templates (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL CHECK (version >= 0),
    code TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    created_at TEXT NOT NULL,
    modified_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS cala_transactions (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL CHECK (version >= 0),
    journal_id TEXT NOT NULL REFERENCES cala_journals(id),
    template_id TEXT REFERENCES cala_tx_templates(id),
    external_id TEXT,
    effective_date TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    created_at TEXT NOT NULL,
    modified_at TEXT NOT NULL
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS cala_transactions_external_id
    ON cala_transactions(external_id)
    WHERE external_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS cala_entries (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL CHECK (version >= 0),
    transaction_id TEXT NOT NULL REFERENCES cala_transactions(id),
    journal_id TEXT NOT NULL REFERENCES cala_journals(id),
    account_id TEXT NOT NULL REFERENCES cala_accounts(id),
    entry_sequence INTEGER NOT NULL CHECK (entry_sequence >= 0),
    unit TEXT NOT NULL,
    layer TEXT NOT NULL CHECK (layer IN ('settled', 'pending', 'encumbrance')),
    direction TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    created_at TEXT NOT NULL,
    UNIQUE (transaction_id, entry_sequence)
) STRICT;

CREATE TABLE IF NOT EXISTS cala_current_balances (
    journal_id TEXT NOT NULL REFERENCES cala_journals(id),
    account_id TEXT NOT NULL REFERENCES cala_accounts(id),
    unit TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 0),
    snapshot TEXT NOT NULL CHECK (json_valid(snapshot)),
    modified_at TEXT NOT NULL,
    PRIMARY KEY (journal_id, account_id, unit)
) STRICT;

CREATE TABLE IF NOT EXISTS cala_balance_history (
    id TEXT PRIMARY KEY,
    journal_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    unit TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 0),
    snapshot TEXT NOT NULL CHECK (json_valid(snapshot)),
    recorded_at TEXT NOT NULL,
    UNIQUE (journal_id, account_id, unit, version),
    FOREIGN KEY (journal_id, account_id, unit)
        REFERENCES cala_current_balances(journal_id, account_id, unit)
) STRICT;

CREATE TABLE IF NOT EXISTS cala_entity_events (
    event_id TEXT PRIMARY KEY,
    entity_kind TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_sequence INTEGER NOT NULL CHECK (entity_sequence >= 0),
    event_type TEXT NOT NULL,
    event_payload TEXT NOT NULL CHECK (json_valid(event_payload)),
    recorded_at TEXT NOT NULL,
    UNIQUE (entity_kind, entity_id, entity_sequence)
) STRICT;

CREATE TABLE IF NOT EXISTS cala_outbox_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    event_payload TEXT NOT NULL CHECK (json_valid(event_payload)),
    recorded_at TEXT NOT NULL,
    published_at TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS cala_idempotency_results (
    idempotency_key TEXT PRIMARY KEY,
    input_hash TEXT NOT NULL,
    result_payload TEXT NOT NULL CHECK (json_valid(result_payload)),
    committed_at TEXT NOT NULL
) STRICT;
