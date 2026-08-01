import { SqliteMigrator } from "@effect/sql-sqlite-bun"
import { Effect } from "effect"
import { SqlClient } from "effect/unstable/sql"

const createSessionEvents = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE IF NOT EXISTS session_events (
    event_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('SessionCreated','UserMessageAdded','AgentMessageAdded')),
    payload TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    UNIQUE (session_id, sequence)
  )`
  yield* sql`CREATE INDEX IF NOT EXISTS session_events_session_id ON session_events (session_id, sequence)`
})

/** A transactional outbox gives every committed event a stable global order. */
const createEventDispatch = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE IF NOT EXISTS event_dispatch (
    position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE REFERENCES session_events(event_id)
  )`
  yield* sql`INSERT OR IGNORE INTO event_dispatch (event_id)
    SELECT event_id FROM session_events ORDER BY occurred_at, session_id, sequence`
  yield* sql`CREATE TABLE IF NOT EXISTS event_consumer_checkpoints (
    consumer TEXT PRIMARY KEY,
    position INTEGER NOT NULL
  )`
})

/**
 * Rebuild the event tables without a closed event-type CHECK so agent activity
 * can be added without rebuilding the source-of-truth table for every new
 * event. Existing outbox positions and consumer checkpoints are preserved.
 */
const allowAgentActivityEvents = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE session_events_v3 (
    event_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    UNIQUE (session_id, sequence)
  )`
  yield* sql`INSERT INTO session_events_v3
    SELECT * FROM session_events`
  yield* sql`CREATE TABLE event_dispatch_v3 (
    position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE REFERENCES session_events_v3(event_id)
  )`
  yield* sql`INSERT INTO event_dispatch_v3 (position, event_id)
    SELECT position, event_id FROM event_dispatch ORDER BY position`
  yield* sql`DROP TABLE event_dispatch`
  yield* sql`DROP TABLE session_events`
  yield* sql`ALTER TABLE session_events_v3 RENAME TO session_events`
  yield* sql`ALTER TABLE event_dispatch_v3 RENAME TO event_dispatch`
  yield* sql`CREATE INDEX session_events_session_id ON session_events (session_id, sequence)`
  yield* sql`CREATE UNIQUE INDEX session_events_tool_call_causation
    ON session_events(
      json_extract(payload, '$.inReplyTo'),
      json_extract(payload, '$.index')
    ) WHERE event_type = 'AgentToolCallAdded'`
})

const addCanonicalCommitsAndLocalHeads = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE session_branch_heads (
    session_id TEXT PRIMARY KEY,
    commit_id TEXT
  )`
  yield* sql`CREATE UNIQUE INDEX session_events_tool_commit_causation
    ON session_events(
      json_extract(payload, '$.inReplyTo'),
      json_extract(payload, '$.index')
    ) WHERE event_type = 'ToolCommit'`
  yield* sql`CREATE UNIQUE INDEX session_events_agent_message_causation
    ON session_events(json_extract(payload, '$.inReplyTo'))
    WHERE event_type = 'AgentMessageCommit'`
})

const addFailureCommitCausation = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE UNIQUE INDEX session_events_failure_commit_causation
    ON session_events(json_extract(payload, '$.inReplyTo'))
    WHERE event_type = 'FailureCommit'`
})

/** Allow independent Agent Runs to share one starting Commit. */
const addAgentRunIdentity = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`DROP INDEX session_events_tool_commit_causation`
  yield* sql`DROP INDEX session_events_agent_message_causation`
  yield* sql`DROP INDEX session_events_failure_commit_causation`
  yield* sql`CREATE UNIQUE INDEX session_events_tool_commit_causation
    ON session_events(
      json_extract(payload, '$.inReplyTo'),
      COALESCE(json_extract(payload, '$.runId'), ''),
      json_extract(payload, '$.index')
    ) WHERE event_type = 'ToolCommit'`
  yield* sql`CREATE UNIQUE INDEX session_events_agent_message_causation
    ON session_events(
      json_extract(payload, '$.inReplyTo'),
      COALESCE(json_extract(payload, '$.runId'), '')
    ) WHERE event_type = 'AgentMessageCommit'`
  yield* sql`CREATE UNIQUE INDEX session_events_failure_commit_causation
    ON session_events(
      json_extract(payload, '$.inReplyTo'),
      COALESCE(json_extract(payload, '$.runId'), '')
    ) WHERE event_type = 'FailureCommit'`
})

/** Branch Heads belong to a Peer and are never synchronized Session facts. */
const addPeerBranchHeads = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE peer_branch_heads (
    peer_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    commit_id TEXT,
    PRIMARY KEY (peer_id, session_id)
  )`
  yield* sql`INSERT INTO peer_branch_heads (peer_id, session_id, commit_id)
    SELECT 'default-peer', session_id, commit_id FROM session_branch_heads`
  yield* sql`DROP TABLE session_branch_heads`
})

/** Keep one durable interruption outcome per Agent Run. */
const addInterruptCommitCausation = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE UNIQUE INDEX session_events_interrupt_commit_causation
    ON session_events(
      json_extract(payload, '$.inReplyTo'),
      COALESCE(json_extract(payload, '$.runId'), '')
    ) WHERE event_type = 'InterruptCommit'`
})

/** Allow only one terminal outcome for each Agent Run. */
const addAgentRunOutcomeCausation = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`DROP INDEX session_events_agent_message_causation`
  yield* sql`DROP INDEX session_events_failure_commit_causation`
  yield* sql`DROP INDEX session_events_interrupt_commit_causation`
  yield* sql`CREATE UNIQUE INDEX session_events_agent_run_outcome_causation
    ON session_events(
      json_extract(payload, '$.inReplyTo'),
      COALESCE(json_extract(payload, '$.runId'), '')
    ) WHERE event_type IN (
      'AgentMessageCommit', 'FailureCommit', 'InterruptCommit'
  )`
})

/** Treat Compaction as the terminal outcome of its stateless Agent Run. */
const addCompactionCommitCausation = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`DROP INDEX session_events_agent_run_outcome_causation`
  yield* sql`CREATE UNIQUE INDEX session_events_agent_run_outcome_causation
    ON session_events(
      json_extract(payload, '$.inReplyTo'),
      COALESCE(json_extract(payload, '$.runId'), '')
    ) WHERE event_type IN (
      'AgentMessageCommit', 'CompactionCommit', 'FailureCommit', 'InterruptCommit'
    )`
})

/** Add durable Workstream ownership and metadata for existing Sessions. */
const addWorkstreams = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE workstreams (
    workstream_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    peer_id TEXT NOT NULL
  )`
  yield* sql`CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY,
    workstream_id TEXT NOT NULL REFERENCES workstreams(workstream_id),
    title TEXT NOT NULL DEFAULT 'New session',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    peer_id TEXT NOT NULL
  )`
  yield* sql`CREATE INDEX sessions_workstream_id ON sessions (workstream_id, updated_at DESC)`

  yield* sql`INSERT INTO workstreams (
    workstream_id, name, created_at, updated_at, peer_id
  )
  SELECT
    'default-workstream',
    'Default Workstream',
    COALESCE(MIN(occurred_at), '1970-01-01T00:00:00.000Z'),
    COALESCE(MAX(occurred_at), '1970-01-01T00:00:00.000Z'),
    'default-peer'
  FROM session_events
  WHERE EXISTS (SELECT 1 FROM session_events)`

  yield* sql`INSERT INTO sessions (
    session_id, workstream_id, title, created_at, updated_at, peer_id
  )
  SELECT
    session_id,
    'default-workstream',
    COALESCE(
      (SELECT json_extract(first.payload, '$.content')
       FROM session_events first
       WHERE first.session_id=events.session_id
         AND first.event_type IN ('UserCommit','UserMessageAdded')
       ORDER BY first.sequence LIMIT 1),
      'New session'
    ),
    MIN(occurred_at),
    MAX(occurred_at),
    'default-peer'
  FROM session_events events
  GROUP BY session_id`
})

/** Persist reversible Session lifecycle state independently of Commit history. */
const addSessionSettlement = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`ALTER TABLE sessions ADD COLUMN settled_at TEXT`
})

export const loader = SqliteMigrator.fromRecord({
  "1_create_session_events": createSessionEvents,
  "2_create_event_dispatch": createEventDispatch,
  "3_allow_agent_activity_events": allowAgentActivityEvents,
  "4_add_canonical_commits_and_local_heads": addCanonicalCommitsAndLocalHeads,
  "5_add_failure_commit_causation": addFailureCommitCausation,
  "6_add_agent_run_identity": addAgentRunIdentity,
  "7_add_peer_branch_heads": addPeerBranchHeads,
  "8_add_workstreams": addWorkstreams,
  "9_add_session_settlement": addSessionSettlement,
  "10_add_interrupt_commit_causation": addInterruptCommitCausation,
  "11_add_agent_run_outcome_causation": addAgentRunOutcomeCausation,
  "12_add_compaction_commit_causation": addCompactionCommitCausation
})
