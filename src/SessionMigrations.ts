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

export const loader = SqliteMigrator.fromRecord({
  "1_create_session_events": createSessionEvents,
  "2_create_event_dispatch": createEventDispatch,
  "3_allow_agent_activity_events": allowAgentActivityEvents,
  "4_add_canonical_commits_and_local_heads": addCanonicalCommitsAndLocalHeads
})
