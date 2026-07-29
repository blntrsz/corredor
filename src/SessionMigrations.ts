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

export const loader = SqliteMigrator.fromRecord({
  "1_create_session_events": createSessionEvents,
  "2_create_event_dispatch": createEventDispatch
})
