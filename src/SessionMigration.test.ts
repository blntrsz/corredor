import { expect, it } from "@effect/vitest"
import { BunCrypto } from "@effect/platform-bun"
import { Database } from "bun:sqlite"
import { Effect } from "effect"
import * as Session from "./Session.ts"
import { temporaryDatabase } from "./TestSupport.ts"

it.live("legacy user, agent, tool-call-only, and navigation history remains readable without rewriting", () =>
  Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-legacy-")
    const database = yield* Effect.acquireRelease(
      Effect.sync(() => new Database(path)),
      (connection) => Effect.sync(() => connection.close())
    )

    database.exec(`
      CREATE TABLE effect_sql_migrations (
        migration_id integer PRIMARY KEY NOT NULL,
        created_at datetime NOT NULL DEFAULT current_timestamp,
        name VARCHAR(255) NOT NULL
      );
      INSERT INTO effect_sql_migrations (migration_id, name) VALUES
        (1, 'create_session_events'),
        (2, 'create_event_dispatch'),
        (3, 'allow_agent_activity_events');

      CREATE TABLE session_events (
        event_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        UNIQUE (session_id, sequence)
      );
      CREATE INDEX session_events_session_id
        ON session_events (session_id, sequence);
      CREATE UNIQUE INDEX session_events_tool_call_causation
        ON session_events(
          json_extract(payload, '$.inReplyTo'),
          json_extract(payload, '$.index')
        ) WHERE event_type = 'AgentToolCallAdded';
      CREATE TABLE event_dispatch (
        position INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE REFERENCES session_events(event_id)
      );
      CREATE TABLE event_consumer_checkpoints (
        consumer TEXT PRIMARY KEY,
        position INTEGER NOT NULL
      );

      INSERT INTO session_events VALUES
        ('created', 'legacy-session', 1, 'SessionCreated', '{}', '2026-01-01T00:00:00.000Z'),
        ('user-legacy', 'legacy-session', 2, 'UserMessageAdded',
          '{"messageId":"message-1","content":"legacy request"}',
          '2026-01-01T00:00:01.000Z'),
        ('tool-legacy', 'legacy-session', 3, 'AgentToolCallAdded',
          '{"toolCallId":"call-1","name":"Bash","input":{"command":"pwd"},"inReplyTo":"user-legacy","index":0}',
          '2026-01-01T00:00:02.000Z'),
        ('agent-legacy', 'legacy-session', 4, 'AgentMessageAdded',
          '{"messageId":"message-2","content":"legacy response","inReplyTo":"user-legacy"}',
          '2026-01-01T00:00:03.000Z'),
        ('navigation-legacy', 'legacy-session', 5, 'SessionTreeNavigated',
          '{"targetId":"user-legacy"}',
          '2026-01-01T00:00:04.000Z');
      INSERT INTO event_dispatch (position, event_id) VALUES
        (1, 'created'),
        (2, 'user-legacy'),
        (3, 'tool-legacy'),
        (4, 'agent-legacy'),
        (5, 'navigation-legacy');
    `)
    const result = yield* Effect.gen(function*() {
      const store = yield* Session.make(path)
      const history = yield* store.history("legacy-session")
      const continued = yield* store.appendUserCommit(
        "legacy-session",
        "continue",
        "user-new"
      )
      return { history, continued }
    }).pipe(Effect.provide(BunCrypto.layer))
    const migrated = yield* Effect.acquireRelease(
      Effect.sync(() => new Database(path, { readonly: true })),
      (connection) => Effect.sync(() => connection.close())
    )
    const rawTypes = migrated.query(
      "SELECT event_type AS type FROM session_events ORDER BY sequence"
    ).all() as ReadonlyArray<{ readonly type: string }>
    expect(result.history.items.map((item) => item.type)).toEqual([
      "SessionCreated",
      "UserCommit",
      "LegacyToolCall",
      "AgentMessageCommit",
      "LegacyNavigation"
    ])
    expect(result.history.items[2]).toMatchObject({
      type: "LegacyToolCall",
      legacyId: "tool-legacy",
      parentId: "user-legacy",
      name: "Bash",
      input: { command: "pwd" }
    })
    expect(result.history.items[1]).toMatchObject({
      type: "UserCommit",
      legacyMessageId: "message-1"
    })
    expect(result.history.items[3]).toMatchObject({
      type: "AgentMessageCommit",
      legacyMessageId: "message-2"
    })
    expect(result.continued.parentId).toBe("user-legacy")
    expect(rawTypes.map((row) => row.type)).toEqual([
      "SessionCreated",
      "UserMessageAdded",
      "AgentToolCallAdded",
      "AgentMessageAdded",
      "SessionTreeNavigated",
      "UserCommit"
    ])
  }))
