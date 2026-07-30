import { SqliteClient, SqliteMigrator } from "@effect/sql-sqlite-bun"
import { mkdirSync } from "node:fs"
import { homedir } from "node:os"
import { dirname, join } from "node:path"
import { Context, Effect, Layer, Schema } from "effect"
import { Reactivity } from "effect/unstable/reactivity"
import { SqlClient } from "effect/unstable/sql"
import * as SessionMigrations from "./SessionMigrations.ts"

export const defaultDatabasePath = join(homedir(), ".local", "share", "corredor", "corredor.db")

/** Commands express intent. They are never persisted. */
export type Command =
  | { readonly type: "CreateSession"; readonly sessionId: string }
  | { readonly type: "AddUserMessage"; readonly sessionId: string; readonly messageId: string; readonly content: string }
  | { readonly type: "NavigateTree"; readonly sessionId: string; readonly targetId: string | null }

/** Events are immutable facts and are the only source of session state. */
export type SessionEvent =
  | { readonly type: "SessionCreated"; readonly payload: Record<string, never> }
  | {
    readonly type: "UserMessageAdded"
    readonly payload: {
      readonly messageId: string
      readonly content: string
      /** Optional only for events written before conversation trees existed. */
      readonly parentId?: string | null
    }
  }
  | {
    readonly type: "AgentToolCallAdded"
    readonly payload: {
      readonly toolCallId: string
      readonly name: string
      readonly input: unknown
      readonly inReplyTo: string
      readonly index: number
      readonly parentId?: string | null
    }
  }
  | {
    readonly type: "AgentMessageAdded"
    readonly payload: {
      readonly messageId: string
      readonly content: string
      readonly inReplyTo: string
      readonly parentId?: string | null
    }
  }
  | { readonly type: "SessionTreeNavigated"; readonly payload: { readonly targetId: string | null } }

export type StoredEvent = SessionEvent & {
  readonly eventId: string
  readonly sessionId: string
  readonly sequence: number
  readonly position: number
  readonly occurredAt: string
}

const storedEventMetadata = {
  eventId: Schema.String,
  sessionId: Schema.String,
  sequence: Schema.Number,
  position: Schema.Number,
  occurredAt: Schema.String
}

/** Runtime decoder shared by the HTTP client and SSE transport. */
export const StoredEventSchema = Schema.Union([
  Schema.Struct({
    ...storedEventMetadata,
    type: Schema.Literal("SessionCreated"),
    payload: Schema.Struct({})
  }),
  Schema.Struct({
    ...storedEventMetadata,
    type: Schema.Literal("UserMessageAdded"),
    payload: Schema.Struct({
      messageId: Schema.String,
      content: Schema.String,
      parentId: Schema.optional(Schema.NullOr(Schema.String))
    })
  }),
  Schema.Struct({
    ...storedEventMetadata,
    type: Schema.Literal("AgentToolCallAdded"),
    payload: Schema.Struct({
      toolCallId: Schema.String,
      name: Schema.String,
      input: Schema.Unknown,
      inReplyTo: Schema.String,
      index: Schema.Number,
      parentId: Schema.optional(Schema.NullOr(Schema.String))
    })
  }),
  Schema.Struct({
    ...storedEventMetadata,
    type: Schema.Literal("AgentMessageAdded"),
    payload: Schema.Struct({
      messageId: Schema.String,
      content: Schema.String,
      inReplyTo: Schema.String,
      parentId: Schema.optional(Schema.NullOr(Schema.String))
    })
  }),
  Schema.Struct({
    ...storedEventMetadata,
    type: Schema.Literal("SessionTreeNavigated"),
    payload: Schema.Struct({ targetId: Schema.NullOr(Schema.String) })
  })
])

export class PersistenceError extends Schema.TaggedErrorClass<PersistenceError>()("@corredor/Session/PersistenceError", { message: Schema.String }) {}
export class AlreadyExists extends Schema.TaggedErrorClass<AlreadyExists>()("@corredor/Session/AlreadyExists", { sessionId: Schema.String }) {}
export class NotFound extends Schema.TaggedErrorClass<NotFound>()("@corredor/Session/NotFound", { sessionId: Schema.String }) {}
export class EntryNotFound extends Schema.TaggedErrorClass<EntryNotFound>()("@corredor/Session/EntryNotFound", {
  sessionId: Schema.String,
  entryId: Schema.String
}) {}
export type Error = PersistenceError | AlreadyExists | NotFound | EntryNotFound

export interface SessionSummary {
  readonly sessionId: string
  readonly createdAt: string
  readonly updatedAt: string
  readonly title: string
  readonly messageCount: number
}

export const SessionSummarySchema = Schema.Struct({
  sessionId: Schema.String,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  title: Schema.String,
  messageCount: Schema.Number
})

export interface Interface {
  readonly check: Effect.Effect<void, PersistenceError>
  readonly execute: (command: Command) => Effect.Effect<StoredEvent, Error>
  readonly appendAgentToolCall: (
    sessionId: string,
    toolCallId: string,
    name: string,
    input: unknown,
    inReplyTo: string,
    index: number
  ) => Effect.Effect<StoredEvent, Error>
  readonly appendAgentMessage: (sessionId: string, messageId: string, content: string, inReplyTo: string) => Effect.Effect<StoredEvent, Error>
  readonly navigateTree: (sessionId: string, targetId: string | null) => Effect.Effect<StoredEvent, Error>
  readonly events: (sessionId: string) => Effect.Effect<ReadonlyArray<StoredEvent>, PersistenceError>
  readonly listSessions: () => Effect.Effect<ReadonlyArray<SessionSummary>, PersistenceError>
  readonly eventsAfter: (position: number, limit?: number) => Effect.Effect<ReadonlyArray<StoredEvent>, PersistenceError>
  readonly checkpoint: (consumer: string) => Effect.Effect<number, PersistenceError>
  readonly saveCheckpoint: (consumer: string, position: number) => Effect.Effect<void, PersistenceError>
  readonly query: (sql: string) => Effect.Effect<ReadonlyArray<Record<string, unknown>>, PersistenceError>
}
export class Service extends Context.Service<Service, Interface>()("@corredor/Session") {}

interface EventRow { readonly position: number; readonly event_id: string; readonly session_id: string; readonly sequence: number; readonly event_type: SessionEvent["type"]; readonly payload: string; readonly occurred_at: string }
const persistenceError = (cause: unknown) => new PersistenceError({ message: cause instanceof Error ? cause.message : String(cause) })
const decode = (event: EventRow): StoredEvent => ({ eventId: event.event_id, sessionId: event.session_id, sequence: event.sequence, position: event.position, type: event.event_type, payload: JSON.parse(event.payload), occurredAt: event.occurred_at } as StoredEvent)

export type ConversationEvent = Extract<StoredEvent, {
  readonly type: "UserMessageAdded" | "AgentToolCallAdded" | "AgentMessageAdded"
}>

export interface ConversationNode {
  readonly event: ConversationEvent
  readonly parentId: string | null
}

export interface ConversationTree {
  readonly nodes: ReadonlyArray<ConversationNode>
  readonly leafId: string | null
}

const isConversationEvent = (event: StoredEvent): event is ConversationEvent =>
  event.type === "UserMessageAdded" ||
  event.type === "AgentToolCallAdded" ||
  event.type === "AgentMessageAdded"

/**
 * Projects the immutable event log into a tree and its currently selected leaf.
 * Old events without parentId are interpreted as the original linear history.
 */
export const conversationTree = (events: ReadonlyArray<StoredEvent>): ConversationTree => {
  const nodes: Array<ConversationNode> = []
  const ids = new Set<string>()
  let leafId: string | null = null

  for (const event of events) {
    if (event.type === "SessionTreeNavigated") {
      leafId = event.payload.targetId === null || ids.has(event.payload.targetId)
        ? event.payload.targetId
        : leafId
      continue
    }
    if (!isConversationEvent(event)) continue

    const parentId = event.payload.parentId === undefined
      ? leafId
      : event.payload.parentId
    nodes.push({ event, parentId })
    ids.add(event.eventId)

    // A delayed response may be committed after the user navigated elsewhere.
    // Keep it in the tree without stealing the selected branch.
    if (parentId === leafId) leafId = event.eventId
  }

  return { nodes, leafId }
}

/** Returns the root-to-leaf events for the active branch (or an explicit leaf). */
export const conversationBranch = (
  events: ReadonlyArray<StoredEvent>,
  requestedLeafId?: string | null
): ReadonlyArray<ConversationEvent> => {
  const tree = conversationTree(events)
  const byId = new Map(tree.nodes.map((node) => [node.event.eventId, node] as const))
  const branch: Array<ConversationEvent> = []
  let id = requestedLeafId === undefined ? tree.leafId : requestedLeafId
  const visited = new Set<string>()

  while (id !== null && !visited.has(id)) {
    visited.add(id)
    const node = byId.get(id)
    if (node === undefined) break
    branch.push(node.event)
    id = node.parentId
  }

  branch.reverse()
  return branch
}

export const conversationParentId = (
  events: ReadonlyArray<StoredEvent>,
  eventId: string
): string | null | undefined => conversationTree(events).nodes.find(
  (node) => node.event.eventId === eventId
)?.parentId

export const make = (path = defaultDatabasePath) => Effect.gen(function*() {
  yield* Effect.try({ try: () => mkdirSync(dirname(path), { recursive: true }), catch: persistenceError })
  const sql = yield* SqliteClient.make({ filename: path })
  const persist = Effect.mapError(persistenceError)
  yield* sql`PRAGMA foreign_keys = ON`.pipe(persist)
  yield* SqliteMigrator.run({ loader: SessionMigrations.loader }).pipe(Effect.provideService(SqlClient.SqlClient, sql), persist)

  type AppendDecision =
    | SessionEvent
    | { readonly entryNotFound: string }
  type DecideEvent = (events: ReadonlyArray<StoredEvent>) => AppendDecision

  const append = (
    sessionId: string,
    eventOrDecide: SessionEvent | DecideEvent
  ): Effect.Effect<StoredEvent, Error> => Effect.gen(function*() {
    const result = yield* Effect.gen(function*() {
      const eventRows = yield* sql<EventRow>`SELECT d.position, e.* FROM event_dispatch d JOIN session_events e ON e.event_id=d.event_id WHERE e.session_id=${sessionId} ORDER BY e.sequence`
      const existing = eventRows.map(decode)
      const sequence = existing.at(-1)?.sequence ?? 0
      if (sequence === 0 && typeof eventOrDecide === "function") {
        return { outcome: "NotFound" as const }
      }
      const decision = typeof eventOrDecide === "function"
        ? eventOrDecide(existing)
        : eventOrDecide
      if ("entryNotFound" in decision) {
        return { outcome: "EntryNotFound" as const, entryId: decision.entryNotFound }
      }
      const event = decision
      if (event.type === "SessionCreated" && sequence !== 0) return { outcome: "AlreadyExists" as const }
      if (event.type !== "SessionCreated" && sequence === 0) return { outcome: "NotFound" as const }
      const eventId = crypto.randomUUID()
      const occurredAt = new Date().toISOString()
      yield* sql`INSERT INTO session_events (event_id, session_id, sequence, event_type, payload, occurred_at) VALUES (${eventId}, ${sessionId}, ${sequence + 1}, ${event.type}, ${JSON.stringify(event.payload)}, ${occurredAt})`
      yield* sql`INSERT INTO event_dispatch (event_id) VALUES (${eventId})`
      const positions = yield* sql<{ readonly position: number }>`SELECT position FROM event_dispatch WHERE event_id = ${eventId}`
      return { outcome: "Appended" as const, stored: { ...event, eventId, sessionId, sequence: sequence + 1, position: positions[0]!.position, occurredAt } as StoredEvent }
    }).pipe(sql.withTransaction, persist)
    if (result.outcome === "AlreadyExists") return yield* new AlreadyExists({ sessionId })
    if (result.outcome === "NotFound") return yield* new NotFound({ sessionId })
    if (result.outcome === "EntryNotFound") return yield* new EntryNotFound({ sessionId, entryId: result.entryId })
    return result.stored
  })

  const responseParent = (
    events: ReadonlyArray<StoredEvent>,
    inReplyTo: string
  ): string => {
    const toolCalls = events.filter((event) =>
      event.type === "AgentToolCallAdded" && event.payload.inReplyTo === inReplyTo
    )
    return toolCalls.at(-1)?.eventId ?? inReplyTo
  }

  return Service.of({
    check: sql`SELECT 1`.pipe(Effect.asVoid, persist),
    execute: (command) => {
      if (command.type === "CreateSession") {
        return append(command.sessionId, { type: "SessionCreated", payload: {} })
      }
      if (command.type === "NavigateTree") {
        return append(command.sessionId, (events) => {
          if (command.targetId !== null && !conversationTree(events).nodes.some(
            (node) => node.event.eventId === command.targetId
          )) {
            return { entryNotFound: command.targetId }
          }
          return {
            type: "SessionTreeNavigated",
            payload: { targetId: command.targetId }
          }
        })
      }
      return append(command.sessionId, (events) => ({
        type: "UserMessageAdded",
        payload: {
          messageId: command.messageId,
          content: command.content,
          parentId: conversationTree(events).leafId
        }
      }))
    },
    appendAgentToolCall: (sessionId, toolCallId, name, input, inReplyTo, index) => append(sessionId, (events) => ({
      type: "AgentToolCallAdded",
      payload: {
        toolCallId,
        name,
        input,
        inReplyTo,
        index,
        parentId: responseParent(events, inReplyTo)
      }
    })),
    appendAgentMessage: (sessionId, messageId, content, inReplyTo) => append(sessionId, (events) => ({
      type: "AgentMessageAdded",
      payload: {
        messageId,
        content,
        inReplyTo,
        parentId: responseParent(events, inReplyTo)
      }
    })),
    navigateTree: (sessionId, targetId) => append(sessionId, (events) => {
      if (targetId !== null && !conversationTree(events).nodes.some(
        (node) => node.event.eventId === targetId
      )) {
        return { entryNotFound: targetId }
      }
      return { type: "SessionTreeNavigated", payload: { targetId } }
    }),
    events: (sessionId) => sql<EventRow>`SELECT d.position, e.* FROM event_dispatch d JOIN session_events e ON e.event_id=d.event_id WHERE e.session_id=${sessionId} ORDER BY e.sequence`.pipe(Effect.map((rows) => rows.map(decode)), persist),
    listSessions: () => sql<{
      readonly sessionId: string
      readonly createdAt: string
      readonly updatedAt: string
      readonly title: string | null
      readonly messageCount: number
    }>`SELECT
      session_id AS sessionId,
      MIN(occurred_at) AS createdAt,
      MAX(occurred_at) AS updatedAt,
      (SELECT json_extract(first.payload, '$.content') FROM session_events first WHERE first.session_id = session_events.session_id AND first.event_type = 'UserMessageAdded' ORDER BY first.sequence LIMIT 1) AS title,
      SUM(CASE WHEN event_type = 'UserMessageAdded' THEN 1 ELSE 0 END) AS messageCount
    FROM session_events GROUP BY session_id ORDER BY updatedAt DESC`.pipe(
      Effect.map((rows) => rows.map((row) => ({ ...row, title: row.title ?? "New session" }))),
      persist
    ),
    eventsAfter: (position, limit = 100) => sql<EventRow>`SELECT d.position, e.* FROM event_dispatch d JOIN session_events e ON e.event_id=d.event_id WHERE d.position>${position} ORDER BY d.position LIMIT ${limit}`.pipe(Effect.map((rows) => rows.map(decode)), persist),
    checkpoint: (consumer) => sql<{ readonly position: number }>`SELECT position FROM event_consumer_checkpoints WHERE consumer=${consumer}`.pipe(Effect.map((rows) => rows[0]?.position ?? 0), persist),
    saveCheckpoint: (consumer, position) => sql`INSERT INTO event_consumer_checkpoints (consumer, position) VALUES (${consumer}, ${position}) ON CONFLICT(consumer) DO UPDATE SET position=excluded.position`.pipe(Effect.asVoid, persist),
    query: (query) => sql.unsafe<Record<string, unknown>>(query).pipe(persist)
  })
}).pipe(Effect.provide(Reactivity.layer))

export const layer = (path = defaultDatabasePath) => Layer.effect(Service, make(path))
