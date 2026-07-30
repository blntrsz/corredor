import { SqliteClient, SqliteMigrator } from "@effect/sql-sqlite-bun"
import { mkdirSync } from "node:fs"
import { homedir } from "node:os"
import { dirname, join } from "node:path"
import { Context, Effect, Layer, Schema } from "effect"
import { Reactivity } from "effect/unstable/reactivity"
import { SqlClient } from "effect/unstable/sql"
import * as SessionMigrations from "./SessionMigrations.ts"

export const defaultDatabasePath = join(homedir(), ".local", "share", "corredor", "corredor.db")

export type ToolOutcome =
  | { readonly type: "Success"; readonly result: unknown }
  | { readonly type: "Failure"; readonly failure: unknown }

interface CommitMetadata {
  readonly commitId: string
  readonly sessionId: string
  readonly parentId: string | null
  readonly sequence: number
  readonly position: number
  readonly createdAt: string
}

export type Commit =
  | CommitMetadata & {
    readonly type: "UserCommit"
    readonly content: string
  }
  | CommitMetadata & {
    readonly type: "ToolCommit"
    readonly toolCallId: string
    readonly name: string
    readonly input: unknown
    readonly outcome: ToolOutcome
    readonly inReplyTo: string
    readonly index: number
  }
  | CommitMetadata & {
    readonly type: "AgentMessageCommit"
    readonly content: string
    readonly inReplyTo: string
  }

export interface LegacyToolCall {
  readonly type: "LegacyToolCall"
  readonly legacyId: string
  readonly sessionId: string
  readonly parentId: string | null
  readonly sequence: number
  readonly position: number
  readonly createdAt: string
  readonly toolCallId: string
  readonly name: string
  readonly input: unknown
  readonly inReplyTo: string
  readonly index: number
}

export interface SessionCreated {
  readonly type: "SessionCreated"
  readonly activityId: string
  readonly sessionId: string
  readonly sequence: number
  readonly position: number
  readonly occurredAt: string
}

export interface LegacyNavigation {
  readonly type: "LegacyNavigation"
  readonly activityId: string
  readonly sessionId: string
  readonly sequence: number
  readonly position: number
  readonly occurredAt: string
  readonly targetId: string | null
}

export type HistoryItem = Commit | LegacyToolCall | SessionCreated | LegacyNavigation
export type GraphEntry = Commit | LegacyToolCall

export interface HistorySnapshot {
  readonly items: ReadonlyArray<HistoryItem>
  readonly branchHeadId: string | null
}

const historyMetadata = {
  sessionId: Schema.String,
  sequence: Schema.Number,
  position: Schema.Number
}

const commitMetadata = {
  ...historyMetadata,
  commitId: Schema.String,
  parentId: Schema.NullOr(Schema.String),
  createdAt: Schema.String
}

const ToolOutcomeSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("Success"),
    result: Schema.Unknown
  }),
  Schema.Struct({
    type: Schema.Literal("Failure"),
    failure: Schema.Unknown
  })
])

const UserCommitSchema = Schema.Struct({
  ...commitMetadata,
  type: Schema.Literal("UserCommit"),
  content: Schema.String
})

const ToolCommitSchema = Schema.Struct({
  ...commitMetadata,
  type: Schema.Literal("ToolCommit"),
  toolCallId: Schema.String,
  name: Schema.String,
  input: Schema.Unknown,
  outcome: ToolOutcomeSchema,
  inReplyTo: Schema.String,
  index: Schema.Number
})

const AgentMessageCommitSchema = Schema.Struct({
  ...commitMetadata,
  type: Schema.Literal("AgentMessageCommit"),
  content: Schema.String,
  inReplyTo: Schema.String
})

export const SessionCreatedSchema = Schema.Struct({
  ...historyMetadata,
  type: Schema.Literal("SessionCreated"),
  activityId: Schema.String,
  occurredAt: Schema.String
})

/** Runtime decoder shared by the HTTP client and SSE transport. */
export const HistoryItemSchema = Schema.Union([
  UserCommitSchema,
  ToolCommitSchema,
  AgentMessageCommitSchema,
  Schema.Struct({
    ...historyMetadata,
    type: Schema.Literal("LegacyToolCall"),
    legacyId: Schema.String,
    parentId: Schema.NullOr(Schema.String),
    createdAt: Schema.String,
    toolCallId: Schema.String,
    name: Schema.String,
    input: Schema.Unknown,
    inReplyTo: Schema.String,
    index: Schema.Number
  }),
  SessionCreatedSchema,
  Schema.Struct({
    ...historyMetadata,
    type: Schema.Literal("LegacyNavigation"),
    activityId: Schema.String,
    occurredAt: Schema.String,
    targetId: Schema.NullOr(Schema.String)
  })
])

export const CommitSchema = Schema.Union([
  UserCommitSchema,
  ToolCommitSchema,
  AgentMessageCommitSchema
])

export const HistorySnapshotSchema = Schema.Struct({
  items: Schema.Array(HistoryItemSchema),
  branchHeadId: Schema.NullOr(Schema.String)
})

export class PersistenceError extends Schema.TaggedErrorClass<PersistenceError>()(
  "@corredor/Session/PersistenceError",
  { message: Schema.String }
) {}
export class AlreadyExists extends Schema.TaggedErrorClass<AlreadyExists>()(
  "@corredor/Session/AlreadyExists",
  { sessionId: Schema.String }
) {}
export class NotFound extends Schema.TaggedErrorClass<NotFound>()(
  "@corredor/Session/NotFound",
  { sessionId: Schema.String }
) {}
export class CommitNotFound extends Schema.TaggedErrorClass<CommitNotFound>()(
  "@corredor/Session/CommitNotFound",
  { sessionId: Schema.String, commitId: Schema.String }
) {}
export type Error = PersistenceError | AlreadyExists | NotFound | CommitNotFound

export interface SessionSummary {
  readonly sessionId: string
  readonly createdAt: string
  readonly updatedAt: string
  readonly title: string
  readonly userCommitCount: number
}

export const SessionSummarySchema = Schema.Struct({
  sessionId: Schema.String,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  title: Schema.String,
  userCommitCount: Schema.Number
})

export const isCommit = (item: HistoryItem): item is Commit =>
  item.type === "UserCommit" ||
  item.type === "ToolCommit" ||
  item.type === "AgentMessageCommit"

export const isGraphEntry = (item: HistoryItem): item is GraphEntry =>
  isCommit(item) || item.type === "LegacyToolCall"

export const graphEntryId = (entry: GraphEntry): string =>
  entry.type === "LegacyToolCall" ? entry.legacyId : entry.commitId

export const historyItemId = (item: HistoryItem): string => {
  if (isCommit(item)) return item.commitId
  if (item.type === "LegacyToolCall") return item.legacyId
  return item.activityId
}

export interface CommitNode {
  readonly entry: GraphEntry
  readonly parentId: string | null
}

export interface CommitGraph {
  readonly nodes: ReadonlyArray<CommitNode>
  readonly headId: string | null
}

/**
 * Projects canonical Commits and compatible legacy context records into the
 * Session graph. Legacy navigation is read only as migration input.
 */
export const commitGraph = (
  history: ReadonlyArray<HistoryItem>,
  requestedHeadId?: string | null
): CommitGraph => {
  const nodes: Array<CommitNode> = []
  const ids = new Set<string>()
  let headId: string | null = null

  for (const item of history) {
    if (item.type === "LegacyNavigation") {
      headId = item.targetId === null || ids.has(item.targetId)
        ? item.targetId
        : headId
      continue
    }
    if (!isGraphEntry(item)) continue

    nodes.push({ entry: item, parentId: item.parentId })
    const id = graphEntryId(item)
    ids.add(id)
    if (item.parentId === headId) headId = id
  }

  if (
    requestedHeadId !== undefined &&
    (requestedHeadId === null || ids.has(requestedHeadId))
  ) {
    headId = requestedHeadId
  }
  return { nodes, headId }
}

/** Returns the root-to-head context entries for a Branch. */
export const branchHistory = (
  history: ReadonlyArray<HistoryItem>,
  requestedHeadId?: string | null
): ReadonlyArray<GraphEntry> => {
  const graph = commitGraph(history)
  const byId = new Map(graph.nodes.map((node) => [graphEntryId(node.entry), node] as const))
  const branch: Array<GraphEntry> = []
  let id = requestedHeadId === undefined ? graph.headId : requestedHeadId
  const visited = new Set<string>()

  while (id !== null && !visited.has(id)) {
    visited.add(id)
    const node = byId.get(id)
    if (node === undefined) break
    branch.push(node.entry)
    id = node.parentId
  }

  branch.reverse()
  return branch
}

export interface Interface {
  readonly check: Effect.Effect<void, PersistenceError>
  readonly createSession: (sessionId: string) => Effect.Effect<SessionCreated, Error>
  readonly appendUserCommit: (
    sessionId: string,
    content: string,
    commitId: string
  ) => Effect.Effect<Extract<Commit, { readonly type: "UserCommit" }>, Error>
  readonly appendToolCommit: (
    sessionId: string,
    toolCallId: string,
    name: string,
    input: unknown,
    outcome: ToolOutcome,
    inReplyTo: string,
    index: number
  ) => Effect.Effect<Extract<Commit, { readonly type: "ToolCommit" }>, Error>
  readonly appendAgentMessageCommit: (
    sessionId: string,
    content: string,
    inReplyTo: string
  ) => Effect.Effect<Extract<Commit, { readonly type: "AgentMessageCommit" }>, Error>
  readonly checkout: (sessionId: string, commitId: string | null) => Effect.Effect<void, Error>
  readonly history: (sessionId: string) => Effect.Effect<HistorySnapshot, PersistenceError>
  readonly listSessions: () => Effect.Effect<ReadonlyArray<SessionSummary>, PersistenceError>
  readonly activityAfter: (position: number, limit?: number) => Effect.Effect<ReadonlyArray<HistoryItem>, PersistenceError>
  readonly checkpoint: (consumer: string) => Effect.Effect<number, PersistenceError>
  readonly saveCheckpoint: (consumer: string, position: number) => Effect.Effect<void, PersistenceError>
  readonly query: (sql: string) => Effect.Effect<ReadonlyArray<Record<string, unknown>>, PersistenceError>
}
export class Service extends Context.Service<Service, Interface>()("@corredor/Session") {}

type RawEventType =
  | "SessionCreated"
  | "UserCommit"
  | "ToolCommit"
  | "AgentMessageCommit"
  | "UserMessageAdded"
  | "AgentToolCallAdded"
  | "AgentMessageAdded"
  | "SessionTreeNavigated"

interface EventRow {
  readonly position: number
  readonly event_id: string
  readonly session_id: string
  readonly sequence: number
  readonly event_type: RawEventType
  readonly payload: string
  readonly occurred_at: string
}

const persistenceError = (cause: unknown) =>
  new PersistenceError({ message: cause instanceof Error ? cause.message : String(cause) })

const decodeHistory = (rows: ReadonlyArray<EventRow>): ReadonlyArray<HistoryItem> => {
  const heads = new Map<string, string | null>()
  const items: Array<HistoryItem> = []

  for (const row of rows) {
    const payload = JSON.parse(row.payload) as Record<string, unknown>
    const metadata = {
      sessionId: row.session_id,
      sequence: row.sequence,
      position: row.position
    }
    const currentHead = heads.get(row.session_id) ?? null

    if (row.event_type === "SessionCreated") {
      heads.set(row.session_id, currentHead)
      items.push({
        ...metadata,
        type: "SessionCreated",
        activityId: row.event_id,
        occurredAt: row.occurred_at
      })
      continue
    }

    if (row.event_type === "SessionTreeNavigated") {
      const targetId = typeof payload.targetId === "string" ? payload.targetId : null
      heads.set(row.session_id, targetId)
      items.push({
        ...metadata,
        type: "LegacyNavigation",
        activityId: row.event_id,
        occurredAt: row.occurred_at,
        targetId
      })
      continue
    }

    const parentId = payload.parentId === undefined
      ? currentHead
      : typeof payload.parentId === "string"
      ? payload.parentId
      : null
    const createdAt = row.occurred_at

    if (row.event_type === "UserCommit" || row.event_type === "UserMessageAdded") {
      const commit: Commit = {
        ...metadata,
        type: "UserCommit",
        commitId: row.event_id,
        parentId,
        createdAt,
        content: String(payload.content ?? "")
      }
      items.push(commit)
      if (parentId === currentHead) heads.set(row.session_id, commit.commitId)
      continue
    }

    if (row.event_type === "AgentMessageCommit" || row.event_type === "AgentMessageAdded") {
      const commit: Commit = {
        ...metadata,
        type: "AgentMessageCommit",
        commitId: row.event_id,
        parentId,
        createdAt,
        content: String(payload.content ?? ""),
        inReplyTo: String(payload.inReplyTo ?? parentId ?? "")
      }
      items.push(commit)
      if (parentId === currentHead) heads.set(row.session_id, commit.commitId)
      continue
    }

    if (row.event_type === "ToolCommit") {
      const rawOutcome = payload.outcome as Record<string, unknown> | undefined
      const outcome: ToolOutcome = rawOutcome?.type === "Failure"
        ? { type: "Failure", failure: rawOutcome.failure }
        : { type: "Success", result: rawOutcome?.result }
      const commit: Commit = {
        ...metadata,
        type: "ToolCommit",
        commitId: row.event_id,
        parentId,
        createdAt,
        toolCallId: String(payload.toolCallId ?? ""),
        name: String(payload.name ?? ""),
        input: payload.input,
        outcome,
        inReplyTo: String(payload.inReplyTo ?? ""),
        index: Number(payload.index ?? 0)
      }
      items.push(commit)
      if (parentId === currentHead) heads.set(row.session_id, commit.commitId)
      continue
    }

    const legacy: LegacyToolCall = {
      ...metadata,
      type: "LegacyToolCall",
      legacyId: row.event_id,
      parentId,
      createdAt,
      toolCallId: String(payload.toolCallId ?? ""),
      name: String(payload.name ?? ""),
      input: payload.input,
      inReplyTo: String(payload.inReplyTo ?? ""),
      index: Number(payload.index ?? 0)
    }
    items.push(legacy)
    if (parentId === currentHead) heads.set(row.session_id, legacy.legacyId)
  }

  return items
}

export const make = (path = defaultDatabasePath) => Effect.gen(function*() {
  yield* Effect.try({
    try: () => mkdirSync(dirname(path), { recursive: true }),
    catch: persistenceError
  })
  const sql = yield* SqliteClient.make({ filename: path })
  const persist = Effect.mapError(persistenceError)
  yield* sql`PRAGMA foreign_keys = ON`.pipe(persist)
  yield* SqliteMigrator.run({ loader: SessionMigrations.loader }).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
    persist
  )

  const allRows = () =>
    sql<EventRow>`SELECT d.position, e.* FROM event_dispatch d JOIN session_events e ON e.event_id=d.event_id ORDER BY e.session_id, e.sequence`

  const sessionRows = (sessionId: string) =>
    sql<EventRow>`SELECT d.position, e.* FROM event_dispatch d JOIN session_events e ON e.event_id=d.event_id WHERE e.session_id=${sessionId} ORDER BY e.sequence`

  // Derive a local Branch Head once for legacy Sessions. No legacy row is
  // rewritten, and later Checkout operations update only local Peer state.
  const existingRows = yield* allRows().pipe(persist)
  const sessionIds = new Set(existingRows.map((row) => row.session_id))
  for (const sessionId of sessionIds) {
    const history = decodeHistory(existingRows.filter((row) => row.session_id === sessionId))
    yield* sql`INSERT OR IGNORE INTO session_branch_heads (session_id, commit_id)
      VALUES (${sessionId}, ${commitGraph(history).headId})`.pipe(persist)
  }

  const insert = (
    sessionId: string,
    eventId: string,
    type: RawEventType,
    payload: Record<string, unknown>,
    sequence: number
  ) => Effect.gen(function*() {
    const occurredAt = new Date().toISOString()
    yield* sql`INSERT INTO session_events (event_id, session_id, sequence, event_type, payload, occurred_at)
      VALUES (${eventId}, ${sessionId}, ${sequence}, ${type}, ${JSON.stringify(payload)}, ${occurredAt})`
    yield* sql`INSERT INTO event_dispatch (event_id) VALUES (${eventId})`
    const positions = yield* sql<{ readonly position: number }>`
      SELECT position FROM event_dispatch WHERE event_id=${eventId}`
    const row: EventRow = {
      position: positions[0]!.position,
      event_id: eventId,
      session_id: sessionId,
      sequence,
      event_type: type,
      payload: JSON.stringify(payload),
      occurred_at: occurredAt
    }
    return decodeHistory([row])[0]!
  })

  const updateHeadIfCurrent = (
    sessionId: string,
    expected: string | null,
    next: string
  ) => sql`UPDATE session_branch_heads SET commit_id=${next}
    WHERE session_id=${sessionId}
      AND (commit_id=${expected} OR (commit_id IS NULL AND ${expected} IS NULL))`

  const responseParent = (
    history: ReadonlyArray<HistoryItem>,
    inReplyTo: string
  ): string => {
    const toolEntries = history.filter(
      (item): item is Extract<GraphEntry, { readonly type: "ToolCommit" | "LegacyToolCall" }> =>
        (item.type === "ToolCommit" || item.type === "LegacyToolCall") &&
        item.inReplyTo === inReplyTo
    )
    return toolEntries.map(graphEntryId).at(-1) ?? inReplyTo
  }

  return Service.of({
    check: sql`SELECT 1`.pipe(Effect.asVoid, persist),
    createSession: Effect.fn("Session.createSession")(function*(sessionId) {
      const result = yield* Effect.gen(function*() {
        const rows = yield* sessionRows(sessionId)
        if (rows.length > 0) return { type: "AlreadyExists" as const }
        const item = yield* insert(sessionId, crypto.randomUUID(), "SessionCreated", {}, 1)
        yield* sql`INSERT INTO session_branch_heads (session_id, commit_id) VALUES (${sessionId}, NULL)`
        return { type: "Created" as const, item: item as SessionCreated }
      }).pipe(sql.withTransaction, persist)
      if (result.type === "AlreadyExists") return yield* new AlreadyExists({ sessionId })
      return result.item
    }),
    appendUserCommit: Effect.fn("Session.appendUserCommit")(
      function*(sessionId, content, commitId) {
        const result = yield* Effect.gen(function*() {
          const rows = yield* sessionRows(sessionId)
          if (rows.length === 0) return { type: "NotFound" as const }
          const heads = yield* sql<{ readonly commitId: string | null }>`
            SELECT commit_id AS commitId FROM session_branch_heads WHERE session_id=${sessionId}`
          const parentId = heads[0]?.commitId ?? null
          const item = yield* insert(
            sessionId,
            commitId,
            "UserCommit",
            { content, parentId },
            rows.at(-1)!.sequence + 1
          )
          yield* updateHeadIfCurrent(sessionId, parentId, commitId)
          return {
            type: "Appended" as const,
            item: item as Extract<Commit, { type: "UserCommit" }>
          }
        }).pipe(sql.withTransaction, persist)
        if (result.type === "NotFound") {
          return yield* new NotFound({ sessionId })
        }
        return result.item
      }
    ),
    appendToolCommit: Effect.fn("Session.appendToolCommit")(function*(
      sessionId,
      toolCallId,
      name,
      input,
      outcome,
      inReplyTo,
      index
    ) {
      const result = yield* Effect.gen(function*() {
        const rows = yield* sessionRows(sessionId)
        if (rows.length === 0) return { type: "NotFound" as const }
        const history = decodeHistory(rows)
        const existing = history.find((item) =>
          item.type === "ToolCommit" &&
          item.inReplyTo === inReplyTo &&
          item.index === index
        )
        if (existing !== undefined) return { type: "Appended" as const, item: existing }
        if (!history.some((item) => isGraphEntry(item) && graphEntryId(item) === inReplyTo)) {
          return { type: "CommitNotFound" as const, commitId: inReplyTo }
        }
        const parentId = responseParent(history, inReplyTo)
        const commitId = crypto.randomUUID()
        const item = yield* insert(
          sessionId,
          commitId,
          "ToolCommit",
          { toolCallId, name, input, outcome, inReplyTo, index, parentId },
          rows.at(-1)!.sequence + 1
        )
        yield* updateHeadIfCurrent(sessionId, parentId, commitId)
        return { type: "Appended" as const, item: item as Extract<Commit, { type: "ToolCommit" }> }
      }).pipe(sql.withTransaction, persist)
      if (result.type === "NotFound") return yield* new NotFound({ sessionId })
      if (result.type === "CommitNotFound") {
        return yield* new CommitNotFound({ sessionId, commitId: result.commitId })
      }
      return result.item as Extract<Commit, { readonly type: "ToolCommit" }>
    }),
    appendAgentMessageCommit: Effect.fn("Session.appendAgentMessageCommit")(
      function*(sessionId, content, inReplyTo) {
        const result = yield* Effect.gen(function*() {
          const rows = yield* sessionRows(sessionId)
          if (rows.length === 0) return { type: "NotFound" as const }
          const history = decodeHistory(rows)
          const existing = history.find((item) =>
            item.type === "AgentMessageCommit" &&
            item.inReplyTo === inReplyTo
          )
          if (existing !== undefined) {
            return { type: "Appended" as const, item: existing }
          }
          if (!history.some(
            (item) => isGraphEntry(item) &&
              graphEntryId(item) === inReplyTo
          )) {
            return { type: "CommitNotFound" as const, commitId: inReplyTo }
          }
          const parentId = responseParent(history, inReplyTo)
          const commitId = crypto.randomUUID()
          const item = yield* insert(
            sessionId,
            commitId,
            "AgentMessageCommit",
            { content, inReplyTo, parentId },
            rows.at(-1)!.sequence + 1
          )
          yield* updateHeadIfCurrent(sessionId, parentId, commitId)
          return {
            type: "Appended" as const,
            item: item as Extract<Commit, { type: "AgentMessageCommit" }>
          }
        }).pipe(sql.withTransaction, persist)
        if (result.type === "NotFound") {
          return yield* new NotFound({ sessionId })
        }
        if (result.type === "CommitNotFound") {
          return yield* new CommitNotFound({
            sessionId,
            commitId: result.commitId
          })
        }
        return result.item as Extract<
          Commit,
          { readonly type: "AgentMessageCommit" }
        >
      }
    ),
    checkout: Effect.fn("Session.checkout")(function*(sessionId, commitId) {
      const rows = yield* sessionRows(sessionId).pipe(persist)
      if (rows.length === 0) return yield* new NotFound({ sessionId })
      const history = decodeHistory(rows)
      if (commitId !== null && !history.some(
        (item) => isGraphEntry(item) && graphEntryId(item) === commitId
      )) {
        return yield* new CommitNotFound({ sessionId, commitId })
      }
      yield* sql`UPDATE session_branch_heads SET commit_id=${commitId}
        WHERE session_id=${sessionId}`.pipe(Effect.asVoid, persist)
    }),
    history: Effect.fn("Session.history")(function*(sessionId) {
      const rows = yield* sessionRows(sessionId).pipe(persist)
      const heads = yield* sql<{ readonly branchHeadId: string | null }>`
        SELECT commit_id AS branchHeadId
        FROM session_branch_heads
        WHERE session_id=${sessionId}`.pipe(persist)
      return {
        items: decodeHistory(rows),
        branchHeadId: heads[0]?.branchHeadId ?? null
      }
    }),
    listSessions: Effect.fn("Session.listSessions")(function*() {
      return yield* sql<{
        readonly sessionId: string
        readonly createdAt: string
        readonly updatedAt: string
        readonly title: string | null
        readonly userCommitCount: number
      }>`SELECT
        session_id AS sessionId,
        MIN(occurred_at) AS createdAt,
        MAX(occurred_at) AS updatedAt,
        (SELECT json_extract(first.payload, '$.content')
          FROM session_events first
          WHERE first.session_id=session_events.session_id
            AND first.event_type IN ('UserCommit','UserMessageAdded')
          ORDER BY first.sequence LIMIT 1) AS title,
        SUM(CASE WHEN event_type IN ('UserCommit','UserMessageAdded') THEN 1 ELSE 0 END) AS userCommitCount
        FROM session_events GROUP BY session_id ORDER BY updatedAt DESC`.pipe(
        Effect.map((rows) => rows.map((row) => ({
          ...row,
          title: row.title ?? "New session"
        }))),
        persist
      )
    }),
    activityAfter: Effect.fn("Session.activityAfter")(
      function*(position, limit = 100) {
        const rows = yield* allRows().pipe(persist)
        return decodeHistory(rows)
          .filter((item) => item.position > position)
          .sort((a, b) => a.position - b.position)
          .slice(0, limit)
      }
    ),
    checkpoint: Effect.fn("Session.checkpoint")(function*(consumer) {
      const rows = yield* sql<{ readonly position: number }>`
        SELECT position FROM event_consumer_checkpoints
        WHERE consumer=${consumer}`.pipe(persist)
      return rows[0]?.position ?? 0
    }),
    saveCheckpoint: Effect.fn("Session.saveCheckpoint")(
      function*(consumer, position) {
        yield* sql`
          INSERT INTO event_consumer_checkpoints (consumer, position)
          VALUES (${consumer}, ${position})
          ON CONFLICT(consumer) DO UPDATE SET position=excluded.position
        `.pipe(Effect.asVoid, persist)
      }
    ),
    query: Effect.fn("Session.query")(function*(query) {
      return yield* sql.unsafe<Record<string, unknown>>(query).pipe(persist)
    })
  })
}).pipe(Effect.provide(Reactivity.layer))

export const layer = (path = defaultDatabasePath) => Layer.effect(Service, make(path))
