import { SqliteClient, SqliteMigrator } from "@effect/sql-sqlite-bun"
import { BunCrypto } from "@effect/platform-bun"
import { mkdirSync } from "node:fs"
import { homedir } from "node:os"
import { dirname, join } from "node:path"
import { isDeepStrictEqual } from "node:util"
import { Clock, Context, Crypto, Effect, Layer, Schema } from "effect"
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
    readonly peerId?: string
    readonly legacyMessageId?: string
  }
  | CommitMetadata & {
    readonly type: "ToolCommit"
    readonly toolCallId: string
    readonly name: string
    readonly input: unknown
    readonly outcome: ToolOutcome
    readonly inReplyTo: string
    readonly runId?: string
    readonly index: number
  }
  | CommitMetadata & {
    readonly type: "AgentMessageCommit"
    readonly content: string
    readonly inReplyTo: string
    readonly runId?: string
    readonly legacyMessageId?: string
  }
  | CommitMetadata & {
    readonly type: "FailureCommit"
    readonly reason: string
    readonly inReplyTo: string
    readonly runId?: string
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
  readonly workstreamId?: string
}

export interface SessionSettled {
  readonly type: "SessionSettled"
  readonly activityId: string
  readonly sessionId: string
  readonly sequence: number
  readonly position: number
  readonly occurredAt: string
}

export interface SessionReopened {
  readonly type: "SessionReopened"
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

export type HistoryItem =
  | Commit
  | LegacyToolCall
  | SessionCreated
  | SessionSettled
  | SessionReopened
  | LegacyNavigation
export type BranchRecord = Commit | LegacyToolCall

type UserCommit = Extract<Commit, { readonly type: "UserCommit" }>
type AgentMessageCommit = Extract<Commit, { readonly type: "AgentMessageCommit" }>
type FailureCommit = Extract<Commit, { readonly type: "FailureCommit" }>
type ToolCommit = Extract<Commit, { readonly type: "ToolCommit" }>

export interface BranchRecordHandlers<A> {
  readonly user: (record: UserCommit) => A
  readonly agentMessage: (record: AgentMessageCommit) => A
  readonly failure: (record: FailureCommit) => A
  readonly tool: (record: ToolCommit) => A
  readonly legacyTool: (record: LegacyToolCall) => A
}

/** Centralizes Branch-record dispatch for runtime and presentation consumers. */
export const foldBranchRecord = <A>(
  record: BranchRecord,
  handlers: BranchRecordHandlers<A>
): A => {
  if (record.type === "UserCommit") return handlers.user(record)
  if (record.type === "AgentMessageCommit") return handlers.agentMessage(record)
  if (record.type === "FailureCommit") return handlers.failure(record)
  if (record.type === "ToolCommit") return handlers.tool(record)
  return handlers.legacyTool(record)
}

export interface HistorySnapshot {
  readonly items: ReadonlyArray<HistoryItem>
  readonly branchHeadId: string | null
  readonly settled: boolean
}

/** Used when callers have not configured an explicit Peer identity. */
export const defaultPeerId = "default-peer"
export const defaultWorkstreamId = "default-workstream"
export const defaultWorkstreamName = "Default Workstream"

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
  content: Schema.String,
  peerId: Schema.optional(Schema.String),
  legacyMessageId: Schema.optional(Schema.String)
})

const ToolCommitSchema = Schema.Struct({
  ...commitMetadata,
  type: Schema.Literal("ToolCommit"),
  toolCallId: Schema.String,
  name: Schema.String,
  input: Schema.Unknown,
  outcome: ToolOutcomeSchema,
  inReplyTo: Schema.String,
  runId: Schema.optional(Schema.String),
  index: Schema.Number
})

const AgentMessageCommitSchema = Schema.Struct({
  ...commitMetadata,
  type: Schema.Literal("AgentMessageCommit"),
  content: Schema.String,
  inReplyTo: Schema.String,
  runId: Schema.optional(Schema.String),
  legacyMessageId: Schema.optional(Schema.String)
})

const FailureCommitSchema = Schema.Struct({
  ...commitMetadata,
  type: Schema.Literal("FailureCommit"),
  reason: Schema.String,
  inReplyTo: Schema.String,
  runId: Schema.optional(Schema.String)
})

export const SessionCreatedSchema = Schema.Struct({
  ...historyMetadata,
  type: Schema.Literal("SessionCreated"),
  activityId: Schema.String,
  occurredAt: Schema.String,
  workstreamId: Schema.optional(Schema.String)
})

export const SessionSettledSchema = Schema.Struct({
  ...historyMetadata,
  type: Schema.Literal("SessionSettled"),
  activityId: Schema.String,
  occurredAt: Schema.String
})

export const SessionReopenedSchema = Schema.Struct({
  ...historyMetadata,
  type: Schema.Literal("SessionReopened"),
  activityId: Schema.String,
  occurredAt: Schema.String
})

/** Runtime decoder shared by the HTTP client and SSE transport. */
export const HistoryItemSchema = Schema.Union([
  UserCommitSchema,
  ToolCommitSchema,
  AgentMessageCommitSchema,
  FailureCommitSchema,
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
  SessionSettledSchema,
  SessionReopenedSchema,
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
  AgentMessageCommitSchema,
  FailureCommitSchema
])

export const HistorySnapshotSchema = Schema.Struct({
  items: Schema.Array(HistoryItemSchema),
  branchHeadId: Schema.NullOr(Schema.String),
  settled: Schema.Boolean
})

export class PersistenceError extends Schema.TaggedErrorClass<PersistenceError>()(
  "@corredor/Session/PersistenceError",
  { message: Schema.String }
) {}
export class AlreadyExists extends Schema.TaggedErrorClass<AlreadyExists>()(
  "@corredor/Session/AlreadyExists",
  { sessionId: Schema.String }
) {}
export class WorkstreamAlreadyExists extends Schema.TaggedErrorClass<WorkstreamAlreadyExists>()(
  "@corredor/Session/WorkstreamAlreadyExists",
  { workstreamId: Schema.String }
) {}
export class WorkstreamNotFound extends Schema.TaggedErrorClass<WorkstreamNotFound>()(
  "@corredor/Session/WorkstreamNotFound",
  { workstreamId: Schema.String }
) {}
export class NotFound extends Schema.TaggedErrorClass<NotFound>()(
  "@corredor/Session/NotFound",
  { sessionId: Schema.String }
) {}
export class CommitNotFound extends Schema.TaggedErrorClass<CommitNotFound>()(
  "@corredor/Session/CommitNotFound",
  { sessionId: Schema.String, commitId: Schema.String }
) {}
export class ToolCommitConflict extends Schema.TaggedErrorClass<ToolCommitConflict>()(
  "@corredor/Session/ToolCommitConflict",
  {
    sessionId: Schema.String,
    inReplyTo: Schema.String,
    index: Schema.Number
  }
) {}
export class Settled extends Schema.TaggedErrorClass<Settled>()(
  "@corredor/Session/Settled",
  { sessionId: Schema.String }
) {}
export class AlreadySettled extends Schema.TaggedErrorClass<AlreadySettled>()(
  "@corredor/Session/AlreadySettled",
  { sessionId: Schema.String }
) {}
export class NotSettled extends Schema.TaggedErrorClass<NotSettled>()(
  "@corredor/Session/NotSettled",
  { sessionId: Schema.String }
) {}
export type Error =
  | PersistenceError
  | AlreadyExists
  | WorkstreamAlreadyExists
  | WorkstreamNotFound
  | NotFound
  | CommitNotFound
  | ToolCommitConflict
  | Settled
  | AlreadySettled
  | NotSettled

export type SessionListView = "active" | "settled"

export interface SessionSummary {
  readonly sessionId: string
  readonly workstreamId: string
  readonly createdAt: string
  readonly updatedAt: string
  readonly title: string
  readonly userCommitCount: number
  readonly settled: boolean
}

export const SessionSummarySchema = Schema.Struct({
  sessionId: Schema.String,
  workstreamId: Schema.String,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  title: Schema.String,
  userCommitCount: Schema.Number,
  settled: Schema.Boolean
})

export interface Workstream {
  readonly workstreamId: string
  readonly name: string
  readonly createdAt: string
  readonly updatedAt: string
  readonly peerId: string
}

export interface WorkstreamSummary extends Workstream {
  readonly sessionCount: number
}

export interface WorkstreamSnapshot {
  readonly workstream: WorkstreamSummary
  readonly sessions: ReadonlyArray<SessionSummary>
}

export const WorkstreamSchema = Schema.Struct({
  workstreamId: Schema.String,
  name: Schema.String,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  peerId: Schema.String
})

export const WorkstreamSummarySchema = Schema.Struct({
  ...WorkstreamSchema.fields,
  sessionCount: Schema.Number
})

export const WorkstreamSnapshotSchema = Schema.Struct({
  workstream: WorkstreamSummarySchema,
  sessions: Schema.Array(SessionSummarySchema)
})

export const isCommit = (item: HistoryItem): item is Commit =>
  item.type === "UserCommit" ||
  item.type === "ToolCommit" ||
  item.type === "AgentMessageCommit" ||
  item.type === "FailureCommit"

export const isBranchRecord = (item: HistoryItem): item is BranchRecord =>
  isCommit(item) || item.type === "LegacyToolCall"

export const branchRecordId = (record: BranchRecord): string =>
  record.type === "LegacyToolCall" ? record.legacyId : record.commitId

export const historyItemId = (item: HistoryItem): string => {
  if (isCommit(item)) return item.commitId
  if (item.type === "LegacyToolCall") return item.legacyId
  return item.activityId
}

export interface CommitNode {
  readonly record: BranchRecord
  readonly parentId: string | null
}

export interface CommitGraph {
  readonly nodes: ReadonlyArray<CommitNode>
  readonly headId: string | null
}

const projectCommitGraph = (
  history: ReadonlyArray<HistoryItem>,
  requestedHeadId?: string | null,
  useLegacyNavigation = false
): CommitGraph => {
  const nodes: Array<CommitNode> = []
  const ids = new Set<string>()
  let headId: string | null = null

  for (const item of history) {
    if (useLegacyNavigation && item.type === "LegacyNavigation") {
      headId = item.targetId === null || ids.has(item.targetId)
        ? item.targetId
        : headId
      continue
    }
    if (!isBranchRecord(item)) continue

    nodes.push({ record: item, parentId: item.parentId })
    const id = branchRecordId(item)
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

/** Projects the current canonical graph; legacy navigation is not authority. */
export const commitGraph = (
  history: ReadonlyArray<HistoryItem>,
  requestedHeadId?: string | null
): CommitGraph => projectCommitGraph(history, requestedHeadId)

/** Used only once while initializing a local Branch Head during migration. */
export const legacyBranchHead = (
  history: ReadonlyArray<HistoryItem>
): string | null => {
  const graph = projectCommitGraph(history, undefined, true)
  const byId = new Map(graph.nodes.map(
    (node) => [branchRecordId(node.record), node] as const
  ))
  let id = graph.headId
  while (id !== null) {
    const node = byId.get(id)
    if (node === undefined) return null
    if (isCommit(node.record)) return node.record.commitId
    id = node.parentId
  }
  return null
}

/** Returns the root-to-head context records for a Branch. */
export const branchHistory = (
  history: ReadonlyArray<HistoryItem>,
  requestedHeadId?: string | null
): ReadonlyArray<BranchRecord> => {
  const graph = commitGraph(history)
  const byId = new Map(graph.nodes.map(
    (node) => [branchRecordId(node.record), node] as const
  ))
  const branch: Array<BranchRecord> = []
  let id = requestedHeadId === undefined ? graph.headId : requestedHeadId
  const visited = new Set<string>()

  while (id !== null && !visited.has(id)) {
    visited.add(id)
    const node = byId.get(id)
    if (node === undefined) break
    branch.push(node.record)
    id = node.parentId
  }

  branch.reverse()
  return branch
}

export interface Interface {
  readonly check: Effect.Effect<void, PersistenceError>
  readonly createWorkstream: (
    workstreamId: string,
    name?: string,
    peerId?: string
  ) => Effect.Effect<Workstream, Error>
  readonly listWorkstreams: () => Effect.Effect<ReadonlyArray<WorkstreamSummary>, PersistenceError>
  readonly workstream: (
    workstreamId: string,
    view?: SessionListView
  ) => Effect.Effect<WorkstreamSnapshot, PersistenceError | WorkstreamNotFound>
  readonly createSession: (
    sessionId: string,
    workstreamId?: string,
    peerId?: string
  ) => Effect.Effect<SessionCreated, Error>
  readonly settle: (
    sessionId: string
  ) => Effect.Effect<SessionSettled, Error>
  readonly reopen: (
    sessionId: string
  ) => Effect.Effect<SessionReopened, Error>
  readonly appendUserCommit: (
    sessionId: string,
    content: string,
    commitId: string,
    peerId?: string
  ) => Effect.Effect<Extract<Commit, { readonly type: "UserCommit" }>, Error>
  readonly appendToolCommit: (
    sessionId: string,
    toolCallId: string,
    name: string,
    input: unknown,
    outcome: ToolOutcome,
    inReplyTo: string,
    index: number,
    runId?: string,
    peerId?: string
  ) => Effect.Effect<Extract<Commit, { readonly type: "ToolCommit" }>, Error>
  readonly appendAgentMessageCommit: (
    sessionId: string,
    content: string,
    inReplyTo: string,
    runId?: string,
    peerId?: string
  ) => Effect.Effect<Extract<Commit, { readonly type: "AgentMessageCommit" }>, Error>
  readonly appendFailureCommit: (
    sessionId: string,
    reason: string,
    inReplyTo: string,
    runId?: string,
    peerId?: string
  ) => Effect.Effect<Extract<Commit, { readonly type: "FailureCommit" }>, Error>
  readonly checkout: (
    sessionId: string,
    commitId: string | null,
    peerId?: string
  ) => Effect.Effect<void, Error>
  readonly history: (
    sessionId: string,
    peerId?: string
  ) => Effect.Effect<HistorySnapshot, PersistenceError>
  readonly listSessions: (
    workstreamId?: string,
    view?: SessionListView
  ) => Effect.Effect<ReadonlyArray<SessionSummary>, PersistenceError>
  readonly activityAfter: (position: number, limit?: number) => Effect.Effect<ReadonlyArray<HistoryItem>, PersistenceError>
  readonly checkpoint: (consumer: string) => Effect.Effect<number, PersistenceError>
  readonly saveCheckpoint: (consumer: string, position: number) => Effect.Effect<void, PersistenceError>
}
export class Service extends Context.Service<Service, Interface>()("@corredor/Session") {}

type RawEventType =
  | "SessionCreated"
  | "UserCommit"
  | "ToolCommit"
  | "AgentMessageCommit"
  | "FailureCommit"
  | "UserMessageAdded"
  | "AgentToolCallAdded"
  | "AgentMessageAdded"
  | "SessionTreeNavigated"
  | "SessionSettled"
  | "SessionReopened"

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
        occurredAt: row.occurred_at,
        ...(typeof payload.workstreamId === "string"
          ? { workstreamId: payload.workstreamId }
          : {})
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

    if (row.event_type === "SessionSettled" || row.event_type === "SessionReopened") {
      items.push({
        ...metadata,
        type: row.event_type,
        activityId: row.event_id,
        occurredAt: row.occurred_at
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
        content: String(payload.content ?? ""),
        ...(typeof payload.peerId === "string" ? { peerId: payload.peerId } : {}),
        ...(row.event_type === "UserMessageAdded" &&
          typeof payload.messageId === "string"
          ? { legacyMessageId: payload.messageId }
          : {})
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
        inReplyTo: String(payload.inReplyTo ?? parentId ?? ""),
        ...(typeof payload.runId === "string" ? { runId: payload.runId } : {}),
        ...(row.event_type === "AgentMessageAdded" &&
          typeof payload.messageId === "string"
          ? { legacyMessageId: payload.messageId }
          : {})
      }
      items.push(commit)
      if (parentId === currentHead) heads.set(row.session_id, commit.commitId)
      continue
    }

    if (row.event_type === "FailureCommit") {
      const commit: Commit = {
        ...metadata,
        type: "FailureCommit",
        commitId: row.event_id,
        parentId,
        createdAt,
        reason: String(payload.reason ?? "Agent run failed"),
        inReplyTo: String(payload.inReplyTo ?? ""),
        ...(typeof payload.runId === "string" ? { runId: payload.runId } : {})
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
        ...(typeof payload.runId === "string" ? { runId: payload.runId } : {}),
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
  const crypto = yield* Crypto.Crypto
  const persist = Effect.mapError(persistenceError)
  const randomId = crypto.randomUUIDv4.pipe(persist)
  yield* sql`PRAGMA foreign_keys = ON`.pipe(persist)
  yield* SqliteMigrator.run({ loader: SessionMigrations.loader }).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
    persist
  )

  const allRows = () =>
    sql<EventRow>`SELECT d.position, e.* FROM event_dispatch d JOIN session_events e ON e.event_id=d.event_id ORDER BY e.session_id, e.sequence`

  const sessionRows = (sessionId: string) =>
    sql<EventRow>`SELECT d.position, e.* FROM event_dispatch d JOIN session_events e ON e.event_id=d.event_id WHERE e.session_id=${sessionId} ORDER BY e.sequence`

  const sessionSummaryRows = (workstreamId?: string) => workstreamId === undefined
    ? sql<{
        readonly sessionId: string
        readonly workstreamId: string
        readonly createdAt: string
        readonly updatedAt: string
        readonly title: string
        readonly userCommitCount: number
        readonly settled: number
      }>`SELECT
        s.session_id AS sessionId,
        s.workstream_id AS workstreamId,
        s.created_at AS createdAt,
        s.updated_at AS updatedAt,
        s.title AS title,
        SUM(CASE WHEN e.event_type IN ('UserCommit','UserMessageAdded') THEN 1 ELSE 0 END)
          AS userCommitCount,
        CASE WHEN s.settled_at IS NULL THEN 0 ELSE 1 END AS settled
      FROM sessions s
      JOIN session_events e ON e.session_id=s.session_id
      GROUP BY s.session_id
      ORDER BY s.updated_at DESC`

    : sql<{
      readonly sessionId: string
      readonly workstreamId: string
        readonly createdAt: string
        readonly updatedAt: string
        readonly title: string
        readonly userCommitCount: number
        readonly settled: number
      }>`SELECT
        s.session_id AS sessionId,
        s.workstream_id AS workstreamId,
        s.created_at AS createdAt,
        s.updated_at AS updatedAt,
        s.title AS title,
        SUM(CASE WHEN e.event_type IN ('UserCommit','UserMessageAdded') THEN 1 ELSE 0 END)
          AS userCommitCount,
        CASE WHEN s.settled_at IS NULL THEN 0 ELSE 1 END AS settled
      FROM sessions s
      JOIN session_events e ON e.session_id=s.session_id
      WHERE s.workstream_id=${workstreamId}
      GROUP BY s.session_id
      ORDER BY s.updated_at DESC`

  const includeSession = (settled: number, view: SessionListView): boolean =>
    view === "settled" ? settled !== 0 : settled === 0

  // Derive the default Peer's local Branch Head once for legacy Sessions. No
  // legacy row is rewritten, and later Checkout operations update only local
  // Peer state.
  const existingRows = yield* allRows().pipe(persist)
  const sessionIds = new Set(existingRows.map((row) => row.session_id))
  for (const sessionId of sessionIds) {
    const history = decodeHistory(existingRows.filter((row) => row.session_id === sessionId))
    const migratedHead = legacyBranchHead(history)
    const currentHeads = yield* sql<{ readonly commitId: string | null }>`
      SELECT commit_id AS commitId FROM peer_branch_heads
      WHERE peer_id=${defaultPeerId} AND session_id=${sessionId}`.pipe(persist)
    yield* sql`INSERT OR IGNORE INTO peer_branch_heads (peer_id, session_id, commit_id)
      VALUES (${defaultPeerId}, ${sessionId}, ${migratedHead})`.pipe(persist)
    const currentHead = currentHeads[0]?.commitId
    const currentRecord = history.find(
      (item) => historyItemId(item) === currentHead
    )
    if (currentRecord?.type === "LegacyToolCall") {
      yield* sql`UPDATE peer_branch_heads SET commit_id=${migratedHead}
        WHERE peer_id=${defaultPeerId} AND session_id=${sessionId}`.pipe(persist)
    }
  }

  const insert = Effect.fn("Session.insert")(function*(
    sessionId: string,
    eventId: string,
    type: RawEventType,
    payload: Record<string, unknown>,
    sequence: number
  ) {
    const occurredAt = new Date(yield* Clock.currentTimeMillis).toISOString()
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
    const title = (type === "UserCommit" || type === "UserMessageAdded") &&
        typeof payload.content === "string"
      ? payload.content
      : undefined
    yield* sql`UPDATE sessions
      SET updated_at=${occurredAt},
          settled_at=CASE
            WHEN ${type}='SessionSettled' THEN ${occurredAt}
            WHEN ${type}='SessionReopened' THEN NULL
            ELSE settled_at
          END,
          title=CASE WHEN ${title ?? null} IS NOT NULL AND title='New session'
            THEN ${title ?? null} ELSE title END
      WHERE session_id=${sessionId}`
    yield* sql`UPDATE workstreams SET updated_at=${occurredAt}
      WHERE workstream_id=(SELECT workstream_id FROM sessions WHERE session_id=${sessionId})`
    return decodeHistory([row])[0]!
  })

  const ensurePeerHead = (sessionId: string, peerId: string) => sql`
    INSERT OR IGNORE INTO peer_branch_heads (peer_id, session_id, commit_id)
    VALUES (${peerId}, ${sessionId}, NULL)
  `

  const sessionState = (sessionId: string) => sql<{
    readonly settledAt: string | null
  }>`SELECT settled_at AS settledAt FROM sessions WHERE session_id=${sessionId}`

  const updateHeadIfCurrent = (
    sessionId: string,
    peerId: string,
    expected: string | null,
    next: string
  ) => sql`UPDATE peer_branch_heads SET commit_id=${next}
    WHERE peer_id=${peerId} AND session_id=${sessionId}
      AND (commit_id=${expected} OR (commit_id IS NULL AND ${expected} IS NULL))`

  const responseParent = (
    history: ReadonlyArray<HistoryItem>,
    inReplyTo: string,
    runId?: string
  ): string => {
    const toolEntries = history.filter(
      (item): item is Extract<
        BranchRecord,
        { readonly type: "ToolCommit" | "LegacyToolCall" }
      > =>
        (item.type === "ToolCommit" || item.type === "LegacyToolCall") &&
        item.inReplyTo === inReplyTo &&
        (item.type === "LegacyToolCall"
          ? runId === undefined
          : item.runId === runId)
    )
    return toolEntries.map(branchRecordId).at(-1) ?? inReplyTo
  }

  type ResponseCommitType = "AgentMessageCommit" | "FailureCommit"
  type ResponseCommitResult =
    | { readonly type: "NotFound" }
    | { readonly type: "Settled" }
    | { readonly type: "CommitNotFound"; readonly commitId: string }
    | { readonly type: "Appended"; readonly item: Commit }

  const appendResponseCommit = (
    sessionId: string,
    eventType: ResponseCommitType,
    inReplyTo: string,
    runId: string | undefined,
    peerId: string,
    payload: Record<string, unknown>,
    matchesExisting: (item: HistoryItem) => boolean
  ): Effect.Effect<ResponseCommitResult, PersistenceError> => Effect.gen(function*() {
    const rows = yield* sessionRows(sessionId)
    if (rows.length === 0) return { type: "NotFound" as const }
    const history = decodeHistory(rows)
    const existing = history.find(matchesExisting)
    if (existing !== undefined) {
      return { type: "Appended" as const, item: existing as Commit }
    }
    const state = yield* sessionState(sessionId)
    if (state[0]?.settledAt !== null && state[0]?.settledAt !== undefined) {
      return { type: "Settled" as const }
    }
    yield* ensurePeerHead(sessionId, peerId)
    if (!history.some(
      (item) => isBranchRecord(item) &&
        branchRecordId(item) === inReplyTo
    )) {
      return { type: "CommitNotFound" as const, commitId: inReplyTo }
    }
    const parentId = responseParent(history, inReplyTo, runId)
    const commitId = yield* randomId
    const item = yield* insert(
      sessionId,
      commitId,
      eventType,
      { ...payload, parentId, runId, peerId },
      rows.at(-1)!.sequence + 1
    )
    yield* updateHeadIfCurrent(sessionId, peerId, parentId, commitId)
    return { type: "Appended" as const, item: item as Commit }
  }).pipe(sql.withTransaction, persist)

  return Service.of({
    check: sql`SELECT 1`.pipe(Effect.asVoid, persist),
    createWorkstream: Effect.fn("Session.createWorkstream")(function*(
      workstreamId,
      requestedName = "New Workstream",
      requestedPeerId = defaultPeerId
    ) {
      const result = yield* Effect.gen(function*() {
        const existing = yield* sql<{ readonly workstreamId: string }>`
          SELECT workstream_id AS workstreamId FROM workstreams
          WHERE workstream_id=${workstreamId}`
        if (existing.length > 0) return { type: "AlreadyExists" as const }
        const now = new Date(yield* Clock.currentTimeMillis).toISOString()
        yield* sql`INSERT INTO workstreams (
          workstream_id, name, created_at, updated_at, peer_id
        ) VALUES (
          ${workstreamId}, ${requestedName}, ${now}, ${now}, ${requestedPeerId}
        )`
        return {
          type: "Created" as const,
          workstream: {
            workstreamId,
            name: requestedName,
            createdAt: now,
            updatedAt: now,
            peerId: requestedPeerId
          } satisfies Workstream
        }
      }).pipe(sql.withTransaction, persist)
      if (result.type === "AlreadyExists") {
        return yield* new WorkstreamAlreadyExists({ workstreamId })
      }
      return result.workstream
    }),
    listWorkstreams: Effect.fn("Session.listWorkstreams")(function*() {
      return yield* sql<{
        readonly workstreamId: string
        readonly name: string
        readonly createdAt: string
        readonly updatedAt: string
        readonly peerId: string
        readonly sessionCount: number
      }>`SELECT
        w.workstream_id AS workstreamId,
        w.name AS name,
        w.created_at AS createdAt,
        w.updated_at AS updatedAt,
        w.peer_id AS peerId,
        COUNT(CASE WHEN s.settled_at IS NULL THEN s.session_id END) AS sessionCount
      FROM workstreams w
      LEFT JOIN sessions s ON s.workstream_id=w.workstream_id
      GROUP BY w.workstream_id
      ORDER BY w.updated_at DESC`.pipe(persist)
    }),
    workstream: Effect.fn("Session.workstream")(function*(
      workstreamId,
      view: SessionListView = "active"
    ) {
      const workstreams = yield* sql<{
        readonly workstreamId: string
        readonly name: string
        readonly createdAt: string
        readonly updatedAt: string
        readonly peerId: string
        readonly sessionCount: number
      }>`SELECT
        w.workstream_id AS workstreamId,
        w.name AS name,
        w.created_at AS createdAt,
        w.updated_at AS updatedAt,
        w.peer_id AS peerId,
        COUNT(CASE WHEN s.settled_at IS NULL THEN s.session_id END) AS sessionCount
      FROM workstreams w
      LEFT JOIN sessions s ON s.workstream_id=w.workstream_id
      WHERE w.workstream_id=${workstreamId}
      GROUP BY w.workstream_id`.pipe(persist)
      const summary = workstreams[0]
      if (summary === undefined) {
        return yield* new WorkstreamNotFound({ workstreamId })
      }
      const sessions = yield* sessionSummaryRows(workstreamId).pipe(
        Effect.map((rows) => rows
          .filter((row) => includeSession(row.settled, view))
          .map((row) => ({
            ...row,
            title: row.title || "New session",
            settled: row.settled !== 0
          }))),
        persist
      )
      return {
        workstream: { ...summary, sessionCount: sessions.length },
        sessions
      }
    }),
    createSession: Effect.fn("Session.createSession")(function*(
      sessionId,
      requestedWorkstreamId = defaultWorkstreamId,
      requestedPeerId = defaultPeerId
    ) {
      const result = yield* Effect.gen(function*() {
        const rows = yield* sessionRows(sessionId)
        if (rows.length > 0) return { type: "AlreadyExists" as const }
        const workstreamRows = yield* sql<{
          readonly workstreamId: string
        }>`SELECT workstream_id AS workstreamId FROM workstreams
          WHERE workstream_id=${requestedWorkstreamId}`
        const now = new Date(yield* Clock.currentTimeMillis).toISOString()
        if (workstreamRows.length === 0 && requestedWorkstreamId === defaultWorkstreamId) {
          yield* sql`INSERT INTO workstreams (
            workstream_id, name, created_at, updated_at, peer_id
          ) VALUES (
            ${defaultWorkstreamId}, ${defaultWorkstreamName},
            ${now}, ${now}, ${requestedPeerId}
          )`
        } else if (workstreamRows.length === 0) {
          return { type: "WorkstreamNotFound" as const }
        }
        yield* sql`INSERT INTO sessions (
          session_id, workstream_id, title, created_at, updated_at, peer_id
        ) VALUES (
          ${sessionId}, ${requestedWorkstreamId}, 'New session',
          ${now}, ${now}, ${requestedPeerId}
        )`
        const item = yield* insert(
          sessionId,
          yield* randomId,
          "SessionCreated",
          { workstreamId: requestedWorkstreamId },
          1
        )
        yield* sql`INSERT INTO peer_branch_heads (peer_id, session_id, commit_id)
          VALUES (${requestedPeerId}, ${sessionId}, NULL)`
        return { type: "Created" as const, item: item as SessionCreated }
      }).pipe(sql.withTransaction, persist)
      if (result.type === "AlreadyExists") return yield* new AlreadyExists({ sessionId })
      if (result.type === "WorkstreamNotFound") {
        return yield* new WorkstreamNotFound({
          workstreamId: requestedWorkstreamId
        })
      }
      return result.item
    }),
    settle: Effect.fn("Session.settle")(function*(sessionId) {
      const result = yield* Effect.gen(function*() {
        const rows = yield* sessionRows(sessionId)
        if (rows.length === 0) return { type: "NotFound" as const }
        const state = yield* sessionState(sessionId)
        if (state[0]?.settledAt !== null && state[0]?.settledAt !== undefined) {
          return { type: "AlreadySettled" as const }
        }
        const item = yield* insert(
          sessionId,
          yield* randomId,
          "SessionSettled",
          {},
          rows.at(-1)!.sequence + 1
        )
        return { type: "Settled" as const, item: item as SessionSettled }
      }).pipe(sql.withTransaction, persist)
      if (result.type === "NotFound") return yield* new NotFound({ sessionId })
      if (result.type === "AlreadySettled") {
        return yield* new AlreadySettled({ sessionId })
      }
      return result.item
    }),
    reopen: Effect.fn("Session.reopen")(function*(sessionId) {
      const result = yield* Effect.gen(function*() {
        const rows = yield* sessionRows(sessionId)
        if (rows.length === 0) return { type: "NotFound" as const }
        const state = yield* sessionState(sessionId)
        if (state[0]?.settledAt === null || state[0]?.settledAt === undefined) {
          return { type: "NotSettled" as const }
        }
        const item = yield* insert(
          sessionId,
          yield* randomId,
          "SessionReopened",
          {},
          rows.at(-1)!.sequence + 1
        )
        return { type: "Reopened" as const, item: item as SessionReopened }
      }).pipe(sql.withTransaction, persist)
      if (result.type === "NotFound") return yield* new NotFound({ sessionId })
      if (result.type === "NotSettled") return yield* new NotSettled({ sessionId })
      return result.item
    }),
    appendUserCommit: Effect.fn("Session.appendUserCommit")(
      function*(sessionId, content, commitId, requestedPeerId = defaultPeerId) {
        const result = yield* Effect.gen(function*() {
          const rows = yield* sessionRows(sessionId)
          if (rows.length === 0) return { type: "NotFound" as const }
          const state = yield* sessionState(sessionId)
          if (state[0]?.settledAt !== null && state[0]?.settledAt !== undefined) {
            return { type: "Settled" as const }
          }
          yield* ensurePeerHead(sessionId, requestedPeerId)
          const heads = yield* sql<{ readonly commitId: string | null }>`
            SELECT commit_id AS commitId FROM peer_branch_heads
            WHERE peer_id=${requestedPeerId} AND session_id=${sessionId}`
          const parentId = heads[0]?.commitId ?? null
          const item = yield* insert(
            sessionId,
            commitId,
            "UserCommit",
            { content, parentId, peerId: requestedPeerId },
            rows.at(-1)!.sequence + 1
          )
          yield* updateHeadIfCurrent(sessionId, requestedPeerId, parentId, commitId)
          return {
            type: "Appended" as const,
            item: item as Extract<Commit, { type: "UserCommit" }>
          }
        }).pipe(sql.withTransaction, persist)
        if (result.type === "NotFound") {
          return yield* new NotFound({ sessionId })
        }
        if (result.type === "Settled") {
          return yield* new Settled({ sessionId })
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
      index,
      runId,
      requestedPeerId = defaultPeerId
    ) {
      const result = yield* Effect.gen(function*() {
        const rows = yield* sessionRows(sessionId)
        if (rows.length === 0) return { type: "NotFound" as const }
        const history = decodeHistory(rows)
        const existing = history.find(
          (item): item is Extract<Commit, { readonly type: "ToolCommit" }> =>
            item.type === "ToolCommit" &&
            item.inReplyTo === inReplyTo &&
            item.runId === runId &&
            item.index === index
        )
        if (existing !== undefined) {
          const sameInteraction =
            existing.toolCallId === toolCallId &&
            existing.name === name &&
            isDeepStrictEqual(existing.input, input) &&
            isDeepStrictEqual(existing.outcome, outcome)
          return sameInteraction
            ? { type: "Appended" as const, item: existing }
            : { type: "ToolCommitConflict" as const }
        }
        const state = yield* sessionState(sessionId)
        if (state[0]?.settledAt !== null && state[0]?.settledAt !== undefined) {
          return { type: "Settled" as const }
        }
        yield* ensurePeerHead(sessionId, requestedPeerId)
        if (!history.some(
          (item) => isBranchRecord(item) &&
            branchRecordId(item) === inReplyTo
        )) {
          return { type: "CommitNotFound" as const, commitId: inReplyTo }
        }
        const parentId = responseParent(history, inReplyTo, runId)
        const commitId = yield* randomId
        const item = yield* insert(
          sessionId,
          commitId,
          "ToolCommit",
          { toolCallId, name, input, outcome, inReplyTo, index, parentId, runId },
          rows.at(-1)!.sequence + 1
        )
        yield* updateHeadIfCurrent(
          sessionId,
          requestedPeerId,
          parentId,
          commitId
        )
        return { type: "Appended" as const, item: item as Extract<Commit, { type: "ToolCommit" }> }
      }).pipe(sql.withTransaction, persist)
      if (result.type === "NotFound") return yield* new NotFound({ sessionId })
      if (result.type === "Settled") return yield* new Settled({ sessionId })
      if (result.type === "CommitNotFound") {
        return yield* new CommitNotFound({ sessionId, commitId: result.commitId })
      }
      if (result.type === "ToolCommitConflict") {
        return yield* new ToolCommitConflict({ sessionId, inReplyTo, index })
      }
      return result.item as Extract<Commit, { readonly type: "ToolCommit" }>
    }),
    appendAgentMessageCommit: Effect.fn("Session.appendAgentMessageCommit")(
      function*(sessionId, content, inReplyTo, runId, requestedPeerId = defaultPeerId) {
        const result = yield* appendResponseCommit(
          sessionId,
          "AgentMessageCommit",
          inReplyTo,
          runId,
          requestedPeerId,
          { content, inReplyTo },
          (item) => item.type === "AgentMessageCommit" &&
            item.inReplyTo === inReplyTo &&
            item.runId === runId
        )
        if (result.type === "NotFound") {
          return yield* new NotFound({ sessionId })
        }
        if (result.type === "Settled") {
          return yield* new Settled({ sessionId })
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
    appendFailureCommit: Effect.fn("Session.appendFailureCommit")(
      function*(sessionId, reason, inReplyTo, runId, requestedPeerId = defaultPeerId) {
        const result = yield* appendResponseCommit(
          sessionId,
          "FailureCommit",
          inReplyTo,
          runId,
          requestedPeerId,
          { reason, inReplyTo },
          (item) => item.type === "FailureCommit" &&
            item.inReplyTo === inReplyTo &&
            item.runId === runId
        )
        if (result.type === "NotFound") {
          return yield* new NotFound({ sessionId })
        }
        if (result.type === "Settled") {
          return yield* new Settled({ sessionId })
        }
        if (result.type === "CommitNotFound") {
          return yield* new CommitNotFound({
            sessionId,
            commitId: result.commitId
          })
        }
        return result.item as Extract<
          Commit,
          { readonly type: "FailureCommit" }
        >
      }
    ),
    checkout: Effect.fn("Session.checkout")(function*(
      sessionId,
      commitId,
      requestedPeerId = defaultPeerId
    ) {
      const rows = yield* sessionRows(sessionId).pipe(persist)
      if (rows.length === 0) return yield* new NotFound({ sessionId })
      const state = yield* sessionState(sessionId).pipe(persist)
      if (state[0]?.settledAt !== null && state[0]?.settledAt !== undefined) {
        return yield* new Settled({ sessionId })
      }
      const history = decodeHistory(rows)
      if (commitId !== null && !history.some(
        (item) => isCommit(item) && item.commitId === commitId
      )) {
        return yield* new CommitNotFound({ sessionId, commitId })
      }
      yield* ensurePeerHead(sessionId, requestedPeerId).pipe(persist)
      yield* sql`UPDATE peer_branch_heads SET commit_id=${commitId}
        WHERE peer_id=${requestedPeerId} AND session_id=${sessionId}`.pipe(
        Effect.asVoid,
        persist
      )
    }),
    history: Effect.fn("Session.history")(function*(
      sessionId,
      requestedPeerId = defaultPeerId
    ) {
      const rows = yield* sessionRows(sessionId).pipe(persist)
      yield* ensurePeerHead(sessionId, requestedPeerId).pipe(persist)
      const heads = yield* sql<{ readonly branchHeadId: string | null }>`
        SELECT commit_id AS branchHeadId
        FROM peer_branch_heads
        WHERE peer_id=${requestedPeerId} AND session_id=${sessionId}`.pipe(persist)
      const states = yield* sessionState(sessionId).pipe(persist)
      return {
        items: decodeHistory(rows),
        branchHeadId: heads[0]?.branchHeadId ?? null,
        settled: states[0]?.settledAt !== null && states[0]?.settledAt !== undefined
      }
    }),
    listSessions: Effect.fn("Session.listSessions")(function*(
      workstreamId,
      view: SessionListView = "active"
    ) {
      return yield* sessionSummaryRows(workstreamId).pipe(
        Effect.map((rows) => rows
          .filter((row) => includeSession(row.settled, view))
          .map((row) => ({
            ...row,
            title: row.title || "New session",
            settled: row.settled !== 0
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
    )
  })
}).pipe(Effect.provide(Reactivity.layer))

export const layerWithoutDependencies = (path = defaultDatabasePath) =>
  Layer.effect(Service, make(path))

export const layer = (path = defaultDatabasePath) =>
  layerWithoutDependencies(path).pipe(
    Layer.provide(BunCrypto.layer)
  )
