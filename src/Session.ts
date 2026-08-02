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

/** Identifies a Commit in another Session that gives context its provenance. */
export interface CommitProvenance {
  readonly workstreamId: string
  readonly sessionId: string
  readonly commitId: string
}

/** Provenance recorded when a Workflow creates a focused child Session. */
export type SessionOrigin = CommitProvenance

export type Commit =
  | CommitMetadata & {
    readonly type: "UserCommit"
    readonly content: string
    readonly peerId?: string
    readonly legacyMessageId?: string
    /** Receiver-local marker preventing synchronized input from auto-running. */
    readonly imported?: boolean
    /** Workflow root context is explicitly started and must not auto-run. */
    readonly autoRun?: boolean
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
    readonly provenance?: CommitProvenance
  }
  | CommitMetadata & {
    readonly type: "CompactionCommit"
    readonly content: string
    readonly inReplyTo: string
    readonly runId?: string
  }
  | CommitMetadata & {
    readonly type: "FailureCommit"
    readonly reason: string
    readonly inReplyTo: string
    readonly runId?: string
  }
  | CommitMetadata & {
    readonly type: "InterruptCommit"
    readonly reason: string
    readonly partialOutput: string
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
  readonly origin?: SessionOrigin
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
type CompactionCommit = Extract<Commit, { readonly type: "CompactionCommit" }>
type FailureCommit = Extract<Commit, { readonly type: "FailureCommit" }>
type InterruptCommit = Extract<Commit, { readonly type: "InterruptCommit" }>
type ToolCommit = Extract<Commit, { readonly type: "ToolCommit" }>

export interface BranchRecordHandlers<A> {
  readonly user: (record: UserCommit) => A
  readonly agentMessage: (record: AgentMessageCommit) => A
  readonly compaction: (record: CompactionCommit) => A
  readonly failure: (record: FailureCommit) => A
  readonly interrupt: (record: InterruptCommit) => A
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
  if (record.type === "CompactionCommit") return handlers.compaction(record)
  if (record.type === "FailureCommit") return handlers.failure(record)
  if (record.type === "InterruptCommit") return handlers.interrupt(record)
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

const CommitProvenanceSchema = Schema.Struct({
  workstreamId: Schema.String,
  sessionId: Schema.String,
  commitId: Schema.String
})

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
  legacyMessageId: Schema.optional(Schema.String),
  imported: Schema.optional(Schema.Boolean),
  autoRun: Schema.optional(Schema.Boolean)
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
  legacyMessageId: Schema.optional(Schema.String),
  provenance: Schema.optional(CommitProvenanceSchema)
})

const CompactionCommitSchema = Schema.Struct({
  ...commitMetadata,
  type: Schema.Literal("CompactionCommit"),
  content: Schema.String,
  inReplyTo: Schema.String,
  runId: Schema.optional(Schema.String)
})

const FailureCommitSchema = Schema.Struct({
  ...commitMetadata,
  type: Schema.Literal("FailureCommit"),
  reason: Schema.String,
  inReplyTo: Schema.String,
  runId: Schema.optional(Schema.String)
})

const InterruptCommitSchema = Schema.Struct({
  ...commitMetadata,
  type: Schema.Literal("InterruptCommit"),
  reason: Schema.String,
  partialOutput: Schema.String,
  inReplyTo: Schema.String,
  runId: Schema.optional(Schema.String)
})

export const SessionCreatedSchema = Schema.Struct({
  ...historyMetadata,
  type: Schema.Literal("SessionCreated"),
  activityId: Schema.String,
  occurredAt: Schema.String,
  workstreamId: Schema.optional(Schema.String),
  origin: Schema.optional(CommitProvenanceSchema)
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

export const LegacyToolCallSchema = Schema.Struct({
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
})

/** Runtime decoder shared by the HTTP client and SSE transport. */
export const HistoryItemSchema = Schema.Union([
  UserCommitSchema,
  ToolCommitSchema,
  AgentMessageCommitSchema,
  CompactionCommitSchema,
  FailureCommitSchema,
  InterruptCommitSchema,
  LegacyToolCallSchema,
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
  CompactionCommitSchema,
  FailureCommitSchema,
  InterruptCommitSchema
])

export const BranchRecordSchema = Schema.Union([
  CommitSchema,
  LegacyToolCallSchema
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
export class BranchHeadConflict extends Schema.TaggedErrorClass<BranchHeadConflict>()(
  "@corredor/Session/BranchHeadConflict",
  {
    sessionId: Schema.String,
    expectedHeadId: Schema.NullOr(Schema.String),
    actualHeadId: Schema.NullOr(Schema.String),
    message: Schema.String
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
export class SyncValidationError extends Schema.TaggedErrorClass<SyncValidationError>()(
  "@corredor/Session/SyncValidationError",
  { message: Schema.String }
) {}
export class SyncConflict extends Schema.TaggedErrorClass<SyncConflict>()(
  "@corredor/Session/SyncConflict",
  {
    sessionId: Schema.String,
    recordId: Schema.String,
    message: Schema.String
  }
) {}
export type Error =
  | PersistenceError
  | AlreadyExists
  | WorkstreamAlreadyExists
  | WorkstreamNotFound
  | NotFound
  | CommitNotFound
  | ToolCommitConflict
  | BranchHeadConflict
  | Settled
  | AlreadySettled
  | NotSettled
  | SyncValidationError
  | SyncConflict

export type SessionListView = "active" | "settled"

export interface UserCommitOptions {
  /** Prevents the server-owned Agent reactor from starting this root commit. */
  readonly autoRun?: boolean
}

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

/** Session metadata carried with a Branch during Peer synchronization. */
export interface SessionTransferMetadata {
  readonly sessionId: string
  readonly workstreamId: string
  readonly title: string
  readonly createdAt: string
  readonly updatedAt: string
  readonly peerId: string
  readonly createdEventId: string
  readonly settledAt: string | null
  readonly settledEventId: string | null
  readonly origin?: SessionOrigin
}

/** The immutable context closure exchanged between two Peers. */
export interface SyncBundle {
  readonly version: 1
  readonly sourcePeerId: string
  readonly workstream: Workstream
  readonly session: SessionTransferMetadata
  readonly branchHeadId: string | null
  readonly records: ReadonlyArray<BranchRecord>
}

export interface SyncResult {
  readonly sessionId: string
  readonly workstreamId: string
  readonly branchHeadId: string | null
  readonly importedRecordIds: ReadonlyArray<string>
  readonly existingRecordIds: ReadonlyArray<string>
  readonly importedCommitIds: ReadonlyArray<string>
  readonly existingCommitIds: ReadonlyArray<string>
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

export const SessionTransferMetadataSchema = Schema.Struct({
  sessionId: Schema.String,
  workstreamId: Schema.String,
  title: Schema.String,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  peerId: Schema.String,
  createdEventId: Schema.String,
  settledAt: Schema.NullOr(Schema.String),
  settledEventId: Schema.NullOr(Schema.String),
  origin: Schema.optional(CommitProvenanceSchema)
})

export const SyncBundleSchema = Schema.Struct({
  version: Schema.Literal(1),
  sourcePeerId: Schema.String,
  workstream: WorkstreamSchema,
  session: SessionTransferMetadataSchema,
  branchHeadId: Schema.NullOr(Schema.String),
  records: Schema.Array(BranchRecordSchema)
})

export const SyncResultSchema = Schema.Struct({
  sessionId: Schema.String,
  workstreamId: Schema.String,
  branchHeadId: Schema.NullOr(Schema.String),
  importedRecordIds: Schema.Array(Schema.String),
  existingRecordIds: Schema.Array(Schema.String),
  importedCommitIds: Schema.Array(Schema.String),
  existingCommitIds: Schema.Array(Schema.String)
})

export const isCommit = (item: HistoryItem): item is Commit =>
  item.type === "UserCommit" ||
  item.type === "ToolCommit" ||
  item.type === "AgentMessageCommit" ||
  item.type === "CompactionCommit" ||
  item.type === "FailureCommit" ||
  item.type === "InterruptCommit"

export const isBranchRecord = (item: HistoryItem): item is BranchRecord =>
  isCommit(item) || item.type === "LegacyToolCall"

export const branchRecordId = (record: BranchRecord): string =>
  record.type === "LegacyToolCall" ? record.legacyId : record.commitId

const commitIdsFor = (
  records: ReadonlyArray<BranchRecord>,
  recordIds: ReadonlyArray<string>
): ReadonlyArray<string> => {
  const selectedIds = new Set(recordIds)
  return records
    .filter((record) => selectedIds.has(branchRecordId(record)))
    .filter(isCommit)
    .map((record) => record.commitId)
}

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
    peerId?: string,
    origin?: SessionOrigin
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
    peerId?: string,
    options?: UserCommitOptions
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
  readonly appendCompactionCommit: (
    sessionId: string,
    content: string,
    inReplyTo: string,
    runId?: string,
    peerId?: string,
    expectedHeadId?: string | null
  ) => Effect.Effect<Extract<Commit, { readonly type: "CompactionCommit" }>, Error>
  readonly appendFailureCommit: (
    sessionId: string,
    reason: string,
    inReplyTo: string,
    runId?: string,
    peerId?: string
  ) => Effect.Effect<Extract<Commit, { readonly type: "FailureCommit" }>, Error>
  readonly appendInterruptCommit: (
    sessionId: string,
    reason: string,
    partialOutput: string,
    inReplyTo: string,
    runId?: string,
    peerId?: string
  ) => Effect.Effect<Extract<Commit, { readonly type: "InterruptCommit" }>, Error>
  readonly exportBranch: (
    sessionId: string,
    headId?: string | null,
    peerId?: string
  ) => Effect.Effect<SyncBundle, Error>
  readonly importBranch: (
    bundle: SyncBundle,
    peerId?: string
  ) => Effect.Effect<SyncResult, Error>
  readonly cherryPick: (
    /** The canonical order is source Session, source Commit, target Session. */
    sourceSessionId: string,
    sourceCommitId: string,
    targetSessionId: string,
    targetPeerId?: string,
    expectedTargetHeadId?: string | null
  ) => Effect.Effect<Extract<Commit, { readonly type: "AgentMessageCommit" }>, Error>
  readonly checkout: (
    sessionId: string,
    commitId: string | null,
    peerId?: string
  ) => Effect.Effect<void, Error>
  readonly history: (
    sessionId: string,
    peerId?: string
  ) => Effect.Effect<HistorySnapshot, PersistenceError | NotFound>
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
  | "CompactionCommit"
  | "FailureCommit"
  | "InterruptCommit"
  | "UserMessageAdded"
  | "AgentToolCallAdded"
  | "AgentMessageAdded"
  | "CherryPickedAgentMessage"
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

const preserveSessionError = <A, E>(
  effect: Effect.Effect<A, E>
): Effect.Effect<A, PersistenceError | Error> =>
  effect.pipe(Effect.mapError((cause) => {
    if (cause !== null && typeof cause === "object" && "_tag" in cause) {
      const tag = (cause as { readonly _tag?: unknown })._tag
      if (typeof tag === "string" && tag.startsWith("@corredor/Session/")) {
        return cause as unknown as Error
      }
    }
    return persistenceError(cause)
  }))

const decodeCommitProvenance = (value: unknown): CommitProvenance | undefined => {
  if (value === null || typeof value !== "object") return undefined
  const candidate = value as Record<string, unknown>
  return typeof candidate.workstreamId === "string" &&
      typeof candidate.sessionId === "string" &&
      typeof candidate.commitId === "string"
    ? {
      workstreamId: candidate.workstreamId,
      sessionId: candidate.sessionId,
      commitId: candidate.commitId
    }
    : undefined
}

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
      const origin = decodeCommitProvenance(payload.origin)
      items.push({
        ...metadata,
        type: "SessionCreated",
        activityId: row.event_id,
        occurredAt: row.occurred_at,
        ...(typeof payload.workstreamId === "string"
          ? { workstreamId: payload.workstreamId }
          : {}),
        ...(origin === undefined ? {} : { origin })
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
        ...(payload.imported === true ? { imported: true } : {}),
        ...(payload.autoRun === false ? { autoRun: false } : {}),
        ...(row.event_type === "UserMessageAdded" &&
          typeof payload.messageId === "string"
          ? { legacyMessageId: payload.messageId }
          : {})
      }
      items.push(commit)
      if (parentId === currentHead) heads.set(row.session_id, commit.commitId)
      continue
    }

    if (
      row.event_type === "AgentMessageCommit" ||
      row.event_type === "AgentMessageAdded" ||
      row.event_type === "CherryPickedAgentMessage"
    ) {
      const provenance = decodeCommitProvenance(payload.provenance)
      const commit: Commit = {
        ...metadata,
        type: "AgentMessageCommit",
        commitId: row.event_id,
        parentId,
        createdAt,
        content: String(payload.content ?? ""),
        inReplyTo: String(payload.inReplyTo ?? parentId ?? ""),
        ...(typeof payload.runId === "string" ? { runId: payload.runId } : {}),
        ...(provenance === undefined ? {} : { provenance }),
        ...(row.event_type === "AgentMessageAdded" &&
          typeof payload.messageId === "string"
          ? { legacyMessageId: payload.messageId }
          : {})
      }
      items.push(commit)
      if (parentId === currentHead) heads.set(row.session_id, commit.commitId)
      continue
    }

    if (row.event_type === "CompactionCommit") {
      const commit: Commit = {
        ...metadata,
        type: "CompactionCommit",
        commitId: row.event_id,
        parentId,
        createdAt,
        content: String(payload.content ?? ""),
        inReplyTo: String(payload.inReplyTo ?? parentId ?? ""),
        ...(typeof payload.runId === "string" ? { runId: payload.runId } : {})
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

    if (row.event_type === "InterruptCommit") {
      const commit: Commit = {
        ...metadata,
        type: "InterruptCommit",
        commitId: row.event_id,
        parentId,
        createdAt,
        reason: String(payload.reason ?? "Agent run interrupted"),
        partialOutput: String(payload.partialOutput ?? ""),
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

const syncRecordPayload = (
  record: BranchRecord
): { readonly type: RawEventType; readonly payload: Record<string, unknown> } => {
  // `imported` is local runtime state, never part of the synchronized identity.
  if (record.type === "LegacyToolCall") {
    return {
      type: "AgentToolCallAdded",
      payload: {
        toolCallId: record.toolCallId,
        name: record.name,
        input: record.input,
        inReplyTo: record.inReplyTo,
        index: record.index,
        parentId: record.parentId
      }
    }
  }

  if (record.type === "UserCommit") {
    return {
      type: record.legacyMessageId === undefined
        ? "UserCommit"
        : "UserMessageAdded",
      payload: {
        content: record.content,
        parentId: record.parentId,
        ...(record.peerId === undefined ? {} : { peerId: record.peerId }),
        ...(record.autoRun === undefined ? {} : { autoRun: record.autoRun }),
        ...(record.legacyMessageId === undefined
          ? {}
          : { messageId: record.legacyMessageId })
      }
    }
  }

  if (record.type === "ToolCommit") {
    return {
      type: "ToolCommit",
      payload: {
        toolCallId: record.toolCallId,
        name: record.name,
        input: record.input,
        outcome: record.outcome,
        inReplyTo: record.inReplyTo,
        index: record.index,
        parentId: record.parentId,
        ...(record.runId === undefined ? {} : { runId: record.runId })
      }
    }
  }

  if (record.type === "AgentMessageCommit") {
    return {
      type: record.legacyMessageId === undefined
        ? "AgentMessageCommit"
        : "AgentMessageAdded",
      payload: {
        content: record.content,
        inReplyTo: record.inReplyTo,
        parentId: record.parentId,
        ...(record.runId === undefined ? {} : { runId: record.runId }),
        ...(record.legacyMessageId === undefined
          ? {}
          : { messageId: record.legacyMessageId }),
        ...(record.provenance === undefined
          ? {}
          : { provenance: record.provenance })
      }
    }
  }

  if (record.type === "CompactionCommit") {
    return {
      type: "CompactionCommit",
      payload: {
        content: record.content,
        inReplyTo: record.inReplyTo,
        parentId: record.parentId,
        ...(record.runId === undefined ? {} : { runId: record.runId })
      }
    }
  }

  if (record.type === "FailureCommit") {
    return {
      type: "FailureCommit",
      payload: {
        reason: record.reason,
        inReplyTo: record.inReplyTo,
        parentId: record.parentId,
        ...(record.runId === undefined ? {} : { runId: record.runId })
      }
    }
  }

  return {
    type: "InterruptCommit",
    payload: {
      reason: record.reason,
      partialOutput: record.partialOutput,
      inReplyTo: record.inReplyTo,
      parentId: record.parentId,
      ...(record.runId === undefined ? {} : { runId: record.runId })
    }
  }
}

const isBranchEventType = (type: RawEventType): boolean =>
  type === "UserCommit" ||
  type === "ToolCommit" ||
  type === "AgentMessageCommit" ||
  type === "AgentMessageAdded" ||
  type === "CherryPickedAgentMessage" ||
  type === "CompactionCommit" ||
  type === "FailureCommit" ||
  type === "InterruptCommit" ||
  type === "UserMessageAdded" ||
  type === "AgentToolCallAdded"

const sameExpectedPayload = (
  actual: Record<string, unknown>,
  expected: Record<string, unknown>
): boolean => Object.entries(expected).every(([key, value]) =>
  Object.prototype.hasOwnProperty.call(actual, key) &&
  isDeepStrictEqual(actual[key], value)
)

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

  const sessionMetadata = (sessionId: string) => sql<{
    readonly sessionId: string
    readonly workstreamId: string
    readonly title: string
    readonly createdAt: string
    readonly updatedAt: string
    readonly peerId: string
    readonly settledAt: string | null
  }>`SELECT
    session_id AS sessionId,
    workstream_id AS workstreamId,
    title,
    created_at AS createdAt,
    updated_at AS updatedAt,
    peer_id AS peerId,
    settled_at AS settledAt
  FROM sessions
  WHERE session_id=${sessionId}`

  const workstreamMetadata = (workstreamId: string) => sql<{
    readonly workstreamId: string
    readonly name: string
    readonly createdAt: string
    readonly updatedAt: string
    readonly peerId: string
  }>`SELECT
    workstream_id AS workstreamId,
    name,
    created_at AS createdAt,
    updated_at AS updatedAt,
    peer_id AS peerId
  FROM workstreams
  WHERE workstream_id=${workstreamId}`

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

  const insertRawEvent = (
    sessionId: string,
    eventId: string,
    type: RawEventType,
    payload: Record<string, unknown>,
    sequence: number,
    occurredAt: string
  ) => Effect.gen(function*() {
    yield* sql`INSERT INTO session_events (
      event_id, session_id, sequence, event_type, payload, occurred_at
    ) VALUES (
      ${eventId}, ${sessionId}, ${sequence}, ${type},
      ${JSON.stringify(payload)}, ${occurredAt}
    )`
    yield* sql`INSERT INTO event_dispatch (event_id) VALUES (${eventId})`
  })

  const ensurePeerHead = (sessionId: string, peerId: string) => sql`
    INSERT OR IGNORE INTO peer_branch_heads (peer_id, session_id, commit_id)
    VALUES (${peerId}, ${sessionId}, NULL)
  `

  const sessionState = (sessionId: string) => sql<{
    readonly settledAt: string | null
  }>`SELECT settled_at AS settledAt FROM sessions WHERE session_id=${sessionId}`

  type LifecycleTransitionResult =
    | { readonly type: "NotFound" }
    | { readonly type: "AlreadySettled" }
    | { readonly type: "NotSettled" }
    | { readonly type: "Settled"; readonly item: SessionSettled }
    | { readonly type: "Reopened"; readonly item: SessionReopened }

  const transitionSession = (
    sessionId: string,
    eventType: "SessionSettled" | "SessionReopened"
  ): Effect.Effect<LifecycleTransitionResult, PersistenceError> => Effect.gen(function*() {
    const rows = yield* sessionRows(sessionId)
    if (rows.length === 0) return { type: "NotFound" as const }
    const state = yield* sessionState(sessionId)
    const settled = state[0]?.settledAt !== null && state[0]?.settledAt !== undefined
    if (eventType === "SessionSettled" && settled) {
      return { type: "AlreadySettled" as const }
    }
    if (eventType === "SessionReopened" && !settled) {
      return { type: "NotSettled" as const }
    }
    const item = yield* insert(
      sessionId,
      yield* randomId,
      eventType,
      {},
      rows.at(-1)!.sequence + 1
    )
    return eventType === "SessionSettled"
      ? { type: "Settled" as const, item: item as SessionSettled }
      : { type: "Reopened" as const, item: item as SessionReopened }
  }).pipe(sql.withTransaction, persist)

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

  type ResponseCommitType =
    | "AgentMessageCommit"
    | "FailureCommit"
    | "InterruptCommit"
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
    const state = yield* sessionState(sessionId)
    if (state[0]?.settledAt !== null && state[0]?.settledAt !== undefined) {
      return { type: "Settled" as const }
    }
    const history = decodeHistory(rows)
    const existing = history.find(matchesExisting)
    if (existing !== undefined) {
      return { type: "Appended" as const, item: existing as Commit }
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

  const appendCompactionCommit = (
    sessionId: string,
    content: string,
    inReplyTo: string,
    runId: string | undefined,
    peerId: string,
    expectedHeadId?: string | null
  ): Effect.Effect<
    Extract<Commit, { readonly type: "CompactionCommit" }>,
    PersistenceError | Error
  > => Effect.gen(function*() {
    const rows = yield* sessionRows(sessionId)
    if (rows.length === 0) return yield* new NotFound({ sessionId })
    const state = yield* sessionState(sessionId)
    if (state[0]?.settledAt !== null && state[0]?.settledAt !== undefined) {
      return yield* new Settled({ sessionId })
    }
    const history = decodeHistory(rows)
    yield* ensurePeerHead(sessionId, peerId)
    if (expectedHeadId !== undefined) {
      const heads = yield* sql<{ readonly commitId: string | null }>`
        SELECT commit_id AS commitId
        FROM peer_branch_heads
        WHERE peer_id=${peerId} AND session_id=${sessionId}`
      const actualHeadId = heads[0]?.commitId ?? null
      if (actualHeadId !== expectedHeadId) {
        return yield* new BranchHeadConflict({
          sessionId,
          expectedHeadId,
          actualHeadId,
          message: "Source Branch Head changed during Compaction"
        })
      }
    }
    const existing = history.find(
      (item): item is Extract<Commit, { readonly type: "CompactionCommit" }> =>
        item.type === "CompactionCommit" &&
        item.inReplyTo === inReplyTo &&
        item.runId === runId
    )
    if (existing !== undefined) return existing
    if (!history.some(
      (item) => isBranchRecord(item) && branchRecordId(item) === inReplyTo
    )) {
      return yield* new CommitNotFound({ sessionId, commitId: inReplyTo })
    }
    const commitId = yield* randomId
    const item = yield* insert(
      sessionId,
      commitId,
      "CompactionCommit",
      { content, inReplyTo, parentId: inReplyTo, runId, peerId },
      rows.at(-1)!.sequence + 1
    )
    yield* updateHeadIfCurrent(sessionId, peerId, inReplyTo, commitId)
    return item as Extract<Commit, { readonly type: "CompactionCommit" }>
  }).pipe(sql.withTransaction, preserveSessionError)

  const exportBranch = (
    sessionId: string,
    requestedHeadId: string | null | undefined,
    requestedPeerId: string
  ): Effect.Effect<SyncBundle, Error> => Effect.gen(function*() {
    const rows = yield* sessionRows(sessionId)
    if (rows.length === 0) return yield* new NotFound({ sessionId })
    const session = (yield* sessionMetadata(sessionId))[0]
    if (session === undefined) return yield* new NotFound({ sessionId })
    const workstream = (yield* workstreamMetadata(session.workstreamId))[0]
    if (workstream === undefined) {
      return yield* new SyncValidationError({
        message: `Session ${sessionId} references missing Workstream ${session.workstreamId}`
      })
    }

    const history = decodeHistory(rows)
    const heads = yield* sql<{ readonly commitId: string | null }>`
      SELECT commit_id AS commitId
      FROM peer_branch_heads
      WHERE peer_id=${requestedPeerId} AND session_id=${sessionId}`
    const branchHeadId = requestedHeadId === undefined
      ? heads[0]?.commitId ?? null
      : requestedHeadId
    if (branchHeadId !== null && !history.some(
      (item) => isBranchRecord(item) && branchRecordId(item) === branchHeadId
    )) {
      return yield* new CommitNotFound({
        sessionId,
        commitId: branchHeadId
      })
    }

    const records = branchHistory(history, branchHeadId)
    if (
      branchHeadId !== null &&
      (records.at(-1) === undefined ||
        branchRecordId(records.at(-1)!) !== branchHeadId ||
        records[0]?.parentId !== null)
    ) {
      return yield* new SyncValidationError({
        message: `Branch Head ${branchHeadId} is not connected to its ancestry`
      })
    }
    const createdEventId = rows.find(
      (row) => row.event_type === "SessionCreated"
    )?.event_id
    if (createdEventId === undefined) {
      return yield* new SyncValidationError({
        message: `Session ${sessionId} has no creation activity`
      })
    }
    const settledEventId = session.settledAt === null
      ? null
      : rows.filter((row) => row.event_type === "SessionSettled").at(-1)?.event_id ?? null

    const created = history.find(
      (item): item is SessionCreated => item.type === "SessionCreated"
    )
    return {
      version: 1 as const,
      sourcePeerId: requestedPeerId,
      workstream,
      session: {
        ...session,
        createdEventId,
        settledEventId,
        ...(created?.origin === undefined ? {} : { origin: created.origin })
      },
      branchHeadId,
      records
    }
  }).pipe(preserveSessionError)

  const importBranch = (
    bundle: SyncBundle,
    requestedPeerId: string
  ): Effect.Effect<SyncResult, Error> => Effect.gen(function*() {
    if (bundle.version !== 1) {
      return yield* new SyncValidationError({
        message: `Unsupported synchronization version: ${String(bundle.version)}`
      })
    }
    if (bundle.sourcePeerId.trim().length === 0) {
      return yield* new SyncValidationError({
        message: "Synchronization source Peer ID must not be empty"
      })
    }
    if (bundle.session.sessionId.length === 0 ||
      bundle.session.workstreamId.length === 0 ||
      bundle.workstream.workstreamId.length === 0) {
      return yield* new SyncValidationError({
        message: "Synchronization metadata must identify a Session and Workstream"
      })
    }
    if (bundle.session.workstreamId !== bundle.workstream.workstreamId) {
      return yield* new SyncValidationError({
        message: "Session and Workstream IDs do not match"
      })
    }
    if (
      bundle.session.settledAt !== null &&
      bundle.session.settledEventId === null
    ) {
      return yield* new SyncValidationError({
        message: "A settled Session must include its stable Settlement activity ID"
      })
    }
    if (
      bundle.session.settledAt === null &&
      bundle.session.settledEventId !== null
    ) {
      return yield* new SyncValidationError({
        message: "An active Session must not include a Settlement activity ID"
      })
    }
    if (
      bundle.session.settledEventId !== null &&
      bundle.session.createdEventId === bundle.session.settledEventId
    ) {
      return yield* new SyncValidationError({
        message: "Session lifecycle activities must have distinct stable IDs"
      })
    }

    const incomingIds = new Set<string>()
    for (const record of bundle.records) {
      const recordId = branchRecordId(record)
      if (recordId.length === 0) {
        return yield* new SyncValidationError({
          message: "Synchronization records must have stable IDs"
        })
      }
      if (incomingIds.has(recordId)) {
        return yield* new SyncValidationError({
          message: `Synchronization payload repeats record ${recordId}`
        })
      }
      incomingIds.add(recordId)
      if (record.sessionId !== bundle.session.sessionId) {
        return yield* new SyncValidationError({
          message: `Record ${recordId} belongs to another Session`
        })
      }
      if (recordId === bundle.session.createdEventId ||
        recordId === bundle.session.settledEventId) {
        return yield* new SyncValidationError({
          message: `Record ${recordId} collides with Session lifecycle activity`
        })
      }
    }

    const lifecycleIds = [
      { id: bundle.session.createdEventId, type: "SessionCreated" as const },
      ...(bundle.session.settledEventId === null
        ? []
        : [{ id: bundle.session.settledEventId, type: "SessionSettled" as const }])
    ]
    for (const lifecycle of lifecycleIds) {
      if (lifecycle.id.length === 0) {
        return yield* new SyncValidationError({
          message: "Session lifecycle activity must have stable IDs"
        })
      }
      const rows = yield* sql<{
        readonly sessionId: string
        readonly eventType: RawEventType
      }>`SELECT session_id AS sessionId, event_type AS eventType
        FROM session_events WHERE event_id=${lifecycle.id}`
      const existing = rows[0]
      if (existing !== undefined && (
        existing.sessionId !== bundle.session.sessionId ||
        existing.eventType !== lifecycle.type
      )) {
        return yield* new SyncConflict({
          sessionId: bundle.session.sessionId,
          recordId: lifecycle.id,
          message: `Session lifecycle activity ${lifecycle.id} conflicts with an existing record`
        })
      }
    }

    const targetRowsBefore = yield* sessionRows(bundle.session.sessionId)
    const targetHistoryBefore = decodeHistory(targetRowsBefore)
    const available = new Set(
      targetHistoryBefore
        .filter(isBranchRecord)
        .map(branchRecordId)
    )
    const existingRecords = new Map<string, {
      readonly sessionId: string
      readonly eventType: RawEventType
      readonly payload: Record<string, unknown>
      readonly occurredAt: string
    }>()
    for (const record of bundle.records) {
      const recordId = branchRecordId(record)
      const rows = yield* sql<{
        readonly sessionId: string
        readonly eventType: RawEventType
        readonly payload: string
        readonly occurredAt: string
      }>`SELECT
        session_id AS sessionId,
        event_type AS eventType,
        payload,
        occurred_at AS occurredAt
      FROM session_events
      WHERE event_id=${recordId}`
      const existing = rows[0]
      if (existing === undefined) continue
      const decodedPayload = JSON.parse(existing.payload) as Record<string, unknown>
      const expected = syncRecordPayload(record)
      const compatibleEventType = existing.eventType === expected.type ||
        (record.type === "AgentMessageCommit" &&
          (existing.eventType === "CherryPickedAgentMessage" ||
            existing.eventType === "AgentMessageAdded"))
      if (
        existing.sessionId !== bundle.session.sessionId ||
        !compatibleEventType ||
        existing.occurredAt !== record.createdAt ||
        !sameExpectedPayload(decodedPayload, expected.payload)
      ) {
        return yield* new SyncConflict({
          sessionId: bundle.session.sessionId,
          recordId,
          message: `Record ${recordId} conflicts with an existing immutable record`
        })
      }
      existingRecords.set(recordId, {
        sessionId: existing.sessionId,
        eventType: existing.eventType,
        payload: decodedPayload,
        occurredAt: existing.occurredAt
      })
    }

    for (const record of bundle.records) {
      if (record.parentId === null || incomingIds.has(record.parentId) ||
        available.has(record.parentId)) continue
      const parentRows = yield* sql<{
        readonly sessionId: string
        readonly eventType: RawEventType
      }>`SELECT session_id AS sessionId, event_type AS eventType
        FROM session_events WHERE event_id=${record.parentId}`
      const parent = parentRows[0]
      if (parent === undefined) {
        return yield* new SyncValidationError({
          message: `Record ${branchRecordId(record)} is missing parent ${record.parentId}`
        })
      }
      if (
        parent.sessionId !== bundle.session.sessionId ||
        !isBranchEventType(parent.eventType)
      ) {
        return yield* new SyncValidationError({
          message: `Record ${branchRecordId(record)} has a parent outside its Session`
        })
      }
      available.add(record.parentId)
    }

    if (bundle.branchHeadId !== null && !incomingIds.has(bundle.branchHeadId) &&
      !available.has(bundle.branchHeadId)) {
      return yield* new SyncValidationError({
        message: `Selected Branch Head ${bundle.branchHeadId} is not present in the import closure`
      })
    }

    const incomingRecords = new Map(
      bundle.records.map((record) => [branchRecordId(record), record] as const)
    )
    const targetRecords = new Map(
      targetHistoryBefore
        .filter(isBranchRecord)
        .map((record) => [branchRecordId(record), record] as const)
    )
    const reachable = new Set<string>()
    if (bundle.branchHeadId === null) {
      if (bundle.records.length > 0) {
        return yield* new SyncValidationError({
          message: "A Branch without a selected Head must not contain records"
        })
      }
    } else {
      let currentId: string | null = bundle.branchHeadId
      while (currentId !== null) {
        if (reachable.has(currentId)) {
          return yield* new SyncValidationError({
            message: `Synchronization ancestry repeats record ${currentId}`
          })
        }
        reachable.add(currentId)
        const parentRecord: BranchRecord | undefined =
          incomingRecords.get(currentId) ?? targetRecords.get(currentId)
        if (parentRecord === undefined) {
          return yield* new SyncValidationError({
            message: `Selected Branch Head ${bundle.branchHeadId} has a missing ancestor ${currentId}`
          })
        }
        currentId = parentRecord.parentId
      }
      const unreachable = bundle.records.find(
        (record) => !reachable.has(branchRecordId(record))
      )
      if (unreachable !== undefined) {
        return yield* new SyncValidationError({
          message: `Record ${branchRecordId(unreachable)} is outside the selected Branch ancestry`
        })
      }
    }

    const preexistingWorkstream = (yield* workstreamMetadata(
      bundle.workstream.workstreamId
    ))[0]
    if (preexistingWorkstream !== undefined && (
      preexistingWorkstream.name !== bundle.workstream.name ||
      preexistingWorkstream.createdAt !== bundle.workstream.createdAt ||
      preexistingWorkstream.peerId !== bundle.workstream.peerId
    )) {
      return yield* new SyncConflict({
        sessionId: bundle.session.sessionId,
        recordId: bundle.workstream.workstreamId,
        message: `Workstream ${bundle.workstream.workstreamId} has conflicting metadata`
      })
    }
    const sessionRowsBefore = yield* sessionRows(bundle.session.sessionId)
    const nextSessionSequence = sessionRowsBefore.reduce(
      (maximum, row) => Math.max(maximum, row.sequence),
      0
    ) + 1
    const preexistingSession = (yield* sessionMetadata(bundle.session.sessionId))[0]
    if (preexistingSession !== undefined && (
      preexistingSession.workstreamId !== bundle.session.workstreamId ||
      preexistingSession.createdAt !== bundle.session.createdAt ||
      preexistingSession.peerId !== bundle.session.peerId
    )) {
      return yield* new SyncConflict({
        sessionId: bundle.session.sessionId,
        recordId: bundle.session.sessionId,
        message: `Session ${bundle.session.sessionId} has conflicting metadata`
      })
    }
    if (preexistingSession !== undefined) {
      const existingCreatedEventId = sessionRowsBefore.find(
        (row) => row.event_type === "SessionCreated"
      )?.event_id
      if (existingCreatedEventId !== bundle.session.createdEventId) {
        return yield* new SyncConflict({
          sessionId: bundle.session.sessionId,
          recordId: bundle.session.createdEventId,
          message: `Session ${bundle.session.sessionId} has a conflicting creation activity`
        })
      }
      const existingOrigin = decodeHistory(sessionRowsBefore).find(
        (item): item is SessionCreated => item.type === "SessionCreated"
      )?.origin
      if (!isDeepStrictEqual(existingOrigin, bundle.session.origin)) {
        return yield* new SyncConflict({
          sessionId: bundle.session.sessionId,
          recordId: bundle.session.createdEventId,
          message: `Session ${bundle.session.sessionId} has conflicting origin provenance`
        })
      }

      const existingSettlementEventId = sessionRowsBefore.filter(
        (row) => row.event_type === "SessionSettled"
      ).at(-1)?.event_id ?? null
      if (
        preexistingSession.settledAt !== null &&
        bundle.session.settledAt !== null &&
        (
          preexistingSession.settledAt !== bundle.session.settledAt ||
          existingSettlementEventId !== bundle.session.settledEventId
        )
      ) {
        return yield* new SyncConflict({
          sessionId: bundle.session.sessionId,
          recordId: bundle.session.settledEventId!,
          message: `Session ${bundle.session.sessionId} has a conflicting Settlement activity`
        })
      }
      if (
        preexistingSession.settledAt === null &&
        bundle.session.settledAt !== null &&
        sessionRowsBefore.some(
          (row) => row.event_id === bundle.session.settledEventId
        )
      ) {
        return yield* new SyncConflict({
          sessionId: bundle.session.sessionId,
          recordId: bundle.session.settledEventId!,
          message: `Settlement activity ${bundle.session.settledEventId} already belongs to this Session history`
        })
      }
    }

    const workstreamRows = yield* workstreamMetadata(bundle.workstream.workstreamId)
    const existingWorkstream = workstreamRows[0]
    if (existingWorkstream === undefined) {
      yield* sql`INSERT INTO workstreams (
        workstream_id, name, created_at, updated_at, peer_id
      ) VALUES (
        ${bundle.workstream.workstreamId},
        ${bundle.workstream.name},
        ${bundle.workstream.createdAt},
        ${bundle.workstream.updatedAt},
        ${bundle.workstream.peerId}
      )`
    }

    const existingSession = preexistingSession
    if (existingSession === undefined) {
      yield* sql`INSERT INTO sessions (
        session_id, workstream_id, title, created_at, updated_at, peer_id, settled_at
      ) VALUES (
        ${bundle.session.sessionId},
        ${bundle.session.workstreamId},
        ${bundle.session.title},
        ${bundle.session.createdAt},
        ${bundle.session.updatedAt},
        ${bundle.session.peerId},
        ${bundle.session.settledAt}
      )`
      yield* insertRawEvent(
        bundle.session.sessionId,
        bundle.session.createdEventId,
        "SessionCreated",
        {
          workstreamId: bundle.session.workstreamId,
          ...(bundle.session.origin === undefined
            ? {}
            : { origin: bundle.session.origin })
        },
        1,
        bundle.session.createdAt
      )
      if (bundle.session.settledAt !== null) {
        yield* insertRawEvent(
          bundle.session.sessionId,
          bundle.session.settledEventId!,
          "SessionSettled",
          {},
          2,
          bundle.session.settledAt
        )
      }
    } else {
      yield* sql`UPDATE sessions SET
        updated_at=CASE WHEN updated_at < ${bundle.session.updatedAt}
          THEN ${bundle.session.updatedAt} ELSE updated_at END,
        title=CASE WHEN title='New session' AND ${bundle.session.title} <> 'New session'
          THEN ${bundle.session.title} ELSE title END,
        settled_at=CASE
          WHEN settled_at IS NULL AND ${bundle.session.settledAt} IS NOT NULL
            THEN ${bundle.session.settledAt}
          ELSE settled_at
        END
      WHERE session_id=${bundle.session.sessionId}`

      if (
        existingSession.settledAt === null &&
        bundle.session.settledAt !== null &&
        bundle.session.settledEventId !== null &&
        sessionRowsBefore.every(
          (row) => row.event_id !== bundle.session.settledEventId
        )
      ) {
        yield* insertRawEvent(
          bundle.session.sessionId,
          bundle.session.settledEventId,
          "SessionSettled",
          {},
          nextSessionSequence,
          bundle.session.settledAt
        )
      }
    }

    let nextSequence = (yield* sql<{ readonly maximum: number | null }>`
      SELECT MAX(sequence) AS maximum
      FROM session_events
      WHERE session_id=${bundle.session.sessionId}`)[0]?.maximum ?? 0
    const pending = bundle.records.filter(
      (record) => !existingRecords.has(branchRecordId(record))
    )
    const importedRecordIds: Array<string> = []
    const existingRecordIds = bundle.records
      .filter((record) => existingRecords.has(branchRecordId(record)))
      .map(branchRecordId)

    while (pending.length > 0) {
      const index = pending.findIndex(
        (record) => record.parentId === null || available.has(record.parentId)
      )
      if (index < 0) {
        return yield* new SyncValidationError({
          message: "Synchronization records do not form a closed ancestry"
        })
      }
      const record = pending.splice(index, 1)[0]!
      const recordId = branchRecordId(record)
      const event = syncRecordPayload(record)
      const payload = record.type === "UserCommit"
        ? { ...event.payload, imported: true }
        : event.payload
      nextSequence += 1
      yield* insertRawEvent(
        bundle.session.sessionId,
        recordId,
        event.type,
        payload,
        nextSequence,
        record.createdAt
      )
      available.add(recordId)
      importedRecordIds.push(recordId)
    }

    const importedCommitIds = commitIdsFor(bundle.records, importedRecordIds)
    const existingCommitIds = commitIdsFor(bundle.records, existingRecordIds)

    yield* sql`UPDATE workstreams SET
      updated_at=CASE WHEN updated_at < ${bundle.workstream.updatedAt}
        THEN ${bundle.workstream.updatedAt} ELSE updated_at END
    WHERE workstream_id=${bundle.workstream.workstreamId}`
    return {
      sessionId: bundle.session.sessionId,
      workstreamId: bundle.workstream.workstreamId,
      branchHeadId: bundle.branchHeadId,
      importedRecordIds,
      existingRecordIds,
      importedCommitIds,
      existingCommitIds
    }
  }).pipe(sql.withTransaction, preserveSessionError)

  const cherryPick = (
    sourceSessionId: string,
    sourceCommitId: string,
    targetSessionId: string,
    requestedPeerId: string,
    expectedTargetHeadId?: string | null
  ): Effect.Effect<
    | { readonly type: "NotFound"; readonly sessionId: string }
    | { readonly type: "Settled"; readonly sessionId: string }
    | {
      readonly type: "CommitNotFound"
      readonly sessionId: string
      readonly commitId: string
    }
    | {
      readonly type: "BranchHeadConflict"
      readonly sessionId: string
      readonly expectedHeadId: string | null
      readonly actualHeadId: string | null
    }
    | {
      readonly type: "Appended"
      readonly item: Extract<Commit, { readonly type: "AgentMessageCommit" }>
    },
    PersistenceError
  > => Effect.gen(function*() {
    const sourceRows = yield* sessionRows(sourceSessionId)
    if (sourceRows.length === 0) {
      return { type: "NotFound" as const, sessionId: sourceSessionId }
    }
    const targetRows = yield* sessionRows(targetSessionId)
    if (targetRows.length === 0) {
      return { type: "NotFound" as const, sessionId: targetSessionId }
    }

    const sourceHistory = decodeHistory(sourceRows)
    const source = sourceHistory.find(
      (item): item is Extract<
        Commit,
        { readonly type: "AgentMessageCommit" | "CompactionCommit" }
      > =>
        (item.type === "AgentMessageCommit" || item.type === "CompactionCommit") &&
        item.commitId === sourceCommitId
    )
    if (source === undefined) {
      return {
        type: "CommitNotFound" as const,
        sessionId: sourceSessionId,
        commitId: sourceCommitId
      }
    }

    const state = yield* sessionState(targetSessionId)
    if (state[0]?.settledAt !== null && state[0]?.settledAt !== undefined) {
      return { type: "Settled" as const, sessionId: targetSessionId }
    }

    const workstreams = yield* sql<{ readonly workstreamId: string }>`
      SELECT workstream_id AS workstreamId
      FROM sessions
      WHERE session_id=${sourceSessionId}
    `
    const sourceWorkstreamId = workstreams[0]?.workstreamId
    if (sourceWorkstreamId === undefined) {
      return { type: "NotFound" as const, sessionId: sourceSessionId }
    }

    yield* ensurePeerHead(targetSessionId, requestedPeerId)
    const heads = yield* sql<{ readonly commitId: string | null }>`
      SELECT commit_id AS commitId
      FROM peer_branch_heads
      WHERE peer_id=${requestedPeerId} AND session_id=${targetSessionId}
    `
    const parentId = heads[0]?.commitId ?? null
    if (expectedTargetHeadId !== undefined && parentId !== expectedTargetHeadId) {
      return {
        type: "BranchHeadConflict" as const,
        sessionId: targetSessionId,
        expectedHeadId: expectedTargetHeadId,
        actualHeadId: parentId
      }
    }
    const commitId = yield* randomId
    const item = yield* insert(
      targetSessionId,
      commitId,
      "CherryPickedAgentMessage",
      {
        content: source.content,
        inReplyTo: source.commitId,
        parentId,
        provenance: {
          workstreamId: sourceWorkstreamId,
          sessionId: sourceSessionId,
          commitId: source.commitId
        }
      },
      targetRows.at(-1)!.sequence + 1
    )
    yield* updateHeadIfCurrent(
      targetSessionId,
      requestedPeerId,
      parentId,
      commitId
    )
    return {
      type: "Appended" as const,
      item: item as Extract<Commit, { readonly type: "AgentMessageCommit" }>
    }
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
      requestedPeerId = defaultPeerId,
      origin?: SessionOrigin
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
          {
            workstreamId: requestedWorkstreamId,
            ...(origin === undefined ? {} : { origin })
          },
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
      const result = yield* transitionSession(sessionId, "SessionSettled")
      if (result.type === "NotFound") return yield* new NotFound({ sessionId })
      if (result.type === "AlreadySettled") {
        return yield* new AlreadySettled({ sessionId })
      }
      if (result.type === "Settled") return result.item
      return yield* Effect.die(`Unexpected lifecycle result: ${result.type}`)
    }),
    reopen: Effect.fn("Session.reopen")(function*(sessionId) {
      const result = yield* transitionSession(sessionId, "SessionReopened")
      if (result.type === "NotFound") return yield* new NotFound({ sessionId })
      if (result.type === "NotSettled") return yield* new NotSettled({ sessionId })
      if (result.type === "Reopened") return result.item
      return yield* Effect.die(`Unexpected lifecycle result: ${result.type}`)
    }),
    appendUserCommit: Effect.fn("Session.appendUserCommit")(
      function*(
        sessionId,
        content,
        commitId,
        requestedPeerId = defaultPeerId,
        options?: UserCommitOptions
      ) {
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
            {
              content,
              parentId,
              peerId: requestedPeerId,
              ...(options?.autoRun === undefined
                ? {}
                : { autoRun: options.autoRun })
            },
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
        const state = yield* sessionState(sessionId)
        if (state[0]?.settledAt !== null && state[0]?.settledAt !== undefined) {
          return { type: "Settled" as const }
        }
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
    appendCompactionCommit: Effect.fn("Session.appendCompactionCommit")(
      function*(
        sessionId,
        content,
        inReplyTo,
        runId,
        requestedPeerId = defaultPeerId,
        expectedHeadId?: string | null
      ) {
        return yield* appendCompactionCommit(
          sessionId,
          content,
          inReplyTo,
          runId,
          requestedPeerId,
          expectedHeadId
        )
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
    appendInterruptCommit: Effect.fn("Session.appendInterruptCommit")(
      function*(
        sessionId,
        reason,
        partialOutput,
        inReplyTo,
        runId,
        requestedPeerId = defaultPeerId
      ) {
        const result = yield* appendResponseCommit(
          sessionId,
          "InterruptCommit",
          inReplyTo,
          runId,
          requestedPeerId,
          { reason, partialOutput, inReplyTo },
          (item) => item.type === "InterruptCommit" &&
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
          { readonly type: "InterruptCommit" }
        >
      }
    ),
    exportBranch: Effect.fn("Session.exportBranch")(function*(
      sessionId,
      headId,
      requestedPeerId = defaultPeerId
    ) {
      return yield* exportBranch(sessionId, headId, requestedPeerId)
    }),
    importBranch: Effect.fn("Session.importBranch")(function*(
      bundle,
      requestedPeerId = defaultPeerId
    ) {
      return yield* importBranch(bundle, requestedPeerId)
    }),
    cherryPick: Effect.fn("Session.cherryPick")(function*(
      sourceSessionId,
      sourceCommitId,
      targetSessionId,
      requestedPeerId = defaultPeerId,
      expectedTargetHeadId?: string | null
    ) {
      const result = yield* cherryPick(
        sourceSessionId,
        sourceCommitId,
        targetSessionId,
        requestedPeerId,
        expectedTargetHeadId
      )
      if (result.type === "NotFound") {
        return yield* new NotFound({ sessionId: result.sessionId })
      }
      if (result.type === "Settled") {
        return yield* new Settled({ sessionId: result.sessionId })
      }
      if (result.type === "CommitNotFound") {
        return yield* new CommitNotFound({
          sessionId: result.sessionId,
          commitId: result.commitId
        })
      }
      if (result.type === "BranchHeadConflict") {
        return yield* new BranchHeadConflict({
          sessionId: result.sessionId,
          expectedHeadId: result.expectedHeadId,
          actualHeadId: result.actualHeadId,
          message: "Target Branch Head changed during Cherry-pick"
        })
      }
      return result.item
    }),
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
      if (rows.length === 0) return yield* new NotFound({ sessionId })
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
