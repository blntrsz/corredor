import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { Agent } from "./Agent.ts"
import * as Session from "./Session.ts"

const consumerName = "agent-runtime-v1"

export interface Interface {
  /** Runs an Agent from an explicit Commit and durable run identity. */
  readonly start: (
    sessionId: string,
    startingCommitId: string,
    definition: Agent.Definition,
    runId?: string,
    peerId?: string,
    waitForOutcome?: boolean
  ) => Effect.Effect<void, Session.Error | Session.PersistenceError>
  /** Summarizes the selected Branch into one durable Compaction Commit. */
  readonly compact: (
    sessionId: string,
    startingCommitId: string,
    definition?: Agent.Definition,
    runId?: string,
    peerId?: string
  ) => Effect.Effect<
    Extract<Session.Commit, { readonly type: "CompactionCommit" }>,
    Session.Error | Session.PersistenceError
  >
  /** Requests an active Agent Run to stop and records its durable outcome. */
  readonly interrupt: (
    sessionId: string,
    startingCommitId: string,
    reason?: string,
    runId?: string
  ) => Effect.Effect<
    Extract<Session.Commit, { readonly type: "InterruptCommit" }> | undefined,
    Session.Error | Session.PersistenceError
  >
  /** Records terminal outcomes for active runs before settling the Session. */
  readonly settle: (
    sessionId: string
  ) => Effect.Effect<Session.SessionSettled, Session.Error | Session.PersistenceError>
  /** Reopens the Session and admits new Agent Runs again. */
  readonly reopen: (
    sessionId: string
  ) => Effect.Effect<Session.SessionReopened, Session.Error | Session.PersistenceError>
}

export class Service extends Context.Service<Service, Interface>()(
  "@corredor/AgentRuntime"
) {}

const defaultFailureReason = "Agent run failed"
const defaultInterruptReason = "Interrupted by user"
const compactionInstruction = [
  "Produce a concise durable summary of the complete active ancestry supplied in context.",
  "Preserve the facts, decisions, unfinished work, and user intent needed by a later Agent Run.",
  "Return only the summary text; do not describe this instruction."
].join(" ")

const visibleInterruptReason = (reason?: string): string => {
  const normalized = reason?.trim() ?? ""
  return normalized.length === 0
    ? defaultInterruptReason
    : normalized.slice(0, 500)
}

const reasonMessage = (value: unknown): string | undefined => {
  if (value instanceof Error) return value.message
  if (typeof value === "string") return value
  if (value === null || typeof value !== "object") return undefined
  if ("message" in value && typeof value.message === "string") {
    return value.message
  }
  if ("reason" in value) return reasonMessage(value.reason)
  return undefined
}

const visibleReason = (value: unknown): string => {
  const raw = reasonMessage(value) ?? defaultFailureReason
  const firstLine = raw.trim().split("\n", 1)[0]?.trim() ?? ""
  return firstLine.length === 0
    ? defaultFailureReason
    : firstLine.slice(0, 500)
}

/** Keeps stack traces, provider payloads, and hidden model state out of history. */
const safeFailureReason = (cause: Cause.Cause<unknown>): string => {
  for (const reason of cause.reasons) {
    if (Cause.isFailReason(reason)) return visibleReason(reason.error)
    if (Cause.isDieReason(reason)) return visibleReason(reason.defect)
  }
  return defaultFailureReason
}

/** Legacy and synchronized users are readable history, not new work to execute. */
export const isRunnableUserCommit = (
  item: Session.HistoryItem
): item is Extract<Session.Commit, { readonly type: "UserCommit" }> =>
  item.type === "UserCommit" &&
  item.legacyMessageId === undefined &&
  item.imported !== true &&
  item.autoRun !== false

const agentContext = (
  history: ReadonlyArray<Session.HistoryItem>,
  headId: string
): ReadonlyArray<Agent.ContextEntry> => {
  const context: Array<Agent.ContextEntry> = []
  const branch = Session.branchHistory(history, headId)
  const latestCompaction = branch.findLastIndex(
    (record) => record.type === "CompactionCommit"
  )
  const activeBranch = latestCompaction < 0
    ? branch
    : branch.slice(latestCompaction)
  for (const record of activeBranch) {
    context.push(Session.foldBranchRecord<Agent.ContextEntry>(record, {
      user: (entry) => ({
        type: "User" as const,
        commitId: entry.commitId,
        content: entry.content
      }),
      agentMessage: (entry) => ({
        type: "AgentMessage" as const,
        commitId: entry.commitId,
        content: entry.content
      }),
      compaction: (entry) => ({
        type: "Compaction" as const,
        commitId: entry.commitId,
        content: entry.content
      }),
      failure: (entry) => ({
        type: "Failure" as const,
        commitId: entry.commitId,
        reason: entry.reason
      }),
      interrupt: (entry) => ({
        type: "Interrupt" as const,
        commitId: entry.commitId,
        reason: entry.reason,
        partialOutput: entry.partialOutput
      }),
      tool: (entry) => ({
        type: "Tool" as const,
        commitId: entry.commitId,
        name: entry.name,
        input: entry.input,
        outcome: entry.outcome.type === "Success"
          ? { type: "Success" as const, value: entry.outcome.result }
          : { type: "Failure" as const, value: entry.outcome.failure }
      }),
      legacyTool: (entry) => ({
        type: "Tool" as const,
        commitId: entry.legacyId,
        name: entry.name,
        input: entry.input,
        outcome: {
          type: "Failure" as const,
          value: "Legacy tool result was not persisted"
        }
      })
    }))
  }
  return context
}

/**
 * Server-owned durable activity reactor. Each User Commit reconstructs a fresh
 * Agent Run from Commit ancestry.
 */
export const make = Effect.gen(function*() {
  const store = yield* Session.Service
  const agent = yield* Agent.Service
  const runtimeScope = yield* Effect.scope

  type InterruptRequest = {
    readonly reason: string
  }
  type InterruptCommit = Extract<
    Session.Commit,
    { readonly type: "InterruptCommit" }
  >
  interface ActiveRun {
    readonly sessionId: string
    readonly startingCommitId: string
    readonly runId?: string
    readonly request: Deferred.Deferred<InterruptRequest>
    readonly completed: Deferred.Deferred<InterruptCommit | undefined>
    partialOutput: string
  }
  interface ActiveCompaction {
    readonly sessionId: string
    readonly startingCommitId: string
    readonly runId?: string
    readonly completed: Deferred.Deferred<void>
  }

  const activeRuns = new Map<string, ActiveRun>()
  const activeCompactions = new Map<string, ActiveCompaction>()
  const completedOperationKinds = new Map<string, "normal" | "compaction">()
  const rememberOperationKind = (
    key: string,
    kind: "normal" | "compaction"
  ): void => {
    completedOperationKinds.set(key, kind)
    if (completedOperationKinds.size <= 1_024) return
    const oldest = completedOperationKinds.keys().next().value
    if (oldest !== undefined) completedOperationKinds.delete(oldest)
  }
  const settlingSessions = new Set<string>()
  const settlementStates = new Map<string, {
    attempts: number
    settled: boolean
  }>()
  const runKey = (
    sessionId: string,
    startingCommitId: string,
    runId?: string
  ): string => JSON.stringify([sessionId, startingCommitId, runId ?? null])

  const start: Interface["start"] = Effect.fn("AgentRuntime.start")(function*(
    sessionId: string,
    startingCommitId: string,
    definition: Agent.Definition,
    requestedRunId?: string,
    requestedPeerId = Session.defaultPeerId,
    waitForOutcome = true
  ) {
    if (settlingSessions.has(sessionId)) {
      return yield* new Session.Settled({ sessionId })
    }
    const runId = requestedRunId
    const snapshot = yield* store.history(sessionId)
    if (snapshot.settled) {
      return yield* new Session.Settled({ sessionId })
    }
    const startingCommit = snapshot.items.find(
      (item): item is Session.Commit =>
        Session.isCommit(item) && item.commitId === startingCommitId
    )
    if (startingCommit === undefined) {
      return yield* new Session.CommitNotFound({
        sessionId,
        commitId: startingCommitId
      })
    }
    const existingOutcome = snapshot.items.find((candidate) =>
      (candidate.type === "AgentMessageCommit" ||
        candidate.type === "CompactionCommit" ||
        candidate.type === "FailureCommit" ||
        candidate.type === "InterruptCommit") &&
      candidate.inReplyTo === startingCommitId &&
      candidate.runId === runId
    )
    if (existingOutcome?.type === "CompactionCommit") {
      return yield* new Session.PersistenceError({
        message: `Agent Run ${runId ?? "<legacy>"} already completed as a Compaction`
      })
    }
    if (existingOutcome !== undefined) return

    const request = yield* Deferred.make<InterruptRequest>()
    const completed = yield* Deferred.make<InterruptCommit | undefined>()
    const active: ActiveRun = {
      sessionId,
      startingCommitId,
      runId,
      request,
      completed,
      partialOutput: ""
    }
    const key = runKey(sessionId, startingCommitId, runId)
    // Settlement may have begun while durable context was being loaded.
    if (settlingSessions.has(sessionId)) {
      return yield* new Session.Settled({ sessionId })
    }
    if (activeCompactions.has(key)) {
      return yield* new Session.PersistenceError({
        message: `Agent Run ${runId ?? "<legacy>"} is active as a Compaction`
      })
    }
    if (completedOperationKinds.get(key) === "compaction") {
      return yield* new Session.PersistenceError({
        message: `Agent Run ${runId ?? "<legacy>"} already completed as a Compaction`
      })
    }
    if (completedOperationKinds.get(key) === "normal") return
    if (activeRuns.has(key)) return
    activeRuns.set(key, active)

    let interruptCommit: InterruptCommit | undefined

    const durableToolCommits = snapshot.items.filter(
      (candidate): candidate is Extract<
        Session.Commit,
        { readonly type: "ToolCommit" }
      > => candidate.type === "ToolCommit" &&
        candidate.inReplyTo === startingCommitId &&
        candidate.runId === runId
    ).sort((left, right) => left.index - right.index)
    const runHeadId = durableToolCommits.at(-1)?.commitId ?? startingCommitId

    const toolCalls = new Map<string, {
      readonly id: string
      readonly name: string
      readonly input: unknown
      readonly index: number
    }>()
    let nextToolIndex = (durableToolCommits.at(-1)?.index ?? -1) + 1

    const fiber = yield* Effect.forkIn(Effect.ensuring(
      Effect.matchCauseEffect(
        Effect.raceFirst(
          agent.run(
            agentContext(snapshot.items, runHeadId),
            (agentEvent) => {
              if (agentEvent.type === "TextDelta") {
                active.partialOutput += agentEvent.text
                return Effect.void
              }
              if (agentEvent.type === "ToolCall") {
                toolCalls.set(agentEvent.id, {
                  id: agentEvent.id,
                  name: agentEvent.name,
                  input: agentEvent.input,
                  index: nextToolIndex++
                })
                return Effect.void
              }

              const call = toolCalls.get(agentEvent.id)
              if (call === undefined) return Effect.void
              return store.appendToolCommit(
                sessionId,
                call.id,
                call.name,
                call.input,
                agentEvent.isFailure
                  ? { type: "Failure", failure: agentEvent.result }
                  : { type: "Success", result: agentEvent.result },
                startingCommitId,
                call.index,
                runId,
                requestedPeerId
              ).pipe(Effect.asVoid)
            },
            definition
          ).pipe(
            Effect.map((response) => ({
              type: "Completed" as const,
              response
            }))
          ),
          Deferred.await(request).pipe(Effect.map((request) => ({
            type: "Interrupted" as const,
            request
          })))
        ),
        {
          onFailure: (cause) => {
            return Effect.uninterruptible(
              store.appendFailureCommit(
                sessionId,
                safeFailureReason(cause),
                startingCommitId,
                runId,
                requestedPeerId
              ).pipe(
                Effect.tap(() => Effect.sync(() => {
                  rememberOperationKind(key, "normal")
                })),
                Effect.asVoid
              )
            )
          },
          onSuccess: (outcome) => outcome.type === "Interrupted"
            ? Effect.uninterruptible(
              store.appendInterruptCommit(
                sessionId,
                outcome.request.reason,
                active.partialOutput,
                startingCommitId,
                runId,
                requestedPeerId
              ).pipe(
                Effect.tap((commit) => Effect.sync(() => {
                  interruptCommit = commit
                  rememberOperationKind(key, "normal")
                })),
                Effect.asVoid
              )
            )
            : Effect.uninterruptible(
              store.appendAgentMessageCommit(
                sessionId,
                outcome.response,
                startingCommitId,
                runId,
                requestedPeerId
              ).pipe(
                Effect.tap(() => Effect.sync(() => {
                  rememberOperationKind(key, "normal")
                })),
                Effect.asVoid
              )
            )
        }
      ),
      Effect.gen(function*() {
        activeRuns.delete(key)
        yield* Deferred.succeed(completed, interruptCommit)
      })
    ), runtimeScope, { startImmediately: true })
    if (waitForOutcome) yield* Fiber.join(fiber)
  })

  const compact: Interface["compact"] = Effect.fn("AgentRuntime.compact")(
    function*(
      sessionId: string,
      startingCommitId: string,
      definition = Agent.defaultDefinition,
      requestedRunId?: string,
      requestedPeerId = Session.defaultPeerId
    ) {
      if (settlingSessions.has(sessionId)) {
        return yield* new Session.Settled({ sessionId })
      }
      const snapshot = yield* store.history(sessionId)
      if (snapshot.settled) {
        return yield* new Session.Settled({ sessionId })
      }
      const startingCommit = snapshot.items.find(
        (item): item is Session.Commit =>
          Session.isCommit(item) && item.commitId === startingCommitId
      )
      if (startingCommit === undefined) {
        return yield* new Session.CommitNotFound({
          sessionId,
          commitId: startingCommitId
        })
      }

      const existingOutcome = snapshot.items.find((candidate) =>
        (candidate.type === "AgentMessageCommit" ||
          candidate.type === "CompactionCommit" ||
          candidate.type === "FailureCommit" ||
          candidate.type === "InterruptCommit") &&
        candidate.inReplyTo === startingCommitId &&
        candidate.runId === requestedRunId
      )
      if (existingOutcome?.type === "CompactionCommit") return existingOutcome
      if (existingOutcome !== undefined) {
        return yield* new Session.PersistenceError({
          message: `Agent Run already has a terminal outcome for ${startingCommitId}`
        })
      }

      const key = runKey(sessionId, startingCommitId, requestedRunId)
      const completed = yield* Deferred.make<void>()
      if (settlingSessions.has(sessionId)) {
        return yield* new Session.Settled({ sessionId })
      }
      const existingCompaction = activeCompactions.get(key)
      if (existingCompaction !== undefined) {
        yield* Deferred.await(existingCompaction.completed)
        const refreshed = yield* store.history(sessionId)
        const outcome = refreshed.items.find(
          (item): item is Extract<
            Session.Commit,
            { readonly type: "CompactionCommit" }
          > => item.type === "CompactionCommit" &&
            item.inReplyTo === startingCommitId &&
            item.runId === requestedRunId
        )
        if (outcome !== undefined) return outcome
        return yield* new Session.PersistenceError({
          message: `Compaction Agent Run failed for ${startingCommitId}`
        })
      }
      if (activeRuns.has(key)) {
        return yield* new Session.PersistenceError({
          message: `Agent Run ${requestedRunId ?? "<legacy>"} is active as a normal Run`
        })
      }
      if (completedOperationKinds.get(key) === "normal") {
        return yield* new Session.PersistenceError({
          message: `Agent Run ${requestedRunId ?? "<legacy>"} already completed as a normal Run`
        })
      }
      if (completedOperationKinds.get(key) === "compaction") {
        const refreshed = yield* store.history(sessionId)
        const outcome = refreshed.items.find(
          (item): item is Extract<
            Session.Commit,
            { readonly type: "CompactionCommit" }
          > => item.type === "CompactionCommit" &&
            item.inReplyTo === startingCommitId &&
            item.runId === requestedRunId
        )
        if (outcome !== undefined) return outcome
        return yield* new Session.PersistenceError({
          message: `Compaction Agent Run failed for ${startingCommitId}`
        })
      }
      const active: ActiveCompaction = {
        sessionId,
        startingCommitId,
        runId: requestedRunId,
        completed
      }
      activeCompactions.set(key, active)

      const compactionDefinition: Agent.Definition = {
        ...definition,
        instructions: `${definition.instructions}\n\n${compactionInstruction}`,
        tools: []
      }
      return yield* Effect.ensuring(
        Effect.gen(function*() {
          const summary = yield* Effect.matchCauseEffect(
            agent.run(
              agentContext(snapshot.items, startingCommitId),
              () => Effect.void,
              compactionDefinition
            ),
            {
              onFailure: (cause) => Effect.gen(function*() {
                const reason = safeFailureReason(cause)
                yield* store.appendFailureCommit(
                  sessionId,
                  reason,
                  startingCommitId,
                  requestedRunId,
                  requestedPeerId
                )
                rememberOperationKind(key, "compaction")
                return yield* new Session.PersistenceError({ message: reason })
              }),
              onSuccess: (response) => Effect.succeed(response)
            }
          )
          return yield* store.appendCompactionCommit(
            sessionId,
            summary,
            startingCommitId,
            requestedRunId,
            requestedPeerId,
            startingCommitId
          ).pipe(Effect.tap(() => Effect.sync(() => {
            rememberOperationKind(key, "compaction")
          })))
        }),
        Effect.gen(function*() {
          activeCompactions.delete(key)
          yield* Deferred.succeed(completed, undefined)
        })
      )
    }
  )

  const interrupt = Effect.fn("AgentRuntime.interrupt")(function*(
    sessionId: string,
    startingCommitId: string,
    reason?: string,
    requestedRunId?: string
  ) {
    const snapshot = yield* store.history(sessionId)
    if (snapshot.settled) {
      return yield* new Session.Settled({ sessionId })
    }
    const startingCommit = snapshot.items.find(
      (item): item is Session.Commit =>
        Session.isCommit(item) && item.commitId === startingCommitId
    )
    if (startingCommit === undefined) {
      return yield* new Session.CommitNotFound({
        sessionId,
        commitId: startingCommitId
      })
    }
    const existingInterrupt = snapshot.items.find(
      (item): item is InterruptCommit =>
        item.type === "InterruptCommit" &&
        item.inReplyTo === startingCommitId &&
        item.runId === requestedRunId
    )
    if (existingInterrupt !== undefined) return existingInterrupt
    const findActive = () => [...activeRuns.values()].find((candidate) =>
      candidate.sessionId === sessionId &&
      candidate.startingCommitId === startingCommitId &&
      (requestedRunId === undefined
        ? candidate.runId === undefined
        : candidate.runId === requestedRunId)
    )
    let active: ActiveRun | undefined
    for (let attempt = 0; attempt < 20; attempt++) {
      active = findActive()
      if (active !== undefined) break
      if (attempt < 19) yield* Effect.sleep("10 millis")
    }
    if (active === undefined) return undefined
    yield* Deferred.succeed(active.request, {
      reason: visibleInterruptReason(reason)
    })
    return yield* Deferred.await(active.completed)
  })

  const settle = Effect.fn("AgentRuntime.settle")(function*(sessionId: string) {
    const existingState = settlementStates.get(sessionId)
    if (existingState !== undefined && existingState.attempts > 0) {
      return yield* new Session.PersistenceError({
        message: `Settlement is already in progress for ${sessionId}`
      })
    }
    const state = existingState ?? {
      attempts: 0,
      settled: false
    }
    state.attempts++
    settlementStates.set(sessionId, state)
    settlingSessions.add(sessionId)
    return yield* Effect.gen(function*() {
      const runs = [...activeRuns.values()].filter(
        (active) => active.sessionId === sessionId
      )
      const compactions = [...activeCompactions.values()].filter(
        (active) => active.sessionId === sessionId
      )
      for (const active of runs) {
        yield* Deferred.succeed(active.request, { reason: "Session settled" })
      }
      for (const active of runs) {
        yield* Deferred.await(active.completed)
      }
      for (const active of compactions) {
        yield* Deferred.await(active.completed)
      }

      const operations = [...runs, ...compactions]
      if (operations.length > 0) {
        const snapshot = yield* store.history(sessionId)
        for (const active of operations) {
          const hasOutcome = snapshot.items.some((item) =>
            (item.type === "AgentMessageCommit" ||
              item.type === "CompactionCommit" ||
              item.type === "FailureCommit" ||
              item.type === "InterruptCommit") &&
            item.inReplyTo === active.startingCommitId &&
            item.runId === active.runId
          )
          if (!hasOutcome) {
            return yield* new Session.PersistenceError({
              message: `Agent Run did not record a terminal outcome for ${active.startingCommitId}`
            })
          }
        }
      }

      const event = yield* store.settle(sessionId)
      state.attempts--
      state.settled = true
      return event
    }).pipe(
      Effect.onExit((exit) => Exit.isFailure(exit)
        ? Effect.sync(() => {
          state.attempts--
          if (state.attempts === 0 && !state.settled) {
            settlementStates.delete(sessionId)
            settlingSessions.delete(sessionId)
          }
        })
        : Effect.void)
    )
  })

  const reopen = Effect.fn("AgentRuntime.reopen")(function*(sessionId: string) {
    const event = yield* store.reopen(sessionId)
    settlementStates.delete(sessionId)
    settlingSessions.delete(sessionId)
    return event
  })

  const react = Effect.fn("AgentRuntime.react")(function*(
    item: Session.HistoryItem
  ) {
    if (!isRunnableUserCommit(item)) return
    yield* start(
      item.sessionId,
      item.commitId,
      Agent.defaultDefinition,
      undefined,
      item.type === "UserCommit" ? item.peerId : undefined
    ).pipe(
      Effect.catchTag("@corredor/Session/Settled", () => Effect.void)
    )
  })

  const drain = Effect.fn("AgentRuntime.drain")(function*() {
    const checkpoint = yield* store.checkpoint(consumerName)
    const activity = yield* store.activityAfter(checkpoint)
    for (const item of activity) {
      yield* react(item)
      yield* store.saveCheckpoint(consumerName, item.position)
    }
  })

  yield* drain().pipe(
    Effect.catchCause(Effect.logError),
    Effect.andThen(Effect.sleep("10 millis")),
    Effect.forever,
    Effect.forkScoped
  )

  return Service.of({ start, compact, interrupt, settle, reopen })
})

export const layerWithoutDependencies = Layer.effect(Service, make)

/** This layer must only be installed in an API/runtime process. */
export const layer = (path = Session.defaultDatabasePath) =>
  layerWithoutDependencies.pipe(
    Layer.provide(Agent.layer),
    Layer.provide(Session.layer(path))
  )
