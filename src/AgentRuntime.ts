import { Cause, Context, Effect, Layer } from "effect"
import { Agent } from "./Agent.ts"
import * as Session from "./Session.ts"

const consumerName = "agent-runtime-v1"

export interface Interface {
  /** Runs an Agent from an explicit Commit and durable run identity. */
  readonly start: (
    sessionId: string,
    startingCommitId: string,
    runId?: string
  ) => Effect.Effect<void, Session.Error | Session.PersistenceError>
}

export class Service extends Context.Service<Service, Interface>()(
  "@corredor/AgentRuntime"
) {}

/** Legacy projected users are readable history, not new work to execute. */
export const isRunnableUserCommit = (
  item: Session.HistoryItem
): item is Extract<Session.Commit, { readonly type: "UserCommit" }> =>
  item.type === "UserCommit" && item.legacyMessageId === undefined

const agentContext = (
  history: ReadonlyArray<Session.HistoryItem>,
  headId: string
): ReadonlyArray<Agent.ContextEntry> => {
  const context: Array<Agent.ContextEntry> = []
  for (const record of Session.branchHistory(history, headId)) {
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
      failure: (entry) => ({
        type: "Failure" as const,
        commitId: entry.commitId,
        reason: entry.reason
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

  const start = Effect.fn("AgentRuntime.start")(function*(
    sessionId: string,
    startingCommitId: string,
    requestedRunId?: string
  ) {
    const runId = requestedRunId
    const snapshot = yield* store.history(sessionId)
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
        candidate.type === "FailureCommit") &&
      candidate.inReplyTo === startingCommitId &&
      candidate.runId === runId
    )
    if (existingOutcome !== undefined) return

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

    yield* Effect.matchCauseEffect(agent.run(
      agentContext(snapshot.items, runHeadId),
      (agentEvent) => {
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
          runId
        ).pipe(Effect.asVoid)
      }
    ), {
      onFailure: (cause) => store.appendFailureCommit(
        sessionId,
        Cause.pretty(cause),
        startingCommitId,
        runId
      ).pipe(Effect.asVoid),
      onSuccess: (response) => store.appendAgentMessageCommit(
        sessionId,
        response,
        startingCommitId,
        runId
      ).pipe(Effect.asVoid)
    })
  })

  const react = Effect.fn("AgentRuntime.react")(function*(
    item: Session.HistoryItem
  ) {
    if (!isRunnableUserCommit(item)) return
    yield* start(item.sessionId, item.commitId)
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

  return Service.of({ start })
})

export const layerWithoutDependencies = Layer.effect(Service, make)

/** This layer must only be installed in an API/runtime process. */
export const layer = (path = Session.defaultDatabasePath) =>
  layerWithoutDependencies.pipe(
    Layer.provide(Agent.layer),
    Layer.provide(Session.layer(path))
  )
