import { Effect, Layer } from "effect"
import { Agent } from "./Agent.ts"
import * as Session from "./Session.ts"

const consumerName = "agent-runtime-v1"

const agentContext = (
  history: ReadonlyArray<Session.HistoryItem>,
  headId: string
): ReadonlyArray<Agent.ContextEntry> => {
  const context: Array<Agent.ContextEntry> = []
  for (const record of Session.branchHistory(history, headId)) {
    if (record.type === "LegacyToolCall") {
      context.push({
        type: "Tool" as const,
        commitId: record.legacyId,
        name: record.name,
        input: record.input,
        outcome: {
          type: "Failure" as const,
          value: "Legacy tool result was not persisted"
        }
      })
      continue
    }
    if (record.type === "UserCommit") {
      context.push({
        type: "User" as const,
        commitId: record.commitId,
        content: record.content
      })
      continue
    }
    if (record.type === "AgentMessageCommit") {
      context.push({
        type: "AgentMessage" as const,
        commitId: record.commitId,
        content: record.content
      })
      continue
    }
    context.push({
      type: "Tool" as const,
      commitId: record.commitId,
      name: record.name,
      input: record.input,
      outcome: record.outcome.type === "Success"
        ? { type: "Success" as const, value: record.outcome.result }
        : { type: "Failure" as const, value: record.outcome.failure }
    })
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

  const react = Effect.fn("AgentRuntime.react")(function*(
    item: Session.HistoryItem
  ) {
    if (item.type !== "UserCommit") return

    const snapshot = yield* store.history(item.sessionId)
    const existingAgentMessage = snapshot.items.find((candidate) =>
      candidate.type === "AgentMessageCommit" &&
      candidate.inReplyTo === item.commitId
    )
    if (existingAgentMessage !== undefined) return

    const durableToolCommits = snapshot.items.filter(
      (candidate): candidate is Extract<
        Session.Commit,
        { readonly type: "ToolCommit" }
      > => candidate.type === "ToolCommit" &&
        candidate.inReplyTo === item.commitId
    ).sort((left, right) => left.index - right.index)
    const runHeadId = durableToolCommits.at(-1)?.commitId ?? item.commitId

    const toolCalls = new Map<string, {
      readonly id: string
      readonly name: string
      readonly input: unknown
      readonly index: number
    }>()
    let nextToolIndex = (durableToolCommits.at(-1)?.index ?? -1) + 1

    const response = yield* agent.run(
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
          item.sessionId,
          call.id,
          call.name,
          call.input,
          agentEvent.isFailure
            ? { type: "Failure", failure: agentEvent.result }
            : { type: "Success", result: agentEvent.result },
          item.commitId,
          call.index
        ).pipe(Effect.asVoid)
      }
    )

    yield* store.appendAgentMessageCommit(
      item.sessionId,
      response,
      item.commitId
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
})

export const layerWithoutDependencies = Layer.effectDiscard(make)

/** This layer must only be installed in an API/runtime process. */
export const layer = layerWithoutDependencies.pipe(
  Layer.provide(Agent.layer),
  Layer.provide(Session.layer())
)
