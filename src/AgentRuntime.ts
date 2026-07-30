import { Effect, Layer } from "effect"
import { Agent } from "./Agent.ts"
import * as Session from "./Session.ts"

const consumerName = "agent-runtime-v1"

const agentContext = (
  history: ReadonlyArray<Session.HistoryItem>,
  headId: string
): ReadonlyArray<Agent.ContextEntry> => {
  const context: Array<Agent.ContextEntry> = []
  for (const entry of Session.branchHistory(history, headId)) {
    if (entry.type === "LegacyToolCall") {
      context.push({
        type: "Tool" as const,
        commitId: entry.legacyId,
        name: entry.name,
        input: entry.input,
        outcome: {
          type: "Failure" as const,
          value: "Legacy tool result was not persisted"
        }
      })
      continue
    }
    if (entry.type === "UserCommit") {
      context.push({
        type: "User" as const,
        commitId: entry.commitId,
        content: entry.content
      })
      continue
    }
    if (entry.type === "AgentMessageCommit") {
      context.push({
        type: "AgentMessage" as const,
        commitId: entry.commitId,
        content: entry.content
      })
      continue
    }
    context.push({
      type: "Tool" as const,
      commitId: entry.commitId,
      name: entry.name,
      input: entry.input,
      outcome: entry.outcome.type === "Success"
        ? { type: "Success" as const, value: entry.outcome.result }
        : { type: "Failure" as const, value: entry.outcome.failure }
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

  const react = (item: Session.HistoryItem) => Effect.gen(function*() {
    if (item.type !== "UserCommit") return

    const snapshot = yield* store.history(item.sessionId)
    const existingReply = snapshot.items.find((candidate) =>
      candidate.type === "AgentMessageCommit" &&
      candidate.inReplyTo === item.commitId
    )
    if (existingReply !== undefined) return

    const toolCalls = new Map<string, {
      readonly id: string
      readonly name: string
      readonly input: unknown
      readonly index: number
    }>()
    let nextToolIndex = 0

    const response = yield* agent.run(
      agentContext(snapshot.items, item.commitId),
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

  const drain = Effect.gen(function*() {
    const checkpoint = yield* store.checkpoint(consumerName)
    const activity = yield* store.activityAfter(checkpoint)
    for (const item of activity) {
      yield* react(item)
      yield* store.saveCheckpoint(consumerName, item.position)
    }
  })

  yield* drain.pipe(
    Effect.catchCause(Effect.logError),
    Effect.andThen(Effect.sleep("10 millis")),
    Effect.forever,
    Effect.forkScoped
  )
})

/** This layer must only be installed in an API/runtime process. */
export const layer = Layer.effectDiscard(make)
