import { Effect, Layer } from "effect"
import { Agent } from "./Agent.ts"
import * as Session from "./Session.ts"

const consumerName = "agent-runtime-v1"

/**
 * The server-owned durable event reactor. SQLite's transactional outbox is the
 * queue and the checkpoint is its acknowledgement.
 */
const make = Effect.gen(function*() {
  const store = yield* Session.Service
  const agents = yield* Agent.Service
  const runners = new Map<string, Agent.Runner>()

  const runnerFor = (sessionId: string, beforeSequence: number) => Effect.gen(function*() {
    const existing = runners.get(sessionId)
    if (existing !== undefined) return existing

    const history: Array<{ role: "user" | "assistant"; content: string }> = []
    for (const event of (yield* store.events(sessionId))) {
      if (event.sequence >= beforeSequence) continue
      if (event.type === "UserMessageAdded") history.push({ role: "user", content: event.payload.content })
      if (event.type === "AgentMessageAdded") history.push({ role: "assistant", content: event.payload.content })
    }

    const runner = yield* agents.create(history)
    runners.set(sessionId, runner)
    return runner
  })

  const react = (event: Session.StoredEvent) => Effect.gen(function*() {
    if (event.type === "SessionCreated") {
      yield* runnerFor(event.sessionId, event.sequence + 1)
      return
    }
    if (event.type !== "UserMessageAdded") return

    // A restart can happen after writing the response but before advancing the
    // checkpoint. Treat the causation event as the idempotency key.
    const existingReply = (yield* store.events(event.sessionId)).find(
      (candidate) => candidate.type === "AgentMessageAdded" && candidate.payload.inReplyTo === event.eventId
    )
    if (existingReply !== undefined) return

    const runner = yield* runnerFor(event.sessionId, event.sequence)
    let toolCallIndex = 0
    const response = yield* runner.run(event.payload.content, (agentEvent) => {
      if (agentEvent.type !== "ToolCall") return Effect.void
      const index = toolCallIndex++
      return Effect.gen(function*() {
        const existingToolCall = (yield* store.events(event.sessionId)).find(
          (candidate) => candidate.type === "AgentToolCallAdded" &&
            candidate.payload.inReplyTo === event.eventId &&
            candidate.payload.index === index
        )
        if (existingToolCall !== undefined) return
        yield* store.appendAgentToolCall(
          event.sessionId,
          agentEvent.id,
          agentEvent.name,
          agentEvent.input,
          event.eventId,
          index
        )
      })
    })
    yield* store.appendAgentMessage(event.sessionId, crypto.randomUUID(), response, event.eventId)
  })

  const drain = Effect.gen(function*() {
    const checkpoint = yield* store.checkpoint(consumerName)
    const events = yield* store.eventsAfter(checkpoint)
    for (const event of events) {
      yield* react(event)
      yield* store.saveCheckpoint(consumerName, event.position)
    }
  })

  yield* drain.pipe(
    Effect.catchCause(Effect.logError),
    Effect.andThen(Effect.sleep("50 millis")),
    Effect.forever,
    Effect.forkScoped
  )
})

/** This layer must only be installed in the API server process. */
export const layer = Layer.effectDiscard(make)
