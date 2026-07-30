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
  interface RunnerState {
    readonly runner: Agent.Runner
    leafId: string | null
  }
  const runners = new Map<string, RunnerState>()

  const runnerFor = (event: Extract<Session.StoredEvent, { readonly type: "UserMessageAdded" }>) => Effect.gen(function*() {
    const events = yield* store.events(event.sessionId)
    const parentId = Session.conversationParentId(events, event.eventId) ?? null
    const existing = runners.get(event.sessionId)
    if (existing !== undefined && existing.leafId === parentId) return existing

    const history: Array<{ role: "user" | "assistant"; content: string }> = []
    for (const ancestor of Session.conversationBranch(events, event.eventId)) {
      if (ancestor.eventId === event.eventId) continue
      if (ancestor.type === "UserMessageAdded") history.push({ role: "user", content: ancestor.payload.content })
      if (ancestor.type === "AgentMessageAdded") history.push({ role: "assistant", content: ancestor.payload.content })
    }

    const state: RunnerState = {
      runner: yield* agents.create(history),
      leafId: parentId
    }
    runners.set(event.sessionId, state)
    return state
  })

  const react = (event: Session.StoredEvent) => Effect.gen(function*() {
    if (event.type === "SessionCreated") {
      runners.set(event.sessionId, {
        runner: yield* agents.create(),
        leafId: null
      })
      return
    }
    if (event.type === "SessionTreeNavigated") {
      runners.delete(event.sessionId)
      return
    }
    if (event.type !== "UserMessageAdded") return

    // A restart can happen after writing the response but before advancing the
    // checkpoint. Treat the causation event as the idempotency key.
    const existingReply = (yield* store.events(event.sessionId)).find(
      (candidate) => candidate.type === "AgentMessageAdded" && candidate.payload.inReplyTo === event.eventId
    )
    if (existingReply !== undefined) {
      runners.delete(event.sessionId)
      return
    }

    const state = yield* runnerFor(event)
    let toolCallIndex = 0
    const response = yield* state.runner.run(event.payload.content, (agentEvent) => {
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
    const reply = yield* store.appendAgentMessage(event.sessionId, crypto.randomUUID(), response, event.eventId)
    if (runners.get(event.sessionId) === state) state.leafId = reply.eventId
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
