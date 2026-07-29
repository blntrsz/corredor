import { Context, Effect, Layer } from "effect"
import { Agent } from "./Agent.ts"
import * as Session from "./Session.ts"

const consumerName = "agent-runtime-v1"

export interface Interface {
  readonly createSession: (sessionId?: string) => Effect.Effect<string, Session.Error>
  readonly submit: (
    sessionId: string,
    content: string,
    onAgentEvent?: (event: Agent.Event) => void
  ) => Effect.Effect<Session.StoredEvent, Session.Error>
}
export class Service extends Context.Service<Service, Interface>()("@corredor/SessionWorkflow") {}

/**
 * Durable event reactor. SQLite's outbox is the queue and the checkpoint is the
 * acknowledgement. A restart resumes at the first unacknowledged event.
 */
export const make = Effect.gen(function*() {
  const store = yield* Session.Service
  const agents = yield* Agent.Service
  const runners = new Map<string, Agent.Runner>()
  // Ephemeral live observers are transport concerns; durable conversation facts
  // continue to flow exclusively through the event store.
  const liveObservers = new Map<string, (event: Agent.Event) => void>()

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
    } else if (event.type === "UserMessageAdded") {
      const runner = yield* runnerFor(event.sessionId, event.sequence)
      const response = yield* runner.run(
        event.payload.content,
        liveObservers.get(event.payload.messageId)
      )
      yield* store.appendAgentMessage(event.sessionId, crypto.randomUUID(), response, event.eventId)
      liveObservers.delete(event.payload.messageId)
    }
  })

  const drain = Effect.gen(function*() {
    const checkpoint = yield* store.checkpoint(consumerName)
    const events = yield* store.eventsAfter(checkpoint)
    for (const event of events) {
      yield* react(event)
      yield* store.saveCheckpoint(consumerName, event.position)
    }
  })

  // Polling a transactional outbox is deliberately used instead of an in-memory
  // pub/sub: commits cannot be lost if the process dies between store and publish.
  yield* drain.pipe(
    Effect.catchCause(Effect.logError),
    Effect.andThen(Effect.sleep("50 millis")),
    Effect.forever,
    Effect.forkScoped
  )

  const awaitReply = (sessionId: string, inReplyTo: string): Effect.Effect<Session.StoredEvent, Session.PersistenceError> =>
    Effect.gen(function*() {
      while (true) {
        const reply = (yield* store.events(sessionId)).find(
          (event) => event.type === "AgentMessageAdded" && event.payload.inReplyTo === inReplyTo
        )
        if (reply !== undefined) return reply
        yield* Effect.sleep("25 millis")
      }
      throw new Error("unreachable")
    })

  return Service.of({
    createSession: (requestedId) => Effect.gen(function*() {
      const sessionId = requestedId ?? crypto.randomUUID()
      yield* store.execute({ type: "CreateSession", sessionId })
      return sessionId
    }),
    submit: (sessionId, content, onAgentEvent) => Effect.gen(function*() {
      const messageId = crypto.randomUUID()
      if (onAgentEvent !== undefined) liveObservers.set(messageId, onAgentEvent)
      const event = yield* store.execute({ type: "AddUserMessage", sessionId, messageId, content }).pipe(
        Effect.tapError(() => Effect.sync(() => liveObservers.delete(messageId)))
      )
      return yield* awaitReply(sessionId, event.eventId).pipe(
        Effect.ensuring(Effect.sync(() => liveObservers.delete(messageId)))
      )
    })
  })
})

export const layer = Layer.effect(Service, make)
