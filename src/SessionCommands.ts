import { Context, Effect, Layer } from "effect"
import * as Session from "./Session.ts"

/**
 * Synchronous command side of the session API. Commands only commit events;
 * agent execution is owned by AgentRuntime and happens after the commit.
 */
export interface Interface {
  readonly createSession: (sessionId?: string) => Effect.Effect<Session.StoredEvent, Session.Error>
  readonly addUserMessage: (
    sessionId: string,
    messageId: string,
    content: string
  ) => Effect.Effect<Session.StoredEvent, Session.Error>
  readonly navigateTree: (
    sessionId: string,
    targetId: string | null
  ) => Effect.Effect<Session.StoredEvent, Session.Error>
  readonly listSessions: () => Effect.Effect<ReadonlyArray<Session.SessionSummary>, Session.PersistenceError>
  readonly events: (sessionId: string) => Effect.Effect<ReadonlyArray<Session.StoredEvent>, Session.PersistenceError>
}

export class Service extends Context.Service<Service, Interface>()("@corredor/SessionCommands") {}

export const make = Effect.gen(function*() {
  const store = yield* Session.Service

  return Service.of({
    createSession: (requestedId) => {
      const sessionId = requestedId ?? crypto.randomUUID()
      return store.execute({ type: "CreateSession", sessionId })
    },
    addUserMessage: (sessionId, messageId, content) =>
      store.execute({ type: "AddUserMessage", sessionId, messageId, content }),
    navigateTree: (sessionId, targetId) =>
      store.execute({ type: "NavigateTree", sessionId, targetId }),
    listSessions: store.listSessions,
    events: store.events
  })
})

export const layer = Layer.effect(Service, make)
