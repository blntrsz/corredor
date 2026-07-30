import { Context, Effect, Layer } from "effect"
import * as Session from "./Session.ts"

/**
 * Public application boundary shared by HTTP, interactive, and Workflow
 * adapters. Callers express domain actions without accessing persistence.
 */
export interface Interface {
  readonly createSession: (
    sessionId?: string
  ) => Effect.Effect<Session.SessionCreated, Session.Error>
  readonly submitUserCommit: (
    sessionId: string,
    content: string,
    commitId?: string
  ) => Effect.Effect<
    Extract<Session.Commit, { readonly type: "UserCommit" }>,
    Session.Error
  >
  readonly checkout: (
    sessionId: string,
    commitId: string | null
  ) => Effect.Effect<void, Session.Error>
  readonly listSessions: () => Effect.Effect<
    ReadonlyArray<Session.SessionSummary>,
    Session.PersistenceError
  >
  readonly history: (
    sessionId: string
  ) => Effect.Effect<Session.HistorySnapshot, Session.PersistenceError>
  readonly activityAfter: (
    position: number,
    limit?: number
  ) => Effect.Effect<ReadonlyArray<Session.HistoryItem>, Session.PersistenceError>
}

export class Service extends Context.Service<Service, Interface>()(
  "@corredor/Application"
) {}

export const make = Effect.gen(function*() {
  const store = yield* Session.Service

  return Service.of({
    createSession: Effect.fn("Application.createSession")(
      function*(requestedId?: string) {
        return yield* store.createSession(requestedId ?? crypto.randomUUID())
      }
    ),
    submitUserCommit: Effect.fn("Application.submitUserCommit")(
      function*(sessionId: string, content: string, requestedId?: string) {
        return yield* store.appendUserCommit(
          sessionId,
          content,
          requestedId ?? crypto.randomUUID()
        )
      }
    ),
    checkout: Effect.fn("Application.checkout")(
      function*(sessionId: string, commitId: string | null) {
        yield* store.checkout(sessionId, commitId)
      }
    ),
    listSessions: Effect.fn("Application.listSessions")(function*() {
      return yield* store.listSessions()
    }),
    history: Effect.fn("Application.history")(function*(sessionId: string) {
      return yield* store.history(sessionId)
    }),
    activityAfter: Effect.fn("Application.activityAfter")(
      function*(position: number, limit?: number) {
        return yield* store.activityAfter(position, limit)
      }
    )
  })
})

export const layer = Layer.effect(Service, make)
