import { BunCrypto } from "@effect/platform-bun"
import { Context, Crypto, Effect, Layer } from "effect"
import { Agent } from "./Agent.ts"
import * as AgentRuntime from "./AgentRuntime.ts"
import * as Session from "./Session.ts"

/**
 * Public application boundary shared by HTTP, interactive, and Workflow
 * adapters. Callers express domain actions without accessing persistence.
 */
export interface Interface {
  readonly createWorkstream: (
    workstreamId?: string,
    name?: string,
    peerId?: string
  ) => Effect.Effect<Session.Workstream, Session.Error>
  readonly listWorkstreams: () => Effect.Effect<
    ReadonlyArray<Session.WorkstreamSummary>,
    Session.PersistenceError
  >
  readonly workstream: (
    workstreamId: string
  ) => Effect.Effect<Session.WorkstreamSnapshot, Session.PersistenceError | Session.WorkstreamNotFound>
  readonly createSession: (
    sessionId?: string,
    workstreamId?: string,
    peerId?: string
  ) => Effect.Effect<Session.SessionCreated, Session.Error>
  readonly submitUserCommit: (
    sessionId: string,
    content: string,
    commitId?: string,
    peerId?: string
  ) => Effect.Effect<
    Extract<Session.Commit, { readonly type: "UserCommit" }>,
    Session.Error
  >
  readonly startAgentRun: (
    sessionId: string,
    startingCommitId: string,
    definition: Agent.Definition,
    runId?: string,
    peerId?: string
  ) => Effect.Effect<void, Session.Error | Session.PersistenceError>
  readonly checkout: (
    sessionId: string,
    commitId: string | null,
    peerId?: string
  ) => Effect.Effect<void, Session.Error>
  readonly listSessions: (workstreamId?: string) => Effect.Effect<
    ReadonlyArray<Session.SessionSummary>,
    Session.PersistenceError
  >
  readonly history: (
    sessionId: string,
    peerId?: string
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
  const runtime = yield* AgentRuntime.Service
  const crypto = yield* Crypto.Crypto
  const randomId = crypto.randomUUIDv4.pipe(
    Effect.mapError((cause) => new Session.PersistenceError({
      message: cause.message
    }))
  )

  return Service.of({
    createWorkstream: Effect.fn("Application.createWorkstream")(
      function*(requestedId?: string, name?: string, peerId?: string) {
        return yield* store.createWorkstream(
          requestedId ?? (yield* randomId),
          name,
          peerId
        )
      }
    ),
    listWorkstreams: Effect.fn("Application.listWorkstreams")(function*() {
      return yield* store.listWorkstreams()
    }),
    workstream: Effect.fn("Application.workstream")(function*(workstreamId) {
      return yield* store.workstream(workstreamId)
    }),
    createSession: Effect.fn("Application.createSession")(
      function*(requestedId?: string, workstreamId?: string, peerId?: string) {
        return yield* store.createSession(
          requestedId ?? (yield* randomId),
          workstreamId,
          peerId
        )
      }
    ),
    submitUserCommit: Effect.fn("Application.submitUserCommit")(
      function*(
        sessionId: string,
        content: string,
        requestedId?: string,
        peerId?: string
      ) {
        return yield* store.appendUserCommit(
          sessionId,
          content,
          requestedId ?? (yield* randomId),
          peerId
        )
      }
    ),
    startAgentRun: Effect.fn("Application.startAgentRun")(
      function*(
        sessionId: string,
        startingCommitId: string,
        definition: Agent.Definition,
        runId?: string,
        peerId?: string
      ) {
        yield* runtime.start(
          sessionId,
          startingCommitId,
          definition,
          runId,
          peerId
        )
      }
    ),
    checkout: Effect.fn("Application.checkout")(
      function*(sessionId: string, commitId: string | null, peerId?: string) {
        yield* store.checkout(sessionId, commitId, peerId)
      }
    ),
    listSessions: Effect.fn("Application.listSessions")(function*(workstreamId?: string) {
      return yield* store.listSessions(workstreamId)
    }),
    history: Effect.fn("Application.history")(function*(
      sessionId: string,
      peerId?: string
    ) {
      return yield* store.history(sessionId, peerId)
    }),
    activityAfter: Effect.fn("Application.activityAfter")(
      function*(position: number, limit?: number) {
        return yield* store.activityAfter(position, limit)
      }
    )
  })
})

export const layerWithoutDependencies = Layer.effect(Service, make)

export const layer = (path = Session.defaultDatabasePath) =>
  layerWithoutDependencies.pipe(
    Layer.provide(AgentRuntime.layer(path)),
    Layer.provide(BunCrypto.layer)
  )
