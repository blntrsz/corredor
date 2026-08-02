import { BunCrypto, BunHttpServer } from "@effect/platform-bun"
import { Effect, Layer, Option, Schema, Stream } from "effect"
import {
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse
} from "effect/unstable/http"
import { Agent } from "./Agent.ts"
import * as AgentRuntime from "./AgentRuntime.ts"
import * as Application from "./Application.ts"
import * as Session from "./Session.ts"

const peerIdFrom = (request: HttpServerRequest.HttpServerRequest): string =>
  request.headers["x-corredor-peer-id"]?.trim() || Session.defaultPeerId

const sessionViewFrom = (
  request: HttpServerRequest.HttpServerRequest
): Session.SessionListView => {
  const params = new URL(request.originalUrl).searchParams
  const state = params.get("state")
  if (state === "settled") return "settled"
  return "active"
}

const internalServerError = () => HttpServerResponse.jsonUnsafe(
  { error: "internal server error" },
  { status: 500 }
)

const missingSessionId = () => HttpServerResponse.jsonUnsafe(
  { error: "missing session id" },
  { status: 400 }
)

const sessionNotFound = (error: Session.NotFound) => Effect.succeed(
  HttpServerResponse.jsonUnsafe(
    { error: `Session not found: ${error.sessionId}` },
    { status: 404 }
  )
)

const sessionSettled = (error: Session.Settled) => Effect.succeed(
  HttpServerResponse.jsonUnsafe(
    { error: `Session is settled: ${error.sessionId}` },
    { status: 409 }
  )
)

const commitNotFound = (error: Session.CommitNotFound) => Effect.succeed(
  HttpServerResponse.jsonUnsafe(
    { error: `Commit not found: ${error.commitId}` },
    { status: 404 }
  )
)

const health = Session.Service.pipe(
  Effect.flatMap((store) => store.check),
  Effect.match({
    onFailure: () => HttpServerResponse.jsonUnsafe(
      { status: "unavailable", service: "corredor", apiVersion: 4 },
      { status: 503 }
    ),
    onSuccess: () => HttpServerResponse.jsonUnsafe({
      status: "ok",
      service: "corredor",
      apiVersion: 4
    })
  })
)

const createSession = Effect.gen(function*() {
  const application = yield* Application.Service
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    sessionId: Schema.optional(Schema.String),
    workstreamId: Schema.optional(Schema.String)
  }))(yield* request.json)
  const session = yield* application.createSession(
    body.sessionId,
    body.workstreamId,
    peerIdFrom(request)
  )
  return HttpServerResponse.jsonUnsafe({ session }, { status: 201 })
}).pipe(
  Effect.catchTag("@corredor/Session/AlreadyExists", (error) => Effect.succeed(
    HttpServerResponse.jsonUnsafe(
      { error: `Session already exists: ${error.sessionId}` },
      { status: 409 }
    )
  )),
  Effect.catchTag("@corredor/Session/WorkstreamNotFound", (error) => Effect.succeed(
    HttpServerResponse.jsonUnsafe(
      { error: `Workstream not found: ${error.workstreamId}` },
      { status: 404 }
    )
  )),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const createWorkstream = Effect.gen(function*() {
  const application = yield* Application.Service
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    workstreamId: Schema.optional(Schema.String),
    name: Schema.optional(Schema.String)
  }))(yield* request.json)
  const workstream = yield* application.createWorkstream(
    body.workstreamId,
    body.name,
    peerIdFrom(request)
  )
  return HttpServerResponse.jsonUnsafe({ workstream }, { status: 201 })
}).pipe(
  Effect.catchTag("@corredor/Session/WorkstreamAlreadyExists", (error) => Effect.succeed(
    HttpServerResponse.jsonUnsafe(
      { error: `Workstream already exists: ${error.workstreamId}` },
      { status: 409 }
    )
  )),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const listWorkstreams = Effect.gen(function*() {
  const application = yield* Application.Service
  return HttpServerResponse.jsonUnsafe({
    workstreams: yield* application.listWorkstreams()
  })
}).pipe(Effect.catchCause(() => Effect.succeed(internalServerError())))

const inspectWorkstream = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const workstreamId = params.workstreamId
  if (workstreamId === undefined) {
    return HttpServerResponse.jsonUnsafe(
      { error: "missing workstream id" },
      { status: 400 }
    )
  }
  return HttpServerResponse.jsonUnsafe(
    yield* application.workstream(workstreamId, sessionViewFrom(request))
  )
}).pipe(
  Effect.catchTag("@corredor/Session/WorkstreamNotFound", (error) => Effect.succeed(
    HttpServerResponse.jsonUnsafe(
      { error: `Workstream not found: ${error.workstreamId}` },
      { status: 404 }
    )
  )),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const listSessions = Effect.gen(function*() {
  const application = yield* Application.Service
  const request = yield* HttpServerRequest.HttpServerRequest
  const workstreamId = new URL(request.originalUrl).searchParams.get("workstreamId") ?? undefined
  return HttpServerResponse.jsonUnsafe({
    sessions: yield* application.listSessions(workstreamId, sessionViewFrom(request))
  })
}).pipe(Effect.catchCause(() => Effect.succeed(internalServerError())))

const submitUserCommit = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    commitId: Schema.optional(Schema.String),
    messageId: Schema.optional(Schema.String),
    content: Schema.String
  }))(yield* request.json)
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return missingSessionId()
  }
  const peerId = peerIdFrom(request)
  const commit = yield* application.submitUserCommit(
    sessionId,
    body.content,
    body.commitId ?? body.messageId,
    peerId
  )
  return HttpServerResponse.jsonUnsafe({ commit }, { status: 202 })
}).pipe(
  Effect.catchTag("@corredor/Session/NotFound", sessionNotFound),
  Effect.catchTag("@corredor/Session/Settled", sessionSettled),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const sessionHistory = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return missingSessionId()
  }
  return HttpServerResponse.jsonUnsafe({
    history: yield* application.history(sessionId, peerIdFrom(request))
  })
}).pipe(Effect.orDie)

const startAgentRun = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    commitId: Schema.String,
    agent: Agent.DefinitionSchema,
    runId: Schema.optional(Schema.String)
  }))(yield* request.json)
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return missingSessionId()
  }
  const peerId = peerIdFrom(request)
  yield* application.startAgentRun(
    sessionId,
    body.commitId,
    body.agent,
    body.runId,
    peerId
  )
  return HttpServerResponse.jsonUnsafe({
    sessionId,
    commitId: body.commitId,
    runId: body.runId ?? body.commitId
  }, { status: 202 })
}).pipe(
  Effect.catchTag("@corredor/Session/NotFound", sessionNotFound),
  Effect.catchTag("@corredor/Session/Settled", sessionSettled),
  Effect.catchTag("@corredor/Session/CommitNotFound", commitNotFound),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const compactSession = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    commitId: Schema.optional(Schema.String),
    startingCommitId: Schema.optional(Schema.String),
    agent: Schema.optional(Agent.DefinitionSchema),
    runId: Schema.optional(Schema.String)
  }))(yield* request.json)
  const sessionId = params.sessionId
  if (sessionId === undefined) return missingSessionId()
  const startingCommitId = body.commitId ?? body.startingCommitId
  if (startingCommitId === undefined) {
    return HttpServerResponse.jsonUnsafe(
      { error: "missing starting commit id" },
      { status: 400 }
    )
  }
  const commit = yield* application.compact(
    sessionId,
    startingCommitId,
    body.agent,
    body.runId,
    peerIdFrom(request)
  )
  return HttpServerResponse.jsonUnsafe({ commit }, { status: 201 })
}).pipe(
  Effect.catchTag("@corredor/Session/NotFound", sessionNotFound),
  Effect.catchTag("@corredor/Session/Settled", sessionSettled),
  Effect.catchTag("@corredor/Session/CommitNotFound", commitNotFound),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const cherryPick = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    sourceSessionId: Schema.optional(Schema.String),
    sourceCommitId: Schema.optional(Schema.String),
    commitId: Schema.optional(Schema.String)
  }))(yield* request.json)
  const targetSessionId = params.sessionId
  if (targetSessionId === undefined) return missingSessionId()
  const sourceCommitId = body.sourceCommitId ?? body.commitId
  if (sourceCommitId === undefined) {
    return HttpServerResponse.jsonUnsafe(
      { error: "missing source commit id" },
      { status: 400 }
    )
  }
  const commit = yield* application.cherryPick(
    body.sourceSessionId ?? targetSessionId,
    sourceCommitId,
    targetSessionId,
    peerIdFrom(request)
  )
  return HttpServerResponse.jsonUnsafe({ commit }, { status: 201 })
}).pipe(
  Effect.catchTag("@corredor/Session/NotFound", sessionNotFound),
  Effect.catchTag("@corredor/Session/Settled", sessionSettled),
  Effect.catchTag("@corredor/Session/CommitNotFound", commitNotFound),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const interruptAgentRun = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    commitId: Schema.String,
    reason: Schema.optional(Schema.String),
    runId: Schema.optional(Schema.String)
  }))(yield* request.json)
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return missingSessionId()
  }
  const commit = yield* application.interruptAgentRun(
    sessionId,
    body.commitId,
    body.reason,
    body.runId
  )
  return HttpServerResponse.jsonUnsafe(
    { commit: commit ?? null },
    { status: 202 }
  )
}).pipe(
  Effect.catchTag("@corredor/Session/NotFound", sessionNotFound),
  Effect.catchTag("@corredor/Session/Settled", sessionSettled),
  Effect.catchTag("@corredor/Session/CommitNotFound", commitNotFound),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const checkout = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return missingSessionId()
  }
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    commitId: Schema.optional(Schema.NullOr(Schema.String)),
    targetId: Schema.optional(Schema.NullOr(Schema.String))
  }))(yield* request.json)
  const peerId = peerIdFrom(request)
  yield* application.checkout(
    sessionId,
    body.commitId === undefined ? body.targetId ?? null : body.commitId,
    peerId
  )
  return HttpServerResponse.empty({ status: 204 })
}).pipe(
  Effect.catchTag("@corredor/Session/NotFound", sessionNotFound),
  Effect.catchTag("@corredor/Session/Settled", sessionSettled),
  Effect.catchTag("@corredor/Session/CommitNotFound", commitNotFound),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const settleSession = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return missingSessionId()
  }
  const event = yield* application.settle(sessionId)
  return HttpServerResponse.jsonUnsafe({ event })
}).pipe(
  Effect.catchTag("@corredor/Session/NotFound", sessionNotFound),
  Effect.catchTag("@corredor/Session/AlreadySettled", (error) => Effect.succeed(
    HttpServerResponse.jsonUnsafe(
      { error: `Session already settled: ${error.sessionId}` },
      { status: 409 }
    )
  )),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const reopenSession = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return missingSessionId()
  }
  const event = yield* application.reopen(sessionId)
  return HttpServerResponse.jsonUnsafe({ event })
}).pipe(
  Effect.catchTag("@corredor/Session/NotFound", sessionNotFound),
  Effect.catchTag("@corredor/Session/NotSettled", (error) => Effect.succeed(
    HttpServerResponse.jsonUnsafe(
      { error: `Session is not settled: ${error.sessionId}` },
      { status: 409 }
    )
  )),
  Effect.catchCause(() => Effect.succeed(internalServerError()))
)

const encoder = new TextEncoder()
const encodeSse = (item: Session.HistoryItem): Uint8Array => encoder.encode(
  `id: ${item.position}\nevent: ${item.type}\ndata: ${JSON.stringify(item)}\n\n`
)

interface ActivityStreamState {
  readonly cursor: number
  readonly lastHeartbeat: number
}

const sessionActivity = Effect.gen(function*() {
  const application = yield* Application.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return missingSessionId()
  }

  const requestedAfter = new URL(request.originalUrl).searchParams.get("after")
  const rawCursor = request.headers["last-event-id"] ?? requestedAfter ?? "0"
  const parsedCursor = Number.parseInt(rawCursor, 10)
  const initialCursor = Number.isFinite(parsedCursor) && parsedCursor >= 0
    ? parsedCursor
    : 0

  const body = Stream.paginate<
    ActivityStreamState,
    Uint8Array,
    Session.PersistenceError
  >(
    { cursor: initialCursor, lastHeartbeat: Date.now() },
    (state) => Effect.gen(function*() {
      const activity = yield* application.activityAfter(state.cursor)
      const cursor = activity.at(-1)?.position ?? state.cursor
      const matching = activity.filter((item) => item.sessionId === sessionId)
      const now = Date.now()

      if (matching.length > 0) {
        return [
          matching.map(encodeSse),
          Option.some({ cursor, lastHeartbeat: now })
        ] as const
      }

      if (activity.length === 0) yield* Effect.sleep("100 millis")
      const heartbeatDue = now - state.lastHeartbeat >= 15_000
      return [
        heartbeatDue ? [encoder.encode(": keep-alive\n\n")] : [],
        Option.some({
          cursor,
          lastHeartbeat: heartbeatDue ? now : state.lastHeartbeat
        })
      ] as const
    })
  )

  return HttpServerResponse.stream(body, {
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      "connection": "keep-alive",
      "x-accel-buffering": "no"
    }
  })
}).pipe(Effect.orDie)

const routes = Layer.mergeAll(
  HttpRouter.add("GET", "/v1/health", health),
  HttpRouter.add("GET", "/v1/workstreams", listWorkstreams),
  HttpRouter.add("POST", "/v1/workstreams", createWorkstream),
  HttpRouter.add("GET", "/v1/workstreams/:workstreamId", inspectWorkstream),
  HttpRouter.add("GET", "/v1/sessions", listSessions),
  HttpRouter.add("POST", "/v1/sessions", createSession),
  HttpRouter.add(
    "POST",
    "/v1/sessions/:sessionId/commits",
    submitUserCommit
  ),
  // Read compatibility for clients using the former endpoint.
  HttpRouter.add(
    "POST",
    "/v1/sessions/:sessionId/messages",
    submitUserCommit
  ),
  HttpRouter.add(
    "GET",
    "/v1/sessions/:sessionId/history",
    sessionHistory
  ),
  HttpRouter.add(
    "POST",
    "/v1/sessions/:sessionId/runs",
    startAgentRun
  ),
  HttpRouter.add(
    "POST",
    "/v1/sessions/:sessionId/compact",
    compactSession
  ),
  HttpRouter.add(
    "POST",
    "/v1/sessions/:sessionId/cherry-pick",
    cherryPick
  ),
  HttpRouter.add(
    "POST",
    "/v1/sessions/:sessionId/interrupt",
    interruptAgentRun
  ),
  HttpRouter.add(
    "POST",
    "/v1/sessions/:sessionId/runs/interrupt",
    interruptAgentRun
  ),
  HttpRouter.add("POST", "/v1/sessions/:sessionId/settle", settleSession),
  HttpRouter.add("POST", "/v1/sessions/:sessionId/reopen", reopenSession),
  HttpRouter.add("POST", "/v1/sessions/:sessionId/head", checkout),
  HttpRouter.add("POST", "/v1/sessions/:sessionId/tree", checkout),
  HttpRouter.add(
    "GET",
    "/v1/sessions/:sessionId/activity",
    sessionActivity
  ),
  HttpRouter.add(
    "GET",
    "/v1/sessions/:sessionId/events",
    sessionActivity
  )
)

export interface Options {
  readonly host: string
  readonly port: number
}

export const layerWithoutDependencies = ({ host, port }: Options) =>
  HttpRouter.serve(routes).pipe(
    Layer.provide(
      Application.layerWithoutDependencies.pipe(
        Layer.provide(AgentRuntime.layerWithoutDependencies)
      )
    ),
    Layer.provide(BunCrypto.layer),
    Layer.provide(BunHttpServer.layer({ hostname: host, port }))
  )

export const layer = (options: Options) =>
  layerWithoutDependencies(options).pipe(
    Layer.provide(Agent.layer),
    Layer.provide(Session.layer())
  )

export const run = (options: Options) => Layer.launch(layer(options))
