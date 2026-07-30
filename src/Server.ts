import { BunHttpServer } from "@effect/platform-bun"
import { Effect, Layer, Option, Schema, Stream } from "effect"
import {
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse
} from "effect/unstable/http"
import { Agent } from "./Agent.ts"
import * as AgentRuntime from "./AgentRuntime.ts"
import * as Session from "./Session.ts"
import * as SessionCommands from "./SessionCommands.ts"

const health = Session.Service.pipe(
  Effect.flatMap((store) => store.check),
  Effect.match({
    onFailure: () => HttpServerResponse.jsonUnsafe(
      { status: "unavailable", service: "corredor", apiVersion: 3 },
      { status: 503 }
    ),
    onSuccess: () => HttpServerResponse.jsonUnsafe({
      status: "ok",
      service: "corredor",
      apiVersion: 3
    })
  })
)

const createSession = Effect.gen(function*() {
  const commands = yield* SessionCommands.Service
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    sessionId: Schema.optional(Schema.String)
  }))(yield* request.json)
  const event = yield* commands.createSession(body.sessionId)
  return HttpServerResponse.jsonUnsafe({ sessionId: event.sessionId, event }, { status: 201 })
}).pipe(Effect.orDie)

const listSessions = Effect.gen(function*() {
  const commands = yield* SessionCommands.Service
  return HttpServerResponse.jsonUnsafe({ sessions: yield* commands.listSessions() })
}).pipe(Effect.orDie)

const submitMessage = Effect.gen(function*() {
  const commands = yield* SessionCommands.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    messageId: Schema.optional(Schema.String),
    content: Schema.String
  }))(yield* request.json)
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return HttpServerResponse.jsonUnsafe({ error: "missing session id" }, { status: 400 })
  }
  const event = yield* commands.addUserMessage(
    sessionId,
    body.messageId ?? crypto.randomUUID(),
    body.content
  )
  return HttpServerResponse.jsonUnsafe({ event }, { status: 202 })
}).pipe(Effect.orDie)

const sessionHistory = Effect.gen(function*() {
  const commands = yield* SessionCommands.Service
  const params = yield* HttpRouter.params
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return HttpServerResponse.jsonUnsafe({ error: "missing session id" }, { status: 400 })
  }
  return HttpServerResponse.jsonUnsafe({ events: yield* commands.events(sessionId) })
}).pipe(Effect.orDie)

const navigateTree = Effect.gen(function*() {
  const commands = yield* SessionCommands.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return HttpServerResponse.jsonUnsafe({ error: "missing session id" }, { status: 400 })
  }
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    targetId: Schema.NullOr(Schema.String)
  }))(yield* request.json)
  const event = yield* commands.navigateTree(sessionId, body.targetId)
  return HttpServerResponse.jsonUnsafe({ event })
}).pipe(Effect.orDie)

const encoder = new TextEncoder()
const encodeSse = (event: Session.StoredEvent): Uint8Array => encoder.encode(
  `id: ${event.position}\nevent: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`
)

interface EventStreamState {
  readonly cursor: number
  readonly lastHeartbeat: number
}

const sessionEvents = Effect.gen(function*() {
  const store = yield* Session.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const sessionId = params.sessionId
  if (sessionId === undefined) {
    return HttpServerResponse.jsonUnsafe({ error: "missing session id" }, { status: 400 })
  }

  const requestedAfter = new URL(request.originalUrl).searchParams.get("after")
  const rawCursor = request.headers["last-event-id"] ?? requestedAfter ?? "0"
  const parsedCursor = Number.parseInt(rawCursor, 10)
  const initialCursor = Number.isFinite(parsedCursor) && parsedCursor >= 0 ? parsedCursor : 0

  const body = Stream.paginate<EventStreamState, Uint8Array, Session.PersistenceError>(
    { cursor: initialCursor, lastHeartbeat: Date.now() },
    (state) => Effect.gen(function*() {
      const events = yield* store.eventsAfter(state.cursor)
      const cursor = events.at(-1)?.position ?? state.cursor
      const matching = events.filter((event) => event.sessionId === sessionId)
      const now = Date.now()

      if (matching.length > 0) {
        return [
          matching.map(encodeSse),
          Option.some({ cursor, lastHeartbeat: now })
        ] as const
      }

      if (events.length === 0) yield* Effect.sleep("100 millis")
      const heartbeatDue = now - state.lastHeartbeat >= 15_000
      return [
        heartbeatDue ? [encoder.encode(": keep-alive\n\n")] : [],
        Option.some({ cursor, lastHeartbeat: heartbeatDue ? now : state.lastHeartbeat })
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
  HttpRouter.add("GET", "/v1/sessions", listSessions),
  HttpRouter.add("POST", "/v1/sessions", createSession),
  HttpRouter.add("POST", "/v1/sessions/:sessionId/messages", submitMessage),
  HttpRouter.add("GET", "/v1/sessions/:sessionId/history", sessionHistory),
  HttpRouter.add("POST", "/v1/sessions/:sessionId/tree", navigateTree),
  HttpRouter.add("GET", "/v1/sessions/:sessionId/events", sessionEvents)
)

export interface Options {
  readonly host: string
  readonly port: number
}

export const layer = ({ host, port }: Options) =>
  Layer.mergeAll(
    HttpRouter.serve(routes),
    AgentRuntime.layer
  ).pipe(
    Layer.provide(SessionCommands.layer),
    Layer.provide(Agent.layer),
    Layer.provide(Session.layer()),
    Layer.provide(BunHttpServer.layer({ hostname: host, port }))
  )

export const run = (options: Options) => Layer.launch(layer(options))
