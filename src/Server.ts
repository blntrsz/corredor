import { BunHttpServer } from "@effect/platform-bun"
import { Effect, Layer, Schema } from "effect"
import {
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse
} from "effect/unstable/http"
import { Agent } from "./Agent.ts"
import * as Session from "./Session.ts"
import * as SessionWorkflow from "./SessionWorkflow.ts"

const health = Session.Service.pipe(
  Effect.flatMap((store) => store.check),
  Effect.match({
    onFailure: () => HttpServerResponse.jsonUnsafe(
      { status: "unavailable" },
      { status: 503 }
    ),
    onSuccess: () => HttpServerResponse.jsonUnsafe({ status: "ok" })
  })
)

const createSession = Effect.gen(function*() {
  const workflow = yield* SessionWorkflow.Service
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({
    sessionId: Schema.optional(Schema.String)
  }))(yield* request.json)
  const sessionId = yield* workflow.createSession(body.sessionId)
  return HttpServerResponse.jsonUnsafe({ sessionId }, { status: 201 })
}).pipe(Effect.orDie)

const submitMessage = Effect.gen(function*() {
  const workflow = yield* SessionWorkflow.Service
  const params = yield* HttpRouter.params
  const request = yield* HttpServerRequest.HttpServerRequest
  const body = yield* Schema.decodeUnknownEffect(Schema.Struct({ content: Schema.String }))(yield* request.json)
  const sessionId = params.sessionId
  if (sessionId === undefined) return HttpServerResponse.jsonUnsafe({ error: "missing session id" }, { status: 400 })
  const reply = yield* workflow.submit(sessionId, body.content)
  return HttpServerResponse.jsonUnsafe({ event: reply })
}).pipe(Effect.orDie)

const sessionEvents = Effect.gen(function*() {
  const store = yield* Session.Service
  const params = yield* HttpRouter.params
  const sessionId = params.sessionId
  if (sessionId === undefined) return HttpServerResponse.jsonUnsafe({ error: "missing session id" }, { status: 400 })
  return HttpServerResponse.jsonUnsafe({ events: yield* store.events(sessionId) })
}).pipe(Effect.orDie)

const routes = Layer.mergeAll(
  HttpRouter.add("GET", "/v1/health", health),
  HttpRouter.add("POST", "/v1/sessions", createSession),
  HttpRouter.add("POST", "/v1/sessions/:sessionId/messages", submitMessage),
  HttpRouter.add("GET", "/v1/sessions/:sessionId/events", sessionEvents)
)

export interface Options {
  readonly host: string
  readonly port: number
}

export const layer = ({ host, port }: Options) =>
  HttpRouter.serve(routes).pipe(
    Layer.provide(SessionWorkflow.layer),
    Layer.provide(Agent.layer),
    Layer.provide(Session.layer()),
    Layer.provide(BunHttpServer.layer({ hostname: host, port }))
  )

export const run = (options: Options) => Layer.launch(layer(options))
