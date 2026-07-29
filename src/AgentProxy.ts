import { Context, Effect, Layer, Schedule, Schema, Stream } from "effect"
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest
} from "effect/unstable/http"
import * as Session from "./Session.ts"

export class ProxyError extends Schema.TaggedErrorClass<ProxyError>()(
  "@corredor/AgentProxy/ProxyError",
  { message: Schema.String }
) {}

export interface Interface {
  readonly createSession: (sessionId?: string) => Effect.Effect<Session.StoredEvent, ProxyError>
  readonly sendMessage: (
    sessionId: string,
    messageId: string,
    content: string
  ) => Effect.Effect<Session.StoredEvent, ProxyError>
  readonly listSessions: () => Effect.Effect<ReadonlyArray<Session.SessionSummary>, ProxyError>
  readonly streamEvents: (
    sessionId: string,
    after?: number
  ) => Stream.Stream<Session.StoredEvent, ProxyError>
}

export class Service extends Context.Service<Service, Interface>()("@corredor/AgentProxy") {}

const CreateSessionResponse = Schema.Struct({
  sessionId: Schema.String,
  event: Session.StoredEventSchema
})
const EventResponse = Schema.Struct({ event: Session.StoredEventSchema })
const SessionsResponse = Schema.Struct({ sessions: Schema.Array(Session.SessionSummarySchema) })

const proxyError = (cause: unknown): ProxyError => new ProxyError({
  message: cause instanceof Error ? cause.message : String(cause)
})

interface SseParserState {
  readonly data: ReadonlyArray<string>
}

const parseEventStream = (
  bytes: Stream.Stream<Uint8Array, unknown>
): Stream.Stream<Session.StoredEvent, ProxyError> => bytes.pipe(
  Stream.decodeText,
  Stream.splitLines,
  Stream.mapAccum(
    (): SseParserState => ({ data: [] }),
    (state, line) => {
      if (line === "") {
        return [{ data: [] }, state.data.length === 0 ? [] : [state.data.join("\n")]] as const
      }
      if (line.startsWith("data:")) {
        const value = line.startsWith("data: ") ? line.slice(6) : line.slice(5)
        return [{ data: [...state.data, value] }, []] as const
      }
      return [state, []] as const
    }
  ),
  Stream.mapEffect((data) => Effect.try({
    try: () => JSON.parse(data),
    catch: proxyError
  }).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Session.StoredEventSchema)),
    Effect.mapError(proxyError)
  )),
  Stream.mapError(proxyError)
)

export const make = (baseUrl: string) => Effect.gen(function*() {
  const client = yield* HttpClient.HttpClient
  const url = (path: string): string => `${baseUrl}${path}`

  const responseJson = (request: HttpClientRequest.HttpClientRequest): Effect.Effect<unknown, ProxyError> =>
    Effect.gen(function*() {
      const response = yield* client.execute(request).pipe(Effect.mapError(proxyError))
      if (response.status < 200 || response.status >= 300) {
        const detail = yield* response.text.pipe(Effect.catch(() => Effect.succeed("")))
        return yield* new ProxyError({
          message: `Corredor API returned ${response.status}${detail.length > 0 ? `: ${detail}` : ""}`
        })
      }
      return yield* response.json.pipe(Effect.mapError(proxyError))
    })

  const openEventStream = (sessionId: string, after: number) => Effect.gen(function*() {
    const request = HttpClientRequest.get(
      url(`/v1/sessions/${encodeURIComponent(sessionId)}/events?after=${after}`)
    ).pipe(HttpClientRequest.accept("text/event-stream"))
    const response = yield* client.execute(request).pipe(Effect.mapError(proxyError))
    if (response.status < 200 || response.status >= 300) {
      return yield* new ProxyError({ message: `Event stream returned ${response.status}` })
    }
    return response
  })

  return Service.of({
    createSession: (sessionId) => Effect.gen(function*() {
      const body = yield* responseJson(HttpClientRequest.post(url("/v1/sessions")).pipe(
        HttpClientRequest.bodyJsonUnsafe(sessionId === undefined ? {} : { sessionId })
      ))
      const decoded = yield* Schema.decodeUnknownEffect(CreateSessionResponse)(body).pipe(Effect.mapError(proxyError))
      return decoded.event as Session.StoredEvent
    }),
    sendMessage: (sessionId, messageId, content) => Effect.gen(function*() {
      const body = yield* responseJson(HttpClientRequest.post(
        url(`/v1/sessions/${encodeURIComponent(sessionId)}/messages`)
      ).pipe(HttpClientRequest.bodyJsonUnsafe({ messageId, content })))
      const decoded = yield* Schema.decodeUnknownEffect(EventResponse)(body).pipe(Effect.mapError(proxyError))
      return decoded.event as Session.StoredEvent
    }),
    listSessions: () => Effect.gen(function*() {
      const body = yield* responseJson(HttpClientRequest.get(url("/v1/sessions")))
      const decoded = yield* Schema.decodeUnknownEffect(SessionsResponse)(body).pipe(Effect.mapError(proxyError))
      return decoded.sessions as ReadonlyArray<Session.SessionSummary>
    }),
    streamEvents: (sessionId, after = 0) => {
      let cursor = after
      const connection = Stream.suspend(() => Stream.unwrap(
        openEventStream(sessionId, cursor).pipe(
          Effect.map((response) => parseEventStream(response.stream))
        )
      ).pipe(
        Stream.tap((event) => Effect.sync(() => {
          cursor = Math.max(cursor, event.position)
        })),
        Stream.concat(Stream.fail(new ProxyError({ message: "Event stream closed" })))
      ))

      return connection.pipe(Stream.retry(Schedule.spaced("500 millis")))
    }
  })
})

export const layer = (options: { readonly baseUrl: string }) =>
  Layer.effect(Service, make(options.baseUrl)).pipe(
    Layer.provide(FetchHttpClient.layer)
  )
