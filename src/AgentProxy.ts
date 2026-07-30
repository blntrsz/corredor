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
  readonly createSession: (
    sessionId?: string
  ) => Effect.Effect<Session.SessionCreated, ProxyError>
  readonly submitUserCommit: (
    sessionId: string,
    content: string,
    commitId: string
  ) => Effect.Effect<
    Extract<Session.Commit, { readonly type: "UserCommit" }>,
    ProxyError
  >
  readonly listSessions: () => Effect.Effect<
    ReadonlyArray<Session.SessionSummary>,
    ProxyError
  >
  readonly history: (
    sessionId: string
  ) => Effect.Effect<Session.HistorySnapshot, ProxyError>
  readonly checkout: (
    sessionId: string,
    commitId: string | null
  ) => Effect.Effect<void, ProxyError>
  readonly streamActivity: (
    sessionId: string,
    after?: number
  ) => Stream.Stream<Session.HistoryItem, ProxyError>
}

export class Service extends Context.Service<Service, Interface>()(
  "@corredor/AgentProxy"
) {}

const CreateSessionResponse = Schema.Struct({
  session: Session.SessionCreatedSchema
})
const CommitResponse = Schema.Struct({ commit: Session.CommitSchema })
const SessionsResponse = Schema.Struct({
  sessions: Schema.Array(Session.SessionSummarySchema)
})
const HistoryResponse = Schema.Struct({
  history: Session.HistorySnapshotSchema
})

const proxyError = (cause: unknown): ProxyError => new ProxyError({
  message: cause instanceof Error ? cause.message : String(cause)
})

interface SseParserState {
  readonly data: ReadonlyArray<string>
}

const parseActivityStream = (
  bytes: Stream.Stream<Uint8Array, unknown>
): Stream.Stream<Session.HistoryItem, ProxyError> => bytes.pipe(
  Stream.decodeText,
  Stream.splitLines,
  Stream.mapAccum(
    (): SseParserState => ({ data: [] }),
    (state, line) => {
      if (line === "") {
        return [
          { data: [] },
          state.data.length === 0 ? [] : [state.data.join("\n")]
        ] as const
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
    Effect.flatMap(Schema.decodeUnknownEffect(Session.HistoryItemSchema)),
    Effect.mapError(proxyError)
  )),
  Stream.mapError(proxyError)
)

export const make = (baseUrl: string) => Effect.gen(function*() {
  const client = yield* HttpClient.HttpClient
  const url = (path: string): string => `${baseUrl}${path}`

  const execute = (request: HttpClientRequest.HttpClientRequest) =>
    client.execute(request).pipe(Effect.mapError(proxyError))

  const responseJson = Effect.fn("AgentProxy.responseJson")(function*(
    request: HttpClientRequest.HttpClientRequest
  ) {
    const response = yield* execute(request)
    if (response.status < 200 || response.status >= 300) {
      const detail = yield* response.text.pipe(
        Effect.catch(() => Effect.succeed(""))
      )
      return yield* new ProxyError({
        message: `Corredor API returned ${response.status}${
          detail.length > 0 ? `: ${detail}` : ""
        }`
      })
    }
    return yield* response.json.pipe(Effect.mapError(proxyError))
  })

  const openActivityStream = Effect.fn("AgentProxy.openActivityStream")(
    function*(sessionId: string, after: number) {
      const request = HttpClientRequest.get(
        url(
          `/v1/sessions/${encodeURIComponent(sessionId)}/activity?after=${after}`
        )
      ).pipe(HttpClientRequest.accept("text/event-stream"))
      const response = yield* execute(request)
      if (response.status < 200 || response.status >= 300) {
        return yield* new ProxyError({
          message: `Activity stream returned ${response.status}`
        })
      }
      return response
    }
  )

  return Service.of({
    createSession: Effect.fn("AgentProxy.createSession")(
      function*(sessionId?: string) {
        const body = yield* responseJson(
          HttpClientRequest.post(url("/v1/sessions")).pipe(
            HttpClientRequest.bodyJsonUnsafe(
              sessionId === undefined ? {} : { sessionId }
            )
          )
        )
        const decoded = yield* Schema.decodeUnknownEffect(
          CreateSessionResponse
        )(body).pipe(Effect.mapError(proxyError))
        return decoded.session as Session.SessionCreated
      }
    ),
    submitUserCommit: Effect.fn("AgentProxy.submitUserCommit")(
      function*(sessionId: string, content: string, commitId: string) {
        const body = yield* responseJson(HttpClientRequest.post(
          url(`/v1/sessions/${encodeURIComponent(sessionId)}/commits`)
        ).pipe(HttpClientRequest.bodyJsonUnsafe({ commitId, content })))
        const decoded = yield* Schema.decodeUnknownEffect(CommitResponse)(body)
          .pipe(Effect.mapError(proxyError))
        return decoded.commit as Extract<
          Session.Commit,
          { readonly type: "UserCommit" }
        >
      }
    ),
    listSessions: Effect.fn("AgentProxy.listSessions")(function*() {
      const body = yield* responseJson(
        HttpClientRequest.get(url("/v1/sessions"))
      )
      const decoded = yield* Schema.decodeUnknownEffect(SessionsResponse)(body)
        .pipe(Effect.mapError(proxyError))
      return decoded.sessions as ReadonlyArray<Session.SessionSummary>
    }),
    history: Effect.fn("AgentProxy.history")(function*(sessionId: string) {
      const body = yield* responseJson(HttpClientRequest.get(
        url(`/v1/sessions/${encodeURIComponent(sessionId)}/history`)
      ))
      const decoded = yield* Schema.decodeUnknownEffect(HistoryResponse)(body)
        .pipe(Effect.mapError(proxyError))
      return decoded.history as Session.HistorySnapshot
    }),
    checkout: Effect.fn("AgentProxy.checkout")(function*(
      sessionId: string,
      commitId: string | null
    ) {
      const response = yield* execute(HttpClientRequest.post(
        url(`/v1/sessions/${encodeURIComponent(sessionId)}/head`)
      ).pipe(HttpClientRequest.bodyJsonUnsafe({ commitId })))
      if (response.status < 200 || response.status >= 300) {
        const detail = yield* response.text.pipe(
          Effect.catch(() => Effect.succeed(""))
        )
        return yield* new ProxyError({
          message: `Corredor API returned ${response.status}${
            detail.length > 0 ? `: ${detail}` : ""
          }`
        })
      }
    }),
    streamActivity: (sessionId, after = 0) => {
      let cursor = after
      const connection = Stream.suspend(() => Stream.unwrap(
        openActivityStream(sessionId, cursor).pipe(
          Effect.map((response) => parseActivityStream(response.stream))
        )
      ).pipe(
        Stream.tap((item) => Effect.sync(() => {
          cursor = Math.max(cursor, item.position)
        })),
        Stream.concat(Stream.fail(
          new ProxyError({ message: "Activity stream closed" })
        ))
      ))

      return connection.pipe(Stream.retry(Schedule.spaced("500 millis")))
    }
  })
})

export const layer = (options: { readonly baseUrl: string }) =>
  Layer.effect(Service, make(options.baseUrl)).pipe(
    Layer.provide(FetchHttpClient.layer)
  )
