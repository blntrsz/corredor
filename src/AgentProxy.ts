import { Context, Effect, Layer, Schedule, Schema, Stream } from "effect"
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse
} from "effect/unstable/http"
import { Agent } from "./Agent.ts"
import * as Session from "./Session.ts"

export class ProxyError extends Schema.TaggedErrorClass<ProxyError>()(
  "@corredor/AgentProxy/ProxyError",
  { message: Schema.String }
) {}

export interface Interface {
  readonly createWorkstream: (
    workstreamId?: string,
    name?: string
  ) => Effect.Effect<Session.Workstream, ProxyError>
  readonly listWorkstreams: () => Effect.Effect<
    ReadonlyArray<Session.WorkstreamSummary>,
    ProxyError
  >
  readonly workstream: (
    workstreamId: string,
    view?: Session.SessionListView
  ) => Effect.Effect<Session.WorkstreamSnapshot, ProxyError>
  readonly createSession: (
    sessionId?: string,
    workstreamId?: string
  ) => Effect.Effect<Session.SessionCreated, ProxyError>
  readonly settle: (
    sessionId: string
  ) => Effect.Effect<Session.SessionSettled, ProxyError>
  readonly reopen: (
    sessionId: string
  ) => Effect.Effect<Session.SessionReopened, ProxyError>
  readonly submitUserCommit: (
    sessionId: string,
    content: string,
    commitId: string
  ) => Effect.Effect<
    Extract<Session.Commit, { readonly type: "UserCommit" }>,
    ProxyError
  >
  readonly startAgentRun: (
    sessionId: string,
    startingCommitId: string,
    definition: Agent.Definition,
    runId?: string
  ) => Effect.Effect<void, ProxyError>
  readonly interruptAgentRun: (
    sessionId: string,
    startingCommitId: string,
    reason?: string,
    runId?: string
  ) => Effect.Effect<
    Extract<Session.Commit, { readonly type: "InterruptCommit" }> | undefined,
    ProxyError
  >
  readonly listSessions: (
    workstreamId?: string,
    view?: Session.SessionListView
  ) => Effect.Effect<
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
const CreateWorkstreamResponse = Schema.Struct({
  workstream: Session.WorkstreamSchema
})
const CommitResponse = Schema.Struct({ commit: Session.CommitSchema })
const InterruptResponse = Schema.Struct({
  commit: Schema.NullOr(Session.CommitSchema)
})
const SessionsResponse = Schema.Struct({
  sessions: Schema.Array(Session.SessionSummarySchema)
})
const WorkstreamsResponse = Schema.Struct({
  workstreams: Schema.Array(Session.WorkstreamSummarySchema)
})
const HistoryResponse = Schema.Struct({
  history: Session.HistorySnapshotSchema
})
const WorkstreamResponse = Session.WorkstreamSnapshotSchema
const LifecycleResponse = Schema.Union([
  Schema.Struct({ event: Session.SessionSettledSchema }),
  Schema.Struct({ event: Session.SessionReopenedSchema })
])

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

export const make = (
  baseUrl: string,
  peerId = Session.defaultPeerId
) => Effect.gen(function*() {
  const client = yield* HttpClient.HttpClient
  const url = (path: string): string => `${baseUrl}${path}`
  const withPeer = (request: HttpClientRequest.HttpClientRequest) =>
    request.pipe(HttpClientRequest.setHeader("x-corredor-peer-id", peerId))

  const execute = (request: HttpClientRequest.HttpClientRequest) =>
    client.execute(request).pipe(Effect.mapError(proxyError))

  const requireSuccess = Effect.fn("AgentProxy.requireSuccess")(function*(
    response: HttpClientResponse.HttpClientResponse,
    operation = "Corredor API"
  ) {
    if (response.status < 200 || response.status >= 300) {
      const detail = yield* response.text.pipe(
        Effect.catch(() => Effect.succeed(""))
      )
      return yield* new ProxyError({
        message: `${operation} returned ${response.status}${
          detail.length > 0 ? `: ${detail}` : ""
        }`
      })
    }
    return response
  })

  const responseJson = Effect.fn("AgentProxy.responseJson")(function*(
    request: HttpClientRequest.HttpClientRequest
  ) {
    const response = yield* requireSuccess(yield* execute(request))
    return yield* response.json.pipe(Effect.mapError(proxyError))
  })

  const openActivityStream = Effect.fn("AgentProxy.openActivityStream")(
    function*(sessionId: string, after: number) {
      const request = withPeer(HttpClientRequest.get(
        url(
          `/v1/sessions/${encodeURIComponent(sessionId)}/activity?after=${after}`
        )
      ).pipe(HttpClientRequest.accept("text/event-stream")))
      return yield* requireSuccess(yield* execute(request), "Activity stream")
    }
  )

  return Service.of({
    createWorkstream: Effect.fn("AgentProxy.createWorkstream")(
      function*(workstreamId?: string, name?: string) {
        const body = yield* responseJson(
          withPeer(HttpClientRequest.post(url("/v1/workstreams"))).pipe(
            HttpClientRequest.bodyJsonUnsafe({
              ...(workstreamId === undefined ? {} : { workstreamId }),
              ...(name === undefined ? {} : { name })
            })
          )
        )
        const decoded = yield* Schema.decodeUnknownEffect(CreateWorkstreamResponse)(body)
          .pipe(Effect.mapError(proxyError))
        return decoded.workstream as Session.Workstream
      }
    ),
    listWorkstreams: Effect.fn("AgentProxy.listWorkstreams")(function*() {
      const body = yield* responseJson(
        HttpClientRequest.get(url("/v1/workstreams"))
      )
      const decoded = yield* Schema.decodeUnknownEffect(WorkstreamsResponse)(body)
        .pipe(Effect.mapError(proxyError))
      return decoded.workstreams as ReadonlyArray<Session.WorkstreamSummary>
    }),
    workstream: Effect.fn("AgentProxy.workstream")(function*(
      workstreamId: string,
      view: Session.SessionListView = "active"
    ) {
      const query = view === "active" ? "" : `?state=${view}`
      const body = yield* responseJson(HttpClientRequest.get(
        url(`/v1/workstreams/${encodeURIComponent(workstreamId)}${query}`)
      ))
      const decoded = yield* Schema.decodeUnknownEffect(WorkstreamResponse)(body)
        .pipe(Effect.mapError(proxyError))
      return decoded as Session.WorkstreamSnapshot
    }),
    createSession: Effect.fn("AgentProxy.createSession")(
      function*(sessionId?: string, workstreamId?: string) {
        const body = yield* responseJson(
          withPeer(HttpClientRequest.post(url("/v1/sessions"))).pipe(
            HttpClientRequest.bodyJsonUnsafe(
              {
                ...(sessionId === undefined ? {} : { sessionId }),
                ...(workstreamId === undefined ? {} : { workstreamId })
              }
            )
          )
        )
        const decoded = yield* Schema.decodeUnknownEffect(
          CreateSessionResponse
        )(body).pipe(Effect.mapError(proxyError))
        return decoded.session as Session.SessionCreated
      }
    ),
    settle: Effect.fn("AgentProxy.settle")(function*(sessionId: string) {
      const body = yield* responseJson(withPeer(HttpClientRequest.post(
        url(`/v1/sessions/${encodeURIComponent(sessionId)}/settle`)
      )))
      const decoded = yield* Schema.decodeUnknownEffect(LifecycleResponse)(body)
        .pipe(Effect.mapError(proxyError))
      return decoded.event as Session.SessionSettled
    }),
    reopen: Effect.fn("AgentProxy.reopen")(function*(sessionId: string) {
      const body = yield* responseJson(withPeer(HttpClientRequest.post(
        url(`/v1/sessions/${encodeURIComponent(sessionId)}/reopen`)
      )))
      const decoded = yield* Schema.decodeUnknownEffect(LifecycleResponse)(body)
        .pipe(Effect.mapError(proxyError))
      return decoded.event as Session.SessionReopened
    }),
    submitUserCommit: Effect.fn("AgentProxy.submitUserCommit")(
      function*(sessionId: string, content: string, commitId: string) {
        const body = yield* responseJson(withPeer(HttpClientRequest.post(
          url(`/v1/sessions/${encodeURIComponent(sessionId)}/commits`)
        ).pipe(HttpClientRequest.bodyJsonUnsafe({ commitId, content }))))
        const decoded = yield* Schema.decodeUnknownEffect(CommitResponse)(body)
          .pipe(Effect.mapError(proxyError))
        return decoded.commit as Extract<
          Session.Commit,
          { readonly type: "UserCommit" }
        >
      }
    ),
    startAgentRun: Effect.fn("AgentProxy.startAgentRun")(
      function*(
        sessionId: string,
        startingCommitId: string,
        definition: Agent.Definition,
        runId?: string
      ) {
        yield* responseJson(withPeer(HttpClientRequest.post(
          url(`/v1/sessions/${encodeURIComponent(sessionId)}/runs`)
        ).pipe(HttpClientRequest.bodyJsonUnsafe({
          commitId: startingCommitId,
          agent: definition,
          ...(runId === undefined ? {} : { runId })
        }))))
      }
    ),
    interruptAgentRun: Effect.fn("AgentProxy.interruptAgentRun")(
      function*(
        sessionId: string,
        startingCommitId: string,
        reason?: string,
        runId?: string
      ) {
        const body = yield* responseJson(withPeer(HttpClientRequest.post(
          url(`/v1/sessions/${encodeURIComponent(sessionId)}/interrupt`)
        ).pipe(HttpClientRequest.bodyJsonUnsafe({
          commitId: startingCommitId,
          ...(reason === undefined ? {} : { reason }),
          ...(runId === undefined ? {} : { runId })
        }))))
        const decoded = yield* Schema.decodeUnknownEffect(InterruptResponse)(body)
          .pipe(Effect.mapError(proxyError))
        return decoded.commit === null
          ? undefined
          : decoded.commit as Extract<
            Session.Commit,
            { readonly type: "InterruptCommit" }
          >
      }
    ),
    listSessions: Effect.fn("AgentProxy.listSessions")(function*(
      workstreamId?: string,
      view: Session.SessionListView = "active"
    ) {
      const params = new URLSearchParams()
      if (workstreamId !== undefined) params.set("workstreamId", workstreamId)
      if (view !== "active") params.set("state", view)
      const encodedQuery = params.toString()
      const query = encodedQuery.length === 0 ? "" : `?${encodedQuery}`
      const body = yield* responseJson(
        HttpClientRequest.get(url(`/v1/sessions${query}`))
      )
      const decoded = yield* Schema.decodeUnknownEffect(SessionsResponse)(body)
        .pipe(Effect.mapError(proxyError))
      return decoded.sessions as ReadonlyArray<Session.SessionSummary>
    }),
    history: Effect.fn("AgentProxy.history")(function*(sessionId: string) {
      const body = yield* responseJson(withPeer(HttpClientRequest.get(
        url(`/v1/sessions/${encodeURIComponent(sessionId)}/history`)
      )))
      const decoded = yield* Schema.decodeUnknownEffect(HistoryResponse)(body)
        .pipe(Effect.mapError(proxyError))
      return decoded.history as Session.HistorySnapshot
    }),
    checkout: Effect.fn("AgentProxy.checkout")(function*(
      sessionId: string,
      commitId: string | null
    ) {
      yield* requireSuccess(yield* execute(withPeer(HttpClientRequest.post(
        url(`/v1/sessions/${encodeURIComponent(sessionId)}/head`)
      ).pipe(HttpClientRequest.bodyJsonUnsafe({ commitId })))))
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

export const layer = (options: {
  readonly baseUrl: string
  readonly peerId?: string
}) =>
  Layer.effect(Service, make(options.baseUrl, options.peerId)).pipe(
    Layer.provide(FetchHttpClient.layer)
  )
