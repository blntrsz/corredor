import { expect, it } from "@effect/vitest"
import { BunCrypto } from "@effect/platform-bun"
import { createServer } from "node:net"
import { Effect, Fiber, Layer, Stream } from "effect"
import {
  FetchHttpClient,
  HttpClient,
  HttpClientError
} from "effect/unstable/http"
import { Agent } from "./Agent.ts"
import * as AgentRuntime from "./AgentRuntime.ts"
import * as AgentProxy from "./AgentProxy.ts"
import * as Application from "./Application.ts"
import * as Server from "./Server.ts"
import * as Session from "./Session.ts"
import { temporaryDatabase } from "./TestSupport.ts"

const ignoredAgent = Layer.succeed(Agent.Service, Agent.Service.of({
  run: () => Effect.succeed("ignored")
}))

const applicationLayer = (
  path: string,
  fakeAgent: Layer.Layer<Agent.Service> = ignoredAgent
) => Application.layerWithoutDependencies.pipe(
  Layer.provide(AgentRuntime.layerWithoutDependencies),
  Layer.provide(Session.layer(path)),
  Layer.provide(fakeAgent),
  Layer.provide(BunCrypto.layer)
)

const responseLossHttpClientLayer = Layer.effect(
  HttpClient.HttpClient,
  Effect.gen(function*() {
    const client = yield* HttpClient.HttpClient
    let loseImportResponse = true
    return client.pipe(
      HttpClient.transform((effect, request) => Effect.gen(function*() {
        const response = yield* effect
        if (loseImportResponse && request.url.includes("/v1/sync/import")) {
          loseImportResponse = false
          return yield* Effect.fail(new HttpClientError.HttpClientError({
            reason: new HttpClientError.TransportError({
              request,
              description: "simulated response loss"
            })
          }))
        }
        return response
      }))
    )
  })
).pipe(Layer.provide(FetchHttpClient.layer))

const availablePort = async (): Promise<number> => {
  const server = createServer()
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", resolve)
  })
  const address = server.address()
  if (address === null || typeof address === "string") {
    throw new Error("could not reserve a local test port")
  }
  await new Promise<void>((resolve, reject) => {
    server.close((error) => error === undefined ? resolve() : reject(error))
  })
  return address.port
}

const waitForOutcome = (
  proxy: AgentProxy.Interface,
  sessionId: string,
  inReplyTo: string
) => Effect.gen(function*() {
  for (let attempt = 0; attempt < 100; attempt++) {
    const history = yield* proxy.history(sessionId)
    if (history.items.some(
      (item) =>
        (item.type === "AgentMessageCommit" ||
          item.type === "CompactionCommit" ||
          item.type === "FailureCommit" ||
          item.type === "InterruptCommit") &&
        item.inReplyTo === inReplyTo
    )) return history
    yield* Effect.sleep("10 millis")
  }
  return yield* Effect.die(`timed out waiting for ${inReplyTo}`)
})

it.live("transfers a selected Branch through the public application boundary", () =>
  Effect.gen(function*() {
    const sourceDatabase = yield* temporaryDatabase("corredor-sync-source-")
    const targetDatabase = yield* temporaryDatabase("corredor-sync-target-")
    const source = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.createWorkstream("sync-workstream", "Shared context", "peer-a")
      const session = yield* application.createSession(
        "sync-session",
        "sync-workstream",
        "peer-a"
      )
      const commit = yield* application.submitUserCommit(
        session.sessionId,
        "source context",
        "source-user",
        "peer-a"
      )
      return yield* application.exportBranch(
        session.sessionId,
        commit.commitId,
        "peer-a"
      )
    }).pipe(Effect.provide(applicationLayer(sourceDatabase.path)))

    const firstImport = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* application.importBranch(source, "peer-b")
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const beforeRetry = yield* application.history("sync-session", "peer-b")
      const retry = yield* application.importBranch(source, "peer-b")
      const afterRetry = yield* application.history("sync-session", "peer-b")
      const workstream = yield* application.workstream("sync-workstream")
      return { beforeRetry, retry, afterRetry, workstream }
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    expect(firstImport).toMatchObject({
      sessionId: "sync-session",
      workstreamId: "sync-workstream",
      branchHeadId: "source-user",
      importedCommitIds: ["source-user"]
    })
    expect(result.beforeRetry.branchHeadId).toBeNull()
    expect(result.beforeRetry.items).toContainEqual(
      expect.objectContaining({
        type: "UserCommit",
        commitId: "source-user",
        parentId: null,
        content: "source context"
      })
    )
    expect(result.retry.importedCommitIds).toEqual([])
    expect(result.afterRetry.items).toEqual(result.beforeRetry.items)
    expect(result.workstream.workstream).toMatchObject({
      workstreamId: "sync-workstream",
      name: "Shared context",
      peerId: "peer-a"
    })
  }))

it.live("rejects an invalid ancestry without partially importing it", () =>
  Effect.gen(function*() {
    const sourceDatabase = yield* temporaryDatabase("corredor-sync-invalid-source-")
    const targetDatabase = yield* temporaryDatabase("corredor-sync-invalid-target-")
    const source = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.createWorkstream(
        "invalid-sync-workstream",
        "Invalid sync",
        "peer-a"
      )
      const session = yield* application.createSession(
        "invalid-sync-session",
        "invalid-sync-workstream",
        "peer-a"
      )
      const commit = yield* application.submitUserCommit(
        session.sessionId,
        "source context",
        "invalid-sync-root",
        "peer-a"
      )
      return yield* application.exportBranch(
        session.sessionId,
        commit.commitId,
        "peer-a"
      )
    }).pipe(Effect.provide(applicationLayer(sourceDatabase.path)))

    const malformed: Session.SyncBundle = {
      ...source,
      records: source.records.map((record) => ({
        ...record,
        parentId: "foreign-parent"
      }))
    }
    const error = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.createWorkstream(
        "foreign-sync-workstream",
        "Foreign sync",
        "peer-b"
      )
      const foreignSession = yield* application.createSession(
        "foreign-sync-session",
        "foreign-sync-workstream",
        "peer-b"
      )
      yield* application.submitUserCommit(
        foreignSession.sessionId,
        "foreign parent",
        "foreign-parent",
        "peer-b"
      )
      return yield* Effect.flip(application.importBranch(malformed, "peer-b"))
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    const target = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return {
        workstreams: yield* application.listWorkstreams(),
        sessions: yield* application.listSessions("foreign-sync-workstream")
      }
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    expect(error).toBeInstanceOf(Session.SyncValidationError)
    expect(target.workstreams).not.toContainEqual(
      expect.objectContaining({
        workstreamId: "invalid-sync-workstream"
      })
    )
    expect(target.sessions).toContainEqual(
      expect.objectContaining({ sessionId: "foreign-sync-session" })
    )
  }))

it.live("rejects a retry with a different Session creation identity", () =>
  Effect.gen(function*() {
    const sourceDatabase = yield* temporaryDatabase("corredor-sync-identity-source-")
    const targetDatabase = yield* temporaryDatabase("corredor-sync-identity-target-")
    const source = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.createWorkstream(
        "identity-sync-workstream",
        "Identity sync",
        "peer-a"
      )
      const session = yield* application.createSession(
        "identity-sync-session",
        "identity-sync-workstream",
        "peer-a"
      )
      const commit = yield* application.submitUserCommit(
        session.sessionId,
        "source context",
        "identity-sync-root",
        "peer-a"
      )
      return yield* application.exportBranch(
        session.sessionId,
        commit.commitId,
        "peer-a"
      )
    }).pipe(Effect.provide(applicationLayer(sourceDatabase.path)))

    const firstImport = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* application.importBranch(source, "peer-b")
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    const malformed: Session.SyncBundle = {
      ...source,
      session: {
        ...source.session,
        createdEventId: "different-session-created-event"
      }
    }
    const error = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* Effect.flip(application.importBranch(malformed, "peer-b"))
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    const afterRejectedRetry = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return {
        history: yield* application.history(source.session.sessionId, "peer-b"),
        sessions: yield* application.listSessions("identity-sync-workstream")
      }
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    expect(firstImport.importedCommitIds).toEqual(["identity-sync-root"])
    expect(error).toBeInstanceOf(Session.SyncConflict)
    expect(afterRejectedRetry.history.items).toContainEqual(
      expect.objectContaining({ commitId: "identity-sync-root" })
    )
    expect(afterRejectedRetry.sessions).toHaveLength(1)
  }))

it.live("rejects records outside the selected Branch ancestry", () =>
  Effect.gen(function*() {
    const sourceDatabase = yield* temporaryDatabase("corredor-sync-branch-source-")
    const targetDatabase = yield* temporaryDatabase("corredor-sync-branch-target-")
    const source = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.createWorkstream(
        "branch-sync-workstream",
        "Branch sync",
        "peer-a"
      )
      const session = yield* application.createSession(
        "branch-sync-session",
        "branch-sync-workstream",
        "peer-a"
      )
      yield* application.submitUserCommit(
        session.sessionId,
        "root context",
        "branch-sync-root",
        "peer-a"
      )
      const child = yield* application.submitUserCommit(
        session.sessionId,
        "child context",
        "branch-sync-child",
        "peer-a"
      )
      return yield* application.exportBranch(
        session.sessionId,
        child.commitId,
        "peer-a"
      )
    }).pipe(Effect.provide(applicationLayer(sourceDatabase.path)))

    const malformed: Session.SyncBundle = {
      ...source,
      branchHeadId: "branch-sync-root"
    }
    const error = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* Effect.flip(application.importBranch(malformed, "peer-b"))
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    const target = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* application.listWorkstreams()
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    expect(error).toBeInstanceOf(Session.SyncValidationError)
    expect(target).not.toContainEqual(
      expect.objectContaining({
        workstreamId: "branch-sync-workstream"
      })
    )
  }))

it.live("rejects duplicate Session lifecycle identities before writing", () =>
  Effect.gen(function*() {
    const sourceDatabase = yield* temporaryDatabase("corredor-sync-lifecycle-source-")
    const targetDatabase = yield* temporaryDatabase("corredor-sync-lifecycle-target-")
    const source = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.createWorkstream(
        "lifecycle-sync-workstream",
        "Lifecycle sync",
        "peer-a"
      )
      const session = yield* application.createSession(
        "lifecycle-sync-session",
        "lifecycle-sync-workstream",
        "peer-a"
      )
      return yield* application.exportBranch(
        session.sessionId,
        null,
        "peer-a"
      )
    }).pipe(Effect.provide(applicationLayer(sourceDatabase.path)))

    const malformed: Session.SyncBundle = {
      ...source,
      session: {
        ...source.session,
        settledAt: source.session.createdAt,
        settledEventId: source.session.createdEventId
      }
    }
    const error = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* Effect.flip(application.importBranch(malformed, "peer-b"))
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    const target = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* application.listWorkstreams()
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    expect(error).toBeInstanceOf(Session.SyncValidationError)
    expect(target).not.toContainEqual(
      expect.objectContaining({
        workstreamId: "lifecycle-sync-workstream"
      })
    )
  }))

it.live("retries a valid Branch after a validation failure without duplicates", () =>
  Effect.gen(function*() {
    const sourceDatabase = yield* temporaryDatabase("corredor-sync-retry-source-")
    const targetDatabase = yield* temporaryDatabase("corredor-sync-retry-target-")
    const bundles = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.createWorkstream(
        "retry-sync-workstream",
        "Retry sync",
        "peer-a"
      )
      const session = yield* application.createSession(
        "retry-sync-session",
        "retry-sync-workstream",
        "peer-a"
      )
      const root = yield* application.submitUserCommit(
        session.sessionId,
        "root context",
        "retry-sync-root",
        "peer-a"
      )
      const child = yield* application.submitUserCommit(
        session.sessionId,
        "child context",
        "retry-sync-child",
        "peer-a"
      )
      return {
        root: yield* application.exportBranch(
          session.sessionId,
          root.commitId,
          "peer-a"
        ),
        full: yield* application.exportBranch(
          session.sessionId,
          child.commitId,
          "peer-a"
        )
      }
    }).pipe(Effect.provide(applicationLayer(sourceDatabase.path)))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const firstImport = yield* application.importBranch(bundles.root, "peer-b")
      const malformed: Session.SyncBundle = {
        ...bundles.full,
        records: bundles.full.records.map((record) =>
          record.type === "UserCommit" && record.commitId === "retry-sync-child"
            ? { ...record, parentId: "missing-retry-parent" }
            : record
        )
      }
      const validationError = yield* Effect.flip(
        application.importBranch(malformed, "peer-b")
      )
      const retry = yield* application.importBranch(bundles.full, "peer-b")
      const history = yield* application.history(
        "retry-sync-session",
        "peer-b"
      )
      return { firstImport, validationError, retry, history }
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    const commitIds = result.history.items
      .filter(Session.isCommit)
      .map((commit) => commit.commitId)
    expect(result.firstImport.importedCommitIds).toEqual(["retry-sync-root"])
    expect(result.validationError).toBeInstanceOf(Session.SyncValidationError)
    expect(result.retry.importedCommitIds).toEqual(["retry-sync-child"])
    expect(commitIds).toEqual(["retry-sync-root", "retry-sync-child"])
  }))

it.live("retries a Push after losing the import response without duplicates", () =>
  Effect.gen(function*() {
    const sourceDatabase = yield* temporaryDatabase("corredor-sync-transport-source-")
    const targetDatabase = yield* temporaryDatabase("corredor-sync-transport-target-")
    const sourcePort = yield* Effect.promise(availablePort)
    const targetPort = yield* Effect.promise(availablePort)
    const sourceUrl = `http://127.0.0.1:${sourcePort}`
    const targetUrl = `http://127.0.0.1:${targetPort}`
    const sourceServer = Server.layerWithoutDependencies({
      host: "127.0.0.1",
      port: sourcePort
    }).pipe(
      Layer.provide(Session.layer(sourceDatabase.path)),
      Layer.provide(ignoredAgent)
    )
    const targetServer = Server.layerWithoutDependencies({
      host: "127.0.0.1",
      port: targetPort
    }).pipe(
      Layer.provide(Session.layer(targetDatabase.path)),
      Layer.provide(ignoredAgent)
    )

    const result = yield* Effect.gen(function*() {
      yield* Layer.launch(sourceServer).pipe(Effect.forkScoped)
      yield* Layer.launch(targetServer).pipe(Effect.forkScoped)
      yield* Effect.sleep("100 millis")
      const source = yield* AgentProxy.make(sourceUrl, "peer-a").pipe(
        Effect.provide(FetchHttpClient.layer)
      )
      const flakySource = yield* AgentProxy.make(sourceUrl, "peer-a").pipe(
        Effect.provide(responseLossHttpClientLayer)
      )
      const target = yield* AgentProxy.make(targetUrl, "peer-b").pipe(
        Effect.provide(FetchHttpClient.layer)
      )
      const remoteTarget: AgentProxy.PeerEndpoint = {
        baseUrl: targetUrl,
        peerId: "peer-b"
      }
      const workstream = yield* source.createWorkstream(
        "transport-sync-workstream",
        "Transport sync"
      )
      const session = yield* source.createSession(
        "transport-sync-session",
        workstream.workstreamId
      )
      const root = yield* source.submitUserCommit(
        session.sessionId,
        "root context",
        "transport-sync-root"
      )
      const transportError = yield* Effect.flip(flakySource.push(
        session.sessionId,
        remoteTarget,
        root.commitId
      ))
      const retry = yield* source.push(
        session.sessionId,
        remoteTarget,
        root.commitId
      )
      const history = yield* target.history(session.sessionId)
      return { transportError, retry, history }
    })

    expect(result.transportError).toBeInstanceOf(AgentProxy.ProxyError)
    expect(result.retry.importedCommitIds).toEqual([])
    expect(result.history.items.filter(
      (item) => item.type === "UserCommit" &&
        item.commitId === "transport-sync-root"
    )).toHaveLength(1)
  }))

it.live("imports stale Peer descendants without reopening a settled Session", () =>
  Effect.gen(function*() {
    const settledPeerDatabase = yield* temporaryDatabase("corredor-sync-settled-peer-")
    const stalePeerDatabase = yield* temporaryDatabase("corredor-sync-stale-peer-")

    const rootBundle = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.createWorkstream(
        "settlement-race-workstream",
        "Settlement race",
        "peer-a"
      )
      const session = yield* application.createSession(
        "settlement-race-session",
        "settlement-race-workstream",
        "peer-a"
      )
      const root = yield* application.submitUserCommit(
        session.sessionId,
        "root context",
        "settlement-race-root",
        "peer-a"
      )
      return yield* application.exportBranch(
        session.sessionId,
        root.commitId,
        "peer-a"
      )
    }).pipe(Effect.provide(applicationLayer(settledPeerDatabase.path)))

    yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* application.importBranch(rootBundle, "peer-b")
    }).pipe(Effect.provide(applicationLayer(stalePeerDatabase.path)))

    const staleBundle = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.checkout(
        "settlement-race-session",
        "settlement-race-root",
        "peer-b"
      )
      const staleCommit = yield* application.submitUserCommit(
        "settlement-race-session",
        "offline work",
        "settlement-race-offline",
        "peer-b"
      )
      return yield* application.exportBranch(
        "settlement-race-session",
        staleCommit.commitId,
        "peer-b"
      )
    }).pipe(Effect.provide(applicationLayer(stalePeerDatabase.path)))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.settle("settlement-race-session")
      const imported = yield* application.importBranch(staleBundle, "peer-a")
      const settledHistory = yield* application.history(
        "settlement-race-session",
        "peer-a"
      )
      const activeBeforeReopen = yield* application.listSessions(
        "settlement-race-workstream"
      )
      const settledBeforeReopen = yield* application.listSessions(
        "settlement-race-workstream",
        "settled"
      )
      const continuationError = yield* Effect.flip(application.checkout(
        "settlement-race-session",
        "settlement-race-offline",
        "peer-a"
      ))
      const reopened = yield* application.reopen("settlement-race-session")
      const activeAfterReopen = yield* application.listSessions(
        "settlement-race-workstream"
      )
      yield* application.checkout(
        "settlement-race-session",
        "settlement-race-offline",
        "peer-a"
      )
      const continued = yield* application.submitUserCommit(
        "settlement-race-session",
        "continue offline work",
        "settlement-race-continued",
        "peer-a"
      )
      return {
        imported,
        settledHistory,
        activeBeforeReopen,
        settledBeforeReopen,
        continuationError,
        reopened,
        activeAfterReopen,
        continued
      }
    }).pipe(Effect.provide(applicationLayer(settledPeerDatabase.path)))

    expect(result.imported.importedCommitIds).toEqual(["settlement-race-offline"])
    expect(result.settledHistory.settled).toBe(true)
    expect(result.settledHistory.items).toContainEqual(
      expect.objectContaining({
        type: "UserCommit",
        commitId: "settlement-race-offline",
        imported: true,
        parentId: "settlement-race-root"
      })
    )
    expect(result.activeBeforeReopen).toEqual([])
    expect(result.settledBeforeReopen).toContainEqual(
      expect.objectContaining({
        sessionId: "settlement-race-session",
        settled: true
      })
    )
    expect(result.continuationError).toBeInstanceOf(Session.Settled)
    expect(result.reopened.type).toBe("SessionReopened")
    expect(result.activeAfterReopen).toContainEqual(
      expect.objectContaining({
        sessionId: "settlement-race-session",
        settled: false
      })
    )
    expect(result.continued).toMatchObject({
      commitId: "settlement-race-continued",
      parentId: "settlement-race-offline"
    })
  }))

it.live("rejects a retry with a different Settlement identity", () =>
  Effect.gen(function*() {
    const sourceDatabase = yield* temporaryDatabase("corredor-sync-settlement-identity-source-")
    const targetDatabase = yield* temporaryDatabase("corredor-sync-settlement-identity-target-")
    const settledBundle = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.createWorkstream(
        "settlement-identity-workstream",
        "Settlement identity",
        "peer-a"
      )
      const session = yield* application.createSession(
        "settlement-identity-session",
        "settlement-identity-workstream",
        "peer-a"
      )
      const root = yield* application.submitUserCommit(
        session.sessionId,
        "root context",
        "settlement-identity-root",
        "peer-a"
      )
      yield* application.settle(session.sessionId)
      return yield* application.exportBranch(
        session.sessionId,
        root.commitId,
        "peer-a"
      )
    }).pipe(Effect.provide(applicationLayer(sourceDatabase.path)))

    const firstImport = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* application.importBranch(settledBundle, "peer-b")
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    const malformed: Session.SyncBundle = {
      ...settledBundle,
      session: {
        ...settledBundle.session,
        settledEventId: "different-settlement-event"
      }
    }
    const error = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* Effect.flip(application.importBranch(malformed, "peer-b"))
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    const afterRejectedRetry = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* application.history(
        "settlement-identity-session",
        "peer-b"
      )
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    expect(firstImport.importedCommitIds).toEqual(["settlement-identity-root"])
    expect(error).toBeInstanceOf(Session.SyncConflict)
    expect(afterRejectedRetry.settled).toBe(true)
    expect(afterRejectedRetry.items.filter(
      (item) => item.type === "SessionSettled"
    )).toHaveLength(1)
    expect(afterRejectedRetry.items).not.toContainEqual(
      expect.objectContaining({ activityId: "different-settlement-event" })
    )
  }))

it.live("Push and Pull exchange divergent Branches through two HTTP Peers", () =>
  Effect.gen(function*() {
    const sourceDatabase = yield* temporaryDatabase("corredor-http-sync-source-")
    const targetDatabase = yield* temporaryDatabase("corredor-http-sync-target-")
    const sourcePort = yield* Effect.promise(availablePort)
    const targetPort = yield* Effect.promise(availablePort)
    const sourceUrl = `http://127.0.0.1:${sourcePort}`
    const targetUrl = `http://127.0.0.1:${targetPort}`
    const sourceServer = Server.layerWithoutDependencies({
      host: "127.0.0.1",
      port: sourcePort
    }).pipe(
      Layer.provide(Session.layer(sourceDatabase.path)),
      Layer.provide(ignoredAgent)
    )
    const targetServer = Server.layerWithoutDependencies({
      host: "127.0.0.1",
      port: targetPort
    }).pipe(
      Layer.provide(Session.layer(targetDatabase.path)),
      Layer.provide(ignoredAgent)
    )
    const remoteTarget: AgentProxy.PeerEndpoint = {
      baseUrl: targetUrl,
      peerId: "peer-b"
    }
    const remoteSource: AgentProxy.PeerEndpoint = {
      baseUrl: sourceUrl,
      peerId: "peer-a"
    }

    const result = yield* Effect.gen(function*() {
      yield* Layer.launch(sourceServer).pipe(Effect.forkScoped)
      yield* Layer.launch(targetServer).pipe(Effect.forkScoped)
      yield* Effect.sleep("100 millis")
      const source = yield* AgentProxy.make(sourceUrl, "peer-a").pipe(
        Effect.provide(FetchHttpClient.layer)
      )
      const target = yield* AgentProxy.make(targetUrl, "peer-b").pipe(
        Effect.provide(FetchHttpClient.layer)
      )
      const workstream = yield* source.createWorkstream(
        "http-sync-workstream",
        "HTTP shared context"
      )
      const session = yield* source.createSession(
        "http-sync-session",
        workstream.workstreamId
      )
      const root = yield* source.submitUserCommit(
        session.sessionId,
        "root",
        "http-sync-root"
      )
      yield* waitForOutcome(source, session.sessionId, root.commitId)
      yield* source.checkout(session.sessionId, root.commitId)

      const activityFiber = yield* target.streamActivity(
        session.sessionId,
        0
      ).pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkScoped
      )
      const firstPush = yield* source.push(
        session.sessionId,
        remoteTarget,
        root.commitId
      )
      const firstActivity = Array.from(yield* Fiber.join(activityFiber))
      const afterFirstPush = yield* target.history(session.sessionId)

      yield* target.checkout(session.sessionId, root.commitId)
      const divergent = yield* target.submitUserCommit(
        session.sessionId,
        "target branch",
        "http-sync-target"
      )
      yield* waitForOutcome(target, session.sessionId, divergent.commitId)
      yield* target.checkout(session.sessionId, divergent.commitId)

      yield* source.checkout(session.sessionId, root.commitId)
      const pushedBranch = yield* source.submitUserCommit(
        session.sessionId,
        "pushed branch",
        "http-sync-pushed"
      )
      yield* waitForOutcome(source, session.sessionId, pushedBranch.commitId)
      yield* source.checkout(session.sessionId, root.commitId)
      const pulledBranch = yield* source.submitUserCommit(
        session.sessionId,
        "pulled branch",
        "http-sync-pulled"
      )
      yield* waitForOutcome(source, session.sessionId, pulledBranch.commitId)
      yield* source.checkout(session.sessionId, root.commitId)

      const beforePull = yield* target.history(session.sessionId)
      const pullActivityFiber = yield* target.streamActivity(
        session.sessionId,
        Math.max(...beforePull.items.map((item) => item.position))
      ).pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped
      )
      const secondPush = yield* source.push(
        session.sessionId,
        remoteTarget,
        pushedBranch.commitId
      )
      const pull = yield* target.pull(
        session.sessionId,
        remoteSource,
        pulledBranch.commitId
      )
      const pullActivity = Array.from(yield* Fiber.join(pullActivityFiber))
      const afterDivergence = yield* target.history(session.sessionId)
      const retry = yield* source.push(
        session.sessionId,
        remoteTarget,
        pushedBranch.commitId
      )
      const reversePush = yield* target.push(
        session.sessionId,
        remoteSource,
        divergent.commitId
      )
      const reverseRetry = yield* target.push(
        session.sessionId,
        remoteSource,
        divergent.commitId
      )
      const sourceAfterReverse = yield* source.history(session.sessionId)
      const workstreams = yield* target.listWorkstreams()
      return {
        firstPush,
        firstActivity,
        afterFirstPush,
        divergent,
        secondPush,
        pull,
        pullActivity,
        afterDivergence,
        retry,
        reversePush,
        reverseRetry,
        sourceAfterReverse,
        workstreams
      }
    })

    expect(result.firstPush.importedCommitIds).toEqual(["http-sync-root"])
    expect(result.firstActivity.map((item) => item.type)).toEqual([
      "SessionCreated",
      "UserCommit"
    ])
    expect(result.afterFirstPush.branchHeadId).toBeNull()
    expect(result.afterFirstPush.items.map((item) => item.type)).toEqual([
      "SessionCreated",
      "UserCommit"
    ])
    expect(result.secondPush.importedCommitIds).toEqual(["http-sync-pushed"])
    expect(result.pull.importedCommitIds).toEqual(["http-sync-pulled"])
    expect(result.pullActivity.map((item) => item.type)).toEqual(["UserCommit"])
    expect(result.afterDivergence.branchHeadId).toBe("http-sync-target")
    expect(result.afterDivergence.items).toContainEqual(
      expect.objectContaining({
        type: "UserCommit",
        commitId: "http-sync-pushed",
        parentId: "http-sync-root"
      })
    )
    expect(result.afterDivergence.items).toContainEqual(
      expect.objectContaining({
        type: "UserCommit",
        commitId: "http-sync-pulled",
        parentId: "http-sync-root"
      })
    )
    expect(result.retry.importedCommitIds).toEqual([])
    expect(result.reversePush.importedCommitIds).toEqual(["http-sync-target"])
    expect(result.reverseRetry.importedCommitIds).toEqual([])
    expect(result.sourceAfterReverse.branchHeadId).toBe("http-sync-root")
    expect(result.sourceAfterReverse.items).toContainEqual(
      expect.objectContaining({
        type: "UserCommit",
        commitId: "http-sync-target",
        parentId: "http-sync-root"
      })
    )
    expect(result.workstreams).toContainEqual(
      expect.objectContaining({
        workstreamId: "http-sync-workstream",
        name: "HTTP shared context"
      })
    )
  }))
