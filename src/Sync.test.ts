import { expect, it } from "@effect/vitest"
import { BunCrypto } from "@effect/platform-bun"
import { createServer } from "node:net"
import { Effect, Fiber, Layer, Stream } from "effect"
import { FetchHttpClient } from "effect/unstable/http"
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
      return yield* Effect.flip(application.importBranch(malformed, "peer-b"))
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    const target = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* application.listWorkstreams()
    }).pipe(Effect.provide(applicationLayer(targetDatabase.path)))

    expect(error).toBeInstanceOf(Session.SyncValidationError)
    expect(target).not.toContainEqual(
      expect.objectContaining({
        workstreamId: "invalid-sync-workstream"
      })
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
