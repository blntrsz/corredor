import { expect, it } from "@effect/vitest"
import { createServer } from "node:net"
import { Effect, Fiber, Layer, Stream } from "effect"
import { Agent } from "./Agent.ts"
import * as AgentProxy from "./AgentProxy.ts"
import * as Server from "./Server.ts"
import * as Session from "./Session.ts"
import { temporaryDatabase } from "./TestSupport.ts"

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

it.live(
  "history and reconnectable activity expose canonical Commits to the client",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-http-")
    const port = yield* Effect.promise(availablePort)
    const baseUrl = `http://127.0.0.1:${port}`

    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, onEvent) => Effect.gen(function*() {
        yield* onEvent({
          type: "ToolCall",
          id: "call-http",
          name: "Lookup",
          input: { subject: "Commits" }
        })
        yield* onEvent({
          type: "ToolResult",
          id: "call-http",
          name: "Lookup",
          result: { durable: true },
          isFailure: false
        })
        return "Commits are durable."
      })
    }))

    const layer = Layer.mergeAll(
      Server.layerWithoutDependencies({ host: "127.0.0.1", port }),
      AgentProxy.layer({ baseUrl })
    ).pipe(
      Layer.provide(Session.layer(path)),
      Layer.provide(fakeAgent)
    )

    const result = yield* Effect.gen(function*() {
      const proxy = yield* AgentProxy.Service
      const session = yield* proxy.createSession("http-session")

      const activityFiber = yield* proxy.streamActivity(
        session.sessionId,
        session.position
      ).pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkScoped
      )

      const user = yield* proxy.submitUserCommit(
        session.sessionId,
        "Explain Commits",
        "http-user"
      )
      const activity = Array.from(yield* Fiber.join(activityFiber))
      const history = yield* proxy.history(session.sessionId)
      yield* proxy.startAgentRun(
        session.sessionId,
        user.commitId,
        "http-independent-run"
      )
      const independentHistory = yield* proxy.history(session.sessionId)
      yield* proxy.checkout(session.sessionId, user.commitId)
      const checkedOut = yield* proxy.history(session.sessionId)
      const sessions = yield* proxy.listSessions()
      return { user, activity, history, independentHistory, checkedOut, sessions }
    }).pipe(Effect.provide(layer))

    expect(result.user).toMatchObject({
      type: "UserCommit",
      commitId: "http-user",
      parentId: null
    })
    expect(result.activity.map((item) => item.type)).toEqual([
      "UserCommit",
      "ToolCommit",
      "AgentMessageCommit"
    ])
    expect(result.activity[0]).toMatchObject({
      type: "UserCommit",
      commitId: "http-user",
      parentId: null,
      content: "Explain Commits"
    })
    expect(result.activity[1]).toMatchObject({
      type: "ToolCommit",
      parentId: "http-user",
      name: "Lookup",
      input: { subject: "Commits" },
      outcome: { type: "Success", result: { durable: true } }
    })
    expect(result.activity[2]).toMatchObject({
      type: "AgentMessageCommit",
      parentId: (result.activity[1] as Session.Commit).commitId,
      inReplyTo: "http-user",
      content: "Commits are durable."
    })
    expect(result.history.items).toEqual([
      expect.objectContaining({ type: "SessionCreated" }),
      ...result.activity
    ])
    expect(result.history.branchHeadId).toBe(
      Session.branchRecordId(result.activity[2] as Session.BranchRecord)
    )
    const independentTool = result.independentHistory.items.find(
      (item) => item.type === "ToolCommit" && item.runId === "http-independent-run"
    )
    const independentMessage = result.independentHistory.items.find(
      (item) => item.type === "AgentMessageCommit" &&
        item.runId === "http-independent-run"
    )
    expect(independentTool).toBeDefined()
    expect(independentMessage).toMatchObject({
      inReplyTo: "http-user",
      runId: "http-independent-run",
      parentId: (independentTool as Session.Commit).commitId
    })
    expect(result.checkedOut.branchHeadId).toBe("http-user")
    expect(result.checkedOut.items).toEqual(result.independentHistory.items)
    expect(result.sessions).toEqual([
      expect.objectContaining({
        sessionId: "http-session",
        userCommitCount: 1
      })
    ])
  })
)
