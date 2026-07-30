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
      yield* proxy.checkout(session.sessionId, user.commitId)
      const checkedOut = yield* proxy.history(session.sessionId)
      const sessions = yield* proxy.listSessions()
      return { user, activity, history, checkedOut, sessions }
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
    expect(result.history.items).toEqual([
      expect.objectContaining({ type: "SessionCreated" }),
      ...result.activity
    ])
    expect(result.history.branchHeadId).toBe(
      Session.graphEntryId(result.activity[2] as Session.GraphEntry)
    )
    expect(result.checkedOut.branchHeadId).toBe("http-user")
    expect(result.checkedOut.items).toEqual(result.history.items)
    expect(result.sessions).toEqual([
      expect.objectContaining({
        sessionId: "http-session",
        userCommitCount: 1
      })
    ])
  })
)
