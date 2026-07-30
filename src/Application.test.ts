import { expect, test } from "bun:test"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Effect, Layer } from "effect"
import { Agent } from "./Agent.ts"
import * as AgentRuntime from "./AgentRuntime.ts"
import * as Application from "./Application.ts"
import * as Session from "./Session.ts"

test("a request becomes an ancestry-preserving user-to-tool-to-agent Commit path", async () => {
  const directory = mkdtempSync(join(tmpdir(), "corredor-commits-"))
  const path = join(directory, "corredor.db")
  let receivedContext: ReadonlyArray<Agent.ContextEntry> = []

  const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
    run: (context, onEvent) => Effect.gen(function*() {
      receivedContext = context
      yield* onEvent({
        type: "ToolCall",
        id: "tool-call-1",
        name: "Lookup",
        input: { query: "durable context" }
      })
      yield* onEvent({
        type: "ToolResult",
        id: "tool-call-1",
        name: "Lookup",
        result: { answer: 42 },
        isFailure: false
      })
      return "The answer is 42."
    })
  }))

  const layer = Layer.mergeAll(Application.layer, AgentRuntime.layer).pipe(
    Layer.provide(Session.layer(path)),
    Layer.provide(fakeAgent)
  )

  try {
    const commits = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("session-1")
      expect(session.type).toBe("SessionCreated")

      const user = yield* application.submitUserCommit(
        session.sessionId,
        "Explain the answer",
        "user-commit-1"
      )
      expect(user).toMatchObject({
        type: "UserCommit",
        commitId: "user-commit-1",
        sessionId: "session-1",
        parentId: null,
        content: "Explain the answer"
      })

      for (let attempt = 0; attempt < 100; attempt++) {
        const history = yield* application.history(session.sessionId)
        if (history.items.some((item) => item.type === "AgentMessageCommit")) {
          return history.items.filter(Session.isCommit)
        }
        yield* Effect.sleep("10 millis")
      }

      return yield* Effect.die("timed out waiting for Agent Message Commit")
    }).pipe(Effect.provide(layer))))

    expect(commits).toEqual([
      expect.objectContaining({
        type: "UserCommit",
        commitId: "user-commit-1",
        parentId: null
      }),
      expect.objectContaining({
        type: "ToolCommit",
        parentId: "user-commit-1",
        name: "Lookup",
        input: { query: "durable context" },
        outcome: {
          type: "Success",
          result: { answer: 42 }
        }
      }),
      expect.objectContaining({
        type: "AgentMessageCommit",
        parentId: commits[1]?.commitId,
        content: "The answer is 42."
      })
    ])
    expect(receivedContext).toEqual([{
      type: "User",
      commitId: "user-commit-1",
      content: "Explain the answer"
    }])
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test("a Tool Commit appears only after completion and records failure atomically", async () => {
  const directory = mkdtempSync(join(tmpdir(), "corredor-tool-failure-"))
  const path = join(directory, "corredor.db")
  let markCallSeen!: () => void
  let releaseResult!: () => void
  const callSeen = new Promise<void>((resolve) => {
    markCallSeen = resolve
  })
  const resultReleased = new Promise<void>((resolve) => {
    releaseResult = resolve
  })

  const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
    run: (_context, onEvent) => Effect.gen(function*() {
      yield* onEvent({
        type: "ToolCall",
        id: "failing-call",
        name: "Lookup",
        input: { query: "missing" }
      })
      markCallSeen()
      yield* Effect.promise(() => resultReleased)
      yield* onEvent({
        type: "ToolResult",
        id: "failing-call",
        name: "Lookup",
        result: { message: "not found" },
        isFailure: true
      })
      return "The lookup failed."
    })
  }))
  const layer = Layer.mergeAll(Application.layer, AgentRuntime.layer).pipe(
    Layer.provide(Session.layer(path)),
    Layer.provide(fakeAgent)
  )

  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("failure-session")
      yield* application.submitUserCommit(
        session.sessionId,
        "Find it",
        "failure-user"
      )
      yield* Effect.promise(() => callSeen)
      const whileRunning = (yield* application.history(session.sessionId)).items
      releaseResult()

      for (let attempt = 0; attempt < 100; attempt++) {
        const history = yield* application.history(session.sessionId)
        if (history.items.some((item) => item.type === "AgentMessageCommit")) {
          return { whileRunning, history: history.items }
        }
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for failed Tool Commit")
    }).pipe(Effect.provide(layer))))

    expect(result.whileRunning.map((item) => item.type)).toEqual([
      "SessionCreated",
      "UserCommit"
    ])
    expect(result.history.find((item) => item.type === "ToolCommit"))
      .toMatchObject({
        type: "ToolCommit",
        parentId: "failure-user",
        input: { query: "missing" },
        outcome: {
          type: "Failure",
          failure: { message: "not found" }
        }
      })
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})
