import { expect, it } from "@effect/vitest"
import { BunCrypto } from "@effect/platform-bun"
import { Effect, Layer } from "effect"
import { Agent } from "./Agent.ts"
import * as AgentRuntime from "./AgentRuntime.ts"
import * as Application from "./Application.ts"
import * as Session from "./Session.ts"
import { temporaryDatabase } from "./TestSupport.ts"

const runtimeLayer = (
  path: string,
  fakeAgent: Layer.Layer<Agent.Service>
) => Application.layerWithoutDependencies.pipe(
  Layer.provide(AgentRuntime.layerWithoutDependencies),
  Layer.provide(Session.layer(path)),
  Layer.provide(fakeAgent),
  Layer.provide(BunCrypto.layer)
)

it.live(
  "a request becomes an ancestry-preserving user-to-tool-to-agent Commit path",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-commits-")
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

    const commits = yield* Effect.gen(function*() {
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
        if (history.items.some(
          (item) => item.type === "AgentMessageCommit"
        )) {
          return history.items.filter(Session.isCommit)
        }
        yield* Effect.sleep("10 millis")
      }

      return yield* Effect.die("timed out waiting for Agent Message Commit")
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

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
  })
)

it.live(
  "a Tool Commit appears only after completion and records failure atomically",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-tool-failure-")
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

    const result = yield* Effect.gen(function*() {
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
        if (history.items.some(
          (item) => item.type === "AgentMessageCommit"
        )) {
          return { whileRunning, history: history.items }
        }
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for failed Tool Commit")
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

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
  })
)

it.live(
  "an unexpected Agent failure becomes a durable Failure Commit",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-agent-failure-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.die("model process crashed")
    }))

    const failure = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("agent-failure-session")
      yield* application.submitUserCommit(
        session.sessionId,
        "Do the work",
        "failure-user"
      )

      for (let attempt = 0; attempt < 100; attempt++) {
        const history = yield* application.history(session.sessionId)
        const commit = history.items.find(
          (item) => item.type === "FailureCommit"
        )
        if (commit?.type === "FailureCommit") return commit
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for Failure Commit")
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(failure).toMatchObject({
      type: "FailureCommit",
      inReplyTo: "failure-user",
      parentId: "failure-user"
    })
    expect(failure.reason).toContain("model process crashed")
  })
)

it.live(
  "a restarted Agent Run resumes from durable Tool ancestry without replacing it",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-tool-restart-")

    yield* Effect.gen(function*() {
      const store = yield* Session.make(path)
      yield* store.createSession("restart-session")
      yield* store.appendUserCommit(
        "restart-session",
        "Find the current value",
        "restart-user"
      )
      yield* store.appendToolCommit(
        "restart-session",
        "old-call",
        "Lookup",
        { query: "value" },
        { type: "Success", result: { value: "old" } },
        "restart-user",
        0
      )
      const conflict = yield* Effect.flip(store.appendToolCommit(
        "restart-session",
        "different-call",
        "Lookup",
        { query: "different" },
        { type: "Success", result: { value: "different" } },
        "restart-user",
        0
      ))
      expect(conflict).toBeInstanceOf(Session.ToolCommitConflict)
    }).pipe(Effect.provide(BunCrypto.layer))

    let receivedContext: ReadonlyArray<Agent.ContextEntry> = []
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context, onEvent) => Effect.gen(function*() {
        receivedContext = context
        yield* onEvent({
          type: "ToolCall",
          id: "new-call",
          name: "Lookup",
          input: { query: "value" }
        })
        yield* onEvent({
          type: "ToolResult",
          id: "new-call",
          name: "Lookup",
          result: { value: "new" },
          isFailure: false
        })
        return "The current value is new."
      })
    }))

    const commits = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      for (let attempt = 0; attempt < 100; attempt++) {
        const snapshot = yield* application.history("restart-session")
        if (snapshot.items.some(
          (item) => item.type === "AgentMessageCommit"
        )) {
          return snapshot.items.filter(Session.isCommit)
        }
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for restarted Agent Run")
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(receivedContext.map((entry) => entry.type)).toEqual([
      "User",
      "Tool"
    ])
    const tools = commits.filter((commit) => commit.type === "ToolCommit")
    expect(tools).toEqual([
      expect.objectContaining({
        toolCallId: "old-call",
        index: 0,
        outcome: { type: "Success", result: { value: "old" } }
      }),
      expect.objectContaining({
        toolCallId: "new-call",
        index: 1,
        outcome: { type: "Success", result: { value: "new" } }
      })
    ])
    expect(commits.at(-1)).toMatchObject({
      type: "AgentMessageCommit",
      parentId: tools[1]?.commitId,
      content: "The current value is new."
    })
  })
)

it.live(
  "two explicit Agent Runs from one Commit create independent descendants",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-independent-runs-")
    const contexts: Array<ReadonlyArray<Agent.ContextEntry>> = []
    const definitions: Array<Agent.Definition> = []
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context, _onEvent, definition) => Effect.sync(() => {
        contexts.push(context)
        definitions.push(definition ?? Agent.defaultDefinition)
        return `response-${contexts.length}`
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("independent-session")
      const user = yield* application.submitUserCommit(
        session.sessionId,
        "Run this independently",
        "independent-user"
      )

      for (let attempt = 0; attempt < 100; attempt++) {
        const history = yield* application.history(session.sessionId)
        if (history.items.some((item) => item.type === "AgentMessageCommit")) {
          break
        }
        yield* Effect.sleep("10 millis")
      }

      yield* application.startAgentRun(
        session.sessionId,
        user.commitId,
        {
          id: "agent-a",
          instructions: "Use the first definition.",
          tools: ["Bash"]
        },
        "independent-run-a"
      )
      yield* application.startAgentRun(
        session.sessionId,
        user.commitId,
        {
          id: "agent-b",
          instructions: "Use the second definition.",
          tools: ["Bash"]
        },
        "independent-run-b"
      )
      return yield* application.history(session.sessionId)
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    const messages = result.items.filter(
      (item): item is Extract<Session.Commit, { type: "AgentMessageCommit" }> =>
        item.type === "AgentMessageCommit"
    )
    expect(messages).toEqual([
      expect.objectContaining({
        inReplyTo: "independent-user",
        parentId: "independent-user"
      }),
      expect.objectContaining({
        inReplyTo: "independent-user",
        runId: "independent-run-a",
        parentId: "independent-user"
      }),
      expect.objectContaining({
        inReplyTo: "independent-user",
        runId: "independent-run-b",
        parentId: "independent-user"
      })
    ])
    expect(contexts).toHaveLength(3)
    expect(definitions.map((definition) => definition.id)).toEqual([
      "default",
      "agent-a",
      "agent-b"
    ])
    expect(contexts.every((context) => context.map((entry) => entry.type).join() === "User"))
      .toBe(true)
  })
)

it.live(
  "recreates the runtime between turns from the public application boundary",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-runtime-restart-")
    let firstSessionId = ""

    const firstAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("First durable answer")
    }))
    yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("runtime-restart-session")
      firstSessionId = session.sessionId
      yield* application.submitUserCommit(
        session.sessionId,
        "First turn",
        "runtime-first-user"
      )
      for (let attempt = 0; attempt < 100; attempt++) {
        const history = yield* application.history(session.sessionId)
        if (history.items.some((item) => item.type === "AgentMessageCommit")) return
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for first runtime")
    }).pipe(Effect.provide(runtimeLayer(path, firstAgent)))

    let secondContext: ReadonlyArray<Agent.ContextEntry> = []
    const secondAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context) => Effect.gen(function*() {
        secondContext = context
        return "Second durable answer"
      })
    }))
    const history = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.submitUserCommit(
        firstSessionId,
        "Second turn",
        "runtime-second-user"
      )
      for (let attempt = 0; attempt < 100; attempt++) {
        const snapshot = yield* application.history(firstSessionId)
        if (snapshot.items.some(
          (item) => item.type === "AgentMessageCommit" &&
            item.content === "Second durable answer"
        )) return snapshot
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for restarted runtime")
    }).pipe(Effect.provide(runtimeLayer(path, secondAgent)))

    expect(secondContext.map((entry) => entry.type)).toEqual([
      "User",
      "AgentMessage",
      "User"
    ])
    expect(history.items.filter(Session.isCommit)).toEqual([
      expect.objectContaining({ type: "UserCommit", commitId: "runtime-first-user" }),
      expect.objectContaining({ type: "AgentMessageCommit", content: "First durable answer" }),
      expect.objectContaining({ type: "UserCommit", commitId: "runtime-second-user" }),
      expect.objectContaining({ type: "AgentMessageCommit", content: "Second durable answer" })
    ])
  })
)
