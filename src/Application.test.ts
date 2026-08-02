import { expect, it } from "@effect/vitest"
import { BunCrypto } from "@effect/platform-bun"
import { Effect, Fiber, Layer } from "effect"
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
  "persists Branch Heads per Peer and keeps Checkout isolated across restart",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-peer-heads-")
    let responseNumber = 0
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.sync(() => `response-${++responseNumber}`)
    }))

    const first = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("peer-session")
      const user = yield* application.submitUserCommit(
        session.sessionId,
        "First request",
        "peer-user-1",
        "peer-a"
      )
      for (let attempt = 0; attempt < 100; attempt++) {
        const history = yield* application.history(session.sessionId, "peer-a")
        if (history.items.some((item) => item.type === "AgentMessageCommit")) {
          return { session, user, history }
        }
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for Peer A")
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const peerA = yield* application.history(first.session.sessionId, "peer-a")
      const peerB = yield* application.history(first.session.sessionId, "peer-b")
      yield* application.checkout(first.session.sessionId, first.user.commitId, "peer-b")
      const branchUser = yield* application.submitUserCommit(
        first.session.sessionId,
        "Divergent request",
        "peer-user-2",
        "peer-b"
      )
      for (let attempt = 0; attempt < 100; attempt++) {
        const history = yield* application.history(first.session.sessionId, "peer-b")
        if (history.items.some(
          (item) => item.type === "AgentMessageCommit" &&
            item.inReplyTo === branchUser.commitId
        )) {
          const peerAAfter = yield* application.history(
            first.session.sessionId,
            "peer-a"
          )
          return { peerA, peerB, peerAAfter, history }
        }
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for Peer B")
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(first.history.branchHeadId).not.toBeNull()
    expect(result.peerA.branchHeadId).toBe(first.history.branchHeadId)
    expect(result.peerAAfter.branchHeadId).toBe(first.history.branchHeadId)
    expect(result.peerB.branchHeadId).toBeNull()
    expect(result.history.branchHeadId).not.toBe("peer-user-2")
    expect(result.history.items).toContainEqual(
      expect.objectContaining({
        type: "AgentMessageCommit",
        inReplyTo: "peer-user-2"
      })
    )
    expect(result.history.items).toContainEqual(
      expect.objectContaining({
        type: "UserCommit",
        commitId: "peer-user-1",
        parentId: null
      })
    )
    expect(result.history.items).toContainEqual(
      expect.objectContaining({
        type: "UserCommit",
        commitId: "peer-user-2",
        parentId: "peer-user-1"
      })
    )
    const activity = yield* Effect.gen(function*() {
      const store = yield* Session.Service
      return yield* store.activityAfter(0)
    }).pipe(Effect.provide(Session.layer(path)))
    expect(activity.some((item) => item.type === "LegacyNavigation")).toBe(false)
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
      run: () => Effect.die(new Error("model process crashed\nhidden stack"))
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
    expect(failure.reason).not.toContain("hidden stack")
  })
)

it.live(
  "interrupts model output into one durable Interrupt Commit without hidden state",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-agent-interrupt-")
    let markOutputSeen!: () => void
    const outputSeen = new Promise<void>((resolve) => {
      markOutputSeen = resolve
    })
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, onEvent) => Effect.gen(function*() {
        yield* onEvent({ type: "TextDelta", text: "partial answer" })
        markOutputSeen()
        yield* Effect.promise(() => new Promise<void>(() => undefined))
        return "unreachable"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("interrupt-session")
      const user = yield* application.submitUserCommit(
        session.sessionId,
        "Answer slowly",
        "interrupt-user"
      )
      yield* Effect.promise(() => outputSeen)
      const before = yield* application.history(session.sessionId)
      yield* application.interruptAgentRun(
        session.sessionId,
        user.commitId,
        "Stopped by the user"
      )
      const after = yield* application.history(session.sessionId)
      const activity = (yield* application.activityAfter(session.position))
        .filter((item) => item.sessionId === session.sessionId)
      return { before, after, activity }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.before.items.map((item) => item.type)).toEqual([
      "SessionCreated",
      "UserCommit"
    ])
    expect(result.after.items.filter(Session.isCommit)).toEqual([
      expect.objectContaining({
        type: "UserCommit",
        commitId: "interrupt-user"
      }),
      expect.objectContaining({
        type: "InterruptCommit",
        inReplyTo: "interrupt-user",
        reason: "Stopped by the user",
        partialOutput: "partial answer"
      })
    ])
    expect(result.after.items.some((item) => item.type === "AgentMessageCommit"))
      .toBe(false)
    expect(result.after.items.some((item) => item.type === "ToolCommit"))
      .toBe(false)
    expect(result.after.branchHeadId).toBe(
      result.after.items.find((item) => item.type === "InterruptCommit")?.commitId
    )
    expect(result.activity.map((item) => item.type)).toEqual([
      "UserCommit",
      "InterruptCommit"
    ])
  })
)

it.live(
  "interrupts an in-progress tool without persisting a half-complete Tool Commit",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-tool-interrupt-")
    let markToolStarted!: () => void
    const toolStarted = new Promise<void>((resolve) => {
      markToolStarted = resolve
    })
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, onEvent) => Effect.gen(function*() {
        yield* onEvent({
          type: "ToolCall",
          id: "slow-tool",
          name: "Lookup",
          input: { query: "still running" }
        })
        markToolStarted()
        yield* Effect.promise(() => new Promise<void>(() => undefined))
        yield* onEvent({
          type: "ToolResult",
          id: "slow-tool",
          name: "Lookup",
          result: { answer: 42 },
          isFailure: false
        })
        return "unreachable"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("tool-interrupt-session")
      const user = yield* application.submitUserCommit(
        session.sessionId,
        "Run a slow lookup",
        "tool-interrupt-user"
      )
      yield* Effect.promise(() => toolStarted)
      yield* application.interruptAgentRun(
        session.sessionId,
        user.commitId,
        "  "
      )
      return yield* application.history(session.sessionId)
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.items.filter((item) => item.type === "ToolCommit")).toEqual([])
    expect(result.items.filter((item) => item.type === "InterruptCommit")).toEqual([
      expect.objectContaining({
        type: "InterruptCommit",
        inReplyTo: "tool-interrupt-user",
        reason: "Interrupted by user",
        partialOutput: ""
      })
    ])
  })
)

it.live(
  "continues after an Interrupt Commit with a fresh stateless Agent Run",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-interrupt-continuation-")
    const contexts: Array<ReadonlyArray<Agent.ContextEntry>> = []
    let markOutputSeen!: () => void
    const outputSeen = new Promise<void>((resolve) => {
      markOutputSeen = resolve
    })
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context, onEvent) => Effect.gen(function*() {
        contexts.push(context)
        if (contexts.length === 1) {
          yield* onEvent({ type: "TextDelta", text: "unfinished" })
          markOutputSeen()
          yield* Effect.promise(() => new Promise<void>(() => undefined))
        }
        return "continued from durable history"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("interrupt-continuation")
      const first = yield* application.submitUserCommit(
        session.sessionId,
        "Start the work",
        "continuation-first-user"
      )
      yield* Effect.promise(() => outputSeen)
      yield* application.interruptAgentRun(
        session.sessionId,
        first.commitId,
        "Pause here"
      )
      const second = yield* application.submitUserCommit(
        session.sessionId,
        "Continue from the interruption",
        "continuation-second-user"
      )

      for (let attempt = 0; attempt < 100; attempt++) {
        const history = yield* application.history(session.sessionId)
        if (history.items.some(
          (item) => item.type === "AgentMessageCommit" &&
            item.inReplyTo === second.commitId
        )) return history
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for continuation")
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(contexts).toHaveLength(2)
    expect(contexts[0]?.map((entry) => entry.type)).toEqual(["User"])
    expect(contexts[1]?.map((entry) => entry.type)).toEqual([
      "User",
      "Interrupt",
      "User"
    ])
    expect(contexts[1]?.find((entry) => entry.type === "Interrupt"))
      .toMatchObject({
        reason: "Pause here",
        partialOutput: "unfinished"
      })
    expect(result.items).toContainEqual(expect.objectContaining({
      type: "InterruptCommit",
      inReplyTo: "continuation-first-user",
      reason: "Pause here"
    }))
    expect(result.items).toContainEqual(expect.objectContaining({
      type: "AgentMessageCommit",
      inReplyTo: "continuation-second-user",
      content: "continued from durable history"
    }))
  })
)

it.live(
  "continues after a Failure Commit when the Agent Runtime is recreated",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-failure-continuation-")
    const firstAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.die("provider stopped unexpectedly")
    }))

    yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("failure-continuation")
      yield* application.submitUserCommit(
        session.sessionId,
        "Do the work",
        "failure-continuation-first"
      )
      for (let attempt = 0; attempt < 100; attempt++) {
        const history = yield* application.history(session.sessionId)
        if (history.items.some((item) => item.type === "FailureCommit")) return
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for Failure Commit")
    }).pipe(Effect.provide(runtimeLayer(path, firstAgent)))

    let secondContext: ReadonlyArray<Agent.ContextEntry> = []
    const secondAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context) => Effect.sync(() => {
        secondContext = context
        return "recovered after restart"
      })
    }))

    const history = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      yield* application.submitUserCommit(
        "failure-continuation",
        "Try again after the failure",
        "failure-continuation-second"
      )
      for (let attempt = 0; attempt < 100; attempt++) {
        const snapshot = yield* application.history("failure-continuation")
        if (snapshot.items.some(
          (item) => item.type === "AgentMessageCommit" &&
            item.inReplyTo === "failure-continuation-second"
        )) return snapshot
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for restarted continuation")
    }).pipe(Effect.provide(runtimeLayer(path, secondAgent)))

    expect(secondContext.map((entry) => entry.type)).toEqual([
      "User",
      "Failure",
      "User"
    ])
    expect(history.items).toContainEqual(expect.objectContaining({
      type: "FailureCommit",
      reason: "provider stopped unexpectedly"
    }))
    expect(history.items).toContainEqual(expect.objectContaining({
      type: "AgentMessageCommit",
      inReplyTo: "failure-continuation-second",
      content: "recovered after restart"
    }))
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
  "rejects replayed Tool and Agent outcomes after a Session is settled",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-settlement-outcomes-")

    const result = yield* Effect.gen(function*() {
      const store = yield* Session.make(path)
      yield* store.createSession("settlement-outcomes-session")
      const user = yield* store.appendUserCommit(
        "settlement-outcomes-session",
        "Do the work",
        "settlement-outcomes-user"
      )
      yield* store.appendToolCommit(
        "settlement-outcomes-session",
        "settlement-outcomes-call",
        "Lookup",
        { query: "same" },
        { type: "Success", result: "done" },
        user.commitId,
        0
      )
      yield* store.appendAgentMessageCommit(
        "settlement-outcomes-session",
        "done",
        user.commitId
      )
      yield* store.settle("settlement-outcomes-session")

      const replayedTool = yield* Effect.flip(store.appendToolCommit(
        "settlement-outcomes-session",
        "settlement-outcomes-call",
        "Lookup",
        { query: "same" },
        { type: "Success", result: "done" },
        user.commitId,
        0
      ))
      const replayedAgent = yield* Effect.flip(store.appendAgentMessageCommit(
        "settlement-outcomes-session",
        "done",
        user.commitId
      ))
      return { replayedTool, replayedAgent }
    }).pipe(Effect.provide(BunCrypto.layer))

    expect(result.replayedTool).toBeInstanceOf(Session.Settled)
    expect(result.replayedAgent).toBeInstanceOf(Session.Settled)
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
  "generates distinct identities for explicit Agent Runs when callers omit them",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-generated-run-ids-")
    let executions = 0
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.sync(() => `response-${++executions}`)
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("generated-run-session")
      const root = yield* application.createRootContext(
        session.sessionId,
        "Run twice",
        "generated-run-root"
      )
      const first = yield* application.startAgentRun(
        session.sessionId,
        root.commitId,
        Agent.defaultDefinition
      )
      const second = yield* application.startAgentRun(
        session.sessionId,
        root.commitId,
        Agent.defaultDefinition
      )
      return {
        first,
        second,
        history: yield* application.history(session.sessionId)
      }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.first.runId).not.toBe(result.second.runId)
    expect(result.history.items.filter(
      (item) => item.type === "AgentMessageCommit"
    )).toEqual([
      expect.objectContaining({ runId: result.first.runId }),
      expect.objectContaining({ runId: result.second.runId })
    ])
    expect(executions).toBe(2)
  })
)

it.live(
  "returns a generated Run ID before completion and rejects identity collisions",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-immediate-run-id-")
    let markStarted!: () => void
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, onEvent) => Effect.gen(function*() {
        yield* onEvent({ type: "TextDelta", text: "partial" })
        markStarted()
        yield* Effect.promise(() => new Promise<void>(() => undefined))
        return "unreachable"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("immediate-run-session")
      const root = yield* application.createRootContext(
        session.sessionId,
        "Long-running explicit work",
        "immediate-run-root"
      )
      const run = yield* application.startAgentRun(
        session.sessionId,
        root.commitId,
        Agent.defaultDefinition
      )
      yield* Effect.promise(() => started)
      const collision = yield* Effect.flip(application.compact(
        session.sessionId,
        root.commitId,
        Agent.defaultDefinition,
        run.runId
      ))
      const interrupt = yield* application.interruptAgentRun(
        session.sessionId,
        root.commitId,
        "Stop generated Run",
        run.runId
      )
      return { run, collision, interrupt }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.run.runId.length).toBeGreaterThan(0)
    expect(result.collision).toBeInstanceOf(Session.PersistenceError)
    expect(result.interrupt).toMatchObject({
      type: "InterruptCommit",
      reason: "Stop generated Run",
      runId: result.run.runId
    })
  })
)

it.live(
  "interrupts active Agent Runs before settling and admits new Runs after reopening",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-settle-active-run-")
    let markStarted!: () => void
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    let executions = 0
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, onEvent) => {
        executions++
        if (executions > 1) return Effect.succeed("completed after reopening")
        return Effect.gen(function*() {
          yield* onEvent({ type: "TextDelta", text: "visible partial" })
          markStarted()
          yield* Effect.promise(() => new Promise<void>(() => undefined))
          return "unreachable"
        })
      }
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("settle-active-run-session")
      const root = yield* application.createRootContext(
        session.sessionId,
        "Long-running work",
        "settle-active-run-root"
      )
      const runFiber = yield* application.startAgentRun(
        session.sessionId,
        root.commitId,
        Agent.defaultDefinition
      ).pipe(Effect.forkScoped)
      yield* Effect.promise(() => started)
      const settled = yield* application.settle(session.sessionId)
      const interruptedRun = yield* Fiber.join(runFiber)
      const settledHistory = yield* application.history(session.sessionId)
      yield* application.reopen(session.sessionId)
      const resumedRun = yield* application.startAgentRun(
        session.sessionId,
        root.commitId,
        Agent.defaultDefinition
      )
      const reopenedHistory = yield* application.history(session.sessionId)
      return {
        settled,
        interruptedRun,
        resumedRun,
        settledHistory,
        reopenedHistory
      }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    const interrupt = result.settledHistory.items.find(
      (item) => item.type === "InterruptCommit"
    )
    expect(interrupt).toMatchObject({
      reason: "Session settled",
      partialOutput: "visible partial",
      runId: result.interruptedRun.runId
    })
    expect(interrupt?.sequence).toBeLessThan(result.settled.sequence)
    expect(result.settledHistory.settled).toBe(true)
    expect(result.reopenedHistory.settled).toBe(false)
    expect(result.reopenedHistory.items).toContainEqual(expect.objectContaining({
      type: "AgentMessageCommit",
      content: "completed after reopening",
      runId: result.resumedRun.runId
    }))
  })
)

it.live(
  "waits for an active Compaction outcome before settling",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-settle-compaction-")
    let markStarted!: () => void
    let releaseCompaction!: () => void
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    const released = new Promise<void>((resolve) => {
      releaseCompaction = resolve
    })
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.gen(function*() {
        markStarted()
        yield* Effect.promise(() => released)
        return "durable summary"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("settle-compaction-session")
      const root = yield* application.createRootContext(
        session.sessionId,
        "Compact this",
        "settle-compaction-root"
      )
      const compactionFiber = yield* application.compact(
        session.sessionId,
        root.commitId
      ).pipe(Effect.forkScoped)
      yield* Effect.promise(() => started)
      let settlementCompleted = false
      const settlementFiber = yield* application.settle(session.sessionId).pipe(
        Effect.tap(() => Effect.sync(() => {
          settlementCompleted = true
        })),
        Effect.forkScoped
      )
      yield* Effect.sleep("20 millis")
      expect(settlementCompleted).toBe(false)
      releaseCompaction()
      const compaction = yield* Fiber.join(compactionFiber)
      const settled = yield* Fiber.join(settlementFiber)
      return {
        compaction,
        settled,
        history: yield* application.history(session.sessionId)
      }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.compaction.type).toBe("CompactionCommit")
    expect(result.compaction.sequence).toBeLessThan(result.settled.sequence)
    expect(result.history.settled).toBe(true)
  })
)

it.live(
  "clears the Settlement admission gate when waiting is interrupted",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-cancel-settlement-")
    let markCompactionStarted!: () => void
    let releaseCompaction!: () => void
    const compactionStarted = new Promise<void>((resolve) => {
      markCompactionStarted = resolve
    })
    const compactionReleased = new Promise<void>((resolve) => {
      releaseCompaction = resolve
    })
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, _onEvent, definition) =>
        definition?.tools.length === 0
          ? Effect.gen(function*() {
            markCompactionStarted()
            yield* Effect.promise(() => compactionReleased)
            return "summary after release"
          })
          : Effect.succeed("new Run admitted")
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("cancel-settlement-session")
      const root = yield* application.createRootContext(
        session.sessionId,
        "Keep active",
        "cancel-settlement-root"
      )
      const compactionFiber = yield* application.compact(
        session.sessionId,
        root.commitId
      ).pipe(Effect.forkScoped)
      yield* Effect.promise(() => compactionStarted)
      const settlementFiber = yield* application.settle(session.sessionId).pipe(
        Effect.forkScoped
      )
      yield* Effect.sleep("20 millis")
      yield* Fiber.interrupt(settlementFiber)
      releaseCompaction()
      yield* Fiber.join(compactionFiber)
      const run = yield* application.startAgentRun(
        session.sessionId,
        root.commitId,
        Agent.defaultDefinition
      )
      for (let attempt = 0; attempt < 100; attempt++) {
        const history = yield* application.history(session.sessionId)
        if (history.items.some(
          (item) => item.type === "AgentMessageCommit" &&
            item.runId === run.runId
        )) return { run, history }
        yield* Effect.sleep("10 millis")
      }
      return yield* Effect.die("timed out waiting for admitted Agent Run")
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.history.settled).toBe(false)
    expect(result.history.items).toContainEqual(expect.objectContaining({
      type: "AgentMessageCommit",
      content: "new Run admitted",
      runId: result.run.runId
    }))
  })
)

it.live(
  "rejects unknown Session history without preventing later creation",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-missing-history-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("unused")
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const error = yield* Effect.flip(application.history("missing-session"))
      const created = yield* application.createSession("missing-session")
      return { error, created }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.error).toBeInstanceOf(Session.NotFound)
    expect(result.created).toMatchObject({
      type: "SessionCreated",
      sessionId: "missing-session"
    })
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

it.live(
  "settles a Session without losing history and reopens it for new work",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-settlement-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("durable answer")
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("settlement-session")
      const user = yield* application.submitUserCommit(
        session.sessionId,
        "Finish this work",
        "settlement-user"
      )
      const settled = yield* application.settle(session.sessionId)
      const settledHistory = yield* application.history(session.sessionId)
      const activeSessions = yield* application.listSessions(undefined, "active")
      const settledSessions = yield* application.listSessions(undefined, "settled")
      const checkoutError = yield* Effect.flip(application.checkout(
        session.sessionId,
        user.commitId
      ))
      const commitError = yield* Effect.flip(application.submitUserCommit(
        session.sessionId,
        "This must wait",
        "settlement-rejected"
      ))
      const runError = yield* Effect.flip(application.startAgentRun(
        session.sessionId,
        user.commitId,
        Agent.defaultDefinition
      ))
      const reopened = yield* application.reopen(session.sessionId)
      const reopenedHistory = yield* application.history(session.sessionId)
      const activity = yield* application.activityAfter(session.position)
      const continued = yield* application.submitUserCommit(
        session.sessionId,
        "Continue after reopening",
        "settlement-continued"
      )
      return {
        session,
        user,
        settled,
        settledHistory,
        activeSessions,
        settledSessions,
        checkoutError,
        commitError,
        runError,
        reopened,
        reopenedHistory,
        activity,
        continued
      }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.settled.type).toBe("SessionSettled")
    expect(result.reopened.type).toBe("SessionReopened")
    expect(result.settledHistory.settled).toBe(true)
    expect(result.settledHistory.items).toContainEqual(result.user)
    expect(result.activeSessions).toEqual([])
    expect(result.settledSessions).toEqual([
      expect.objectContaining({
        sessionId: result.session.sessionId,
        settled: true
      })
    ])
    expect(result.checkoutError).toBeInstanceOf(Session.Settled)
    expect(result.commitError).toBeInstanceOf(Session.Settled)
    expect(result.runError).toBeInstanceOf(Session.Settled)
    expect(result.reopenedHistory.settled).toBe(false)
    expect(result.activity.map((item) => item.type)).toEqual([
      "UserCommit",
      "SessionSettled",
      "SessionReopened"
    ])
    expect(result.continued).toMatchObject({
      type: "UserCommit",
      commitId: "settlement-continued"
    })
  })
)

it.live(
  "restores Settlement state and lifecycle history after the Peer restarts",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-settlement-restart-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("")
    }))

    const sessionId = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("settlement-restart")
      yield* application.settle(session.sessionId)
      return session.sessionId
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    const restored = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return {
        history: yield* application.history(sessionId),
        active: yield* application.listSessions(),
        settled: yield* application.listSessions(undefined, "settled")
      }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(restored.history.settled).toBe(true)
    expect(restored.history.items.map((item) => item.type)).toEqual([
      "SessionCreated",
      "SessionSettled"
    ])
    expect(restored.active).toEqual([])
    expect(restored.settled).toEqual([
      expect.objectContaining({ sessionId, settled: true })
    ])
  })
)

it.live(
  "compacts a complete Branch while preserving provenance and replacing later context",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-compaction-")
    const contexts: Array<ReadonlyArray<Agent.ContextEntry>> = []
    const definitions: Array<Agent.Definition> = []
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context, _onEvent, definition) => Effect.sync(() => {
        contexts.push(context)
        definitions.push(definition ?? Agent.defaultDefinition)
        return definitions.at(-1)?.id === "compactor"
          ? "A compact summary"
          : `response-${contexts.length}`
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("compaction-session")
      yield* application.submitUserCommit(
        session.sessionId,
        "Remember the durable answer",
        "compaction-user"
      )

      let beforeCompaction = yield* application.history(session.sessionId)
      for (let attempt = 0; attempt < 100; attempt++) {
        if (beforeCompaction.items.some(
          (item) => item.type === "AgentMessageCommit"
        )) break
        yield* Effect.sleep("10 millis")
        beforeCompaction = yield* application.history(session.sessionId)
      }
      const sourceHeadId = beforeCompaction.branchHeadId
      if (sourceHeadId === null) {
        return yield* Effect.die("timed out waiting for the source Branch Head")
      }

      const compaction = yield* application.compact(
        session.sessionId,
        sourceHeadId,
        {
          id: "compactor",
          instructions: "Summarize the complete active ancestry.",
          tools: []
        },
        "compaction-run"
      )
      const compacted = yield* application.history(session.sessionId)
      yield* application.submitUserCommit(
        session.sessionId,
        "Continue from the summary",
        "post-compaction-user"
      )

      let afterFollowUp = compacted
      for (let attempt = 0; attempt < 100; attempt++) {
        afterFollowUp = yield* application.history(session.sessionId)
        if (afterFollowUp.items.some(
          (item) => item.type === "AgentMessageCommit" &&
            item.inReplyTo === "post-compaction-user"
        )) break
        yield* Effect.sleep("10 millis")
      }
      return { beforeCompaction, compacted, afterFollowUp, compaction }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.compaction).toMatchObject({
      type: "CompactionCommit",
      parentId: result.beforeCompaction.branchHeadId,
      inReplyTo: result.beforeCompaction.branchHeadId,
      runId: "compaction-run",
      content: "A compact summary"
    })
    expect(result.compacted.branchHeadId).toBe(result.compaction.commitId)
    expect(result.compacted.items.map((item) => item.type)).toEqual([
      "SessionCreated",
      "UserCommit",
      "AgentMessageCommit",
      "CompactionCommit"
    ])
    expect(result.afterFollowUp.items.map((item) => item.type)).toEqual([
      "SessionCreated",
      "UserCommit",
      "AgentMessageCommit",
      "CompactionCommit",
      "UserCommit",
      "AgentMessageCommit"
    ])
    expect(contexts.map((context) => context.map((entry) => entry.type))).toEqual([
      ["User"],
      ["User", "AgentMessage"],
      ["Compaction", "User"]
    ])
    expect(definitions.map((definition) => definition.id)).toEqual([
      "default",
      "compactor",
      "default"
    ])
  })
)

it.live(
  "cherry-picks an Agent Message onto a same-Session Branch with provenance",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-cherry-pick-")
    let agentRuns = 0
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.sync(() => {
        agentRuns += 1
        return "source Agent Message"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("cherry-pick-session")
      const user = yield* application.submitUserCommit(
        session.sessionId,
        "Create source context",
        "cherry-pick-user"
      )

      let history = yield* application.history(session.sessionId)
      for (let attempt = 0; attempt < 100; attempt++) {
        if (history.items.some((item) => item.type === "AgentMessageCommit")) break
        yield* Effect.sleep("10 millis")
        history = yield* application.history(session.sessionId)
      }
      const source = history.items.find(
        (item): item is Extract<Session.Commit, { readonly type: "AgentMessageCommit" }> =>
          item.type === "AgentMessageCommit"
      )
      if (source === undefined) {
        return yield* Effect.die("timed out waiting for source Agent Message")
      }

      yield* application.checkout(session.sessionId, user.commitId, "target-peer")
      const first = yield* application.cherryPick(
        session.sessionId,
        source.commitId,
        session.sessionId,
        "target-peer"
      )
      const second = yield* application.cherryPick(
        session.sessionId,
        source.commitId,
        session.sessionId,
        "target-peer"
      )
      const sourceHistory = yield* application.history(
        session.sessionId,
        Session.defaultPeerId
      )
      const targetHistory = yield* application.history(
        session.sessionId,
        "target-peer"
      )
      return { source, first, second, sourceHistory, targetHistory }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.first).toMatchObject({
      type: "AgentMessageCommit",
      parentId: "cherry-pick-user",
      content: "source Agent Message",
      provenance: {
        workstreamId: Session.defaultWorkstreamId,
        sessionId: "cherry-pick-session",
        commitId: result.source.commitId
      }
    })
    expect(result.second.commitId).not.toBe(result.first.commitId)
    expect(result.second.parentId).toBe(result.first.commitId)
    expect(result.targetHistory.branchHeadId).toBe(result.second.commitId)
    expect(result.sourceHistory.branchHeadId).toBe(result.source.commitId)
    expect(agentRuns).toBe(1)
  })
)

it.live(
  "cherry-picks a Compaction across Sessions as target context without compacting it",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-cherry-compaction-")
    const contexts: Array<ReadonlyArray<Agent.ContextEntry>> = []
    let agentRuns = 0
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context, _onEvent, definition) => Effect.sync(() => {
        contexts.push(context)
        agentRuns += 1
        return definition?.id === "compactor"
          ? "source Compaction summary"
          : "source Agent Message"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workstream = yield* application.createWorkstream(
        "cherry-source-workstream",
        "Cherry Source"
      )
      const sourceSession = yield* application.createSession(
        "cherry-compaction-source",
        workstream.workstreamId
      )
      yield* application.submitUserCommit(
        sourceSession.sessionId,
        "Build source context",
        "cherry-compaction-user"
      )

      let sourceHistory = yield* application.history(sourceSession.sessionId)
      for (let attempt = 0; attempt < 100; attempt++) {
        if (sourceHistory.items.some((item) => item.type === "AgentMessageCommit")) break
        yield* Effect.sleep("10 millis")
        sourceHistory = yield* application.history(sourceSession.sessionId)
      }
      const sourceHeadId = sourceHistory.branchHeadId
      if (sourceHeadId === null) {
        return yield* Effect.die("timed out waiting for source Branch Head")
      }
      const compaction = yield* application.compact(
        sourceSession.sessionId,
        sourceHeadId,
        {
          id: "compactor",
          instructions: "Summarize the source Branch.",
          tools: []
        },
        "cherry-compaction-run"
      )
      const targetSession = yield* application.createSession("cherry-compaction-target")
      const runsBeforePick = agentRuns
      const picked = yield* application.cherryPick(
        sourceSession.sessionId,
        compaction.commitId,
        targetSession.sessionId,
        "cherry-target-peer"
      )
      const targetAfterPick = yield* application.history(
        targetSession.sessionId,
        "cherry-target-peer"
      )
      yield* application.submitUserCommit(
        targetSession.sessionId,
        "Continue with the imported summary",
        "cherry-target-user",
        "cherry-target-peer"
      )
      let targetAfterRun = targetAfterPick
      for (let attempt = 0; attempt < 100; attempt++) {
        targetAfterRun = yield* application.history(
          targetSession.sessionId,
          "cherry-target-peer"
        )
        if (targetAfterRun.items.some(
          (item) => item.type === "AgentMessageCommit" &&
            item.inReplyTo === "cherry-target-user"
        )) break
        yield* Effect.sleep("10 millis")
      }
      return {
        sourceSession,
        compaction,
        picked,
        targetAfterPick,
        targetAfterRun,
        runsBeforePick
      }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.picked).toMatchObject({
      type: "AgentMessageCommit",
      parentId: null,
      content: "source Compaction summary",
      provenance: {
        workstreamId: "cherry-source-workstream",
        sessionId: result.sourceSession.sessionId,
        commitId: result.compaction.commitId
      }
    })
    expect(result.targetAfterPick.branchHeadId).toBe(result.picked.commitId)
    expect(result.targetAfterPick.items.some(
      (item) => item.type === "CompactionCommit"
    )).toBe(false)
    expect(result.runsBeforePick).toBe(2)
    expect(result.targetAfterRun.items.map((item) => item.type)).toEqual([
      "SessionCreated",
      "AgentMessageCommit",
      "UserCommit",
      "AgentMessageCommit"
    ])
    expect(contexts.at(-1)?.map((entry) => entry.type)).toEqual([
      "AgentMessage",
      "User"
    ])
  })
)

it.live(
  "rejects User, Tool, Interrupt, and Failure Commits as Cherry-pick sources",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-cherry-rejections-")
    let markInterruptStarted!: () => void
    const interruptStarted = new Promise<void>((resolve) => {
      markInterruptStarted = resolve
    })
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context, onEvent, definition) => {
        if (definition?.id === "tool") {
          return Effect.gen(function*() {
            yield* onEvent({
              type: "ToolCall",
              id: "cherry-tool-call",
              name: "Lookup",
              input: { query: "source" }
            })
            yield* onEvent({
              type: "ToolResult",
              id: "cherry-tool-call",
              name: "Lookup",
              result: { found: true },
              isFailure: false
            })
            return "tool response"
          })
        }
        if (definition?.id === "failure") {
          return Effect.die(new Error("source run failed"))
        }
        if (context.some(
          (entry) => entry.type === "User" && entry.content === "Interrupt me"
        )) {
          return Effect.gen(function*() {
            yield* onEvent({ type: "TextDelta", text: "partial source" })
            markInterruptStarted()
            yield* Effect.promise(() => new Promise<void>(() => undefined))
            return "unreachable"
          })
        }
        return Effect.succeed("normal source response")
      }
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const sourceSession = yield* application.createSession("cherry-rejection-source")
      const user = yield* application.submitUserCommit(
        sourceSession.sessionId,
        "Create a source root",
        "cherry-rejection-user"
      )

      let history = yield* application.history(sourceSession.sessionId)
      for (let attempt = 0; attempt < 100; attempt++) {
        if (history.items.some((item) => item.type === "AgentMessageCommit")) break
        yield* Effect.sleep("10 millis")
        history = yield* application.history(sourceSession.sessionId)
      }
      yield* application.startAgentRun(
        sourceSession.sessionId,
        user.commitId,
        { id: "tool", instructions: "Use a tool.", tools: [] },
        "cherry-tool-run"
      )
      let tool = (yield* application.history(sourceSession.sessionId)).items.find(
        (item): item is Extract<Session.Commit, { readonly type: "ToolCommit" }> =>
          item.type === "ToolCommit" && item.runId === "cherry-tool-run"
      )
      for (let attempt = 0; attempt < 100 && tool === undefined; attempt++) {
        yield* Effect.sleep("10 millis")
        tool = (yield* application.history(sourceSession.sessionId)).items.find(
          (item): item is Extract<Session.Commit, { readonly type: "ToolCommit" }> =>
            item.type === "ToolCommit" && item.runId === "cherry-tool-run"
        )
      }

      yield* application.startAgentRun(
        sourceSession.sessionId,
        user.commitId,
        { id: "failure", instructions: "Fail.", tools: [] },
        "cherry-failure-run"
      )
      let failure = (yield* application.history(sourceSession.sessionId)).items.find(
        (item): item is Extract<Session.Commit, { readonly type: "FailureCommit" }> =>
          item.type === "FailureCommit" && item.runId === "cherry-failure-run"
      )
      for (let attempt = 0; attempt < 100 && failure === undefined; attempt++) {
        yield* Effect.sleep("10 millis")
        failure = (yield* application.history(sourceSession.sessionId)).items.find(
          (item): item is Extract<Session.Commit, { readonly type: "FailureCommit" }> =>
            item.type === "FailureCommit" && item.runId === "cherry-failure-run"
        )
      }

      const interruptUser = yield* application.submitUserCommit(
        sourceSession.sessionId,
        "Interrupt me",
        "cherry-interrupt-user"
      )
      yield* Effect.promise(() => interruptStarted)
      const interrupt = yield* application.interruptAgentRun(
        sourceSession.sessionId,
        interruptUser.commitId
      )
      const targetSession = yield* application.createSession("cherry-rejection-target")
      const rejected = []
      for (const sourceCommitId of [
        user.commitId,
        tool?.commitId,
        failure?.commitId,
        interrupt?.commitId
      ]) {
        if (sourceCommitId === undefined) continue
        rejected.push(yield* Effect.flip(application.cherryPick(
          sourceSession.sessionId,
          sourceCommitId,
          targetSession.sessionId
        )))
      }
      return { rejected, tool, failure, interrupt }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.rejected).toHaveLength(4)
    expect(result.rejected.every((error) => error instanceof Session.CommitNotFound))
      .toBe(true)
  })
)

it.live(
  "records a durable Failure Commit when compaction fails",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-compaction-failure-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, _onEvent, definition) => definition?.id === "failing-compactor"
        ? Effect.die(new Error("compaction crashed\nhidden detail"))
        : Effect.succeed("durable source answer")
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const session = yield* application.createSession("compaction-failure-session")
      yield* application.submitUserCommit(
        session.sessionId,
        "Build a summary",
        "compaction-failure-user"
      )

      let beforeCompaction = yield* application.history(session.sessionId)
      for (let attempt = 0; attempt < 100; attempt++) {
        if (beforeCompaction.items.some(
          (item) => item.type === "AgentMessageCommit"
        )) break
        yield* Effect.sleep("10 millis")
        beforeCompaction = yield* application.history(session.sessionId)
      }
      const sourceHeadId = beforeCompaction.branchHeadId
      if (sourceHeadId === null) {
        return yield* Effect.die("timed out waiting for the source Branch Head")
      }
      const error = yield* Effect.flip(application.compact(
        session.sessionId,
        sourceHeadId,
        {
          id: "failing-compactor",
          instructions: "Summarize the Branch.",
          tools: []
        },
        "failing-compaction-run"
      ))
      return { error, history: yield* application.history(session.sessionId) }
    }).pipe(Effect.provide(runtimeLayer(path, fakeAgent)))

    expect(result.error).toMatchObject({ message: "compaction crashed" })
    expect(result.error.message).not.toContain("hidden detail")
    expect(result.history.items).toContainEqual(expect.objectContaining({
      type: "FailureCommit",
      inReplyTo: result.history.items.find(
        (item) => item.type === "AgentMessageCommit"
      )?.commitId,
      runId: "failing-compaction-run",
      reason: "compaction crashed"
    }))
  })
)
