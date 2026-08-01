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
