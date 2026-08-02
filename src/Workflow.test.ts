import { expect, it } from "@effect/vitest"
import { BunCrypto } from "@effect/platform-bun"
import { Effect, Fiber, Layer } from "effect"
import { Agent } from "./Agent.ts"
import * as AgentRuntime from "./AgentRuntime.ts"
import * as Application from "./Application.ts"
import * as Session from "./Session.ts"
import * as Workflow from "./Workflow.ts"
import { temporaryDatabase } from "./TestSupport.ts"

const workflowLayer = (
  path: string,
  fakeAgent: Layer.Layer<Agent.Service>
) => Workflow.layerWithoutDependencies.pipe(
  Layer.provideMerge(Application.layerWithoutDependencies.pipe(
    Layer.provide(AgentRuntime.layerWithoutDependencies),
    Layer.provide(Session.layer(path)),
    Layer.provide(fakeAgent),
    Layer.provide(BunCrypto.layer)
  ))
)

it.live(
  "runs a focused child Session from explicit root context",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-workflow-")
    const contexts: Array<ReadonlyArray<Agent.ContextEntry>> = []
    const definitions: Array<Agent.Definition> = []
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context, _onEvent, definition) => Effect.sync(() => {
        contexts.push(context)
        definitions.push(definition ?? Agent.defaultDefinition)
        return definition?.id === "focused-agent"
          ? "focused durable outcome"
          : "caller outcome"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const workstream = yield* application.createWorkstream(
        "workflow-workstream",
        "Workflow Workstream"
      )
      const caller = yield* application.createSession(
        "workflow-caller",
        workstream.workstreamId
      )
      const starting = yield* application.submitUserCommit(
        caller.sessionId,
        "caller ancestry that must not leak",
        "workflow-starting-commit"
      )

      const focused = yield* workflow.runFocused({
        agent: {
          id: "focused-agent",
          instructions: "Run the focused workflow Agent.",
          tools: []
        },
        sessionId: caller.sessionId,
        startingCommitId: starting.commitId
      }, {
        sessionId: "workflow-child",
        rootContext: "Only this explicit root context belongs in the child."
      })
      return { caller, starting, focused }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.focused.session).toMatchObject({
      type: "SessionCreated",
      sessionId: "workflow-child",
      workstreamId: "workflow-workstream",
      origin: {
        workstreamId: "workflow-workstream",
        sessionId: "workflow-caller",
        commitId: "workflow-starting-commit"
      }
    })
    expect(result.focused.root).toMatchObject({
      type: "UserCommit",
      sessionId: "workflow-child",
      parentId: null,
      content: "Only this explicit root context belongs in the child."
    })
    expect(result.focused.outcome).toMatchObject({
      type: "AgentMessageCommit",
      sessionId: "workflow-child",
      inReplyTo: result.focused.root.commitId,
      content: "focused durable outcome"
    })
    expect(contexts.find((_, index) => definitions[index]?.id === "focused-agent")).toEqual([
      {
        type: "User",
        commitId: result.focused.root.commitId,
        content: "Only this explicit root context belongs in the child."
      }
    ])

    const childHistory = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      return yield* application.history("workflow-child")
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))
    expect(childHistory.items).toEqual([
      result.focused.session,
      result.focused.root,
      result.focused.outcome
    ])
    expect(childHistory.items).not.toContainEqual(
      expect.objectContaining({ content: result.starting.content })
    )
  })
)

it.live(
  "runs Code Review through Integration and starts an explicit fixer Run",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-code-review-")
    const contexts: Array<ReadonlyArray<Agent.ContextEntry>> = []
    const definitions: Array<Agent.Definition> = []
    const reviewer: Agent.Definition = {
      id: "reviewer",
      instructions: "Inspect the current repository and report actionable findings.",
      tools: []
    }
    const fixer: Agent.Definition = {
      id: "fixer",
      instructions: "Address the integrated Code Review findings.",
      tools: []
    }
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context, _onEvent, definition) => Effect.sync(() => {
        contexts.push(context)
        definitions.push(definition ?? Agent.defaultDefinition)
        return definition?.id === "fixer"
          ? "fixer completed the remediation"
          : definition?.id === "reviewer"
          ? context.some((entry) => entry.type === "AgentMessage")
            ? "durable review summary"
            : "review findings"
          : "unexpected Agent"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const workstream = yield* application.createWorkstream(
        "code-review-workstream",
        "Code Review Workstream"
      )
      const caller = yield* application.createSession(
        "code-review-caller",
        workstream.workstreamId
      )
      const starting = yield* application.createRootContext(
        caller.sessionId,
        "Current implementation context",
        "code-review-starting"
      )

      const codeReview = yield* workflow.codeReview({
        agent: reviewer,
        sessionId: caller.sessionId,
        startingCommitId: starting.commitId
      }, {
        reviewInstructions: "Review the current repository for correctness issues.",
        remediationInstructions: "Fix every actionable finding from the Code Review.",
        settlement: "integrate and settle",
        reviewSessionId: "code-review-session",
        reviewRunId: "code-review-run",
        integrationRunId: "code-review-compaction",
        remediationCommitId: "code-review-remediation",
        fixerRunId: "code-review-fixer-run",
        fixer
      })

      return {
        caller,
        starting,
        codeReview,
        reviewHistory: yield* application.history(codeReview.review.session.sessionId),
        callerHistory: yield* application.history(caller.sessionId)
      }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.codeReview.review.session).toMatchObject({
      type: "SessionCreated",
      sessionId: "code-review-session",
      workstreamId: "code-review-workstream",
      origin: {
        workstreamId: "code-review-workstream",
        sessionId: result.caller.sessionId,
        commitId: result.starting.commitId
      }
    })
    expect(result.codeReview.review.root).toMatchObject({
      type: "UserCommit",
      parentId: null,
      content: "Review the current repository for correctness issues.",
      autoRun: false
    })
    expect(result.codeReview.review.outcome).toMatchObject({
      type: "AgentMessageCommit",
      content: "review findings",
      inReplyTo: result.codeReview.review.root.commitId,
      runId: "code-review-run"
    })
    expect(result.codeReview.integration.compaction).toMatchObject({
      type: "CompactionCommit",
      parentId: result.codeReview.review.outcome.commitId,
      inReplyTo: result.codeReview.review.outcome.commitId,
      content: "durable review summary",
      runId: "code-review-compaction"
    })
    expect(result.codeReview.integration.picked).toMatchObject({
      type: "AgentMessageCommit",
      sessionId: result.caller.sessionId,
      parentId: result.starting.commitId,
      content: "durable review summary",
      provenance: {
        workstreamId: "code-review-workstream",
        sessionId: "code-review-session",
        commitId: result.codeReview.integration.compaction.commitId
      }
    })
    expect(result.codeReview.integration.settlement).toMatchObject({
      type: "SessionSettled",
      sessionId: "code-review-session"
    })
    expect(result.codeReview.remediation).toMatchObject({
      type: "UserCommit",
      parentId: result.codeReview.integration.picked.commitId,
      content: "Fix every actionable finding from the Code Review.",
      autoRun: false
    })
    expect(result.codeReview.fixer).toMatchObject({
      type: "AgentMessageCommit",
      parentId: result.codeReview.remediation.commitId,
      inReplyTo: result.codeReview.remediation.commitId,
      content: "fixer completed the remediation",
      runId: "code-review-fixer-run"
    })
    expect(result.reviewHistory.settled).toBe(true)
    expect(result.callerHistory.branchHeadId).toBe(result.codeReview.fixer.commitId)
    expect(result.callerHistory.items.map((item) => item.type)).toEqual([
      "SessionCreated",
      "UserCommit",
      "AgentMessageCommit",
      "UserCommit",
      "AgentMessageCommit"
    ])
    expect(result.reviewHistory.items.map((item) => item.type)).toEqual([
      "SessionCreated",
      "UserCommit",
      "AgentMessageCommit",
      "CompactionCommit",
      "SessionSettled"
    ])
    expect(definitions.map((definition) => definition.id)).toEqual([
      "reviewer",
      "reviewer",
      "fixer"
    ])
    expect(contexts).toEqual([
      [{
        type: "User",
        commitId: result.codeReview.review.root.commitId,
        content: "Review the current repository for correctness issues."
      }],
      [
        {
          type: "User",
          commitId: result.codeReview.review.root.commitId,
          content: "Review the current repository for correctness issues."
        },
        {
          type: "AgentMessage",
          commitId: result.codeReview.review.outcome.commitId,
          content: "review findings"
        }
      ],
      [
        {
          type: "User",
          commitId: result.starting.commitId,
          content: "Current implementation context"
        },
        {
          type: "AgentMessage",
          commitId: result.codeReview.integration.picked.commitId,
          content: "durable review summary"
        },
        {
          type: "User",
          commitId: result.codeReview.remediation.commitId,
          content: "Fix every actionable finding from the Code Review."
        }
      ]
    ])
  })
)

it.live(
  "keeps completed child operations when later Workflow orchestration fails",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-workflow-failure-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("focused durable outcome")
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const workstream = yield* application.createWorkstream(
        "workflow-failure-workstream",
        "Workflow Failure Workstream"
      )
      const caller = yield* application.createSession(
        "workflow-failure-caller",
        workstream.workstreamId
      )
      const starting = yield* application.submitUserCommit(
        caller.sessionId,
        "starting context",
        "workflow-failure-starting"
      )

      const failure = yield* Effect.flip(workflow.invoke({
        agent: Agent.defaultDefinition,
        workstreamId: workstream.workstreamId,
        sessionId: caller.sessionId,
        startingCommitId: starting.commitId
      }, (context) => Effect.gen(function*() {
        const focused = yield* context.createFocusedSession({
          rootContext: "completed before orchestration failed",
          sessionId: "workflow-failure-child"
        })
        return yield* Effect.fail(new Error(
          `stop after ${focused.root.commitId}`
        ))
      })))

      return {
        failure,
        history: yield* application.history("workflow-failure-child")
      }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.failure).toBeInstanceOf(Error)
    expect(result.history.items).toEqual([
      expect.objectContaining({ type: "SessionCreated" }),
      expect.objectContaining({
        type: "UserCommit",
        parentId: null,
        content: "completed before orchestration failed"
      })
    ])
  })
)

it.live(
  "leaves the review Session active when Code Review is not settled",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-code-review-active-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, _onEvent, definition) => Effect.succeed(
        definition?.id === "fixer" ? "fixed findings" : "review summary"
      )
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const caller = yield* application.createSession("code-review-active-caller")
      const starting = yield* application.createRootContext(
        caller.sessionId,
        "implementation",
        "code-review-active-starting"
      )
      const codeReview = yield* workflow.codeReview({
        agent: {
          id: "reviewer",
          instructions: "Review.",
          tools: []
        },
        sessionId: caller.sessionId,
        startingCommitId: starting.commitId
      }, {
        reviewInstructions: "Review the implementation.",
        remediationInstructions: "Fix the findings.",
        settlement: "integrate",
        reviewSessionId: "code-review-active-session",
        fixerRunId: "code-review-active-fixer-run",
        fixer: {
          id: "fixer",
          instructions: "Fix.",
          tools: []
        }
      })
      return {
        codeReview,
        reviewHistory: yield* application.history("code-review-active-session")
      }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.codeReview.integration.settlement).toBeUndefined()
    expect(result.reviewHistory.settled).toBe(false)
    expect(result.reviewHistory.branchHeadId).toBe(
      result.codeReview.integration.compaction.commitId
    )
  })
)

it.live(
  "returns a durable fixer Failure Commit after Code Review progress is integrated",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-code-review-failure-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, _onEvent, definition) => definition?.id === "fixer"
        ? Effect.die(new Error("fixer crashed\nhidden detail"))
        : Effect.succeed("review summary")
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const caller = yield* application.createSession("code-review-failure-caller")
      const starting = yield* application.createRootContext(
        caller.sessionId,
        "implementation",
        "code-review-failure-starting"
      )
      const codeReview = yield* workflow.codeReview({
        agent: Agent.defaultDefinition,
        sessionId: caller.sessionId,
        startingCommitId: starting.commitId
      }, {
        reviewInstructions: "Review the implementation.",
        remediationInstructions: "Fix the findings.",
        settlement: "integrate and settle",
        reviewSessionId: "code-review-failure-session",
        fixerRunId: "code-review-failure-fixer-run",
        fixer: {
          id: "fixer",
          instructions: "Fix.",
          tools: []
        }
      })
      return {
        codeReview,
        reviewHistory: yield* application.history("code-review-failure-session"),
        callerHistory: yield* application.history(caller.sessionId)
      }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.codeReview.fixer).toMatchObject({
      type: "FailureCommit",
      inReplyTo: result.codeReview.remediation.commitId,
      runId: "code-review-failure-fixer-run",
      reason: "fixer crashed"
    })
    expect(result.reviewHistory.settled).toBe(true)
    expect(result.reviewHistory.items).toContainEqual(expect.objectContaining({
      type: "CompactionCommit"
    }))
    expect(result.callerHistory.items).toContainEqual(expect.objectContaining({
      type: "UserCommit",
      commitId: result.codeReview.remediation.commitId,
      autoRun: false
    }))
    expect(result.callerHistory.branchHeadId).toBe(result.codeReview.fixer.commitId)
  })
)

it.live(
  "requires the invoking Commit to be the caller's local Branch Head",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-workflow-head-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("unreachable")
    }))

    const error = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const workstream = yield* application.createWorkstream(
        "workflow-head-workstream",
        "Workflow Head Workstream"
      )
      const caller = yield* application.createSession(
        "workflow-head-caller",
        workstream.workstreamId
      )
      const root = yield* application.createRootContext(
        caller.sessionId,
        "historical root",
        "workflow-head-root"
      )
      yield* application.createRootContext(
        caller.sessionId,
        "current head",
        "workflow-head-current"
      )

      return yield* Effect.flip(workflow.runFocused({
        agent: Agent.defaultDefinition,
        workstreamId: workstream.workstreamId,
        sessionId: caller.sessionId,
        startingCommitId: root.commitId
      }, {
        rootContext: "must not run"
      }))
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(error).toMatchObject({
      _tag: "@corredor/Workflow/InvalidInvocation",
      message: "Workflow invocation must start from the caller's Branch Head"
    })
  })
)

it.live(
  "integrates a selected source Branch into a distinct target without running the target",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-integration-")
    const contexts: Array<ReadonlyArray<Agent.ContextEntry>> = []
    const definitions: Array<Agent.Definition> = []
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context, _onEvent, definition) => Effect.sync(() => {
        contexts.push(context)
        definitions.push(definition ?? Agent.defaultDefinition)
        return "integrated source summary"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const workstream = yield* application.createWorkstream(
        "integration-workstream",
        "Integration Workstream"
      )
      const source = yield* application.createSession(
        "integration-source",
        workstream.workstreamId
      )
      const sourceRoot = yield* application.createRootContext(
        source.sessionId,
        "source Branch context",
        "integration-source-root"
      )
      const target = yield* application.createSession(
        "integration-target",
        workstream.workstreamId
      )
      const targetRoot = yield* application.createRootContext(
        target.sessionId,
        "target Branch context",
        "integration-target-root"
      )

      const integrated = yield* workflow.integrate({
        agent: {
          id: "integration-compactor",
          instructions: "Summarize the source Branch.",
          tools: []
        },
        sessionId: target.sessionId,
        startingCommitId: targetRoot.commitId
      }, {
        sourceSessionId: source.sessionId,
        sourceBranchHeadId: sourceRoot.commitId,
        targetSessionId: target.sessionId,
        targetBranchHeadId: targetRoot.commitId,
        settlement: "integrate"
      })

      return {
        source,
        target,
        sourceRoot,
        targetRoot,
        integrated,
        sourceHistory: yield* application.history(source.sessionId),
        targetHistory: yield* application.history(target.sessionId)
      }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.integrated.compaction).toMatchObject({
      type: "CompactionCommit",
      sessionId: result.source.sessionId,
      parentId: result.sourceRoot.commitId,
      inReplyTo: result.sourceRoot.commitId,
      content: "integrated source summary"
    })
    expect(result.integrated.picked).toMatchObject({
      type: "AgentMessageCommit",
      sessionId: result.target.sessionId,
      parentId: result.targetRoot.commitId,
      content: "integrated source summary",
      provenance: {
        workstreamId: "integration-workstream",
        sessionId: result.source.sessionId,
        commitId: result.integrated.compaction.commitId
      }
    })
    expect(result.integrated.settlement).toBeUndefined()
    expect(result.sourceHistory.branchHeadId).toBe(
      result.integrated.compaction.commitId
    )
    expect(result.targetHistory.branchHeadId).toBe(
      result.integrated.picked.commitId
    )
    expect(result.sourceHistory.settled).toBe(false)
    expect(result.targetHistory.items.some(
      (item) => item.type === "CompactionCommit"
    )).toBe(false)
    expect(definitions.map((definition) => definition.id)).toEqual([
      "integration-compactor"
    ])
    expect(contexts).toEqual([[
      {
        type: "User",
        commitId: result.sourceRoot.commitId,
        content: "source Branch context"
      }
    ]])
  })
)

it.live(
  "integrates and settles only the source Session when explicitly chosen",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-integration-settle-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("summary before Settlement")
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const source = yield* application.createSession("integration-settle-source")
      const sourceRoot = yield* application.createRootContext(
        source.sessionId,
        "source work",
        "integration-settle-source-root"
      )
      const target = yield* application.createSession("integration-settle-target")
      const targetRoot = yield* application.createRootContext(
        target.sessionId,
        "target work",
        "integration-settle-target-root"
      )

      const integrated = yield* workflow.integrate({
        agent: Agent.defaultDefinition,
        sessionId: target.sessionId,
        startingCommitId: targetRoot.commitId
      }, {
        sourceSessionId: source.sessionId,
        sourceBranchHeadId: sourceRoot.commitId,
        targetSessionId: target.sessionId,
        targetBranchHeadId: targetRoot.commitId,
        settlement: "integrate and settle"
      })

      return {
        integrated,
        sourceHistory: yield* application.history(source.sessionId),
        targetHistory: yield* application.history(target.sessionId)
      }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.integrated.settlement).toMatchObject({
      type: "SessionSettled",
      sessionId: "integration-settle-source"
    })
    expect(result.sourceHistory.settled).toBe(true)
    expect(result.targetHistory.settled).toBe(false)
    expect(result.targetHistory.branchHeadId).toBe(
      result.integrated.picked.commitId
    )
  })
)

it.live(
  "preserves source Compaction when a later target handoff fails",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-integration-failure-")
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("durable summary before target failure")
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const source = yield* application.createSession("integration-failure-source")
      const sourceRoot = yield* application.createRootContext(
        source.sessionId,
        "source context",
        "integration-failure-source-root"
      )
      const target = yield* application.createSession("integration-failure-target")
      const targetRoot = yield* application.createRootContext(
        target.sessionId,
        "settled target context",
        "integration-failure-target-root"
      )
      yield* application.settle(target.sessionId)

      const failure = yield* Effect.flip(workflow.integrate({
        agent: Agent.defaultDefinition,
        sessionId: target.sessionId,
        startingCommitId: targetRoot.commitId
      }, {
        sourceSessionId: source.sessionId,
        sourceBranchHeadId: sourceRoot.commitId,
        targetSessionId: target.sessionId,
        targetBranchHeadId: targetRoot.commitId,
        settlement: "integrate and settle"
      }))

      return {
        failure,
        sourceHistory: yield* application.history(source.sessionId),
        targetHistory: yield* application.history(target.sessionId)
      }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.failure).toMatchObject({
      _tag: "@corredor/Session/Settled",
      sessionId: "integration-failure-target"
    })
    expect(result.sourceHistory.items).toContainEqual(expect.objectContaining({
      type: "CompactionCommit",
      content: "durable summary before target failure"
    }))
    expect(result.sourceHistory.settled).toBe(false)
    expect(result.targetHistory.items).not.toContainEqual(
      expect.objectContaining({ type: "AgentMessageCommit" })
    )
    expect(result.targetHistory.settled).toBe(true)
  })
)

it.live(
  "rejects same-Session Integration and stale selected Branch Heads before Compaction",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-integration-validation-")
    let compactions = 0
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.sync(() => {
        compactions += 1
        return "must not compact"
      })
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const session = yield* application.createSession("integration-validation-session")
      const root = yield* application.createRootContext(
        session.sessionId,
        "one Session only",
        "integration-validation-root"
      )
      const sameSessionError = yield* Effect.flip(workflow.integrate({
        agent: Agent.defaultDefinition,
        sessionId: session.sessionId,
        startingCommitId: root.commitId
      }, {
        sourceSessionId: session.sessionId,
        sourceBranchHeadId: root.commitId,
        targetSessionId: session.sessionId,
        targetBranchHeadId: root.commitId,
        settlement: "integrate"
      }))

      const target = yield* application.createSession("integration-validation-target")
      const targetRoot = yield* application.createRootContext(
        target.sessionId,
        "target head",
        "integration-validation-target-root"
      )
      const staleHeadError = yield* Effect.flip(workflow.integrate({
        agent: Agent.defaultDefinition,
        sessionId: target.sessionId,
        startingCommitId: targetRoot.commitId
      }, {
        sourceSessionId: session.sessionId,
        sourceBranchHeadId: root.commitId,
        targetSessionId: target.sessionId,
        targetBranchHeadId: null,
        settlement: "integrate"
      }))

      return { sameSessionError, staleHeadError }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.sameSessionError).toMatchObject({
      _tag: "@corredor/Application/InvalidIntegration",
      message: "Integration requires distinct source and target Sessions"
    })
    expect(result.staleHeadError).toMatchObject({
      _tag: "@corredor/Application/InvalidIntegration",
      message: "Selected target Branch Head is not the local Branch Head"
    })
    expect(compactions).toBe(0)
  })
)

it.live(
  "does not attach a handoff to a target Branch Head that changes during Compaction",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-integration-race-")
    let markCompactionStarted!: () => void
    const compactionStarted = new Promise<void>((resolve) => {
      markCompactionStarted = resolve
    })
    let releaseCompaction!: () => void
    const compactionGate = new Promise<void>((resolve) => {
      releaseCompaction = resolve
    })
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, _onEvent, definition) => definition?.id === "race-compactor"
        ? Effect.gen(function*() {
          markCompactionStarted()
          yield* Effect.promise(() => compactionGate)
          return "race summary"
        })
        : Effect.succeed("unrelated run")
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const source = yield* application.createSession("integration-race-source")
      const sourceRoot = yield* application.createRootContext(
        source.sessionId,
        "source context",
        "integration-race-source-root"
      )
      const target = yield* application.createSession("integration-race-target")
      const targetRoot = yield* application.createRootContext(
        target.sessionId,
        "selected target context",
        "integration-race-target-root"
      )

      const integration = workflow.integrate({
        agent: {
          id: "race-compactor",
          instructions: "Summarize the source Branch.",
          tools: []
        },
        sessionId: target.sessionId,
        startingCommitId: targetRoot.commitId
      }, {
        sourceSessionId: source.sessionId,
        sourceBranchHeadId: sourceRoot.commitId,
        targetSessionId: target.sessionId,
        targetBranchHeadId: targetRoot.commitId,
        settlement: "integrate"
      })
      const fiber = yield* Effect.forkScoped(integration)
      yield* Effect.promise(() => compactionStarted)
      const changedTarget = yield* application.createRootContext(
        target.sessionId,
        "newer target context",
        "integration-race-target-newer"
      )
      releaseCompaction()
      const failure = yield* Effect.flip(Fiber.join(fiber))
      return {
        changedTarget,
        failure,
        sourceHistory: yield* application.history(source.sessionId),
        targetHistory: yield* application.history(target.sessionId)
      }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.failure).toMatchObject({
      _tag: "@corredor/Application/InvalidIntegration",
      message: "Selected target Branch Head changed during Integration"
    })
    expect(result.sourceHistory.items).toContainEqual(expect.objectContaining({
      type: "CompactionCommit",
      content: "race summary"
    }))
    expect(result.targetHistory.branchHeadId).toBe(result.changedTarget.commitId)
    expect(result.targetHistory.items).not.toContainEqual(
      expect.objectContaining({ type: "AgentMessageCommit", content: "race summary" })
    )
  })
)

it.live(
  "does not hand off a source Branch Head that changes during Compaction",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-integration-source-race-")
    let markCompactionStarted!: () => void
    const compactionStarted = new Promise<void>((resolve) => {
      markCompactionStarted = resolve
    })
    let releaseCompaction!: () => void
    const compactionGate = new Promise<void>((resolve) => {
      releaseCompaction = resolve
    })
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, _onEvent, definition) => definition?.id === "source-race-compactor"
        ? Effect.gen(function*() {
          markCompactionStarted()
          yield* Effect.promise(() => compactionGate)
          return "must not be handed off"
        })
        : Effect.succeed("unrelated run")
    }))

    const result = yield* Effect.gen(function*() {
      const application = yield* Application.Service
      const workflow = yield* Workflow.Service
      const source = yield* application.createSession("integration-source-race-source")
      const sourceRoot = yield* application.createRootContext(
        source.sessionId,
        "selected source context",
        "integration-source-race-root"
      )
      const target = yield* application.createSession("integration-source-race-target")
      const targetRoot = yield* application.createRootContext(
        target.sessionId,
        "target context",
        "integration-source-race-target-root"
      )

      const fiber = yield* Effect.forkScoped(workflow.integrate({
        agent: {
          id: "source-race-compactor",
          instructions: "Summarize the source Branch.",
          tools: []
        },
        sessionId: target.sessionId,
        startingCommitId: targetRoot.commitId
      }, {
        sourceSessionId: source.sessionId,
        sourceBranchHeadId: sourceRoot.commitId,
        targetSessionId: target.sessionId,
        targetBranchHeadId: targetRoot.commitId,
        settlement: "integrate"
      }))
      yield* Effect.promise(() => compactionStarted)
      const changedSource = yield* application.createRootContext(
        source.sessionId,
        "newer source context",
        "integration-source-race-newer"
      )
      releaseCompaction()
      const failure = yield* Effect.flip(Fiber.join(fiber))
      return {
        changedSource,
        failure,
        sourceHistory: yield* application.history(source.sessionId),
        targetHistory: yield* application.history(target.sessionId)
      }
    }).pipe(Effect.provide(workflowLayer(path, fakeAgent)))

    expect(result.failure).toMatchObject({
      _tag: "@corredor/Application/InvalidIntegration",
      message: "Selected source Branch Head changed during Integration"
    })
    expect(result.sourceHistory.branchHeadId).toBe(result.changedSource.commitId)
    expect(result.sourceHistory.items).not.toContainEqual(
      expect.objectContaining({ type: "CompactionCommit", content: "must not be handed off" })
    )
    expect(result.targetHistory.items).not.toContainEqual(
      expect.objectContaining({ type: "AgentMessageCommit" })
    )
  })
)
