import { expect, it } from "@effect/vitest"
import { BunCrypto } from "@effect/platform-bun"
import { Effect, Layer } from "effect"
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
