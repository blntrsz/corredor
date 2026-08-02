import { Agent } from "./Agent.ts"
import { Context, Effect, Layer, Schema } from "effect"
import * as Application from "./Application.ts"
import * as Session from "./Session.ts"

/** A terminal outcome produced by a stateless Agent Run. */
export type TerminalCommit = Extract<
  Session.Commit,
  {
    readonly type:
      | "AgentMessageCommit"
      | "CompactionCommit"
      | "FailureCommit"
      | "InterruptCommit"
  }
>

export interface Invocation {
  readonly agent: Agent.Definition
  readonly workstreamId?: string
  readonly sessionId: string
  readonly startingCommitId: string
  readonly peerId?: string
}

export interface FocusedSessionInput {
  readonly rootContext: string
  readonly sessionId?: string
  readonly rootCommitId?: string
  readonly runId?: string
  readonly workstreamId?: string
  readonly peerId?: string
}

export interface FocusedSessionRoot {
  readonly session: Session.SessionCreated
  readonly root: Extract<Session.Commit, { readonly type: "UserCommit" }>
}

export interface FocusedSession extends FocusedSessionRoot {
  readonly outcome: TerminalCommit
}

export interface CodeReviewInput {
  /** Explicit instructions recorded as the review Session's root User Commit. */
  readonly reviewInstructions: string
  /** Explicit User Commit content supplied to the fixer after Integration. */
  readonly remediationInstructions: string
  readonly settlement: IntegrationChoice
  readonly reviewSessionId?: string
  readonly reviewRootCommitId?: string
  readonly reviewRunId?: string
  readonly integrationRunId?: string
  readonly remediationCommitId?: string
  readonly fixerRunId?: string
  readonly fixer?: Agent.Definition
}

export interface CodeReviewResult {
  readonly review: FocusedSession
  readonly integration: IntegrationResult
  readonly remediation: Extract<
    Session.Commit,
    { readonly type: "UserCommit" }
  >
  readonly fixer: TerminalCommit
}

export const integrationChoices = Application.integrationChoices
export type IntegrationChoice = Application.IntegrationChoice

/** Selected source and target Branch Heads for a cross-Session handoff. */
export type IntegrationInput = Application.IntegrationInput

export type IntegrationResult = Application.IntegrationResult

export class InvalidInvocation extends Schema.TaggedErrorClass<InvalidInvocation>()(
  "@corredor/Workflow/InvalidInvocation",
  {
    sessionId: Schema.String,
    startingCommitId: Schema.String,
    message: Schema.String
  }
) {}

export class OutcomeTimeout extends Schema.TaggedErrorClass<OutcomeTimeout>()(
  "@corredor/Workflow/OutcomeTimeout",
  {
    sessionId: Schema.String,
    startingCommitId: Schema.String,
    attempts: Schema.Number
  }
) {}

export type Error =
  | Session.Error
  | Session.PersistenceError
  | Application.InvalidIntegration
  | InvalidInvocation
  | OutcomeTimeout

export interface Context {
  readonly invocation: Invocation
  readonly workstreamId: string
  readonly origin: Session.SessionOrigin
  readonly application: Application.Interface
  readonly createFocusedSession: (
    input: FocusedSessionInput
  ) => Effect.Effect<FocusedSessionRoot, Error>
  readonly runFocused: (
    input: FocusedSessionInput
  ) => Effect.Effect<FocusedSession, Error>
  readonly runAgent: (
    sessionId: string,
    startingCommitId: string,
    runId?: string,
    peerId?: string,
    agent?: Agent.Definition
  ) => Effect.Effect<TerminalCommit, Error>
  readonly integrate: (
    input: IntegrationInput
  ) => Effect.Effect<IntegrationResult, Error>
}

export interface Interface {
  /**
   * Runs a Workflow program with typed access to the public Application
   * boundary. Completed domain operations are intentionally not wrapped in a
   * transaction with the program's later orchestration.
   */
  readonly invoke: <A, E, R>(
    invocation: Invocation,
    program: (context: Context) => Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E | Error, R>
  /** Creates a focused child Session, runs the invoking Agent, and observes its outcome. */
  readonly runFocused: (
    invocation: Invocation,
    input: FocusedSessionInput
  ) => Effect.Effect<FocusedSession, Error>
  /** Compacts a source Branch, Cherry-picks it into a target, and optionally settles the source. */
  readonly integrate: (
    invocation: Invocation,
    input: IntegrationInput
  ) => Effect.Effect<IntegrationResult, Error>
  /** Reviews the current Branch, integrates findings, and starts remediation. */
  readonly codeReview: (
    invocation: Invocation,
    input: CodeReviewInput
  ) => Effect.Effect<CodeReviewResult, Error>
}

export class Service extends Context.Service<Service, Interface>()(
  "@corredor/Workflow"
) {}

const terminalCommit = (item: Session.HistoryItem): item is TerminalCommit =>
  item.type === "AgentMessageCommit" ||
  item.type === "CompactionCommit" ||
  item.type === "FailureCommit" ||
  item.type === "InterruptCommit"

const pollAttempts = 24_000
const pollIntervalMillis = 25

interface ResolvedInvocation {
  readonly workstreamId: string
  readonly origin: Session.SessionOrigin
}

export const make = Effect.gen(function*() {
  const application = yield* Application.Service

  const resolveInvocation = Effect.fn("Workflow.resolveInvocation")(
    function*(invocation: Invocation) {
      const history = yield* application.history(
        invocation.sessionId,
        invocation.peerId
      )
      const created = history.items.find(
        (item): item is Session.SessionCreated => item.type === "SessionCreated"
      )
      if (created === undefined) {
        return yield* new Session.NotFound({ sessionId: invocation.sessionId })
      }
      if (!history.items.some(
        (item) => Session.isCommit(item) &&
          item.commitId === invocation.startingCommitId
      )) {
        return yield* new Session.CommitNotFound({
          sessionId: invocation.sessionId,
          commitId: invocation.startingCommitId
        })
      }
      if (history.branchHeadId !== invocation.startingCommitId) {
        return yield* new InvalidInvocation({
          sessionId: invocation.sessionId,
          startingCommitId: invocation.startingCommitId,
          message: "Workflow invocation must start from the caller's Branch Head"
        })
      }

      let workstreamId = invocation.workstreamId ?? created.workstreamId
      if (workstreamId === undefined) {
        const active = yield* application.listSessions(undefined, "active")
        const settled = active.some(
          (session) => session.sessionId === invocation.sessionId
        )
          ? undefined
          : yield* application.listSessions(undefined, "settled")
        workstreamId = (active.find(
          (session) => session.sessionId === invocation.sessionId
        ) ?? settled?.find(
          (session) => session.sessionId === invocation.sessionId
        ))?.workstreamId
      }
      if (workstreamId === undefined) {
        return yield* new InvalidInvocation({
          sessionId: invocation.sessionId,
          startingCommitId: invocation.startingCommitId,
          message: "Could not resolve the caller's Workstream"
        })
      }
      if (
        created.workstreamId !== undefined &&
        invocation.workstreamId !== undefined &&
        created.workstreamId !== invocation.workstreamId
      ) {
        return yield* new InvalidInvocation({
          sessionId: invocation.sessionId,
          startingCommitId: invocation.startingCommitId,
          message: "Invocation Workstream does not own the caller Session"
        })
      }

      return {
        workstreamId,
        origin: {
          workstreamId,
          sessionId: invocation.sessionId,
          commitId: invocation.startingCommitId
        }
      }
    }
  )

  const runAgent = Effect.fn("Workflow.runAgent")(
    function*(
      invocation: Invocation,
      sessionId: string,
      startingCommitId: string,
      runId: string | undefined,
      peerId: string | undefined,
      agent?: Agent.Definition
    ) {
      const run = yield* application.startAgentRun(
        sessionId,
        startingCommitId,
        agent ?? invocation.agent,
        runId,
        peerId
      )

      for (let attempt = 0; attempt < pollAttempts; attempt++) {
        const history = yield* application.history(sessionId, peerId)
        const outcome = history.items.find(
          (item): item is TerminalCommit =>
            terminalCommit(item) &&
            item.inReplyTo === startingCommitId &&
            item.runId === run.runId
        )
        if (outcome !== undefined) return outcome
        yield* Effect.sleep(`${pollIntervalMillis} millis`)
      }

      return yield* new OutcomeTimeout({
        sessionId,
        startingCommitId,
        attempts: pollAttempts
      })
    }
  )

  const createFocusedSession = Effect.fn("Workflow.createFocusedSession")(
    function*(
      invocation: Invocation,
      resolved: ResolvedInvocation,
      input: FocusedSessionInput
    ) {
      const peerId = input.peerId ?? invocation.peerId
      const session = yield* application.createSession(
        input.sessionId,
        input.workstreamId ?? resolved.workstreamId,
        peerId,
        resolved.origin
      )
      const root = yield* application.createRootContext(
        session.sessionId,
        input.rootContext,
        input.rootCommitId,
        peerId
      )
      return { session, root }
    }
  )

  const runFocusedChild = Effect.fn("Workflow.runFocusedChild")(
    function*(
      invocation: Invocation,
      resolved: ResolvedInvocation,
      input: FocusedSessionInput
    ) {
      const focused = yield* createFocusedSession(invocation, resolved, input)
      const peerId = input.peerId ?? invocation.peerId
      const outcome = yield* runAgent(
        invocation,
        focused.session.sessionId,
        focused.root.commitId,
        input.runId,
        peerId
      )
      return { ...focused, outcome }
    }
  )

  const invoke: Interface["invoke"] = Effect.fn("Workflow.invoke")(
    function*<A, E, R>(
      invocation: Invocation,
      program: (context: Context) => Effect.Effect<A, E, R>
    ) {
      const resolved = yield* resolveInvocation(invocation)
      const context: Context = {
        invocation,
        workstreamId: resolved.workstreamId,
        origin: resolved.origin,
        application,
        createFocusedSession: (input) => createFocusedSession(
          invocation,
          resolved,
          input
        ),
        runFocused: (input) => runFocusedChild(
          invocation,
          resolved,
          input
        ),
        runAgent: (sessionId, startingCommitId, runId, peerId, agent) => runAgent(
          invocation,
          sessionId,
          startingCommitId,
          runId,
          peerId,
          agent
        ),
        integrate: (input) => application.integrate({
          ...input,
          agent: input.agent ?? invocation.agent,
          peerId: input.peerId ?? invocation.peerId
        })
      }
      return yield* program(context)
    }
  )

  const runFocused: Interface["runFocused"] = Effect.fn("Workflow.runFocused")(
    function*(invocation, input) {
      return yield* invoke(invocation, (context) => context.runFocused(input))
    }
  )

  const integrate: Interface["integrate"] = Effect.fn("Workflow.integrate")(
    function*(invocation, input) {
      return yield* invoke(invocation, (context) => context.integrate(input))
    }
  )

  const codeReview: Interface["codeReview"] = Effect.fn("Workflow.codeReview")(
    function*(invocation, input) {
      return yield* invoke(invocation, (context) => Effect.gen(function*() {
        const review = yield* context.runFocused({
          rootContext: input.reviewInstructions,
          sessionId: input.reviewSessionId,
          rootCommitId: input.reviewRootCommitId,
          runId: input.reviewRunId
        })
        const integration = yield* context.integrate({
          sourceSessionId: review.session.sessionId,
          sourceBranchHeadId: review.outcome.commitId,
          targetSessionId: invocation.sessionId,
          targetBranchHeadId: invocation.startingCommitId,
          settlement: input.settlement,
          runId: input.integrationRunId
        })
        const remediation = yield* context.application.submitUserCommit(
          invocation.sessionId,
          input.remediationInstructions,
          input.remediationCommitId,
          invocation.peerId,
          { autoRun: false }
        )
        const fixerOutcome = yield* context.runAgent(
          invocation.sessionId,
          remediation.commitId,
          input.fixerRunId,
          invocation.peerId,
          input.fixer
        )
        return { review, integration, remediation, fixer: fixerOutcome }
      }))
    }
  )

  return Service.of({ invoke, runFocused, integrate, codeReview })
})

export const layerWithoutDependencies = Layer.effect(Service, make)

export const layer = (path = Session.defaultDatabasePath) =>
  layerWithoutDependencies.pipe(
    Layer.provide(Application.layer(path))
  )
