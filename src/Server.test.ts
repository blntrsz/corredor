import { expect, it } from "@effect/vitest"
import { createServer } from "node:net"
import { Database } from "bun:sqlite"
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
        Agent.defaultDefinition,
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

it.live(
  "compacts a Branch through HTTP and emits the Compaction Commit on the stream",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-http-compaction-")
    const port = yield* Effect.promise(availablePort)
    const baseUrl = `http://127.0.0.1:${port}`
    const contexts: Array<ReadonlyArray<Agent.ContextEntry>> = []
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (context, _onEvent, definition) => Effect.sync(() => {
        contexts.push(context)
        return definition?.id === "compactor"
          ? "HTTP compacted summary"
          : "HTTP answer"
      })
    }))
    const layer = Layer.mergeAll(
      Server.layerWithoutDependencies({ host: "127.0.0.1", port }),
      AgentProxy.layer({ baseUrl, peerId: "http-compaction-peer" })
    ).pipe(
      Layer.provide(Session.layer(path)),
      Layer.provide(fakeAgent)
    )

    const result = yield* Effect.gen(function*() {
      const proxy = yield* AgentProxy.Service
      const session = yield* proxy.createSession("http-compaction-session")
      const activityFiber = yield* proxy.streamActivity(
        session.sessionId,
        session.position
      ).pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkScoped
      )
      yield* proxy.submitUserCommit(
        session.sessionId,
        "Explain the HTTP boundary",
        "http-compaction-user"
      )

      let beforeCompaction = yield* proxy.history(session.sessionId)
      for (let attempt = 0; attempt < 100; attempt++) {
        if (beforeCompaction.items.some(
          (item) => item.type === "AgentMessageCommit"
        )) break
        yield* Effect.sleep("10 millis")
        beforeCompaction = yield* proxy.history(session.sessionId)
      }
      const sourceHeadId = beforeCompaction.branchHeadId
      if (sourceHeadId === null) {
        return yield* Effect.die("timed out waiting for the HTTP Branch Head")
      }
      const compaction = yield* proxy.compact(
        session.sessionId,
        sourceHeadId,
        {
          id: "compactor",
          instructions: "Summarize the HTTP Branch.",
          tools: []
        },
        "http-compaction-run"
      )
      const activity = Array.from(yield* Fiber.join(activityFiber))
      yield* proxy.submitUserCommit(
        session.sessionId,
        "Continue after HTTP compaction",
        "http-post-compaction-user"
      )
      let history = yield* proxy.history(session.sessionId)
      for (let attempt = 0; attempt < 100; attempt++) {
        if (history.items.some(
          (item) => item.type === "AgentMessageCommit" &&
            item.inReplyTo === "http-post-compaction-user"
        )) break
        yield* Effect.sleep("10 millis")
        history = yield* proxy.history(session.sessionId)
      }
      return { sourceHeadId, compaction, activity, history, contexts }
    }).pipe(Effect.provide(layer))

    expect(result.compaction).toMatchObject({
      type: "CompactionCommit",
      parentId: result.sourceHeadId,
      inReplyTo: result.sourceHeadId,
      runId: "http-compaction-run",
      content: "HTTP compacted summary"
    })
    expect(result.activity.map((item) => item.type)).toEqual([
      "UserCommit",
      "AgentMessageCommit",
      "CompactionCommit"
    ])
    expect(result.history.items).toContainEqual(result.compaction)
    expect(result.history.branchHeadId).not.toBe(result.compaction.commitId)
    expect(result.contexts.map((context) => context.map((entry) => entry.type))).toEqual([
      ["User"],
      ["User", "AgentMessage"],
      ["Compaction", "User"]
    ])
  })
)

it.live(
  "cherry-picks source context across Sessions through HTTP",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-http-cherry-pick-")
    const port = yield* Effect.promise(availablePort)
    const baseUrl = `http://127.0.0.1:${port}`
    let agentRuns = 0
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.sync(() => {
        agentRuns += 1
        return "HTTP source context"
      })
    }))
    const layer = Layer.mergeAll(
      Server.layerWithoutDependencies({ host: "127.0.0.1", port }),
      AgentProxy.layer({ baseUrl, peerId: "http-cherry-pick-peer" })
    ).pipe(
      Layer.provide(Session.layer(path)),
      Layer.provide(fakeAgent)
    )

    const result = yield* Effect.gen(function*() {
      const proxy = yield* AgentProxy.Service
      const sourceSession = yield* proxy.createSession("http-cherry-source")
      yield* proxy.submitUserCommit(
        sourceSession.sessionId,
        "Create context for another Session",
        "http-cherry-user"
      )

      let sourceHistory = yield* proxy.history(sourceSession.sessionId)
      for (let attempt = 0; attempt < 100; attempt++) {
        if (sourceHistory.items.some((item) => item.type === "AgentMessageCommit")) break
        yield* Effect.sleep("10 millis")
        sourceHistory = yield* proxy.history(sourceSession.sessionId)
      }
      const source = sourceHistory.items.find(
        (item): item is Extract<Session.Commit, { readonly type: "AgentMessageCommit" }> =>
          item.type === "AgentMessageCommit"
      )
      if (source === undefined) {
        return yield* Effect.die("timed out waiting for HTTP source Agent Message")
      }

      const targetSession = yield* proxy.createSession("http-cherry-target")
      const picked = yield* proxy.cherryPick(
        sourceSession.sessionId,
        source.commitId,
        targetSession.sessionId
      )
      const targetHistory = yield* proxy.history(targetSession.sessionId)
      return { sourceSession, source, picked, targetHistory }
    }).pipe(Effect.provide(layer))

    expect(result.picked).toMatchObject({
      type: "AgentMessageCommit",
      parentId: null,
      content: "HTTP source context",
      provenance: {
        workstreamId: Session.defaultWorkstreamId,
        sessionId: result.sourceSession.sessionId,
        commitId: result.source.commitId
      }
    })
    expect(result.targetHistory.branchHeadId).toBe(result.picked.commitId)
    expect(agentRuns).toBe(1)
  })
)

it.live(
  "interrupts a streamed Agent Run through the HTTP boundary",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-http-interrupt-")
    const port = yield* Effect.promise(availablePort)
    const baseUrl = `http://127.0.0.1:${port}`
    let markOutputSeen!: () => void
    const outputSeen = new Promise<void>((resolve) => {
      markOutputSeen = resolve
    })
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: (_context, onEvent) => Effect.gen(function*() {
        yield* onEvent({ type: "TextDelta", text: "partial over HTTP" })
        markOutputSeen()
        yield* Effect.promise(() => new Promise<void>(() => undefined))
        return "unreachable"
      })
    }))
    const layer = Layer.mergeAll(
      Server.layerWithoutDependencies({ host: "127.0.0.1", port }),
      AgentProxy.layer({ baseUrl, peerId: "http-interrupt-peer" })
    ).pipe(
      Layer.provide(Session.layer(path)),
      Layer.provide(fakeAgent)
    )

    const result = yield* Effect.gen(function*() {
      const proxy = yield* AgentProxy.Service
      const session = yield* proxy.createSession("http-interrupt-session")
      const activityFiber = yield* proxy.streamActivity(
        session.sessionId,
        session.position
      ).pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkScoped
      )
      const user = yield* proxy.submitUserCommit(
        session.sessionId,
        "Answer until stopped",
        "http-interrupt-user"
      )
      yield* Effect.promise(() => outputSeen)
      const interrupt = yield* proxy.interruptAgentRun(
        session.sessionId,
        user.commitId,
        "Stopped from the client"
      )
      const activity = Array.from(yield* Fiber.join(activityFiber))
      const history = yield* proxy.history(session.sessionId)
      return { interrupt, activity, history }
    }).pipe(Effect.provide(layer))

    expect(result.interrupt).toMatchObject({
      type: "InterruptCommit",
      inReplyTo: "http-interrupt-user",
      reason: "Stopped from the client",
      partialOutput: "partial over HTTP"
    })
    expect(result.activity.map((item) => item.type)).toEqual([
      "UserCommit",
      "InterruptCommit"
    ])
    expect(result.history.items.some((item) => item.type === "FailureCommit"))
      .toBe(false)
  })
)

it.live(
  "creates Workstreams and groups public Session operations under them",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-workstreams-")
    const port = yield* Effect.promise(availablePort)
    const baseUrl = `http://127.0.0.1:${port}`
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("")
    }))
    const layer = Layer.mergeAll(
      Server.layerWithoutDependencies({ host: "127.0.0.1", port }),
      AgentProxy.layer({ baseUrl, peerId: "workstream-peer" })
    ).pipe(
      Layer.provide(Session.layer(path)),
      Layer.provide(fakeAgent)
    )

    const result = yield* Effect.gen(function*() {
      const proxy = yield* AgentProxy.Service
      const workstream = yield* proxy.createWorkstream(
        "workstream-http",
        "HTTP Workstream"
      )
      const first = yield* proxy.createSession(
        "workstream-session-1",
        workstream.workstreamId
      )
      const second = yield* proxy.createSession(
        "workstream-session-2",
        workstream.workstreamId
      )
      const workstreams = yield* proxy.listWorkstreams()
      const inspected = yield* proxy.workstream(workstream.workstreamId)
      const filtered = yield* proxy.listSessions(workstream.workstreamId)
      const duplicateStatus = yield* Effect.promise(async () => {
        const response = await fetch(`${baseUrl}/v1/workstreams`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ workstreamId: workstream.workstreamId })
        })
        await response.text()
        return response.status
      })
      const missingStatus = yield* Effect.promise(async () => {
        const response = await fetch(
          `${baseUrl}/v1/workstreams/missing-workstream`
        )
        await response.text()
        return response.status
      })
      const missingOwnerStatus = yield* Effect.promise(async () => {
        const response = await fetch(`${baseUrl}/v1/sessions`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            sessionId: "missing-owner-session",
            workstreamId: "missing-workstream"
          })
        })
        await response.text()
        return response.status
      })
      return {
        workstream,
        first,
        second,
        workstreams,
        inspected,
        filtered,
        duplicateStatus,
        missingStatus,
        missingOwnerStatus
      }
    }).pipe(Effect.provide(layer))

    expect(result.workstream).toMatchObject({
      workstreamId: "workstream-http",
      name: "HTTP Workstream",
      peerId: "workstream-peer"
    })
    expect(result.workstreams).toContainEqual(expect.objectContaining({
      workstreamId: "workstream-http",
      sessionCount: 2
    }))
    expect(result.inspected.workstream).toMatchObject({
      workstreamId: "workstream-http",
      sessionCount: 2
    })
    expect(result.inspected.sessions.map((session) => session.sessionId)).toEqual([
      result.second.sessionId,
      result.first.sessionId
    ])
    expect(result.filtered.map((session) => session.workstreamId)).toEqual([
      "workstream-http",
      "workstream-http"
    ])
    expect(result.duplicateStatus).toBe(409)
    expect(result.missingStatus).toBe(404)
    expect(result.missingOwnerStatus).toBe(404)
  })
)

it.live(
  "exposes migrated Sessions through the Workstream HTTP boundary",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-workstream-migration-")
    const legacy = new Database(path)
    legacy.exec(`
      CREATE TABLE effect_sql_migrations (
        migration_id integer PRIMARY KEY NOT NULL,
        created_at datetime NOT NULL DEFAULT current_timestamp,
        name VARCHAR(255) NOT NULL
      );
      INSERT INTO effect_sql_migrations (migration_id, name) VALUES
        (1, 'create_session_events'),
        (2, 'create_event_dispatch'),
        (3, 'allow_agent_activity_events');
      CREATE TABLE session_events (
        event_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        UNIQUE (session_id, sequence)
      );
      CREATE INDEX session_events_session_id
        ON session_events (session_id, sequence);
      CREATE UNIQUE INDEX session_events_tool_call_causation
        ON session_events(
          json_extract(payload, '$.inReplyTo'),
          json_extract(payload, '$.index')
        ) WHERE event_type = 'AgentToolCallAdded';
      CREATE TABLE event_dispatch (
        position INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE REFERENCES session_events(event_id)
      );
      CREATE TABLE event_consumer_checkpoints (
        consumer TEXT PRIMARY KEY,
        position INTEGER NOT NULL
      );
      INSERT INTO session_events VALUES
        ('legacy-created', 'legacy-http-session', 1, 'SessionCreated', '{}',
          '2026-01-01T00:00:00.000Z'),
        ('legacy-user', 'legacy-http-session', 2, 'UserMessageAdded',
          '{"messageId":"legacy-message","content":"Legacy request"}',
          '2026-01-01T00:00:01.000Z');
      INSERT INTO event_dispatch (position, event_id) VALUES
        (1, 'legacy-created'),
        (2, 'legacy-user');
    `)
    legacy.close()

    const port = yield* Effect.promise(availablePort)
    const baseUrl = `http://127.0.0.1:${port}`
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("")
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
      const workstreams = yield* proxy.listWorkstreams()
      const sessions = yield* proxy.listSessions(Session.defaultWorkstreamId)
      const workstream = yield* proxy.workstream(Session.defaultWorkstreamId)
      return { workstreams, sessions, workstream }
    }).pipe(Effect.provide(layer))

    expect(result.workstreams).toEqual([
      expect.objectContaining({
        workstreamId: Session.defaultWorkstreamId,
        sessionCount: 1
      })
    ])
    expect(result.sessions).toEqual([
      expect.objectContaining({
        sessionId: "legacy-http-session",
        workstreamId: Session.defaultWorkstreamId,
        title: "Legacy request"
      })
    ])
    expect(result.workstream.sessions).toEqual(result.sessions)
  })
)

it.live(
  "settles and reopens Sessions through HTTP while keeping lifecycle activity reconnectable",
  () => Effect.gen(function*() {
    const { path } = yield* temporaryDatabase("corredor-settlement-http-")
    const port = yield* Effect.promise(availablePort)
    const baseUrl = `http://127.0.0.1:${port}`
    const fakeAgent = Layer.succeed(Agent.Service, Agent.Service.of({
      run: () => Effect.succeed("not reached while settled")
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
      const session = yield* proxy.createSession("settlement-http-session")
      const activityFiber = yield* proxy.streamActivity(
        session.sessionId,
        session.position
      ).pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkScoped
      )
      const settled = yield* proxy.settle(session.sessionId)
      const activeSessions = yield* proxy.listSessions()
      const settledSessions = yield* proxy.listSessions(undefined, "settled")
      const inspectedSettled = yield* proxy.workstream(
        Session.defaultWorkstreamId,
        "settled"
      )
      const history = yield* proxy.history(session.sessionId)
      const checkoutError = yield* Effect.flip(proxy.checkout(
        session.sessionId,
        null
      ))
      const commitError = yield* Effect.flip(proxy.submitUserCommit(
        session.sessionId,
        "must wait for reopening",
        "settlement-http-rejected"
      ))
      const runError = yield* Effect.flip(proxy.startAgentRun(
        session.sessionId,
        "missing-while-settled",
        Agent.defaultDefinition
      ))
      const reopened = yield* proxy.reopen(session.sessionId)
      const continued = yield* proxy.submitUserCommit(
        session.sessionId,
        "continue after reopening",
        "settlement-http-continued"
      )
      const activity = Array.from(yield* Fiber.join(activityFiber))
      return {
        session,
        settled,
        activeSessions,
        settledSessions,
        inspectedSettled,
        history,
        checkoutError,
        commitError,
        runError,
        reopened,
        continued,
        activity
      }
    }).pipe(Effect.provide(layer))

    expect(result.settled).toMatchObject({ type: "SessionSettled" })
    expect(result.reopened).toMatchObject({ type: "SessionReopened" })
    expect(result.activeSessions).toEqual([])
    expect(result.settledSessions).toEqual([
      expect.objectContaining({
        sessionId: result.session.sessionId,
        settled: true
      })
    ])
    expect(result.inspectedSettled.workstream.sessionCount).toBe(1)
    expect(result.inspectedSettled.sessions[0]).toMatchObject({ settled: true })
    expect(result.history.settled).toBe(true)
    expect(result.history.items.map((item) => item.type)).toContain("SessionSettled")
    expect(result.checkoutError.message).toContain("409")
    expect(result.commitError.message).toContain("409")
    expect(result.runError.message).toContain("409")
    expect(result.continued).toMatchObject({
      type: "UserCommit",
      commitId: "settlement-http-continued"
    })
    expect(result.activity.map((item) => item.type)).toEqual([
      "SessionSettled",
      "SessionReopened"
    ])
  })
)
