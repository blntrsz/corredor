# Corredor

A durable, Git-like context history for Agent work.

## Run

```bash
bun install
DEEPSEEK_API_KEY=... bun run agent
```

The interactive command verifies `GET /v1/health` on port `5050`. It reuses a compatible server or starts the API as a detached process and waits for the health endpoint to become ready.

Or start the API in the foreground:

```bash
DEEPSEEK_API_KEY=... bun run agent server
```

## Test

```bash
bun run typecheck
bun run test
```

The suite uses Vitest with `@effect/vitest`. Effect integration tests return
their programs directly through `it.live`, so scoped Layers and resources are
acquired and released by the test runtime. Vitest is launched with Bun because
the SQLite and HTTP adapters use `bun:` modules.

In the interactive client, use `/history` to browse every Commit and compatible
legacy tool record in the current Session. Selecting a User Commit restores its
content to the editor so it can be edited and submitted on a new Branch;
selecting another Commit checks out that Branch Head. Previous Branches remain
available in the same Session.

```bash
# Create a session (body may also contain a chosen sessionId)
curl -X POST http://127.0.0.1:5050/v1/sessions \
  -H 'content-type: application/json' -d '{}'

# Submit a User Commit. The API persists it and returns immediately.
curl -X POST http://127.0.0.1:5050/v1/sessions/$SESSION_ID/commits \
  -H 'content-type: application/json' \
  -d '{"commitId":"client-generated-id","content":"Hello"}'

# Replay and follow durable activity using SSE.
curl -N http://127.0.0.1:5050/v1/sessions/$SESSION_ID/activity

# Or retrieve a finite JSON snapshot.
curl http://127.0.0.1:5050/v1/sessions/$SESSION_ID/history

# Checkout a Commit as the local Branch Head (use null for an empty Branch).
curl -X POST http://127.0.0.1:5050/v1/sessions/$SESSION_ID/head \
  -H 'content-type: application/json' -d '{"commitId":"COMMIT_ID"}'

# Start another independent Agent Run from an existing Commit.
curl -X POST http://127.0.0.1:5050/v1/sessions/$SESSION_ID/runs \
  -H 'content-type: application/json' \
  -d '{"commitId":"COMMIT_ID","runId":"independent-run-id"}'
```

## Commit flow

```text
Create Session -> SessionCreated activity -> durable outbox

Submit User Commit -> UserCommit -> durable outbox
                                -> Agent Runtime reconstructs ancestry
                                -> completed ToolCommit(s)
                                -> AgentMessageCommit
                                -> API/client observes durable activity
```

`session_events` is the source of truth. `event_dispatch` is a transactional
outbox with a global position, and `event_consumer_checkpoints` records durable
consumer acknowledgements. The server-owned Agent Runtime reacts to User
Commits and starts each Agent Run statelessly from durable Commit ancestry.
Explicit runs can start from any existing Commit; provide a distinct `runId`
to create an independent descendant from the same starting point.

The TUI is a thin client. `AgentProxy` creates Sessions and submits User Commits
over HTTP, then renders the Session's SSE activity stream. It does not open
SQLite or run an Agent consumer in the harness process. A completed tool
interaction is persisted atomically as one Tool Commit containing its name,
input, and success result or failure. SSE IDs are durable outbox positions, so
clients can reconnect with `Last-Event-ID` or `?after=<position>`.

Legacy user and Agent messages are projected as canonical Commits when read.
Legacy tool-call-only and navigation records remain inspectable without
rewriting their stored rows.
