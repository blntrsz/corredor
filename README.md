# corredor

An event-driven, event-sourced agent runtime.

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

```bash
# Create a session (body may also contain a chosen sessionId)
curl -X POST http://127.0.0.1:5050/v1/sessions \
  -H 'content-type: application/json' -d '{}'

# Submit a command. The API commits UserMessageAdded and returns immediately.
curl -X POST http://127.0.0.1:5050/v1/sessions/$SESSION_ID/messages \
  -H 'content-type: application/json' \
  -d '{"messageId":"client-generated-id","content":"Hello"}'

# Replay and follow committed events using SSE.
curl -N http://127.0.0.1:5050/v1/sessions/$SESSION_ID/events

# Or retrieve a finite JSON snapshot.
curl http://127.0.0.1:5050/v1/sessions/$SESSION_ID/history
```

## Event flow

```text
CreateSession command -> SessionCreated event -> durable outbox
                                           -> agent-runtime consumer initializes agent

AddUserMessage command -> UserMessageAdded event -> durable outbox
                                              -> agent-runtime consumer runs agent
                                              -> AgentToolCallAdded event(s)
                                              -> AgentMessageAdded event
                                              -> API/TUI observes activity and reply
```

`session_events` is the source of truth. `event_dispatch` is a transactional outbox with a global position, and `event_consumer_checkpoints` records durable consumer acknowledgements. The server-owned agent consumer only reacts to committed events and resumes after restart. Each session gets an isolated in-memory agent; its chat history is rebuilt by replaying events when needed.

The TUI is a thin client. `AgentProxy` creates sessions and submits messages over HTTP, then renders the session's SSE stream. It does not open SQLite or run an agent consumer in the harness process. Agent tool calls are persisted as `AgentToolCallAdded` events and use the same stream as conversation messages. SSE event IDs are durable outbox positions, so clients can reconnect with `Last-Event-ID` or `?after=<position>`.
