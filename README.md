# corredor

An event-driven, event-sourced agent runtime.

## Run

```bash
bun install
DEEPSEEK_API_KEY=... bun run agent
```

Or start the API:

```bash
DEEPSEEK_API_KEY=... bun run agent server
```

```bash
# Create a session (body may also contain a chosen sessionId)
curl -X POST http://127.0.0.1:4096/v1/sessions \
  -H 'content-type: application/json' -d '{}'

# Submit a command; the response is the resulting AgentMessageAdded event
curl -X POST http://127.0.0.1:4096/v1/sessions/$SESSION_ID/messages \
  -H 'content-type: application/json' -d '{"content":"Hello"}'

curl http://127.0.0.1:4096/v1/sessions/$SESSION_ID/events
```

## Event flow

```text
CreateSession command -> SessionCreated event -> durable outbox
                                           -> agent-runtime consumer initializes agent

AddUserMessage command -> UserMessageAdded event -> durable outbox
                                              -> agent-runtime consumer runs agent
                                              -> AgentMessageAdded event
                                              -> API/TUI observes reply
```

`session_events` is the source of truth. `event_dispatch` is a transactional outbox with a global position, and `event_consumer_checkpoints` records durable consumer acknowledgements. The agent consumer only reacts to committed events and resumes after restart. Each session gets an isolated in-memory agent; its chat history is rebuilt by replaying events when needed.
