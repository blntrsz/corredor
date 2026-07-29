# OpenCode-style shared daemon

## Status

Research and proposed architecture for running Corredor as an on-demand, detached backend shared by CLI and TUI invocations.

## OpenCode CLI v2 behavior

OpenCode CLI v2 uses a detached daemon server shared by its CLI and TUI processes.

### Launch flow

1. The default command requests a daemon transport in:
   - `packages/cli/src/commands/handlers/default.ts`
   - This calls `daemon.transport()`.
2. `Daemon.start()` reads `$OPENCODE_STATE/server.json` and sends an authenticated `/v2/health` request:
   - `packages/cli/src/services/daemon.ts`
3. A compatible, healthy server is reused.
4. Otherwise the CLI launches a detached server:

   ```ts
   spawn(process.execPath, [entrypoint, "serve", "--register"], {
     detached: true,
     stdio: "ignore",
   }).unref()
   ```

5. The server binds to the first available port starting at `4096`, then atomically writes a registration like:

   ```json
   {
     "id": "...",
     "version": "...",
     "url": "http://127.0.0.1:4096",
     "pid": 1234
   }
   ```

   to `$OPENCODE_STATE/server.json`.
6. Other clients read the registration and authenticate using the persistent secret in `$OPENCODE_STATE/password`.

### Single-server convergence

OpenCode does not rely on a conventional lock file. Registration ownership provides eventual convergence:

- Every server writes a unique registration ID.
- Every ten seconds, each server rereads `server.json`.
- A server sends itself `SIGTERM` when its ID is no longer current.
- An authenticated incompatible server is stopped and replaced.
- Stale registrations are removed.
- PID termination happens only after authenticating the registered server, preventing an unrelated process from being killed after PID reuse.

Concurrent startup can therefore create multiple servers briefly. Registration ownership causes all but the current registered server to terminate.

The main OpenCode implementation is in:

- `packages/cli/src/services/daemon.ts`
- `packages/cli/src/commands/handlers/serve.ts`

## Proposed Corredor architecture

This on-demand daemon model is preferable to requiring users to configure `launchd` or `systemd`. The daemon starts on the first invocation, remains ready for later clients, and is restarted by a future client after a crash.

```text
Corredor CLI/TUI
       │ authenticated HTTP
       ▼
Detached Corredor server
  ├── model client loaded once
  ├── session registry
  ├── per-session request serialization
  └── persistent conversation state
```

Suggested modules:

```text
src/
  Cli.ts
  Daemon.ts          # Client-side discovery, startup, and replacement
  Server.ts          # Foreground hidden `serve --register` command
  State.ts           # State paths, password, and registration
  AgentManager.ts    # Per-session agents and concurrency
  Harness.ts         # TUI using daemon transport
```

The `serve` command should remain a foreground command. The invoking client is responsible for launching it detached.

## Client startup protocol

The default Corredor command should:

1. Call `Daemon.transport()`.
2. Read `$CORREDOR_STATE_DIR/server.json`.
3. Send an authenticated `GET /v1/health` request.
4. Reuse the server if its identity and protocol version match the registration and it is compatible with the client.
5. Otherwise, shut down an authenticated incompatible server or remove a stale registration.
6. Spawn `serve --register` as a detached process.
7. Poll registration and authenticated health until the daemon is ready or startup times out.
8. Connect the TUI to the resulting transport.

A Bun launch would be equivalent to:

```ts
const child = Bun.spawn(
  [process.execPath, entrypoint, "serve", "--register"],
  {
    detached: true,
    stdin: "ignore",
    stdout: "ignore",
    stderr: "ignore",
  },
)

child.unref()
```

The exact launch arguments should be encapsulated because development execution from a TypeScript entrypoint and a future compiled executable may differ.

## State and registration

Corredor should support a `CORREDOR_STATE_DIR` override and otherwise choose a stable per-user state directory. The directory must be private to the user.

Suggested contents:

```text
$CORREDOR_STATE_DIR/
  password
  server.json
```

Suggested registration:

```json
{
  "id": "019fa...",
  "version": "0.1.0",
  "protocolVersion": 1,
  "url": "http://127.0.0.1:4096",
  "pid": 1234,
  "startedAt": 1785350000000
}
```

`protocolVersion` should be separate from the package version because application releases do not necessarily imply wire-protocol incompatibility.

### Atomic registration writes

Registration updates should:

1. Write a temporary file in the state directory.
2. Set file permissions to `0600`.
3. Rename the temporary file to `server.json` atomically.

The state directory should use permissions `0700`.

A server may remove `server.json` during shutdown only when the file still contains its own registration ID. This prevents an older server from deleting a newer server's registration.

## Authentication

The daemon must bind only to loopback addresses and require authentication for health, shutdown, and agent operations.

The persistent password should:

- contain at least 32 bytes of cryptographically random data;
- be created with exclusive file-creation semantics;
- use permissions `0600`;
- be reused by concurrent starters after one process wins creation;
- be sent as `Authorization: Bearer <secret>`;
- never appear in URLs or logs.

An authenticated health response should include server identity:

```json
{
  "id": "019fa...",
  "version": "0.1.0",
  "protocolVersion": 1
}
```

Clients must check that the returned ID matches the registration they read.

## Ownership and replacement

Each registered server should periodically reread `server.json`:

```ts
if (registration.id !== ownId) {
  await gracefulShutdown()
}
```

Replacement of an existing registration should follow these rules:

1. Authenticate the URL in the registration.
2. Verify that `/v1/health` returns the registered server ID.
3. If the server is incompatible, request graceful shutdown over authenticated HTTP.
4. Wait briefly for shutdown before starting or registering the replacement.
5. Do not blindly signal the stored PID.
6. If the URL cannot be authenticated, conditionally remove the stale registration but do not kill its PID.

Avoiding unauthenticated PID termination protects against PID reuse. The PID remains useful for diagnostics but is not proof of server identity.

Concurrent startup can briefly produce multiple processes. Clients should retry discovery if the server they selected loses registration ownership during connection. A server should stop accepting new work once it detects that it no longer owns the registration.

## Agent session ownership

The current `src/Agent.ts` constructs one `Chat` and one semaphore in `Agent.make`. Placing that service directly in a shared daemon would cause all clients to share one conversation and serialize all requests globally.

For independent CLI/TUI sessions, the daemon should own an agent-session registry:

```ts
Map<SessionId, {
  agent: Agent.Interface
  cwd: string
  lastUsedAt: number
}>
```

Each session should have:

- its own `Chat`;
- its own single-request semaphore;
- an immutable or explicitly managed working directory;
- active-run cancellation state;
- optional persisted conversation history;
- idle-expiration policy.

A separate global semaphore can limit total concurrent model requests without unnecessarily blocking independent sessions.

The session ID and working directory must be explicit transport inputs. The server must not silently reuse one client's working directory for another client.

## Initial HTTP API

A minimal versioned API could expose:

```text
GET    /v1/health
POST   /v1/sessions
POST   /v1/sessions/:id/prompts
DELETE /v1/sessions/:id/run
DELETE /v1/sessions/:id
POST   /v1/shutdown
```

The first implementation may return completed prompt responses. Server-Sent Events or WebSockets should be added when the agent supports streaming text and tool events.

## Graceful shutdown

On `SIGTERM` or authenticated shutdown, the server should:

1. Stop accepting new prompts.
2. Abort or briefly drain active requests.
3. Persist session metadata and conversation state where configured.
4. Remove registration only if it still owns the current registration ID.
5. Close the HTTP listener.
6. Release the Effect application scope.

Detached startup should use ignored standard streams so the child does not retain the invoking terminal. Operational diagnostics should be written to bounded log files or another explicit logging sink rather than inherited standard output.

## Implementation sequence

1. Add state-directory handling, password creation, registration schemas, and atomic file operations.
2. Add a foreground `serve --register` command with authenticated `/v1/health` and `/v1/shutdown` endpoints.
3. Add `Daemon.transport()` discovery, compatibility checks, detached startup, timeout handling, and stale-registration cleanup.
4. Add registration ownership monitoring and graceful convergence.
5. Introduce `AgentManager` with independent per-session `Chat` instances.
6. Add session and prompt endpoints.
7. Change `Harness.ts` to use the daemon transport instead of receiving `Agent.Interface` directly.
8. Add response streaming, persistence, idle eviction, and operational logging.

## Required tests

At minimum, cover:

- healthy compatible server reuse;
- authenticated incompatible server replacement;
- stale registration cleanup without PID termination;
- concurrent password creation;
- concurrent server startup convergence;
- atomic registration replacement;
- old server cleanup not deleting a newer registration;
- health identity mismatch rejection;
- startup timeout and child-process failure;
- independent conversation state across sessions;
- per-session serialization and cross-session concurrency;
- graceful shutdown during active work;
- state-file permission enforcement where supported.
