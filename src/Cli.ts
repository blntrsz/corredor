import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Effect, Option } from "effect"
import { Argument, Command, Flag } from "effect/unstable/cli"
import * as AgentProxy from "./AgentProxy.ts"
import * as DatabaseAdmin from "./DatabaseAdmin.ts"
import { Harness } from "./Harness.ts"
import * as Server from "./Server.ts"
import { defaultServerPort, defaultServerUrl, ensureServer } from "./ServerManager.ts"

const formatTsvValue = (value: unknown): string => {
  if (value === null || value === undefined) return ""
  if (typeof value === "object") return JSON.stringify(value)
  return String(value)
}

const runDatabaseQuery = Effect.fn("Cli.runDatabaseQuery")(
  function*(query: string, format: "json" | "tsv") {
    const database = yield* DatabaseAdmin.Service
    const rows = yield* database.query(query).pipe(Effect.orDie)

    if (format === "json") {
      console.log(JSON.stringify(rows, null, 2))
      return
    }

    const first = rows[0]
    if (first === undefined) return

    const keys = Object.keys(first)
    console.log(keys.join("\t"))
    for (const row of rows) {
      console.log(keys.map((key) => formatTsvValue(row[key])).join("\t"))
    }
  }
)

const databaseCommand = Command.make(
  "db",
  {
    query: Argument.string("query").pipe(
      Argument.withDescription("The SQL query to execute")
    ),
    format: Flag.choice("format", ["json", "tsv"]).pipe(
      Flag.withDefault("tsv"),
      Flag.withDescription("The output format")
    )
  },
  ({ query, format }) => runDatabaseQuery(query, format).pipe(
    Effect.provide(DatabaseAdmin.layer())
  )
).pipe(
  Command.withDescription("Execute SQL against the Corredor database")
)

const serverCommand = Command.make(
  "server",
  {
    host: Flag.string("host").pipe(
      Flag.withDefault("127.0.0.1"),
      Flag.withDescription("The host address to bind")
    ),
    port: Flag.integer("port").pipe(
      Flag.withDefault(defaultServerPort),
      Flag.withDescription("The port to listen on")
    )
  },
  Server.run
).pipe(
  Command.withDescription("Run the Corredor API server")
)

const command = Command.make(
  "corredor",
  {
    prompt: Argument.string("prompt").pipe(
      Argument.withDescription("An optional first request for the agent"),
      Argument.optional
    )
  },
  Effect.fn("Cli.run")(function*({ prompt }) {
    yield* Effect.tryPromise({
      try: () => ensureServer(import.meta.path),
      catch: (cause) => new Error("Unable to start the Corredor server", { cause })
    }).pipe(Effect.orDie)
    yield* Harness.run(Option.getOrUndefined(prompt))
  })
).pipe(
  Command.withDescription("Run the interactive Corredor agent"),
  Command.withExamples([
    {
      command: "corredor",
      description: "Start an interactive agent session"
    },
    {
      command: "corredor \"What time is it?\"",
      description: "Start a session with an initial request"
    }
  ]),
  Command.withSubcommands([databaseCommand, serverCommand])
)

command.pipe(
  Command.run({ version: "0.1.0" }),
  Effect.provide(AgentProxy.layer({ baseUrl: defaultServerUrl })),
  Effect.provide(BunServices.layer),
  BunRuntime.runMain
)
