import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai-compat"
import { Config, Context, Effect, Layer, Schema, Semaphore, Stream } from "effect"
import { AiError, Chat, LanguageModel, Tool, Toolkit } from "effect/unstable/ai"
import { FetchHttpClient } from "effect/unstable/http"

export namespace Agent {
  const maxTurns = 10

  const systemPrompt = [
    "You are a helpful assistant.",
    "Use the available tools when they are needed to answer the user's question.",
    "Call at most one tool in each response.",
    "When several shell operations are needed, combine them into one Bash command instead of making parallel tool calls.",
    "When you have enough information, respond with a final answer without calling another tool."
  ].join(" ")

  export type ToolName = "Bash"

  /** Serializable configuration selecting the Agent's behavior and tools. */
  export interface Definition {
    readonly id: string
    readonly instructions: string
    readonly tools: ReadonlyArray<ToolName>
  }

  export const DefinitionSchema = Schema.Struct({
    id: Schema.String,
    instructions: Schema.String,
    tools: Schema.Array(Schema.Literal("Bash"))
  })

  export const defaultDefinition: Definition = {
    id: "default",
    instructions: systemPrompt,
    tools: ["Bash"]
  }

  export class BashError extends Schema.TaggedErrorClass<BashError>()(
    "@corredor/Agent/BashError",
    { message: Schema.String }
  ) { }

  const Bash = Tool.make("Bash", {
    description: [
      "Execute a Bash command in the agent process's current working directory.",
      "Use it to inspect files, run programs, and perform shell operations."
    ].join(" "),
    parameters: Schema.Struct({
      command: Schema.String
    }),
    success: Schema.Struct({
      exitCode: Schema.Number,
      stdout: Schema.String,
      stderr: Schema.String
    }),
    failure: BashError
  })

  const Tools = Toolkit.make(Bash)

  const toolsLayer = Tools.toLayer({
    Bash: Effect.fn("Agent.Bash")(({ command }) =>
      Effect.tryPromise({
        try: async (signal) => {
          const process = Bun.spawn(["/bin/bash", "-lc", command], {
            cwd: globalThis.process.cwd(),
            env: globalThis.process.env,
            stdout: "pipe",
            stderr: "pipe",
            signal
          })
          const [exitCode, stdout, stderr] = await Promise.all([
            process.exited,
            new Response(process.stdout).text(),
            new Response(process.stderr).text()
          ])
          return { exitCode, stdout, stderr }
        },
        catch: (cause) => new BashError({
          message: cause instanceof Error ? cause.message : String(cause)
        })
      })
    )
  })

  export class ModelError extends Schema.TaggedErrorClass<ModelError>()(
    "@corredor/Agent/ModelError",
    {
      reason: AiError.AiErrorReason
    }
  ) { }

  export class TurnLimitExceeded extends Schema.TaggedErrorClass<TurnLimitExceeded>()(
    "@corredor/Agent/TurnLimitExceeded",
    {
      maxTurns: Schema.Number
    }
  ) { }

  export type Error = ModelError | TurnLimitExceeded | BashError

  const modelStreamErrorMessage = (value: unknown): string => {
    if (value instanceof Error) return value.message
    if (typeof value === "string") return value
    if (value !== null && typeof value === "object" &&
      "message" in value && typeof value.message === "string") {
      return value.message
    }
    return "Model stream failed"
  }

  const modelStreamErrorReason = (value: unknown): AiError.AiErrorReason =>
    AiError.isAiError(value)
      ? value.reason
      : AiError.isAiErrorReason(value)
      ? value
      : new AiError.UnknownError({
        description: modelStreamErrorMessage(value)
      })

  export type Event =
    | {
      readonly type: "TextDelta"
      readonly text: string
    }
    | {
      readonly type: "ToolCall"
      readonly id: string
      readonly name: string
      readonly input: unknown
    }
    | {
      readonly type: "ToolResult"
      readonly id: string
      readonly name: string
      readonly result: unknown
      readonly isFailure: boolean
    }

  export type ContextEntry =
    | {
      readonly type: "User"
      readonly commitId: string
      readonly content: string
    }
    | {
      readonly type: "AgentMessage"
      readonly commitId: string
      readonly content: string
    }
    | {
      readonly type: "Compaction"
      readonly commitId: string
      readonly content: string
    }
    | {
      readonly type: "Tool"
      readonly commitId: string
      readonly name: string
      readonly input: unknown
      readonly outcome: {
        readonly type: "Success" | "Failure"
        readonly value: unknown
      }
    }
    | {
      readonly type: "Failure"
      readonly commitId: string
      readonly reason: string
    }
    | {
      readonly type: "Interrupt"
      readonly commitId: string
      readonly reason: string
      readonly partialOutput: string
    }

  export interface Interface {
    /**
     * Runs statelessly from durable Commit ancestry. The implementation may
     * allocate transient chat state, but none survives this operation.
     */
    readonly run: <E = never, R = never>(
      context: ReadonlyArray<ContextEntry>,
      onEvent: (event: Event) => Effect.Effect<void, E, R>,
      definition?: Definition
    ) => Effect.Effect<string, Error | E, R>
  }

  export class Service extends Context.Service<Service, Interface>()(
    "@corredor/Agent"
  ) { }

  export const make = Effect.gen(function*() {
    const languageModel = yield* LanguageModel.LanguageModel
    const toolkit = yield* Tools
    const semaphore = yield* Semaphore.make(1)
    const run: Interface["run"] = Effect.fn("Agent.run")(
      function*<E = never, R = never>(
        context: ReadonlyArray<ContextEntry>,
        onEvent: (event: Event) => Effect.Effect<void, E, R>,
        definition = defaultDefinition
      ) {
        const latest = context.at(-1)
        const resumingAfterTool = latest?.type === "Tool"
        const prompt = latest?.type === "User"
          ? latest.content
          : resumingAfterTool
          ? "Continue from the durable completed tool interaction. Use another tool only if needed."
          : ""
        const historyEntries = latest?.type === "User"
          ? context.slice(0, -1)
          : context
        const history = historyEntries.map((entry) => {
          if (entry.type === "User") {
            return { role: "user" as const, content: entry.content }
          }
          if (entry.type === "AgentMessage") {
            return { role: "assistant" as const, content: entry.content }
          }
          if (entry.type === "Compaction") {
            return {
              role: "assistant" as const,
              content: `[Compacted context]\n${entry.content}`
            }
          }
          if (entry.type === "Failure") {
            return {
              role: "assistant" as const,
              content: `[Previous Agent Run failed: ${entry.reason}]`
            }
          }
          if (entry.type === "Interrupt") {
            return {
              role: "assistant" as const,
              content: `[Previous Agent Run interrupted: ${entry.reason}]${
                entry.partialOutput.length > 0 ? `\n${entry.partialOutput}` : ""
              }`
            }
          }
          const outcome = entry.outcome.type === "Success"
            ? { result: entry.outcome.value }
            : { failure: entry.outcome.value }
          return {
            role: "assistant" as const,
            content: `[Completed tool ${entry.name}: ${JSON.stringify({
              input: entry.input,
              ...outcome
            })}]`
          }
        })
        const chat = yield* Chat.fromPrompt([
          { role: "system", content: definition.instructions },
          ...history
        ])

        for (let turn = 0; turn < maxTurns; turn++) {
          let responseText = ""
          let hasToolCall = false
          const stream = definition.tools.length > 0
            ? chat.streamText({
              prompt: turn === 0 ? prompt : [],
              toolkit
            })
            : chat.streamText({
              prompt: turn === 0 ? prompt : []
            })
          yield* Stream.runForEach(stream, (part) => {
            if (part.type === "text-delta") {
              responseText += part.delta
              return part.delta.length === 0
                ? Effect.void
                : onEvent({ type: "TextDelta", text: part.delta })
            }
            if (part.type === "tool-call") {
              hasToolCall = true
              return onEvent({
                type: "ToolCall",
                id: part.id,
                name: part.name,
                input: part.params
              })
            }
            if (part.type === "tool-result") {
              return onEvent({
                type: "ToolResult",
                id: part.id,
                name: part.name,
                result: part.result,
                isFailure: part.isFailure
              })
            }
            if (part.type === "error") {
              return Effect.fail<E | ModelError>(new ModelError({
                reason: modelStreamErrorReason(part.error)
              }))
            }
            return Effect.void
          }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, languageModel),
            Effect.catchTag("AiError", (error) =>
              Effect.fail(new ModelError({
                reason: (error as AiError.AiError).reason
              }))),
          )

          if (!hasToolCall) return responseText
        }

        return yield* new TurnLimitExceeded({ maxTurns })
      },
      (effect) => effect.pipe(semaphore.withPermits(1))
    )

    return Service.of({ run })
  })

  export const layerWithoutDependencies = Layer.effect(Service, make)

  const deepSeekClientLayer = OpenAiClient.layerConfig({
    apiKey: Config.redacted("DEEPSEEK_API_KEY"),
    apiUrl: Config.succeed("https://api.deepseek.com")
  }).pipe(
    Layer.provide(FetchHttpClient.layer)
  )

  const deepSeekLanguageModelLayer = OpenAiLanguageModel.layer({
    model: "deepseek-v4-pro",
    config: {
      // Effect's OpenAI-compatible adapter does not yet replay DeepSeek's
      // reasoning_content across tool rounds, so thinking must remain disabled.
      thinking: { type: "disabled" },
      // The OpenAI-compatible adapter currently replays parallel tool calls as
      // separate assistant messages. DeepSeek requires all calls from one
      // assistant turn to be followed by their tool results, so keep tool calls
      // sequential until the adapter groups them correctly.
      parallel_tool_calls: false,
      strictJsonSchema: true
    }
  }).pipe(
    Layer.provide(deepSeekClientLayer)
  )

  export const layer = layerWithoutDependencies.pipe(
    Layer.provide([deepSeekLanguageModelLayer, toolsLayer])
  )
}
