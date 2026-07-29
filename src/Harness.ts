import {
  Container,
  Editor,
  Key,
  Loader,
  Markdown,
  type MarkdownTheme,
  matchesKey,
  ProcessTerminal,
  type SelectListTheme,
  Spacer,
  Text,
  TUI
} from "@earendil-works/pi-tui"
import { Effect } from "effect"
import { Agent } from "./Agent.ts"
import * as SessionWorkflow from "./SessionWorkflow.ts"

const ansi = (open: string, close: string) =>
  (text: string): string => `${open}${text}${close}`

const bold = ansi("\x1b[1m", "\x1b[22m")
const dim = ansi("\x1b[2m", "\x1b[22m")
const cyan = ansi("\x1b[36m", "\x1b[39m")
const blue = ansi("\x1b[34m", "\x1b[39m")
const green = ansi("\x1b[32m", "\x1b[39m")
const yellow = ansi("\x1b[33m", "\x1b[39m")
const red = ansi("\x1b[31m", "\x1b[39m")
const italic = ansi("\x1b[3m", "\x1b[23m")
const underline = ansi("\x1b[4m", "\x1b[24m")
const strikethrough = ansi("\x1b[9m", "\x1b[29m")

const selectListTheme: SelectListTheme = {
  selectedPrefix: cyan,
  selectedText: bold,
  description: dim,
  scrollInfo: dim,
  noMatch: yellow
}

const markdownTheme: MarkdownTheme = {
  heading: (text) => bold(cyan(text)),
  link: blue,
  linkUrl: dim,
  code: yellow,
  codeBlock: green,
  codeBlockBorder: dim,
  quote: italic,
  quoteBorder: dim,
  hr: dim,
  listBullet: cyan,
  bold,
  italic,
  strikethrough,
  underline
}

interface Handle {
  readonly done: Promise<void>
  readonly stop: () => void
}

const formatError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error)

const make = (workflow: SessionWorkflow.Interface, sessionId: string, initialPrompt?: string): Handle => {
  const tui = new TUI(new ProcessTerminal())
  const messages = new Container()
  const editor = new Editor(tui, {
    borderColor: dim,
    selectList: selectListTheme
  }, {
    paddingX: 1
  })

  let stopped = false
  let activeController: AbortController | undefined
  let activeLoader: Loader | undefined
  let resolveDone!: () => void

  const done = new Promise<void>((resolve) => {
    resolveDone = resolve
  })

  const stop = () => {
    if (stopped) return
    stopped = true
    activeController?.abort()
    activeLoader?.stop()
    tui.stop()
    resolveDone()
  }

  const finish = () => {
    if (activeLoader !== undefined) {
      activeLoader.stop()
      messages.removeChild(activeLoader)
      activeLoader = undefined
    }
    activeController = undefined
    editor.disableSubmit = false
    tui.setFocus(editor)
    tui.requestRender()
  }

  const addAssistantMessage = (response: string) => {
    messages.addChild(new Text(bold(green("Agent")), 1, 0))
    messages.addChild(new Markdown(response, 1, 1, markdownTheme))
    messages.addChild(new Spacer(1))
  }

  const addErrorMessage = (message: string) => {
    messages.addChild(new Text(bold(red("Error")), 1, 0))
    messages.addChild(new Text(message, 1, 1))
    messages.addChild(new Spacer(1))
  }

  const addAgentEvent = (event: Agent.Event) => {
    if (event.type === "ToolCall") {
      const input = event.input as { readonly command?: unknown }
      const detail = event.name === "Bash" && typeof input.command === "string"
        ? JSON.stringify(input.command)
        : JSON.stringify(event.input)
      messages.addChild(new Text(
        bold(dim(`Tool · ${event.name}(${detail})`)),
        1,
        0
      ))
      messages.addChild(new Spacer(1))
      tui.requestRender()
    }
  }

  const submit = (input: string) => {
    const prompt = input.trim()
    if (prompt.length === 0 || editor.disableSubmit || stopped) return

    if (prompt === "/exit") {
      stop()
      return
    }

    editor.addToHistory(prompt)
    editor.disableSubmit = true
    messages.addChild(new Text(bold(cyan("You")), 1, 0))
    messages.addChild(new Markdown(prompt, 1, 1, markdownTheme))

    const loader = new Loader(tui, cyan, dim, "Thinking...")
    activeLoader = loader
    messages.addChild(loader)
    loader.start()

    const controller = new AbortController()
    activeController = controller
    tui.requestRender()

    const request = workflow.submit(sessionId, prompt, addAgentEvent).pipe(
      Effect.map((event) => event.type === "AgentMessageAdded" ? event.payload.content : ""),
      Effect.match({
        onFailure: (error) => ({ _tag: "Failure" as const, error }),
        onSuccess: (response) => ({ _tag: "Success" as const, response })
      })
    )

    void Effect.runPromise(request, { signal: controller.signal }).then((result) => {
      if (stopped) return
      finish()
      if (result._tag === "Success") {
        addAssistantMessage(result.response)
      } else {
        addErrorMessage(formatError(result.error))
      }
      tui.requestRender()
    }).catch((cause: unknown) => {
      if (stopped) return
      finish()
      addErrorMessage(cause instanceof Error ? cause.message : String(cause))
      tui.requestRender()
    })
  }

  editor.onSubmit = submit

  tui.addChild(new Text(
    `${bold(cyan("Corredor"))}\n${dim("DeepSeek V4 Pro · /exit or Ctrl+C to quit")}`,
    1,
    1
  ))
  tui.addChild(messages)
  tui.addChild(new Text(dim("Send a message; follow-ups retain the conversation context."), 1, 0))
  tui.addChild(editor)
  tui.setFocus(editor)

  tui.addInputListener((data) => {
    if (matchesKey(data, Key.ctrl("c"))) {
      stop()
      return { consume: true }
    }
    return undefined
  })

  tui.start()

  if (initialPrompt !== undefined && initialPrompt.trim().length > 0) {
    queueMicrotask(() => submit(initialPrompt))
  }

  return { done, stop }
}

export namespace Harness {
  export const run = (initialPrompt?: string): Effect.Effect<void, never, SessionWorkflow.Service> =>
    Effect.gen(function*() {
      const workflow = yield* SessionWorkflow.Service
      const sessionId = yield* workflow.createSession().pipe(Effect.orDie)
      yield* Effect.acquireUseRelease(
        Effect.sync(() => make(workflow, sessionId, initialPrompt)),
        (handle) => Effect.promise(() => handle.done),
        (handle) => Effect.sync(handle.stop)
      )
    })
}
