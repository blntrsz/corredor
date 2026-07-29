import {
  CombinedAutocompleteProvider,
  type Component,
  Container,
  Editor,
  type Focusable,
  Key,
  Loader,
  Markdown,
  type MarkdownTheme,
  matchesKey,
  ProcessTerminal,
  type SelectListTheme,
  Spacer,
  Text,
  truncateToWidth,
  TUI
} from "@earendil-works/pi-tui"
import { Effect, Stream } from "effect"
import * as AgentProxy from "./AgentProxy.ts"
import type { SessionSummary, StoredEvent } from "./Session.ts"

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

class SessionPicker implements Component, Focusable {
  focused = false
  private query = ""
  private selected = 0
  private newestFirst = true
  private showIds = false

  constructor(
    private readonly sessions: ReadonlyArray<SessionSummary>,
    private readonly onSelect: (sessionId: string) => void,
    private readonly onCancel: () => void,
    private readonly requestRender: () => void
  ) {}

  private filtered(): ReadonlyArray<SessionSummary> {
    const query = this.query.toLowerCase()
    return [...this.sessions]
      .filter((session) => session.title.toLowerCase().includes(query) || session.sessionId.toLowerCase().includes(query))
      .sort((a, b) => this.newestFirst
        ? b.updatedAt.localeCompare(a.updatedAt)
        : a.updatedAt.localeCompare(b.updatedAt))
  }

  handleInput(data: string): void {
    const sessions = this.filtered()
    if (matchesKey(data, Key.escape) || matchesKey(data, Key.ctrl("c"))) return this.onCancel()
    if (matchesKey(data, Key.up)) this.selected = Math.max(0, this.selected - 1)
    else if (matchesKey(data, Key.down)) this.selected = Math.min(sessions.length - 1, this.selected + 1)
    else if (matchesKey(data, Key.enter)) {
      const session = sessions[this.selected]
      if (session !== undefined) this.onSelect(session.sessionId)
      return
    } else if (matchesKey(data, Key.backspace)) {
      this.query = this.query.slice(0, -1)
      this.selected = 0
    } else if (matchesKey(data, Key.ctrl("s"))) {
      this.newestFirst = !this.newestFirst
      this.selected = 0
    } else if (matchesKey(data, Key.ctrl("p"))) {
      this.showIds = !this.showIds
    } else if (data.length === 1 && data >= " ") {
      this.query += data
      this.selected = 0
    }
    this.requestRender()
  }

  render(width: number): string[] {
    const sessions = this.filtered()
    const lines = [
      "",
      bold(cyan("  Resume Session")),
      dim(`  Search: ${this.query || "type to filter…"}`),
      dim(`  ${sessions.length} sessions · ${this.newestFirst ? "newest first" : "oldest first"}`),
      ""
    ]
    if (sessions.length === 0) lines.push(yellow("  No matching sessions"))
    for (const [index, session] of sessions.slice(Math.max(0, this.selected - 7), Math.max(0, this.selected - 7) + 15).entries()) {
      const absoluteIndex = index + Math.max(0, this.selected - 7)
      const selected = absoluteIndex === this.selected
      const prefix = selected ? cyan("› ") : "  "
      const title = session.title.replace(/\s+/g, " ")
      lines.push(truncateToWidth(`  ${prefix}${selected ? bold(title) : title}`, width))
      const id = this.showIds ? ` · ${session.sessionId}` : ""
      lines.push(truncateToWidth(dim(`      ${new Date(session.updatedAt).toLocaleString()} · ${session.messageCount} messages${id}`), width))
    }
    lines.push("", dim("  ↑↓ navigate · enter resume · type search · ctrl+s sort · ctrl+p IDs · esc cancel"), "")
    return lines.map((line) => truncateToWidth(line, width))
  }

  invalidate(): void {}
}

interface Handle {
  readonly done: Promise<void>
  readonly stop: () => void
}

const formatError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error)

const make = (proxy: AgentProxy.Interface, initialSessionId: string, initialPrompt?: string): Handle => {
  const tui = new TUI(new ProcessTerminal())
  const app = new Container()
  const messages = new Container()
  const editor = new Editor(tui, {
    borderColor: dim,
    selectList: selectListTheme
  }, {
    paddingX: 1
  })

  let sessionId = initialSessionId
  let stopped = false
  let activeController: AbortController | undefined
  let streamController: AbortController | undefined
  let activeLoader: Loader | undefined
  let pending: { readonly messageId: string; inReplyTo?: string } | undefined
  const renderedEventIds = new Set<string>()
  const optimisticMessageIds = new Set<string>()

  const header = new Text(
    `${bold(cyan("Corredor"))}\n${dim("DeepSeek V4 Pro · type / for commands · Ctrl+C to quit")}`,
    1,
    1
  )
  const footer = new Text(dim("Send a message; follow-ups retain the conversation context."), 1, 0)

  const showConversation = () => {
    app.clear()
    app.addChild(header)
    app.addChild(messages)
    app.addChild(footer)
    app.addChild(editor)
    tui.setFocus(editor)
    tui.requestRender()
  }

  let resolveDone!: () => void
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve
  })

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

  const stop = () => {
    if (stopped) return
    stopped = true
    activeController?.abort()
    streamController?.abort()
    activeLoader?.stop()
    tui.stop()
    resolveDone()
  }

  editor.setAutocompleteProvider(new CombinedAutocompleteProvider([
    { name: "new", description: "Start a new conversation" },
    { name: "resume", description: "Resume a previous session" },
    { name: "exit", description: "Exit Corredor" }
  ], process.cwd()))
  editor.setAutocompleteMaxVisible(6)

  const addUserMessage = (content: string) => {
    messages.addChild(new Text(bold(cyan("You")), 1, 0))
    messages.addChild(new Markdown(content, 1, 1, markdownTheme))
    messages.addChild(new Spacer(1))
  }

  const addAssistantMessage = (response: string) => {
    messages.addChild(new Text(bold(green("Agent")), 1, 0))
    messages.addChild(new Markdown(response, 1, 1, markdownTheme))
    messages.addChild(new Spacer(1))
  }

  const addToolCall = (name: string, input: unknown) => {
    const params = input as { readonly command?: unknown }
    const detail = name === "Bash" && typeof params.command === "string"
      ? JSON.stringify(params.command)
      : JSON.stringify(input)
    messages.addChild(new Text(bold(dim(`Tool · ${name}(${detail})`)), 1, 0))
    messages.addChild(new Spacer(1))
  }

  const addErrorMessage = (message: string) => {
    messages.addChild(new Text(bold(red("Error")), 1, 0))
    messages.addChild(new Text(message, 1, 1))
    messages.addChild(new Spacer(1))
  }

  const renderEvent = (event: StoredEvent) => {
    if (renderedEventIds.has(event.eventId)) return
    renderedEventIds.add(event.eventId)

    if (event.type === "UserMessageAdded") {
      if (!optimisticMessageIds.delete(event.payload.messageId)) {
        addUserMessage(event.payload.content)
      }
      if (pending?.messageId === event.payload.messageId) pending.inReplyTo = event.eventId
    } else if (event.type === "AgentToolCallAdded") {
      addToolCall(event.payload.name, event.payload.input)
    } else if (event.type === "AgentMessageAdded") {
      addAssistantMessage(event.payload.content)
      if (pending?.inReplyTo === event.payload.inReplyTo) {
        pending = undefined
        finish()
      }
    }
    tui.requestRender()
  }

  const startEventStream = (id: string) => {
    streamController?.abort()
    const controller = new AbortController()
    streamController = controller
    const consume = proxy.streamEvents(id).pipe(
      Stream.runForEach((event) => Effect.sync(() => {
        if (sessionId === id && !stopped) renderEvent(event)
      }))
    )
    void Effect.runPromise(consume, { signal: controller.signal }).catch((error: unknown) => {
      if (stopped || controller.signal.aborted || streamController !== controller) return
      addErrorMessage(`Event stream failed: ${formatError(error)}`)
      tui.requestRender()
    })
  }

  const switchSession = (id: string) => {
    activeController?.abort()
    pending = undefined
    finish()
    sessionId = id
    messages.clear()
    renderedEventIds.clear()
    optimisticMessageIds.clear()
    editor.setText("")
    showConversation()
    startEventStream(id)
  }

  const startNewSession = () => {
    void Effect.runPromise(proxy.createSession()).then((event) => {
      switchSession(event.sessionId)
    }).catch((error: unknown) => {
      addErrorMessage(formatError(error))
      tui.requestRender()
    })
  }

  const resumeSession = () => {
    void Effect.runPromise(proxy.listSessions()).then((sessions) => {
      const previous = sessions.filter((session) => session.sessionId !== sessionId)
      if (previous.length === 0) {
        addErrorMessage("No previous sessions found.")
        tui.requestRender()
        return
      }
      const picker = new SessionPicker(
        previous,
        (selectedSessionId) => switchSession(selectedSessionId),
        showConversation,
        () => tui.requestRender()
      )
      app.clear()
      app.addChild(picker)
      tui.setFocus(picker)
      tui.requestRender()
    }).catch((error: unknown) => {
      addErrorMessage(formatError(error))
      tui.requestRender()
    })
  }

  const submit = (input: string) => {
    const prompt = input.trim()
    if (prompt.length === 0 || editor.disableSubmit || stopped) return

    if (prompt === "/exit") return stop()
    if (prompt === "/new") return startNewSession()
    if (prompt === "/resume") return resumeSession()

    const messageId = crypto.randomUUID()
    pending = { messageId }
    optimisticMessageIds.add(messageId)
    editor.addToHistory(prompt)
    editor.disableSubmit = true
    addUserMessage(prompt)

    const loader = new Loader(tui, cyan, dim, "Thinking...")
    activeLoader = loader
    messages.addChild(loader)
    loader.start()

    const controller = new AbortController()
    activeController = controller
    tui.requestRender()

    void Effect.runPromise(
      proxy.sendMessage(sessionId, messageId, prompt),
      { signal: controller.signal }
    ).then((event) => {
      if (stopped || controller.signal.aborted) return
      if (activeController === controller) activeController = undefined
      if (pending?.messageId === messageId) pending.inReplyTo ??= event.eventId
    }).catch((error: unknown) => {
      if (stopped || controller.signal.aborted) return
      if (pending?.messageId === messageId) pending = undefined
      finish()
      addErrorMessage(formatError(error))
      tui.requestRender()
    })
  }

  editor.onSubmit = submit

  tui.addChild(app)
  showConversation()

  tui.addInputListener((data) => {
    if (matchesKey(data, Key.ctrl("c"))) {
      stop()
      return { consume: true }
    }
    return undefined
  })

  tui.start()
  startEventStream(sessionId)

  if (initialPrompt !== undefined && initialPrompt.trim().length > 0) {
    queueMicrotask(() => submit(initialPrompt))
  }

  return { done, stop }
}

export namespace Harness {
  export const run = (initialPrompt?: string): Effect.Effect<void, never, AgentProxy.Service> =>
    Effect.gen(function*() {
      const proxy = yield* AgentProxy.Service
      const event = yield* proxy.createSession().pipe(Effect.orDie)
      yield* Effect.acquireUseRelease(
        Effect.sync(() => make(proxy, event.sessionId, initialPrompt)),
        (handle) => Effect.promise(() => handle.done),
        (handle) => Effect.sync(handle.stop)
      )
    })
}
