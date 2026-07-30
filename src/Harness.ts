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
import * as Session from "./Session.ts"
import type { HistoryItem, SessionSummary } from "./Session.ts"

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
      lines.push(truncateToWidth(dim(`      ${new Date(session.updatedAt).toLocaleString()} · ${session.userCommitCount} User Commits${id}`), width))
    }
    lines.push("", dim("  ↑↓ navigate · enter resume · type search · ctrl+s sort · ctrl+p IDs · esc cancel"), "")
    return lines.map((line) => truncateToWidth(line, width))
  }

  invalidate(): void {}
}

type TreeEntryEvent = Session.GraphEntry

interface TreeEntryItem {
  readonly event: TreeEntryEvent
  readonly parentId: string | null
  /** Tree guides are added only where a parent has multiple children. */
  readonly treePrefix: string
  readonly active: boolean
  readonly current: boolean
}

class TreePicker implements Component, Focusable {
  focused = false
  private selected = 0
  private readonly items: ReadonlyArray<TreeEntryItem>

  constructor(
    events: ReadonlyArray<HistoryItem>,
    branchHeadId: string | null,
    private readonly onSelect: (item: TreeEntryItem) => void,
    private readonly onCancel: () => void,
    private readonly requestRender: () => void
  ) {
    const tree = Session.commitGraph(events, branchHeadId)
    const byId = new Map(tree.nodes.map(
      (node) => [Session.graphEntryId(node.entry), node] as const
    ))
    const activeIds = new Set<string>()
    let activeId = tree.headId
    while (activeId !== null && !activeIds.has(activeId)) {
      activeIds.add(activeId)
      activeId = byId.get(activeId)?.parentId ?? null
    }

    const children = new Map<string | null, Array<Session.CommitNode>>()
    for (const node of tree.nodes) {
      const siblings = children.get(node.parentId) ?? []
      siblings.push(node)
      children.set(node.parentId, siblings)
    }

    const items: Array<TreeEntryItem> = []
    const visit = (parentId: string | null, ancestorGutters: ReadonlyArray<boolean>) => {
      const nodes = children.get(parentId) ?? []
      const branchesHere = nodes.length > 1
      for (const [index, node] of nodes.entries()) {
        const isLastBranch = index === nodes.length - 1
        const ancestorPrefix = ancestorGutters
          .map((hasLaterSibling) => hasLaterSibling ? "│  " : "   ")
          .join("")
        items.push({
          event: node.entry,
          parentId: node.parentId,
          treePrefix: branchesHere
            ? `${ancestorPrefix}${isLastBranch ? "└─ " : "├─ "}`
            : ancestorPrefix,
          active: activeIds.has(Session.graphEntryId(node.entry)),
          current: Session.graphEntryId(node.entry) === tree.headId
        })
        visit(
          Session.graphEntryId(node.entry),
          branchesHere
            ? [...ancestorGutters, !isLastBranch]
            : ancestorGutters
        )
      }
    }
    visit(null, [])
    this.items = items

    const currentIndex = items.findIndex((item) => item.current)
    this.selected = currentIndex >= 0
      ? currentIndex
      : Math.max(0, items.findLastIndex((item) => item.active))
  }

  handleInput(data: string): void {
    if (matchesKey(data, Key.escape) || matchesKey(data, Key.ctrl("c"))) return this.onCancel()
    if (matchesKey(data, Key.up)) this.selected = Math.max(0, this.selected - 1)
    else if (matchesKey(data, Key.down)) this.selected = Math.min(this.items.length - 1, this.selected + 1)
    else if (matchesKey(data, Key.left)) this.selected = Math.max(0, this.selected - 10)
    else if (matchesKey(data, Key.right)) this.selected = Math.min(this.items.length - 1, this.selected + 10)
    else if (matchesKey(data, Key.enter)) {
      const item = this.items[this.selected]
      if (item !== undefined) this.onSelect(item)
      return
    }
    this.requestRender()
  }

  render(width: number): string[] {
    const start = Math.max(0, Math.min(
      this.selected - 8,
      Math.max(0, this.items.length - 17)
    ))
    const lines = [
      "",
      bold(cyan("  Commit History")),
      dim(`  ${this.items.length} Commits and legacy tool records · ● active Branch`),
      ""
    ]

    for (const [offset, item] of this.items.slice(start, start + 17).entries()) {
      const index = start + offset
      const selected = index === this.selected
      const cursor = selected ? cyan("›") : " "
      const marker = item.current ? green("◆") : item.active ? cyan("●") : dim("○")
      const [role, rawContent] = item.event.type === "UserCommit"
        ? [cyan("You"), item.event.content]
        : item.event.type === "AgentMessageCommit"
        ? [green("Agent"), item.event.content]
        : [dim(`Tool · ${item.event.name}`), JSON.stringify(item.event.input)]
      const content = (rawContent ?? "").replace(/\s+/g, " ").trim() || "(empty)"
      const line = `  ${cursor} ${marker} ${item.treePrefix}${role}: ${content}`
      lines.push(truncateToWidth(selected ? bold(line) : line, width))
    }

    lines.push(
      "",
      dim("  ↑↓ navigate · ←→ page · enter jump · esc cancel"),
      dim("  User Commits are restored for editing; other records continue from that Branch Head."),
      ""
    )
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
  let branchHeadId: string | null = null
  let stopped = false
  let activeController: AbortController | undefined
  let streamController: AbortController | undefined
  let activeLoader: Loader | undefined
  let pending: {
    readonly commitId: string
    readonly content: string
    inReplyTo?: string
  } | undefined
  const knownItems = new Map<string, HistoryItem>()
  const errors: Array<string> = []

  const header = new Text(
    `${bold(cyan("Corredor"))}\n${dim("DeepSeek V4 Pro · type / for commands · Ctrl+C to quit")}`,
    1,
    1
  )
  const footer = new Text(dim("Submit a User Commit; follow-ups retain Branch context."), 1, 0)

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
    { name: "new", description: "Start a new Session" },
    { name: "resume", description: "Resume a previous session" },
    { name: "history", description: "Check out an earlier Commit" },
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

  const orderedHistory = (): ReadonlyArray<HistoryItem> =>
    [...knownItems.values()].sort((a, b) => a.sequence - b.sequence)

  const renderMessages = () => {
    messages.clear()
    const history = orderedHistory()
    for (const entry of Session.branchHistory(history, branchHeadId)) {
      if (entry.type === "UserCommit") addUserMessage(entry.content)
      else if (entry.type === "AgentMessageCommit") {
        addAssistantMessage(entry.content)
      } else {
        addToolCall(entry.name, entry.input)
      }
    }

    const pendingIsCommitted = pending !== undefined && history.some(
      (item) => item.type === "UserCommit" &&
        item.commitId === pending?.commitId
    )
    if (pending !== undefined && !pendingIsCommitted) addUserMessage(pending.content)

    for (const message of errors) {
      messages.addChild(new Text(bold(red("Error")), 1, 0))
      messages.addChild(new Text(message, 1, 1))
      messages.addChild(new Spacer(1))
    }
    if (activeLoader !== undefined) messages.addChild(activeLoader)
    tui.requestRender()
  }

  const addErrorMessage = (message: string) => {
    errors.push(message)
    renderMessages()
  }

  const finish = () => {
    if (activeLoader !== undefined) {
      activeLoader.stop()
      activeLoader = undefined
    }
    activeController = undefined
    editor.disableSubmit = false
    renderMessages()
    tui.setFocus(editor)
    tui.requestRender()
  }

  const renderActivity = (item: HistoryItem) => {
    const itemId = Session.historyItemId(item)
    if (knownItems.has(itemId)) return
    knownItems.set(itemId, item)
    if (
      Session.isGraphEntry(item) &&
      item.parentId === branchHeadId
    ) {
      branchHeadId = Session.graphEntryId(item)
    }

    if (item.type === "UserCommit" && pending?.commitId === item.commitId) {
      pending.inReplyTo = item.commitId
    } else if (
      item.type === "AgentMessageCommit" &&
      pending?.inReplyTo === item.inReplyTo
    ) {
      pending = undefined
      finish()
      return
    }
    renderMessages()
  }

  const startEventStream = (id: string) => {
    streamController?.abort()
    const controller = new AbortController()
    streamController = controller

    void Effect.runPromise(proxy.history(id), { signal: controller.signal }).then((snapshot) => {
      if (stopped || controller.signal.aborted || streamController !== controller || sessionId !== id) return
      branchHeadId = snapshot.branchHeadId
      for (const item of snapshot.items) {
        knownItems.set(Session.historyItemId(item), item)
      }
      renderMessages()

      const after = snapshot.items.reduce(
        (position, item) => Math.max(position, item.position),
        0
      )
      const consume = proxy.streamActivity(id, after).pipe(
        Stream.runForEach((item) => Effect.sync(() => {
          if (sessionId === id && !stopped) renderActivity(item)
        }))
      )
      return Effect.runPromise(consume, { signal: controller.signal })
    }).catch((error: unknown) => {
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
    branchHeadId = null
    knownItems.clear()
    errors.length = 0
    editor.setText("")
    renderMessages()
    showConversation()
    startEventStream(id)
  }

  const startNewSession = () => {
    void Effect.runPromise(proxy.createSession()).then((session) => {
      switchSession(session.sessionId)
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

  const openTree = () => {
    const treeSessionId = sessionId
    void Effect.runPromise(proxy.history(treeSessionId)).then((snapshot) => {
      if (stopped || sessionId !== treeSessionId) return
      branchHeadId = snapshot.branchHeadId
      for (const item of snapshot.items) {
        knownItems.set(Session.historyItemId(item), item)
      }
      const hasEntries = Session.commitGraph(
        snapshot.items,
        snapshot.branchHeadId
      ).nodes.length > 0
      if (!hasEntries) {
        addErrorMessage("There are no Commits to check out yet.")
        return
      }

      let navigating = false
      const picker = new TreePicker(
        snapshot.items,
        snapshot.branchHeadId,
        (item) => {
          if (navigating) return
          navigating = true
          const targetId = item.event.type === "UserCommit"
            ? item.parentId
            : Session.graphEntryId(item.event)
          void Effect.runPromise(proxy.checkout(treeSessionId, targetId)).then(() => {
            if (stopped || sessionId !== treeSessionId) return
            branchHeadId = targetId
            editor.setText(
              item.event.type === "UserCommit" ? item.event.content : ""
            )
            renderMessages()
            showConversation()
          }).catch((error: unknown) => {
            if (stopped || sessionId !== treeSessionId) return
            showConversation()
            addErrorMessage(formatError(error))
          })
        },
        showConversation,
        () => tui.requestRender()
      )
      app.clear()
      app.addChild(picker)
      tui.setFocus(picker)
      tui.requestRender()
    }).catch((error: unknown) => {
      if (stopped || sessionId !== treeSessionId) return
      addErrorMessage(formatError(error))
    })
  }

  const submit = (input: string) => {
    const prompt = input.trim()
    if (prompt.length === 0 || editor.disableSubmit || stopped) return

    if (prompt === "/exit") return stop()
    if (prompt === "/new") return startNewSession()
    if (prompt === "/resume") return resumeSession()
    if (prompt === "/history" || prompt === "/tree") return openTree()

    const commitId = crypto.randomUUID()
    pending = { commitId, content: prompt }
    editor.addToHistory(prompt)
    editor.disableSubmit = true

    const loader = new Loader(tui, cyan, dim, "Thinking...")
    activeLoader = loader
    loader.start()
    renderMessages()

    const controller = new AbortController()
    activeController = controller
    tui.requestRender()

    void Effect.runPromise(
      proxy.submitUserCommit(sessionId, prompt, commitId),
      { signal: controller.signal }
    ).then((commit) => {
      if (stopped || controller.signal.aborted) return
      if (activeController === controller) activeController = undefined
      if (pending?.commitId === commitId) {
        pending.inReplyTo ??= commit.commitId
      }
    }).catch((error: unknown) => {
      if (stopped || controller.signal.aborted) return
      if (pending?.commitId === commitId) pending = undefined
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
      const session = yield* proxy.createSession().pipe(Effect.orDie)
      yield* Effect.acquireUseRelease(
        Effect.sync(() => make(proxy, session.sessionId, initialPrompt)),
        (handle) => Effect.promise(() => handle.done),
        (handle) => Effect.sync(handle.stop)
      )
    })
}
