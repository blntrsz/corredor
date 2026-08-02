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
import { Crypto, Effect, Stream } from "effect"
import * as AgentProxy from "./AgentProxy.ts"
import * as Application from "./Application.ts"
import * as Session from "./Session.ts"
import type {
  HistoryItem,
  SessionSummary,
  WorkstreamSummary
} from "./Session.ts"

export const pendingRunMatchesOutcome = (
  pending: { readonly commitId: string; readonly inReplyTo?: string } | undefined,
  outcomeInReplyTo: string
): boolean => pending !== undefined && (
  pending.inReplyTo === outcomeInReplyTo ||
  pending.commitId === outcomeInReplyTo
)

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
    private readonly requestRender: () => void,
    private readonly heading = "Resume Session"
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
      bold(cyan(`  ${this.heading}`)),
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
      const lifecycle = session.settled ? ` · ${yellow("Settled")}` : ""
      lines.push(truncateToWidth(dim(`      ${new Date(session.updatedAt).toLocaleString()} · ${session.userCommitCount} User Commits${lifecycle}${id}`), width))
    }
    lines.push("", dim("  ↑↓ navigate · enter resume · type search · ctrl+s sort · ctrl+p IDs · esc cancel"), "")
    return lines.map((line) => truncateToWidth(line, width))
  }

  invalidate(): void {}
}

class WorkstreamPicker implements Component, Focusable {
  focused = false
  private query = ""
  private selected = 0

  constructor(
    private readonly workstreams: ReadonlyArray<WorkstreamSummary>,
    private readonly onSelect: (workstreamId: string) => void,
    private readonly onCancel: () => void,
    private readonly requestRender: () => void
  ) {}

  private filtered(): ReadonlyArray<WorkstreamSummary> {
    const query = this.query.toLowerCase()
    return this.workstreams.filter((workstream) =>
      workstream.name.toLowerCase().includes(query) ||
      workstream.workstreamId.toLowerCase().includes(query)
    )
  }

  handleInput(data: string): void {
    const workstreams = this.filtered()
    if (matchesKey(data, Key.escape) || matchesKey(data, Key.ctrl("c"))) {
      return this.onCancel()
    }
    if (matchesKey(data, Key.up)) this.selected = Math.max(0, this.selected - 1)
    else if (matchesKey(data, Key.down)) {
      this.selected = Math.min(workstreams.length - 1, this.selected + 1)
    } else if (matchesKey(data, Key.enter)) {
      const workstream = workstreams[this.selected]
      if (workstream !== undefined) this.onSelect(workstream.workstreamId)
      return
    } else if (matchesKey(data, Key.backspace)) {
      this.query = this.query.slice(0, -1)
      this.selected = 0
    } else if (data.length === 1 && data >= " ") {
      this.query += data
      this.selected = 0
    }
    this.requestRender()
  }

  render(width: number): string[] {
    const workstreams = this.filtered()
    const lines = [
      "",
      bold(cyan("  Choose Workstream")),
      dim(`  Search: ${this.query || "type to filter…"}`),
      dim(`  ${workstreams.length} Workstreams`),
      ""
    ]
    if (workstreams.length === 0) lines.push(yellow("  No matching Workstreams"))
    for (const [index, workstream] of workstreams.entries()) {
      const selected = index === this.selected
      const prefix = selected ? cyan("› ") : "  "
      lines.push(truncateToWidth(
        `  ${prefix}${selected ? bold(workstream.name) : workstream.name}`,
        width
      ))
      lines.push(truncateToWidth(
        dim(`      ${workstream.sessionCount} Sessions · ${workstream.workstreamId}`),
        width
      ))
    }
    lines.push("", dim("  ↑↓ navigate · enter open · type search · esc cancel"), "")
    return lines.map((line) => truncateToWidth(line, width))
  }

  invalidate(): void {}
}

class IntegrationChoicePicker implements Component, Focusable {
  focused = false
  private selected = 0

  constructor(
    private readonly onSelect: (choice: Application.IntegrationChoice) => void,
    private readonly onCancel: () => void,
    private readonly requestRender: () => void
  ) {}

  handleInput(data: string): void {
    if (matchesKey(data, Key.escape) || matchesKey(data, Key.ctrl("c"))) {
      return this.onCancel()
    }
    if (matchesKey(data, Key.up)) {
      this.selected = Math.max(0, this.selected - 1)
    } else if (matchesKey(data, Key.down)) {
      this.selected = Math.min(Application.integrationChoices.length - 1, this.selected + 1)
    } else if (matchesKey(data, Key.enter)) {
      const choice = Application.integrationChoices[this.selected]
      if (choice !== undefined) this.onSelect(choice)
      return
    }
    this.requestRender()
  }

  render(width: number): string[] {
    const lines = [
      "",
      bold(cyan("  Integrate Session")),
      dim("  Choose how to finish the source Session."),
      ""
    ]
    for (const [index, choice] of Application.integrationChoices.entries()) {
      const selected = index === this.selected
      const prefix = selected ? cyan("› ") : "  "
      lines.push(truncateToWidth(
        `  ${prefix}${selected ? bold(choice) : choice}`,
        width
      ))
    }
    lines.push("", dim("  ↑↓ navigate · enter choose · esc cancel"), "")
    return lines.map((line) => truncateToWidth(line, width))
  }

  invalidate(): void {}
}

interface HistoryTreeItem {
  readonly record: Session.BranchRecord
  readonly parentId: string | null
  /** Tree guides are added only where a parent has multiple children. */
  readonly treePrefix: string
  readonly active: boolean
  readonly current: boolean
}

class TreePicker implements Component, Focusable {
  focused = false
  private selected = 0
  private readonly items: ReadonlyArray<HistoryTreeItem>

  constructor(
    history: ReadonlyArray<HistoryItem>,
    branchHeadId: string | null,
    private readonly onSelect: (item: HistoryTreeItem) => void,
    private readonly onCancel: () => void,
    private readonly requestRender: () => void
  ) {
    const tree = Session.commitGraph(history, branchHeadId)
    const byId = new Map(tree.nodes.map(
      (node) => [Session.branchRecordId(node.record), node] as const
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

    const items: Array<HistoryTreeItem> = []
    const visit = (parentId: string | null, ancestorGutters: ReadonlyArray<boolean>) => {
      const nodes = children.get(parentId) ?? []
      const branchesHere = nodes.length > 1
      for (const [index, node] of nodes.entries()) {
        const isLastBranch = index === nodes.length - 1
        const ancestorPrefix = ancestorGutters
          .map((hasLaterSibling) => hasLaterSibling ? "│  " : "   ")
          .join("")
        items.push({
          record: node.record,
          parentId: node.parentId,
          treePrefix: branchesHere
            ? `${ancestorPrefix}${isLastBranch ? "└─ " : "├─ "}`
            : ancestorPrefix,
          active: activeIds.has(Session.branchRecordId(node.record)),
          current: Session.branchRecordId(node.record) === tree.headId
        })
        visit(
          Session.branchRecordId(node.record),
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
      const [role, rawContent] = Session.foldBranchRecord(item.record, {
        user: (record) => [cyan("You"), record.content] as const,
        agentMessage: (record) => [green("Agent"), record.content] as const,
        compaction: (record) => [
          cyan("Compaction Commit"),
          record.content
        ] as const,
        failure: (record) => [red("Agent Failure"), record.reason] as const,
        interrupt: (record) => [
          yellow("Interrupt Commit"),
          `${record.reason}${record.partialOutput.length > 0
            ? ` · ${record.partialOutput}`
            : ""}`
        ] as const,
        tool: (record) => [
          dim(`Tool Commit · ${record.name}`),
          JSON.stringify({
            input: record.input,
            outcome: record.outcome
          })
        ] as const,
        legacyTool: (record) => [
          dim(`Legacy Tool Record · ${record.name}`),
          JSON.stringify({ input: record.input, result: "not persisted" })
        ] as const
      })
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

const make = (
  proxy: AgentProxy.Interface,
  randomCommitId: Effect.Effect<string, unknown>,
  initialSessionId: string,
  initialPrompt?: string
): Handle => {
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
  let settled = false
  let lifecycleNotice: string | undefined
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
    { name: "resume-settled", description: "Inspect a settled Session" },
    { name: "history", description: "Check out an earlier Commit" },
    { name: "compact", description: "Compact the current Branch" },
    { name: "integrate", description: "Integrate another Session" },
    { name: "settle", description: "Settle the current Session" },
    { name: "reopen", description: "Reopen the current Session" },
    { name: "interrupt", description: "Interrupt the active Agent Run" },
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

  const addCompaction = (summary: string) => {
    messages.addChild(new Text(bold(cyan("Compaction Commit")), 1, 0))
    messages.addChild(new Markdown(summary, 1, 1, markdownTheme))
    messages.addChild(new Spacer(1))
  }

  const addToolRecord = (
    record: Extract<Session.BranchRecord, {
      readonly type: "ToolCommit" | "LegacyToolCall"
    }>
  ) => {
    const params = record.input as { readonly command?: unknown }
    const detail = record.name === "Bash" && typeof params.command === "string"
      ? JSON.stringify(params.command)
      : JSON.stringify(record.input)
    const label = record.type === "ToolCommit"
      ? "Tool Commit"
      : "Legacy Tool Record"
    messages.addChild(
      new Text(bold(dim(`${label} · ${record.name}(${detail})`)), 1, 0)
    )
    if (record.type === "ToolCommit") {
      const outcome = record.outcome.type === "Success"
        ? record.outcome.result
        : record.outcome.failure
      const outcomeLabel = record.outcome.type === "Success"
        ? "Result"
        : "Failure"
      messages.addChild(new Text(
        record.outcome.type === "Success"
          ? dim(`${outcomeLabel}: ${JSON.stringify(outcome)}`)
          : red(`${outcomeLabel}: ${JSON.stringify(outcome)}`),
        1,
        1
      ))
    } else {
      messages.addChild(new Text(dim("Result was not persisted."), 1, 1))
    }
    messages.addChild(new Spacer(1))
  }

  const addFailure = (reason: string) => {
    messages.addChild(new Text(bold(red("Agent Failure")), 1, 0))
    messages.addChild(new Text(red(reason), 1, 1))
    messages.addChild(new Spacer(1))
  }

  const addInterrupt = (reason: string, partialOutput: string) => {
    messages.addChild(new Text(bold(yellow("Interrupt Commit")), 1, 0))
    messages.addChild(new Text(yellow(reason), 1, 1))
    if (partialOutput.length > 0) {
      messages.addChild(new Markdown(partialOutput, 1, 1, markdownTheme))
    }
    messages.addChild(new Spacer(1))
  }

  const orderedHistory = (): ReadonlyArray<HistoryItem> =>
    [...knownItems.values()].sort((a, b) => a.sequence - b.sequence)

  const renderMessages = () => {
    messages.clear()
    const history = orderedHistory()
    if (settled) {
      messages.addChild(new Text(
        bold(yellow("Session settled · use /reopen before continuing")),
        1,
        0
      ))
      messages.addChild(new Spacer(1))
    }
    if (lifecycleNotice !== undefined) {
      messages.addChild(new Text(dim(lifecycleNotice), 1, 0))
      messages.addChild(new Spacer(1))
    }
    for (const record of Session.branchHistory(history, branchHeadId)) {
      Session.foldBranchRecord(record, {
        user: (entry) => addUserMessage(entry.content),
        agentMessage: (entry) => addAssistantMessage(entry.content),
        compaction: (entry) => addCompaction(entry.content),
        failure: (entry) => addFailure(entry.reason),
        interrupt: (entry) => addInterrupt(entry.reason, entry.partialOutput),
        tool: (entry) => addToolRecord(entry),
        legacyTool: (entry) => addToolRecord(entry)
      })
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
    if (item.type === "SessionSettled") {
      settled = true
      lifecycleNotice = "Session settled. History remains available for inspection."
      activeController?.abort()
      pending = undefined
      finish()
      return
    }
    if (item.type === "SessionReopened") {
      settled = false
      lifecycleNotice = "Session reopened. New work is available."
      renderMessages()
      return
    }
    if (item.type === "UserCommit" && pending?.commitId === item.commitId) {
      branchHeadId = item.commitId
      pending.inReplyTo = item.commitId
    } else if (
      (item.type === "AgentMessageCommit" ||
        item.type === "CompactionCommit" ||
        item.type === "FailureCommit" ||
        item.type === "InterruptCommit") &&
      pendingRunMatchesOutcome(pending, item.inReplyTo)
    ) {
      branchHeadId = item.commitId
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
      settled = snapshot.settled
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
    settled = false
    lifecycleNotice = undefined
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

  const resumeSession = (view: Session.SessionListView = "active") => {
    void Effect.runPromise(proxy.listWorkstreams()).then((workstreams) => {
      if (workstreams.length === 0) {
        addErrorMessage("No Workstreams found.")
        tui.requestRender()
        return
      }
      const loadSessionsForWorkstream = (workstreamId: string) => {
        void Effect.runPromise(proxy.workstream(workstreamId, view)).then((snapshot) => {
          const previous = snapshot.sessions.filter(
            (session) => session.sessionId !== sessionId
          )
          if (previous.length === 0) {
            showConversation()
            addErrorMessage(
              view === "settled"
                ? "No settled Sessions found in this Workstream."
                : "No previous Sessions found in this Workstream."
            )
            return
          }
          const picker = new SessionPicker(
            previous,
            (selectedSessionId) => switchSession(selectedSessionId),
            showConversation,
            () => tui.requestRender(),
            view === "settled" ? "Settled Sessions" : "Resume Session"
          )
          app.clear()
          app.addChild(picker)
          tui.setFocus(picker)
          tui.requestRender()
        }).catch((error: unknown) => {
          showConversation()
          addErrorMessage(formatError(error))
        })
      }
      const picker = new WorkstreamPicker(
        workstreams,
        loadSessionsForWorkstream,
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
      settled = snapshot.settled
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
          const targetId = Session.branchRecordId(item.record)
          void Effect.runPromise(proxy.checkout(treeSessionId, targetId)).then(() => {
            if (stopped || sessionId !== treeSessionId) return
            branchHeadId = targetId
            editor.setText(
              item.record.type === "UserCommit" ? item.record.content : ""
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

  const compactCurrentBranch = () => {
    if (settled) {
      addErrorMessage("Session is settled; use /reopen before compacting.")
      return
    }
    if (branchHeadId === null) {
      addErrorMessage("There is no Branch Head to compact yet.")
      return
    }
    const sourceHeadId = branchHeadId
    void Effect.runPromise(proxy.compact(sessionId, sourceHeadId)).then((commit) => {
      if (stopped) return
      renderActivity(commit)
      lifecycleNotice = "Branch compacted. Earlier Commits remain available in history."
      renderMessages()
    }).catch((error: unknown) => {
      if (stopped) return
      addErrorMessage(formatError(error))
    })
  }

  const chooseIntegration = (
    sourceSessionId: string,
    sourceHistory: Session.HistorySnapshot
  ) => {
    const sourceBranchHeadId = sourceHistory.branchHeadId
    if (sourceBranchHeadId === null) {
      showConversation()
      addErrorMessage("The source Session has no Branch Head to integrate.")
      return
    }
    const targetSessionId = sessionId
    const choicePicker = new IntegrationChoicePicker(
      (settlement) => {
        app.clear()
        app.addChild(new Loader(tui, cyan, dim, "Integrating…"))
        tui.requestRender()
        void Effect.runPromise(proxy.history(targetSessionId)).then((targetHistory) => {
          if (targetHistory.branchHeadId !== branchHeadId) {
            throw new Error("The target Branch Head changed; choose Integration again.")
          }
          return Effect.runPromise(proxy.integrate({
            sourceSessionId,
            sourceBranchHeadId,
            targetSessionId,
            targetBranchHeadId: targetHistory.branchHeadId,
            settlement
          }))
        }).then((integration) => {
          if (stopped || sessionId !== targetSessionId) return
          branchHeadId = integration.picked.commitId
          lifecycleNotice = settlement === "integrate and settle"
            ? "Source Session integrated and settled."
            : "Source Session integrated."
          renderActivity(integration.picked)
          showConversation()
        }).catch((error: unknown) => {
          if (stopped) return
          showConversation()
          addErrorMessage(formatError(error))
        })
      },
      showConversation,
      () => tui.requestRender()
    )
    app.clear()
    app.addChild(choicePicker)
    tui.setFocus(choicePicker)
    tui.requestRender()
  }

  const integrateAnotherSession = () => {
    if (settled) {
      addErrorMessage("Session is settled; use /reopen before integrating.")
      return
    }
    void Effect.runPromise(proxy.listWorkstreams()).then((workstreams) => {
      if (workstreams.length === 0) {
        showConversation()
        addErrorMessage("No Workstreams found.")
        return
      }
      const loadSources = (workstreamId: string) => {
        void Effect.runPromise(proxy.workstream(workstreamId, "active")).then((snapshot) => {
          const sources = snapshot.sessions.filter(
            (candidate) => candidate.sessionId !== sessionId
          )
          if (sources.length === 0) {
            showConversation()
            addErrorMessage("No other active Sessions found in this Workstream.")
            return
          }
          const picker = new SessionPicker(
            sources,
            (sourceSessionId) => {
              void Effect.runPromise(proxy.history(sourceSessionId)).then(
                (sourceHistory) => chooseIntegration(sourceSessionId, sourceHistory)
              ).catch((error: unknown) => {
                showConversation()
                addErrorMessage(formatError(error))
              })
            },
            showConversation,
            () => tui.requestRender(),
            "Integrate Source Session"
          )
          app.clear()
          app.addChild(picker)
          tui.setFocus(picker)
          tui.requestRender()
        }).catch((error: unknown) => {
          showConversation()
          addErrorMessage(formatError(error))
        })
      }
      const picker = new WorkstreamPicker(
        workstreams,
        loadSources,
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

  const settleCurrentSession = () => {
    void Effect.runPromise(proxy.settle(sessionId)).then((event) => {
      if (stopped) return
      renderActivity(event)
    }).catch((error: unknown) => {
      if (stopped) return
      addErrorMessage(formatError(error))
    })
  }

  const reopenCurrentSession = () => {
    void Effect.runPromise(proxy.reopen(sessionId)).then((event) => {
      if (stopped) return
      renderActivity(event)
    }).catch((error: unknown) => {
      if (stopped) return
      addErrorMessage(formatError(error))
    })
  }

  const interruptCurrentRun = () => {
    const startingCommitId = pending?.inReplyTo ?? pending?.commitId
    if (startingCommitId === undefined) {
      addErrorMessage("No active Agent Run to interrupt.")
      return
    }
    void Effect.runPromise(proxy.interruptAgentRun(
      sessionId,
      startingCommitId,
      "Interrupted by user"
    )).then((commit) => {
      if (stopped) return
      if (commit === undefined) {
        addErrorMessage("No active Agent Run to interrupt.")
        return
      }
      renderActivity(commit)
    }).catch((error: unknown) => {
      if (stopped) return
      addErrorMessage(formatError(error))
    })
  }

  const submit = (input: string) => {
    const prompt = input.trim()
    if (prompt.length === 0 || stopped) return

    if (prompt === "/exit") return stop()
    if (prompt === "/new") return startNewSession()
    if (prompt === "/resume") return resumeSession()
    if (prompt === "/resume-settled") return resumeSession("settled")
    if (prompt === "/history" || prompt === "/tree") return openTree()
    if (prompt === "/compact") return compactCurrentBranch()
    if (prompt === "/integrate") return integrateAnotherSession()
    if (prompt === "/settle") return settleCurrentSession()
    if (prompt === "/reopen") return reopenCurrentSession()
    if (prompt === "/interrupt") return interruptCurrentRun()
    if (editor.disableSubmit) return
    if (settled) {
      addErrorMessage("Session is settled; use /reopen before continuing.")
      return
    }

    editor.addToHistory(prompt)
    editor.disableSubmit = true

    void Effect.runPromise(randomCommitId).then((commitId) => {
      if (stopped) return
      pending = { commitId, content: prompt }

      const loader = new Loader(tui, cyan, dim, "Thinking...")
      activeLoader = loader
      loader.start()
      renderMessages()

      const controller = new AbortController()
      activeController = controller
      tui.requestRender()

      return Effect.runPromise(
        proxy.submitUserCommit(sessionId, prompt, commitId),
        { signal: controller.signal }
      ).then((commit) => {
        if (stopped || controller.signal.aborted) return
        if (activeController === controller) activeController = undefined
        if (pending?.commitId === commitId) {
          branchHeadId = commit.commitId
          pending.inReplyTo ??= commit.commitId
        }
      }).catch((error: unknown) => {
        if (stopped || controller.signal.aborted) return
        if (pending?.commitId === commitId) pending = undefined
        finish()
        addErrorMessage(formatError(error))
        tui.requestRender()
      })
    }).catch((error: unknown) => {
      if (stopped) return
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
      if (pending !== undefined) interruptCurrentRun()
      else stop()
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
  export const run = (
    initialPrompt?: string
  ): Effect.Effect<void, never, AgentProxy.Service | Crypto.Crypto> =>
    Effect.gen(function*() {
      const proxy = yield* AgentProxy.Service
      const crypto = yield* Crypto.Crypto
      const session = yield* proxy.createSession().pipe(Effect.orDie)
      yield* Effect.acquireUseRelease(
        Effect.sync(() =>
          make(proxy, crypto.randomUUIDv4, session.sessionId, initialPrompt)
        ),
        (handle) => Effect.promise(() => handle.done),
        (handle) => Effect.sync(handle.stop)
      )
    })
}
