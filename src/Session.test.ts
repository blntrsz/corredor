import { describe, expect, it } from "@effect/vitest"
import {
  branchHistory,
  commitGraph,
  type Commit,
  type HistoryItem,
  type LegacyNavigation
} from "./Session.ts"

type CommitInput = Commit extends infer Variant
  ? Variant extends Commit
    ? Omit<Variant, "sessionId" | "sequence" | "position" | "createdAt">
    : never
  : never

const commit = (
  sequence: number,
  value: CommitInput
): Commit => ({
  ...value,
  sessionId: "session",
  sequence,
  position: sequence,
  createdAt: new Date(sequence).toISOString()
} as Commit)

describe("Commit graph", () => {
  it("preserves canonical single-parent ancestry", () => {
    const history = [
      commit(1, {
        type: "UserCommit",
        commitId: "u1",
        parentId: null,
        content: "hello"
      }),
      commit(2, {
        type: "AgentMessageCommit",
        commitId: "a1",
        parentId: "u1",
        content: "hi",
        inReplyTo: "u1"
      })
    ]

    expect(commitGraph(history).headId).toBe("a1")
    expect(commitGraph(history).nodes[1]?.parentId).toBe("u1")
    expect(branchHistory(history).map((entry) =>
      entry.type === "LegacyToolCall" ? entry.legacyId : entry.commitId
    )).toEqual(["u1", "a1"])
  })

  it("preserves divergent Branches without letting a delayed response move the derived head", () => {
    const history = [
      commit(1, {
        type: "UserCommit",
        commitId: "u1",
        parentId: null,
        content: "one"
      }),
      commit(2, {
        type: "UserCommit",
        commitId: "u2",
        parentId: "u1",
        content: "two"
      }),
      commit(3, {
        type: "AgentMessageCommit",
        commitId: "a1",
        parentId: "u1",
        content: "late",
        inReplyTo: "u1"
      })
    ]

    expect(commitGraph(history).headId).toBe("u2")
    expect(branchHistory(history).map((entry) =>
      entry.type === "LegacyToolCall" ? entry.legacyId : entry.commitId
    )).toEqual(["u1", "u2"])
    expect(branchHistory(history, "a1").map((entry) =>
      entry.type === "LegacyToolCall" ? entry.legacyId : entry.commitId
    )).toEqual(["u1", "a1"])
  })

  it("keeps a completed Tool Commit atomic in ancestry", () => {
    const history = [
      commit(1, {
        type: "UserCommit",
        commitId: "u1",
        parentId: null,
        content: "inspect"
      }),
      commit(2, {
        type: "ToolCommit",
        commitId: "t1",
        parentId: "u1",
        toolCallId: "call-1",
        name: "Bash",
        input: { command: "pwd" },
        outcome: {
          type: "Success",
          result: { exitCode: 0, stdout: "/repo\n", stderr: "" }
        },
        inReplyTo: "u1",
        index: 0
      })
    ]

    expect(branchHistory(history)).toEqual(history)
  })

  it("uses legacy navigation only as readable migration input", () => {
    const navigation: LegacyNavigation = {
      type: "LegacyNavigation",
      activityId: "navigation",
      sessionId: "session",
      sequence: 3,
      position: 3,
      occurredAt: new Date(3).toISOString(),
      targetId: "u1"
    }
    const history: ReadonlyArray<HistoryItem> = [
      commit(1, {
        type: "UserCommit",
        commitId: "u1",
        parentId: null,
        content: "root"
      }),
      commit(2, {
        type: "AgentMessageCommit",
        commitId: "a1",
        parentId: "u1",
        content: "reply",
        inReplyTo: "u1"
      }),
      navigation,
      commit(4, {
        type: "UserCommit",
        commitId: "u2",
        parentId: "u1",
        content: "branch"
      })
    ]

    expect(commitGraph(history).headId).toBe("u2")
    expect(commitGraph(history).nodes).toHaveLength(3)
  })
})
