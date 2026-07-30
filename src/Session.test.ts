import { describe, expect, test } from "bun:test"
import {
  conversationBranch,
  conversationParentId,
  conversationTree,
  type SessionEvent,
  type StoredEvent
} from "./Session.ts"

const stored = (
  eventId: string,
  sequence: number,
  event: SessionEvent
): StoredEvent => ({
  ...event,
  eventId,
  sessionId: "session",
  sequence,
  position: sequence,
  occurredAt: new Date(sequence).toISOString()
} as StoredEvent)

describe("conversation tree", () => {
  test("keeps old linear events compatible", () => {
    const events = [
      stored("u1", 1, {
        type: "UserMessageAdded",
        payload: { messageId: "message-1", content: "hello" }
      }),
      stored("a1", 2, {
        type: "AgentMessageAdded",
        payload: { messageId: "message-2", content: "hi", inReplyTo: "u1" }
      })
    ]

    expect(conversationTree(events).leafId).toBe("a1")
    expect(conversationParentId(events, "a1")).toBe("u1")
    expect(conversationBranch(events).map((event) => event.eventId)).toEqual(["u1", "a1"])
  })

  test("navigates to an earlier point and preserves both branches", () => {
    const events = [
      stored("u1", 1, {
        type: "UserMessageAdded",
        payload: { messageId: "message-1", content: "root", parentId: null }
      }),
      stored("a1", 2, {
        type: "AgentMessageAdded",
        payload: { messageId: "message-2", content: "root reply", inReplyTo: "u1", parentId: "u1" }
      }),
      stored("u2", 3, {
        type: "UserMessageAdded",
        payload: { messageId: "message-3", content: "first branch", parentId: "a1" }
      }),
      stored("a2", 4, {
        type: "AgentMessageAdded",
        payload: { messageId: "message-4", content: "first reply", inReplyTo: "u2", parentId: "u2" }
      }),
      stored("navigation", 5, {
        type: "SessionTreeNavigated",
        payload: { targetId: "a1" }
      }),
      stored("u3", 6, {
        type: "UserMessageAdded",
        payload: { messageId: "message-5", content: "second branch", parentId: "a1" }
      }),
      stored("a3", 7, {
        type: "AgentMessageAdded",
        payload: { messageId: "message-6", content: "second reply", inReplyTo: "u3", parentId: "u3" }
      })
    ]

    const tree = conversationTree(events)
    expect(tree.nodes.map((node) => node.event.eventId)).toEqual(["u1", "a1", "u2", "a2", "u3", "a3"])
    expect(tree.leafId).toBe("a3")
    expect(conversationBranch(events).map((event) => event.eventId)).toEqual(["u1", "a1", "u3", "a3"])
    expect(conversationBranch(events, "a2").map((event) => event.eventId)).toEqual(["u1", "a1", "u2", "a2"])
  })

  test("can branch directly from a tool call", () => {
    const events = [
      stored("u1", 1, {
        type: "UserMessageAdded",
        payload: { messageId: "message-1", content: "inspect", parentId: null }
      }),
      stored("tool1", 2, {
        type: "AgentToolCallAdded",
        payload: {
          toolCallId: "call-1",
          name: "Bash",
          input: { command: "pwd" },
          inReplyTo: "u1",
          index: 0,
          parentId: "u1"
        }
      }),
      stored("navigation", 3, {
        type: "SessionTreeNavigated",
        payload: { targetId: "tool1" }
      }),
      stored("u2", 4, {
        type: "UserMessageAdded",
        payload: { messageId: "message-2", content: "continue differently", parentId: "tool1" }
      })
    ]

    expect(conversationTree(events).nodes.map((node) => node.event.eventId)).toEqual(["u1", "tool1", "u2"])
    expect(conversationBranch(events).map((event) => event.eventId)).toEqual(["u1", "tool1", "u2"])
  })

  test("does not let a delayed response steal the active branch", () => {
    const events = [
      stored("u1", 1, {
        type: "UserMessageAdded",
        payload: { messageId: "message-1", content: "one", parentId: null }
      }),
      stored("u2", 2, {
        type: "UserMessageAdded",
        payload: { messageId: "message-2", content: "two", parentId: "u1" }
      }),
      stored("a1", 3, {
        type: "AgentMessageAdded",
        payload: { messageId: "message-3", content: "late", inReplyTo: "u1", parentId: "u1" }
      })
    ]

    expect(conversationTree(events).leafId).toBe("u2")
    expect(conversationBranch(events).map((event) => event.eventId)).toEqual(["u1", "u2"])
    expect(conversationBranch(events, "a1").map((event) => event.eventId)).toEqual(["u1", "a1"])
  })
})
