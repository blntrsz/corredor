import { expect, it } from "vitest"
import { pendingRunMatchesOutcome } from "./Harness.ts"

it("matches terminal activity that arrives before the User Commit activity", () => {
  expect(pendingRunMatchesOutcome({ commitId: "user-1" }, "user-1")).toBe(true)
  expect(pendingRunMatchesOutcome({
    commitId: "client-1",
    inReplyTo: "user-1"
  }, "user-1")).toBe(true)
  expect(pendingRunMatchesOutcome({ commitId: "user-1" }, "other-user")).toBe(false)
  expect(pendingRunMatchesOutcome(undefined, "user-1")).toBe(false)
})
