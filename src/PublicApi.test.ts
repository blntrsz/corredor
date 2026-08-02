import { expect, it } from "vitest"
import { Agent, AgentProxy, Application, Session, Workflow } from "../index.ts"

it("exports the public SDK from the package entrypoint", () => {
  expect(Agent.defaultDefinition.id).toBe("default")
  expect(AgentProxy.Service).toBeDefined()
  expect(Application.Service).toBeDefined()
  expect(Session.Service).toBeDefined()
  expect(Workflow.Service).toBeDefined()
})
