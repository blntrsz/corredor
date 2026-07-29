import { spawn } from "node:child_process"
import { createConnection } from "node:net"

export const defaultServerPort = 5050
export const defaultServerUrl = `http://127.0.0.1:${defaultServerPort}`

const isPortOpen = (port: number): Promise<boolean> => new Promise((resolve) => {
  const socket = createConnection({ host: "127.0.0.1", port })
  socket.once("connect", () => {
    socket.destroy()
    resolve(true)
  })
  socket.once("error", () => resolve(false))
})

const isHealthy = async (baseUrl: string): Promise<boolean> => {
  try {
    const response = await fetch(`${baseUrl}/v1/health`, {
      signal: AbortSignal.timeout(500)
    })
    if (!response.ok) return false
    const body = await response.json() as Record<string, unknown>
    return body.status === "ok" && body.service === "corredor" && body.apiVersion === 2
  } catch {
    return false
  }
}

/** Ensure the compatible API daemon exists before constructing the harness. */
export const ensureServer = async (cliEntrypoint: string): Promise<void> => {
  if (await isHealthy(defaultServerUrl)) return
  if (await isPortOpen(defaultServerPort)) {
    throw new Error(`Port ${defaultServerPort} is occupied by an incompatible or unhealthy server`)
  }

  const child = spawn(
    process.execPath,
    [cliEntrypoint, "server", "--port", String(defaultServerPort)],
    {
      detached: true,
      stdio: "ignore",
      env: process.env
    }
  )
  child.unref()

  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    if (await isHealthy(defaultServerUrl)) return
    if (child.exitCode !== null) {
      throw new Error(`Corredor server exited during startup (code ${child.exitCode})`)
    }
    await Bun.sleep(50)
  }

  throw new Error(`Timed out waiting for Corredor health endpoint at ${defaultServerUrl}`)
}
