import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Effect } from "effect"

export const temporaryDatabase = (prefix: string) => Effect.acquireRelease(
  Effect.sync(() => {
    const directory = mkdtempSync(join(tmpdir(), prefix))
    return { directory, path: join(directory, "corredor.db") }
  }),
  ({ directory }) => Effect.sync(() => {
    rmSync(directory, { recursive: true, force: true })
  })
)
