import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Context, Effect, Layer, Schema } from "effect"
import { Reactivity } from "effect/unstable/reactivity"
import { defaultDatabasePath } from "./Session.ts"

export class QueryError extends Schema.TaggedErrorClass<QueryError>()(
  "@corredor/DatabaseAdmin/QueryError",
  { message: Schema.String }
) {}

export interface Interface {
  readonly query: (
    sql: string
  ) => Effect.Effect<ReadonlyArray<Record<string, unknown>>, QueryError>
}

export class Service extends Context.Service<Service, Interface>()(
  "@corredor/DatabaseAdmin"
) {}

const queryError = (cause: unknown) => new QueryError({
  message: cause instanceof Error ? cause.message : String(cause)
})

export const make = (path = defaultDatabasePath) => Effect.gen(function*() {
  const sql = yield* SqliteClient.make({ filename: path })

  return Service.of({
    query: Effect.fn("DatabaseAdmin.query")(function*(query) {
      return yield* sql.unsafe<Record<string, unknown>>(query).pipe(
        Effect.mapError(queryError)
      )
    })
  })
}).pipe(Effect.provide(Reactivity.layer))

export const layer = (path = defaultDatabasePath) =>
  Layer.effect(Service, make(path))
