# Effect-native Vitest tests

## Recommendation

Use the version-matched Effect adapter and Vitest:

```json
{
  "devDependencies": {
    "@effect/vitest": "4.0.0-beta.102",
    "vitest": "4.1.10"
  }
}
```

The published `@effect/vitest@4.0.0-beta.102` package requires
`effect ^4.0.0-beta.102` and accepts Vitest `^3.0.0 || ^4.0.0`; Effect develops
that adapter against Vitest `4.1.10`. This repository already pins
`effect@4.0.0-beta.102`, so pinning the adapter to the same beta and Vitest to
`4.1.10` is the least surprising combination.
([published package metadata](https://www.npmjs.com/package/@effect/vitest/v/4.0.0-beta.102),
[Effect source package metadata](https://github.com/Effect-TS/effect/blob/de2a9a69099993087e57c64df58537c765ac0224/packages/vitest/package.json))

## The beta.102 API

Import the enhanced Effect-aware API as `it`, or alias it to `test`:

```ts
import { expect, it as test } from "@effect/vitest"
import { Effect } from "effect"

test.effect("runs an Effect directly", () =>
  Effect.gen(function*() {
    const value = yield* Effect.succeed(42)
    expect(value).toBe(42)
  }))
```

No `async`, `Effect.runPromise`, or manual Promise bridge is needed.
`@effect/vitest` re-exports Vitest's assertions and suite helpers, while its
own enhanced export is named `it`. A direct
`import { test } from "@effect/vitest"` receives Vitest's ordinary `test`
re-export and therefore does **not** have `.effect`, `.live`, or `.layer`.
Alias `it as test` if the repository prefers the word `test`.
([public exports and types](https://github.com/Effect-TS/effect/blob/de2a9a69099993087e57c64df58537c765ac0224/packages/vitest/src/index.ts#L15-L20),
[enhanced `it` export](https://github.com/Effect-TS/effect/blob/de2a9a69099993087e57c64df58537c765ac0224/packages/vitest/src/index.ts#L234-L242))

`test.effect`:

- accepts a callback that returns an `Effect`;
- supplies `TestClock` and `TestConsole`;
- creates and closes a scope for every test;
- forwards Vitest's abort signal into the Effect runtime;
- reports Effect failures through Vitest.

`test.live` has the same automatic per-test scope but keeps the live services
instead of installing the test clock and console.
([runner implementation](https://github.com/Effect-TS/effect/blob/de2a9a69099993087e57c64df58537c765ac0224/packages/vitest/src/internal/internal.ts#L20-L40),
[method construction](https://github.com/Effect-TS/effect/blob/de2a9a69099993087e57c64df58537c765ac0224/packages/vitest/src/internal/internal.ts#L328-L343))

There is no `test.scoped` or `it.scoped` in the beta.102 public types.
`test.effect` and `test.live` already apply `Effect.scoped`, so a scoped
resource can be yielded directly:

```ts
test.effect("releases resources", () =>
  Effect.gen(function*() {
    const resource = yield* Effect.acquireRelease(acquire, release)
    expect(resource).toBeDefined()
  }))
```

The package README still describes the older `it.scoped` and
`it.scopedLive` names, but the beta.102 source and declarations are
authoritative and expose only `effect`, `live`, and `layer`.
([current method types](https://github.com/Effect-TS/effect/blob/de2a9a69099993087e57c64df58537c765ac0224/packages/vitest/src/index.ts#L91-L150),
[README showing the stale names](https://github.com/Effect-TS/effect/blob/de2a9a69099993087e57c64df58537c765ac0224/packages/vitest/README.md))

## Cleanup and Layer patterns

Prefer Effect finalizers over outer `try`/`finally` blocks:

```ts
test.effect("uses an isolated directory", () =>
  Effect.gen(function*() {
    const directory = yield* Effect.acquireRelease(
      Effect.sync(() => mkdtempSync(join(tmpdir(), "corredor-"))),
      (directory) =>
        Effect.sync(() => rmSync(directory, { recursive: true, force: true }))
    )

    // Exercise the program. The directory is removed when the test scope closes.
  }))
```

For a Layer that should be fresh for every test, provide it inside
`test.effect`. Because the adapter scopes each test, `Layer.scoped` resources
and Layer finalizers are released at the end of that test.
([per-test scoping implementation](https://github.com/Effect-TS/effect/blob/de2a9a69099993087e57c64df58537c765ac0224/packages/vitest/src/internal/internal.ts#L328-L343))

For an intentionally shared, expensive fixture, use the top-level `layer`
helper or `test.layer`:

```ts
import { expect, it as test } from "@effect/vitest"

test.layer(ApplicationTestLayer)("Application", (test) => {
  test.effect("first case", () =>
    Effect.gen(function*() {
      const application = yield* Application.Service
      expect(application).toBeDefined()
    }))

  test.layer(AdditionalLayer)("nested cases", (test) => {
    test.effect("sees both layers", () => Effect.void)
  })
})
```

The helper builds and memoizes the shared Layer once, provides its Context to
each enclosed `test.effect`, and closes the Layer scope after the enclosed
tests. Nested `test.layer` composes with the parent Layer and uses a forked
memo map. The callback intentionally receives a non-live test API, preventing
`test.live` from bypassing the shared test environment.
([public Layer API and example](https://github.com/Effect-TS/effect/blob/de2a9a69099993087e57c64df58537c765ac0224/packages/vitest/src/index.ts#L167-L216),
[Layer acquisition and cleanup](https://github.com/Effect-TS/effect/blob/de2a9a69099993087e57c64df58537c765ac0224/packages/vitest/src/internal/internal.ts#L193-L304))

Share a Layer only when sharing its mutable state is intentional. Corredor's
SQLite tests generally need an isolated database, so a per-test scoped
directory/database Layer is the safer default; suite-level `test.layer` is
appropriate only for read-only or explicitly reset fixtures.

## Running Vitest in this Bun repository

Do not use `bun test`: that command selects Bun's built-in test runner, not
Vitest. Vitest's own Bun installation guidance explicitly says to use
`bun run test`, and `vitest run` performs one non-watch run.
([Vitest getting started](https://vitest.dev/guide/),
[Vitest CLI](https://vitest.dev/guide/cli),
[Bun test-runner command](https://bun.com/docs/guides/test/run-tests))

This repository also imports Bun-only modules such as `bun:sqlite`. Bun
normally respects Vitest's Node shebang, so a plain `bun run vitest` launches
Vitest under Node. Force the Bun runtime with `--bun`:

```json
{
  "scripts": {
    "test": "bun run --bun vitest run",
    "test:watch": "bun run --bun vitest"
  }
}
```

Then use:

```sh
bun run test
bun run test -- src/Application.test.ts
bun run test:watch
```

Bun documents that locally installed CLIs with a Node shebang run under Node
unless `bun run --bun` is used. A local probe with Bun `1.3.14`,
`@effect/vitest@4.0.0-beta.102`, and Vitest `4.1.10` confirmed the practical
difference: a native Effect test importing `bun:sqlite` failed under
`bun run vitest run` and passed under
`bun run --bun vitest run`.
([Bun runtime and `--bun`](https://bun.com/docs/runtime),
[`bunx --bun` shebang behavior](https://bun.com/docs/pm/bunx))

## Migration shape for the current tests

The current outer structure:

```ts
test("...", async () => {
  try {
    const result = await Effect.runPromise(Effect.scoped(program))
    expect(result).toEqual(expected)
  } finally {
    cleanup()
  }
})
```

can become:

```ts
test.effect("...", () =>
  Effect.gen(function*() {
    const fixture = yield* Effect.acquireRelease(acquire, release)
    const result = yield* program(fixture)
    expect(result).toEqual(expected)
  }))
```

That keeps acquisition, assertions, interruption, and cleanup in one Effect
scope managed by the adapter.
