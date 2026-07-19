# Distributed Job Queue — Project Context

## What this is

A production-grade distributed job queue built from scratch in TypeScript,
as a backend engineering portfolio piece. Built on PostgreSQL + Redis
Streams directly — **no BullMQ, Celery, RabbitMQ, or similar libraries**
that would hide the internals. The entire point is to demonstrate (and
genuinely understand) how these systems work under the hood: consumer
groups, delivery semantics, crash recovery, retries, backpressure.

Stack: Node.js, TypeScript (strict), Fastify, PostgreSQL, Redis Streams,
Prisma, Zod, Pino, Docker/Docker Compose, Jest, ESLint + Prettier,
OpenAPI/Swagger.

## Roadmap (15 milestones)

**Phase 0 — Foundations**
1. Project skeleton & environment — done
2. Job data model & Prisma schema (state machine: pending → running → succeeded/failed) — done
3. Job submission API (`POST /jobs`, Zod validation) — done

**Phase 1 — Core Queue Mechanics**
4. Redis Streams as queue transport — done
5. Worker process & consumer groups ← **currently here, see status below**
6. Acknowledgment & crash recovery (XACK, PEL, XCLAIM)
7. Retry logic & backoff (exponential + jitter)
8. Dead-letter queue

**Phase 2 — Correctness Under Concurrency**
9. Idempotency & exactly-once effects
10. Priority queues
11. Scheduled & delayed jobs

**Phase 3 — Making It a Real Service**
12. Auth, API keys & multi-tenancy
13. Rate limiting & quotas (token bucket, Lua scripts for atomicity)
14. Observability: Prometheus metrics, structured logging tied to job IDs
15. Load testing & backpressure

Optional Phase 4 (only if time remains): web dashboard, CI/CD, Grafana.
Not required for the portfolio thesis — depth over breadth.

## Status: Milestone 1 (Project Skeleton & Environment)

Implemented:
- Fastify + TypeScript API, `/health` liveness endpoint
- Env validation via Zod in `src/config/env.ts` — fails fast at boot,
  not at first use. `DATABASE_URL`/`REDIS_URL` are required even though
  unused so far, since docker-compose already provisions both.
- `dotenv/config` loads `.env` locally; no-ops in prod where real env
  vars are already injected.
- Pino logging: pretty-printed in dev, raw JSON in prod.
- Graceful shutdown on SIGTERM/SIGINT (`app.close()` before exit).
- `buildApp()` in `src/app.ts` is deliberately separate from `.listen()`
  in `src/server.ts` — lets tests use Fastify's `app.inject()` (in-process
  fake requests, no real socket/port) instead of binding real ports.
  This is the pattern all future route tests will use.
- `docker-compose.yml` provisions Postgres + Redis only — **not** the app
  itself. Deliberate: host-run `tsx watch` has a much faster/reliabler
  dev loop than volume-mounted Docker hot reload. Production `Dockerfile`
  is real and multi-stage (builder stage has the TS compiler, production
  stage doesn't; runs as non-root `node` user) but isn't part of the
  everyday inner loop.
- Only a liveness check exists (`/health`) — no readiness check yet,
  since there's nothing to check readiness *of* until Postgres/Redis
  clients exist (lands around Milestone 2–4).

Known gotcha already hit and fixed: `tsconfig.json` has
`exactOptionalPropertyTypes: true` on (deliberate — will matter for job
state machine correctness later). This means `field: cond ? x : undefined`
is a type error for optional fields — the key must be *absent*, not
present-with-`undefined`. Fixed in `app.ts`'s logger transport config using
conditional object spread (`...(cond ? { transport: {...} } : {})`)
instead of a ternary that resolves to `undefined`. Expect this pattern to
recur — flag it early rather than reaching for `as any`.

Verified (2026-07-16, in Claude Code): `npm test` (1 suite/1 test pass),
`npm run lint` (clean), `npm run build` (clean `tsc`), and `npm run dev` +
`curl localhost:3000/health` (200, `{"status":"ok",...}`) all pass.
Milestone 1 is done.

## Status: Milestone 2 (Job Data Model & Prisma Schema)

Implemented:
- `prisma/schema.prisma`: `Job` model with `JobStatus` enum
  (`PENDING`/`RUNNING`/`SUCCEEDED`/`FAILED`), `id` as `uuid`, `payload` as
  `Json`/JSONB, `error` (nullable, set on `FAILED`), `createdAt`/`updatedAt`
  (auto-managed) plus explicit `startedAt`/`finishedAt` for latency
  metrics later (Milestone 14). Indexed on `status` since that's the
  worker's dispatch-candidate lookup.
- Deliberately *not* in the schema yet: `attempts`/`maxAttempts`
  (Milestone 7), `priority` (10), scheduling fields (11), `idempotencyKey`
  (9), tenant/API-key ownership (12). Each lands as its own migration when
  its milestone starts, so the migration history documents incremental
  design rather than a speculative all-up-front schema.
- State-transition validity (e.g. can't go `SUCCEEDED` → `PENDING`) is a
  conscious *non*-goal at the DB layer — no CHECK constraints or triggers,
  since Postgres CHECK can't see the previous row value without a trigger,
  and a trigger is more machinery than this guarantee is worth right now.
  Will be enforced at the repository/service layer when that's built.
- First migration: `prisma/migrations/20260716235404_init_job_model/`,
  applied via `prisma migrate dev` (not `db push`, to keep real migration
  history) against the docker-compose Postgres.
- Round-trip verified directly against the live DB (create → update to
  `RUNNING` with `startedAt` set → read back → delete) using a throwaway
  script, not just "migration applied without error."

Gotchas hit, both environment-level rather than design-level:
- **Node version mismatch.** The system's default `node` is v18.19.1, but
  `package.json` already declares `engines: >=20.0.0` and Prisma 7 hard-
  refuses to install below Node 20.19. Fixed by using `nvm`'s v24.18.0
  (already installed, just not the shell default) for all Prisma-related
  commands: `source ~/.nvm/nvm.sh && nvm use 24`. Since fixed at the repo
  level: `.nvmrc` pinned to `24.18.0`, and `test`/`lint`/`build`/`dev` were
  all re-verified clean under it (no regressions from the version bump).
- **Prisma 7 architecture change.** The new `prisma-client` generator
  (Query Compiler / WASM based, replacing the old bundled Rust engine) no
  longer implicitly reads `DATABASE_URL` and connects on its own —
  `new PrismaClient()` with no args now throws
  `PrismaClientInitializationError`. It requires an explicit driver
  adapter: `new PrismaClient({ adapter: new PrismaPg({ connectionString }) })`,
  via the added `@prisma/adapter-pg` + `pg` packages. This will matter for
  wherever the app instantiates its shared client (Milestone 3) — don't
  reach for the old no-args pattern from most existing Prisma tutorials.
- Prisma Client generates to `src/generated/prisma` (this generator's
  default output path) — gitignored, since it's build output, not source.

Milestone 2 was schema + migration + verified DB round-trip only — no
shared `PrismaClient` in the app, no routes touching `Job`. That's
Milestone 3, below.

## Status: Milestone 3 (Job Submission API)

Implemented:
- `POST /jobs` — Zod-validated (`src/jobs/job.schema.ts`: `type` non-empty
  string, `payload` must be an object — `z.record(z.string(), z.unknown())`,
  deliberately rejecting bare arrays/primitives as job payloads even though
  Prisma's `Json` column would accept them). Returns `201` with the full
  created row (`status: "PENDING"`, generated `id`, timestamps).
- `src/db/client.ts`: `createPrismaClient()` factory wrapping
  `@prisma/adapter-pg` + `env.DATABASE_URL`. Not a module-level singleton —
  a factory function, so callers control instantiation (matches the
  `buildApp()` precedent from Milestone 1).
- `src/jobs/job.repository.ts`: `createJob(prisma, data)` — takes the
  Prisma client as an explicit parameter rather than importing a shared
  instance. Reasoning (discussed at length before implementing): the
  worker (Milestone 5+) will need the same job-state operations with its
  own client/connection pool, and tests may want to pass a transaction-
  scoped client (`tx` from `prisma.$transaction()`) for rollback-based
  isolation. A hard-coded import would foreclose both. Kept deliberately
  thin — just the one function Milestone 3 needs, not a speculative
  interface with stub methods for retries/idempotency that don't exist
  yet.
- `buildApp({ prisma? })`: `prisma` is an optional dependency. If omitted,
  `buildApp()` creates its own client *and owns closing it* (registers an
  `onClose` hook calling `$disconnect()`). If a caller injects their own
  client, `buildApp()` does **not** close it — the caller managed its
  lifecycle, so the caller cleans it up. This distinction is load-bearing
  for the test suite: `jobs.test.ts` shares one `prisma` client across
  three tests via multiple `buildApp({ prisma })` + `app.close()` calls;
  if `buildApp()` had unconditionally disconnected on every `app.close()`,
  the second test would run against an already-closed client.
- Centralized error handling: `app.setErrorHandler()` special-cases
  `ZodError` into a consistent `400 { error, details }` shape; everything
  else falls through to Fastify's own default handling via
  `reply.send(error)` rather than reimplementing it.
- Tests (`src/routes/jobs.test.ts`) hit the real dev Postgres, not a
  mocked Prisma client — consistent with this project's "no shortcuts"
  stance; a mocked DB layer is exactly the kind of thing that passes in
  CI while masking a real schema/query mismatch. Verifies both the HTTP
  response *and* a fresh `findUnique` read, so the test proves the row
  was actually persisted, not just that the handler returned something
  that looked right. Created rows are deleted in `afterAll`.
- Manually verified against the live `npm run dev` server too (valid
  request → 201 persisted row; missing `type` → 400; non-object `payload`
  → 400), not just the automated suite.

Not yet done: no `GET /jobs/:id` — wasn't asked for by this milestone's
roadmap line, staying scoped to `POST /jobs` only.

## Status: Milestone 4 (Redis Streams as Queue Transport)

Deliberately narrow scope: this milestone only gets a job announced on a
Redis Stream when it's submitted. Nothing reads from the stream yet — no
consumer groups, no workers, no ack/PEL. That's Milestones 5–6. Verified
by inspecting the stream directly (`XRANGE`), not by anything consuming
it, since nothing capable of consuming it exists yet.

Implemented:
- `src/queue/redis.ts`: `createRedisClient()` factory (`ioredis`, reads
  `env.REDIS_URL`) — same shape as `createPrismaClient()`: not a
  module-level singleton, so `buildApp()` and tests each own their own
  connection.
- `src/queue/stream.ts`: `enqueueJob(redis, jobId)` — `XADD jobs:stream *
  jobId <id>`. **Entry carries only the job ID, not `type`/`payload`.**
  Postgres stays the single source of truth for job data; the stream's
  only job is dispatch notification. The alternative (duplicating
  `type`/`payload` into the stream to save the worker a DB round-trip)
  was considered and rejected — not worth two copies of job data that
  could drift, for one extra read per dequeue.
- `buildApp({ prisma?, redis? })`: `redis` follows the exact same
  optional-dependency/ownership pattern established for `prisma` in
  Milestone 3 — owns and closes its own client via `onClose` unless one
  is injected, in which case the caller owns its lifecycle.
- `POST /jobs` now, after the Postgres insert succeeds, attempts
  `enqueueJob`. **Dual-write problem, addressed head-on rather than
  ignored:** the Postgres `INSERT` and the Redis `XADD` are not atomic —
  two separate systems, no shared transaction. Two failure-mode designs
  were discussed and explicitly decided against the simpler one:
  - Rejected: on `XADD` failure, log and leave the row `PENDING`,
    return `201` anyway. Silently produces orphan rows — durably
    `PENDING` in Postgres forever, nothing will ever dispatch them.
  - **Chosen: compensating delete.** On `XADD` failure, delete the
    just-inserted row and return `500`. Not true cross-system atomicity
    (the delete itself could in principle fail too, though that's a much
    smaller window than "Redis is down"), but it makes the `500`
    response honest — nothing is left behind in the common failure case.
  - This is *not* the last word on this problem — once idempotency
    (Milestone 9) exists, revisit whether an outbox-table pattern is
    worth it instead of a best-effort compensating delete.
- Stream retention/trimming (`MAXLEN` or similar) is a deliberate
  non-goal right now — the stream currently grows unboundedly. Picking a
  trimming policy safely requires knowing what "safe to trim" means once
  a consumer group exists (you must not trim entries a group hasn't read
  yet), so it's deferred to Milestone 5/6 rather than guessed at now.
- Tests (`src/routes/jobs.test.ts`) now also inject a shared `redis`
  client (same lifecycle pattern as the shared `prisma` client) and
  assert the stream actually received the entry via `XREVRANGE` — not
  just that the handler didn't throw — then `XDEL` it to keep the test
  stream clean.
- Manually verified against the live `npm run dev` server + real Redis:
  valid submission → `201` and a matching `XRANGE jobs:stream` entry
  with the same job ID. Also verified the failure path directly (pointed
  a throwaway script's Redis client at a closed port): `500` returned,
  error logged with the job ID and reason, zero matching rows left in
  Postgres — the compensating delete works.

Known gotcha hit, environment-level: `npm test` reliably prints Jest's
"A worker process has failed to exit gracefully and has been force
exited" after adding the Redis client. Diagnosed, not ignored: a
standalone Node script (no Jest) proved `new Redis(url)` → `quit()`
closes instantly and cleanly even with zero commands issued, so there's
no actual leak. The warning only appears in Jest's default parallel-
worker mode; `--detectOpenHandles` (which also disables Jest's 1-second
force-exit timer) always passes clean — meaning the socket closes fine,
just not within Jest's default 1-second grace window inside its sandboxed
worker process. Tried `redis.disconnect()` after `quit()` as a fix — no
effect, reverted. Decided to leave the warning as-is rather than add
`--forceExit` to mask it. Doesn't affect production (`server.ts` exits
via explicit `process.exit()` on shutdown regardless), and all tests
pass.

## Status: Milestone 5 (Worker Process & Consumer Groups)

Two scope-boundary decisions were made explicitly before implementing,
since the roadmap's literal milestone split (XACK listed under M6) left
them open:
- **XACK on success now, not deferred to M6.** A successfully processed
  job is acked immediately; a failed one (unknown type, handler throws,
  or its Postgres row has vanished) is deliberately left un-acked in the
  Pending Entries List — M6 owns deciding what happens to those (XCLAIM,
  retry/backoff). The failure is still durably recorded in Postgres
  either way; leaving it un-acked is purely about stream redelivery
  bookkeeping, not about losing the failure record.
- **Minimal job handler registry**, since nothing in the roadmap has a
  dedicated milestone for "how does a job type map to actual work" and
  the worker needs *something* to call. `src/jobs/handlers.ts`: a
  `Record<type, handler>` map, one demo handler (`send-email`, logs and
  resolves — no real send). Unknown `type` throws inside the same
  try/catch as a real handler failure, so it's treated as an ordinary
  job failure (FAILED + un-acked), not a special case.

Implemented:
- `src/queue/stream.ts`: `CONSUMER_GROUP = 'workers'` and
  `ensureConsumerGroup(redis)` — `XGROUP CREATE jobs:stream workers 0
  MKSTREAM`, catching `BUSYGROUP` to make repeated calls (every worker
  startup) idempotent. Starts from **`'0'`, not `'$'`** — deliberate: a
  worker starting up must still see jobs already sitting on the stream
  from before it existed, not just new ones. (This is *why* the M4
  leftover manual-verification job briefly broke the first version of
  the M5 test suite — see gotcha below.)
- `src/jobs/job.repository.ts`: added `getJob`, `markJobRunning`,
  `markJobSucceeded`, `markJobFailed` — same "take `prisma` as a
  parameter" shape as `createJob`/`deleteJob`.
- `src/queue/consumer.ts`: `processNextJob(prisma, redis, logger,
  consumerName, blockMs?)` — reads and processes at most one entry via
  `XREADGROUP ... BLOCK ... COUNT 1`, returns `null` on a `BLOCK`
  timeout with nothing new. Parses the response by hand rather than
  trusting ioredis's types: `xreadgroup` (and `xpending`, hit again in
  the test file) is typed as returning plain `unknown[]`, not the
  precise tuple shape `xread` gets — a real library typing gap, not
  guesswork, and `noUncheckedIndexedAccess` forces explicit
  undefined-checks at every level of the parse regardless.
- `src/logger.ts`: `createLogger()` — a standalone pino instance for the
  worker process, since it has no Fastify instance to hang a logger off
  of. Deliberately duplicates app.ts's small dev/prod transport config
  rather than extracting a shared helper — that would mean touching
  already-verified Milestone 1 code for a Milestone 5 concern.
- `src/worker.ts`: the actual worker entry point — a separate process
  from `server.ts`, no HTTP surface. Consumer name is
  `${hostname}:${pid}`, unique per process so multiple workers (same or
  different machines) get independent PELs. Graceful shutdown mirrors
  `server.ts`'s SIGTERM/SIGINT handling: a `running` flag, checked once
  per loop iteration, so shutdown latency is bounded by one `BLOCK`
  timeout (default 5s) plus however long any already-in-flight job takes
  to finish — never abandons a job mid-work.
- `npm run worker` (`tsx watch src/worker.ts`) added alongside `npm run
  dev`, meant to run concurrently with it during local development.
- Tests (`src/queue/consumer.test.ts`): real Postgres + Redis, not
  mocked. Covers both the success path (job reaches `SUCCEEDED`, stream
  entry is acked — confirmed absent from `XPENDING`) and the failure
  path (unknown type → `FAILED` with the type name in `error`, entry
  confirmed still present in `XPENDING`, manually acked by the test
  itself afterward since nothing else will yet).
- Manually verified against live `npm run dev` + `npm run worker` +
  real Postgres/Redis: submitted job → picked up in ~milliseconds →
  `SUCCEEDED` row with `startedAt`/`finishedAt` set, empty `XPENDING`.
  Separately verified the failure path (unregistered `type` → `FAILED`
  row, entry left in `XPENDING` with delivery count 1) and graceful
  `SIGTERM` shutdown (worker logs the signal, finishes its current
  `BLOCK` cycle, exits — confirmed via process log, not just reading the
  code).

Gotcha hit, test-isolation rather than design: the first version of
`consumer.test.ts` assumed "the next entry `processNextJob` reads is the
one this test just enqueued." That's false in general — `ensureConsumerGroup`
starts from `'0'`, so a fresh group delivers oldest-first, and a stray
job from Milestone 4's manual `curl` verification (never consumed, since
no worker existed yet) was still sitting on the stream and got delivered
first, failing the test on an ID mismatch. Real bug was leftover local
state, not the worker logic — confirmed by checking Postgres directly
(the stray job actually *did* process correctly, just wasn't the one the
assertion expected). Fixed by wiping the stream key in `beforeAll`
(`redis.del(JOBS_STREAM_KEY)`, then recreating the group — deleting a
stream key also destroys its consumer group) — safe here since this is
local dev/test-only Redis, not shared state. Also manually purged the
stray M4 job from both Postgres and Redis in the actual dev environment
so it doesn't cause confusion in later milestones.

Not yet done, explicitly deferred to Milestone 6: `XCLAIM` / stale-
consumer recovery, retry/backoff for failed jobs, and any policy for
what eventually happens to entries left in the PEL. Also deferred:
stream trimming/retention (noted as a gap back in M4, still unaddressed).

## Design decisions worth preserving

- Test pattern going forward: `buildApp()` + `app.inject()`, not real
  HTTP calls against a bound port.
- Env contract is declared fully upfront in the Zod schema even before
  code consumes every field, to match provisioned infra and fail loudly
  on misconfiguration.
- Bind `0.0.0.0`, never `localhost`, in `server.ts` — binding `localhost`
  inside a container is unreachable from outside it.
