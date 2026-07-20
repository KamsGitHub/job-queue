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
5. Worker process & consumer groups — done
6. Acknowledgment & crash recovery (XACK, PEL, XCLAIM) — done
7. Retry logic & backoff (exponential + jitter) ← **currently here, see status below**
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

## Status: Milestone 6 (Acknowledgment & Crash Recovery)

Key framing decided before implementing: Redis Streams makes **no
protocol-level distinction** between "this consumer crashed mid-job" and
"this entry is un-acked for any other reason" (e.g. M5's un-acked failed
jobs) — both just look like PEL entries idle past some threshold. So
crash recovery and "what happens to a failed job" turn out to be the same
mechanism: any live consumer notices an entry has been idle too long,
steals it via reclaim, and reprocesses it from scratch, identically
regardless of *why* it was stuck.

Implemented:
- `src/queue/consumer.ts` refactored: the shared per-entry logic (fetch
  job, `markJobRunning`, dispatch to handler, ack-on-success /
  leave-un-acked-on-failure) pulled out into `processEntry()`, used by
  both `processNextJob()` (fresh `XREADGROUP` reads) and the new
  `reclaimStaleEntries()` — guaranteeing reclaimed entries get *exactly*
  the same treatment as freshly delivered ones, not a parallel code path
  that could drift.
- `reclaimStaleEntries(prisma, redis, logger, consumerName, idleMs,
  count?)`: uses **`XAUTOCLAIM`**, not the roadmap's literal `XCLAIM` —
  deliberate deviation. `XAUTOCLAIM` (Redis 6.2+, available on the
  `redis:7-alpine` image already in use) does "scan the PEL for entries
  idle ≥ idleMs, then claim them" as one atomic, cursor-paginated call.
  The classic `XPENDING` (find stale entries) + `XCLAIM` (take them)
  two-step has a real race — another consumer can claim the same entry
  between your `XPENDING` read and your `XCLAIM` call — that `XAUTOCLAIM`
  closes by doing both in a single round trip. Always scans from cursor
  `'0'` rather than persisting a cursor across sweeps: if there are more
  stale entries than `count` (10, not env-configurable — only the
  interval and idle threshold were asked for), they're simply picked up
  on the *next* sweep, since idle time only grows. Avoids needing any
  cursor state for a sweep that already runs every few seconds.
- Two new env vars (`src/config/env.ts`), both defaulting to **5000ms**
  (a placeholder — no real job-duration data exists yet to calibrate
  against): `WORKER_STALE_SWEEP_INTERVAL_MS` (how often a worker sweeps)
  and `WORKER_STALE_IDLE_MS` (how long an entry must sit un-acked before
  it's eligible for reclaim).
- `src/worker.ts`: every worker sweeps on its own `setInterval`, running
  concurrently with (not coupled to) its own main read loop — no
  dedicated reaper process/role. The group self-heals as long as *any*
  worker is alive. A `sweeping` guard skips a tick if the previous sweep
  is still in flight rather than overlapping. Shutdown now also
  `clearInterval`s the sweep timer and awaits any in-flight sweep before
  disconnecting, alongside the existing "let the current read finish"
  logic from M5.
- Deliberately **no retry cap or backoff** — that's Milestone 7. Until
  then, a permanently-failing job gets reclaimed and retried immediately
  and unconditionally on every sweep. Flagged explicitly, not an
  oversight.
- Named but *not* solved: a crash between `markJobSucceeded` (Postgres)
  and `XACK` (Redis) in `processEntry()`'s success path means a
  `SUCCEEDED` job's entry could still be sitting un-acked — which the
  reclaim sweep would then pick up and reprocess an already-successful
  handler. Real at-least-once-delivery gap; exactly what Milestone 9
  (idempotency) exists to close.
- Tests (`src/queue/consumer.test.ts`): added two cases for
  `reclaimStaleEntries`, simulating a dead consumer by reading an entry
  as `'dead-consumer'` via a raw `XREADGROUP` call and never processing
  it, then calling `reclaimStaleEntries` under the real `consumerName`
  with `idleMs=0` (claim immediately, no need to sleep past a real
  threshold in a test). Covers both outcomes: success (job reaches
  `SUCCEEDED`, entry fully acked) and failure (job reaches `FAILED`,
  entry still pending but **ownership verifiably transferred** to the
  reclaiming consumer — checked via `XPENDING`'s consumer field, not just
  "still pending somewhere").

**Real bug caught during manual verification, not by the test suite** —
worth recording in detail since it's a genuine Redis-client gotcha:
`worker.ts` originally shared *one* ioredis connection across
`ensureConsumerGroup`, the main loop's `XREADGROUP ... BLOCK 5000`, and
the independent sweep timer's `XAUTOCLAIM`/`XACK` calls. Redis processes
a single client connection's commands strictly in order — it will not
respond to command N+1 until command N is fully answered. While the main
loop had a `BLOCK 5000` read outstanding, any command the sweep sent on
that *same* connection queued behind it, invisibly delayed by up to the
full block duration. Symptom observed live: reclaiming a stale job and
running its (near-instant) handler logged correctly, but "Job succeeded"
didn't log until exactly 5 seconds later — the `XACK` was stuck in queue.
Confirmed the mechanism in isolation before touching code: a `PING`
issued immediately after a `BLOCK 5000` read on the same connection also
took ~5089ms to resolve, not ~1ms. **Fix:** `sweepRedis =
redis.duplicate()` — the sweep gets its own dedicated connection,
separate from the one used for blocking reads. Standard rule for Redis
clients: never share a connection between a blocking command and other
concurrent commands. Re-verified live after the fix: reclaim → handler →
ack all logged at the identical timestamp.

Manually verified end-to-end, twice (once catching the bug above, once
confirming the fix): submitted a job, used a throwaway script to read it
onto the stream as `'dead-consumer'` (simulating a worker that died right
after delivery, before doing any work), started a real worker with the
real 5s/5s env defaults (not a test's `idleMs=0` shortcut), and watched
it detect the stale entry on its very first sweep tick, reclaim it,
reprocess it, and reach `SUCCEEDED` with an empty `XPENDING`. Also
re-verified graceful `SIGTERM` shutdown still cleanly closes both Redis
connections.

## Status: Milestone 7 (Retry Logic & Backoff)

Key mechanism decision made before implementing: M6's `XAUTOCLAIM` sweep
claims any PEL entry idle past `WORKER_STALE_IDLE_MS` (a single global
threshold) — it has no way to express "wait longer for this specific
entry based on how many times it's already failed." Real per-job
exponential backoff needed one of two designs:
- Rejected: switch to manual `XPENDING` (to read each entry's Redis-native
  `deliveryCount`) + selective `XCLAIM` only for entries whose computed
  backoff has elapsed. Reintroduces the two-round-trip race `XAUTOCLAIM`
  was chosen in M6 specifically to avoid, and makes Redis's delivery count
  — not Postgres — the attempt-tracking source of truth.
- **Chosen: leave M6's sweep completely untouched**, gate the actual
  retry decision in Postgres instead. The sweep keeps claiming on its
  fixed cheap cadence; `processEntry()` checks the job's Postgres
  `nextRetryAt` before running the handler and declines (leaving the
  entry un-acked again) if it's not yet due. Postgres stays the single
  clock for "is this job allowed to run yet," at the cost of claiming
  (cheap, no real work) jobs that are still deep in backoff.

Implemented:
- New migration (`prisma/migrations/..._add_retry_tracking/`): `Job` gets
  `attempts Int @default(0)`, `maxAttempts Int @default(5)`,
  `nextRetryAt DateTime?` — flagged as reserved for this milestone all the
  way back in M2's notes.
- `src/queue/backoff.ts`: `computeBackoffMs(attempts, baseMs, capMs)` —
  full jitter (AWS's well-known algorithm): `random(0, min(cap, base *
  2^(attempts-1)))`. Spreads retries out instead of every failed job
  waking up at the same instant. `WORKER_RETRY_BASE_MS` (1000) /
  `WORKER_RETRY_MAX_MS` (60000) added to `src/config/env.ts`, same
  placeholder-pending-real-data framing as M6's stale-entry settings.
- `job.repository.ts`: `markJobRunning` now also clears `nextRetryAt` to
  `null` — once a handler is actually about to run (first attempt or a
  matured retry), any prior backoff deadline is stale. `markJobFailed`'s
  signature changed from a bare `error: string` to `{ error, attempts,
  nextRetryAt }` — attempts/backoff are computed by the caller
  (`processEntry`), not the repository function.
- `consumer.ts`'s `processEntry()` (shared by fresh reads and reclaimed
  entries, per M6) gained **two** guards before running the handler, in
  order:
  1. `job.attempts >= job.maxAttempts` → permanently give up (`outcome:
     'deferred'`), no further processing, ever.
  2. `job.nextRetryAt` still in the future → defer for now (`outcome:
     'deferred'`), to be claimed and re-checked on a later sweep.
  `ProcessedJob.outcome` gained the `'deferred'` variant accordingly.
- On failure: `attempts` increments; if the new count reaches
  `maxAttempts`, `nextRetryAt` is set to `null` (exhausted, no further
  retry scheduled) — otherwise it's set to `now + computeBackoffMs(...)`.

**Real bug caught by tracing the logic before running it live, not by the
test suite** (the tests as first written wouldn't have caught it either —
worth recording why): the very first version only checked
`nextRetryAt` to decide whether to defer. But the exhausted case *also*
sets `nextRetryAt` to `null` — indistinguishable, from that check alone,
from "no backoff needed, free to run now." An exhausted job would have
kept re-running its handler and incrementing `attempts` **past**
`maxAttempts`, forever, on every future sweep — silently recreating
exactly the unbounded-retry problem this milestone exists to fix, just
disguised as "working as intended" since the job would still show
`FAILED` with a growing `attempts` count. Fixed by adding the explicit
`attempts >= maxAttempts` guard *before* the backoff-window check, and
added a test (`'stops scheduling retries once attempts reaches
maxAttempts, and stays stopped on a later attempt'`) that verifies a
*second*, later reclaim doesn't move `attempts` past 5 — not just that
the exhausting call alone set `nextRetryAt` to `null`.

Manually verified end-to-end with real timers (not a test's synthetic
`idleMs=0`): submitted a job with an unregistered `type`, ran a real
worker for ~40s, and watched `attempts` climb 1→4 across successive
sweeps (each backoff window smaller than the 5s sweep interval, so it
retried almost every tick), one genuine `'Job is still within its backoff
window; deferring'` at attempt 4 (that backoff happened to exceed the
next sweep), the exhausting 5th attempt, and then — the part that matters
— `'Job has exhausted its retry budget'` on every subsequent sweep with
`attempts` staying at exactly `5`, never climbing further. Also confirmed
`XPENDING`'s Redis-native `deliveryCount` had reached `13` by the end
(every sweep claims it regardless of whether `processEntry` actually
reprocesses it) versus Postgres's `attempts: 5` — a concrete, observed
number behind the "cheap busywork" tradeoff named above, not just a
theoretical one.

Not yet done, explicitly deferred to Milestone 8: what happens to a
permanently-exhausted job (currently: `FAILED` forever, entry un-acked
forever, silently reclaimed-and-declined by every future sweep
indefinitely) — that's the dead-letter queue's job to resolve.

## Design decisions worth preserving

- Test pattern going forward: `buildApp()` + `app.inject()`, not real
  HTTP calls against a bound port.
- Env contract is declared fully upfront in the Zod schema even before
  code consumes every field, to match provisioned infra and fail loudly
  on misconfiguration.
- Bind `0.0.0.0`, never `localhost`, in `server.ts` — binding `localhost`
  inside a container is unreachable from outside it.
