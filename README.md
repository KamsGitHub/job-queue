# Distributed Job Queue

A production-grade distributed job queue built from scratch on PostgreSQL and
Redis Streams — no BullMQ, Celery, or RabbitMQ. Built incrementally,
milestone by milestone, as a portfolio project.

## Milestone 1: Project Skeleton & Environment

What exists so far:
- Fastify + TypeScript API server with a `/health` liveness endpoint
- Environment variables validated at boot with Zod (fail fast, not at 2am)
- Structured logging via Pino (pretty-printed locally, raw JSON in prod)
- Graceful shutdown on SIGTERM/SIGINT
- Dockerized Postgres + Redis for local dev, via Docker Compose
- A multi-stage production Dockerfile for the app itself
- ESLint (flat config) + Prettier
- Jest, with a first test using Fastify's `app.inject()` pattern

## Prerequisites

- Node.js >= 20
- Docker + Docker Compose

## Setup

```bash
cp .env.example .env
npm install
docker compose up -d        # starts Postgres + Redis
npm run dev                 # starts the API server with hot reload
```

Then, in another terminal:

```bash
curl http://localhost:3000/health
```

Expected response:

```json
{ "status": "ok", "uptime": 12.345, "timestamp": "2026-07-14T..." }
```

## Commands

| Command | Purpose |
|---|---|
| `npm run dev` | Run the server locally with hot reload (tsx watch) |
| `npm run build` | Compile TypeScript to `dist/` |
| `npm start` | Run the compiled server (production mode) |
| `npm test` | Run the Jest test suite |
| `npm run lint` | Lint with ESLint |
| `npm run format` | Format with Prettier |
| `docker compose up -d` | Start Postgres + Redis in the background |
| `docker compose down` | Stop and remove containers (data persists in named volumes) |

## Why the app isn't in docker-compose

You'll notice `docker-compose.yml` only provisions Postgres and Redis, not
the app itself. This is a deliberate tradeoff, not an oversight.

Running the app in Docker for local dev means every code change either
requires a volume mount (works, but file-watching across the Docker Desktop
VM boundary on macOS/Windows is often slow and unreliable) or a rebuild
(slow, breaks the hot-reload feedback loop entirely). Running the app
directly on the host with `tsx watch` gives near-instant reload and native
filesystem performance, at the cost of "it's not fully containerized
end-to-end" during development.

The `Dockerfile` in this repo is real and production-shaped — multi-stage
build, non-root user, `npm ci` for reproducible installs — and you should
use it to sanity-check containerized behavior before considering any
milestone truly "done," and for CI/deployment later. We're just not making
it part of the everyday dev inner loop.

## Design decisions worth remembering

- **Env validation is required, not optional, even for unused vars.**
  `DATABASE_URL` and `REDIS_URL` are validated at boot even though no code
  reads them yet. The infra (docker-compose) already exists; the config
  contract should match it from day one, so a misconfigured environment
  fails immediately and loudly instead of failing obscurely three
  milestones from now.

- **`buildApp()` is separate from `listen()`.** Tests construct the app and
  use `app.inject()` — an in-process fake request/response cycle — instead
  of binding a real port. Faster tests, no port collisions.

- **Only a liveness check exists so far (`/health`).** It answers "is the
  process up?" not "can it reach its dependencies?" A `/health/ready`
  endpoint that actually pings Postgres and Redis comes once we have
  clients for both — building it now would just be checking connections
  we don't have yet.
