import Fastify, { FastifyInstance } from 'fastify';
import { ZodError } from 'zod';
import { env } from './config/env';
import { healthRoutes } from './routes/health';
import { jobRoutes } from './routes/jobs';
import { createPrismaClient } from './db/client';
import type { PrismaClient } from './generated/prisma/client';

/**
 * Separating "build the app" from "start listening" is a small decision
 * with a real payoff: tests can construct a fully configured Fastify
 * instance and exercise routes via app.inject() — an in-memory request/
 * response cycle — without binding a real TCP port. No port collisions
 * between parallel test files, no network flakiness, and it's fast because
 * there's no actual socket I/O.
 *
 * `prisma` is an optional dependency, not a hard-coded import: production
 * (server.ts) lets buildApp() create its own client and takes ownership of
 * closing it. Tests can instead inject a shared client they manage
 * themselves — in which case buildApp() must NOT close it on app.close(),
 * since a shared client is still needed by the next test in the file.
 */
export function buildApp(deps: { prisma?: PrismaClient } = {}): FastifyInstance {
  const prisma = deps.prisma ?? createPrismaClient();
  const ownsPrisma = deps.prisma === undefined;

  const app = Fastify({
    logger: {
      level: env.LOG_LEVEL,
      // Pretty-printed, colorized logs are for human eyes during local dev.
      // In production we emit raw JSON: it's what log aggregators (Datadog,
      // ELK, CloudWatch) expect, it's machine-parseable, and pino-pretty's
      // formatting work is overhead we don't want in a hot request path.
      ...(env.NODE_ENV === 'development'
        ? {
            transport: {
              target: 'pino-pretty',
              options: {
                colorize: true,
                translateTime: 'HH:MM:ss',
                ignore: 'pid,hostname',
              },
            },
          }
        : {}),
    },
  });

  // A single Zod-aware branch, everything else falls through to Fastify's
  // own default error serialization (respects error.statusCode, logs it,
  // etc.) via reply.send(error) — we're not reimplementing that.
  app.setErrorHandler((error, _request, reply) => {
    if (error instanceof ZodError) {
      reply.code(400).send({
        error: 'Validation failed',
        details: error.flatten().fieldErrors,
      });
      return;
    }
    reply.send(error);
  });

  if (ownsPrisma) {
    app.addHook('onClose', async () => {
      await prisma.$disconnect();
    });
  }

  app.register(healthRoutes);
  app.register(jobRoutes, { prisma });

  return app;
}
