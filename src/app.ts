import Fastify, { FastifyInstance } from 'fastify';
import { env } from './config/env';
import { healthRoutes } from './routes/health';

/**
 * Separating "build the app" from "start listening" is a small decision
 * with a real payoff: tests can construct a fully configured Fastify
 * instance and exercise routes via app.inject() — an in-memory request/
 * response cycle — without binding a real TCP port. No port collisions
 * between parallel test files, no network flakiness, and it's fast because
 * there's no actual socket I/O.
 */
export function buildApp(): FastifyInstance {
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

  app.register(healthRoutes);

  return app;
}
