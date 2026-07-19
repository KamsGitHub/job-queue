import pino from 'pino';
import { env } from './config/env';

/**
 * The worker process has no Fastify instance to piggyback a logger on
 * (that's the API's job), so it gets its own pino instance here. Mirrors
 * app.ts's inline logger config deliberately rather than sharing a helper
 * with it — app.ts's config lives inside Fastify's own options object, and
 * extracting a shared abstraction would mean touching already-verified
 * Milestone 1 code for a Milestone 5 concern.
 */
export function createLogger() {
  return pino({
    level: env.LOG_LEVEL,
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
  });
}

export type Logger = ReturnType<typeof createLogger>;
