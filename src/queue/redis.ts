import Redis from 'ioredis';
import { env } from '../config/env';

/**
 * Same shape as createPrismaClient(): a factory, not a module-level
 * singleton, so buildApp() (production) and tests can each own their own
 * connection and control its lifecycle independently.
 */
export function createRedisClient(): Redis {
  return new Redis(env.REDIS_URL);
}
