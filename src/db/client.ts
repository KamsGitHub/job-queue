import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '../generated/prisma/client';
import { env } from '../config/env';

/**
 * Prisma 7's client generator no longer implicitly reads DATABASE_URL and
 * connects on its own — `new PrismaClient()` with no args now throws.
 * It requires an explicit driver adapter, which is what actually owns the
 * connection pool (a plain `pg.Pool` under the hood).
 */
export function createPrismaClient(): PrismaClient {
  const adapter = new PrismaPg({ connectionString: env.DATABASE_URL });
  return new PrismaClient({ adapter });
}
