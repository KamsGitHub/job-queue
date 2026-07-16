import 'dotenv/config';
import { z } from 'zod';

/**
 * dotenv/config reads a local .env file and merges it into process.env,
 * WITHOUT overwriting variables that are already set. In Docker/production,
 * real env vars are injected by the platform (docker-compose, ECS task
 * definition, k8s manifest) and there's no .env file — dotenv silently
 * finds nothing and does nothing. This one import safely covers both cases.
 *
 * The full environment contract for this service.
 *
 * We declare DATABASE_URL and REDIS_URL as required here even though no code
 * uses them yet (Prisma and Redis Streams land in later milestones). This is
 * deliberate: docker-compose already provisions both services, so the
 * contract should exist now. Validating config at boot — not at first use —
 * means a misconfigured deployment fails in the first 100ms, with a clear
 * error, instead of failing obscurely the first time a job is submitted.
 */
const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
  DATABASE_URL: z.string().url(),
  REDIS_URL: z.string().url(),
});

export type Env = z.infer<typeof envSchema>;

function loadEnv(): Env {
  const parsed = envSchema.safeParse(process.env);

  if (!parsed.success) {
    console.error('❌ Invalid environment variables:');
    console.error(parsed.error.flatten().fieldErrors);
    process.exit(1);
  }

  return parsed.data;
}

export const env = loadEnv();
