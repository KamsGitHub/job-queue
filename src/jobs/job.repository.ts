import { PrismaClient, Prisma } from '../generated/prisma/client';
import type { CreateJobInput } from './job.schema';

/**
 * Takes `prisma` as a parameter rather than importing a shared singleton.
 * This module will grow more callers than just the submission API — the
 * worker (Milestone 5+) needs the same job-state operations, and tests
 * may want to pass a transaction-scoped client. A hard-coded import would
 * make both of those impossible.
 */
export async function createJob(prisma: PrismaClient, data: CreateJobInput) {
  return prisma.job.create({
    data: {
      type: data.type,
      // `data.payload` came from a Zod-validated, already-JSON-parsed HTTP
      // body, so it's guaranteed to be JSON-safe — this assertion just
      // bridges Zod's `unknown` to Prisma's own JSON input type.
      payload: data.payload as Prisma.InputJsonValue,
    },
  });
}

export async function deleteJob(prisma: PrismaClient, id: string) {
  await prisma.job.delete({ where: { id } });
}
