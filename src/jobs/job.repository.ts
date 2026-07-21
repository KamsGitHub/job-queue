import { PrismaClient, Prisma, JobStatus, type Job } from '../generated/prisma/client';
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
      // exactOptionalPropertyTypes gotcha (see CLAUDE.md): `idempotencyKey:
      // data.idempotencyKey` would set the key present-with-value-undefined
      // when absent, which Prisma's optional field type rejects. Spread it
      // in only when actually provided, so the key is truly absent instead.
      ...(data.idempotencyKey ? { idempotencyKey: data.idempotencyKey } : {}),
    },
  });
}

export async function findJobByIdempotencyKey(prisma: PrismaClient, idempotencyKey: string) {
  return prisma.job.findUnique({ where: { idempotencyKey } });
}

/**
 * Milestone 9: the actual dedup logic for POST /jobs. If no key is given,
 * behaves exactly like createJob. If a key is given, checks for an existing
 * job with that key first — but a plain check-then-insert would still race
 * under concurrent requests carrying the same key, so the insert's own
 * unique-constraint violation (Prisma error P2002) is the real guard: two
 * concurrent submissions can both pass the initial check, but only one
 * insert wins, and the loser re-fetches and returns the winner's row
 * instead of surfacing the constraint error to its caller.
 */
export async function findOrCreateJob(
  prisma: PrismaClient,
  data: CreateJobInput,
): Promise<{ job: Job; created: boolean }> {
  if (data.idempotencyKey) {
    const existing = await findJobByIdempotencyKey(prisma, data.idempotencyKey);
    if (existing) {
      return { job: existing, created: false };
    }
  }

  try {
    const job = await createJob(prisma, data);
    return { job, created: true };
  } catch (err) {
    if (
      data.idempotencyKey &&
      err instanceof Prisma.PrismaClientKnownRequestError &&
      err.code === 'P2002'
    ) {
      const existing = await findJobByIdempotencyKey(prisma, data.idempotencyKey);
      if (existing) {
        return { job: existing, created: false };
      }
    }
    throw err;
  }
}

export async function deleteJob(prisma: PrismaClient, id: string) {
  await prisma.job.delete({ where: { id } });
}

export async function getJob(prisma: PrismaClient, id: string) {
  return prisma.job.findUnique({ where: { id } });
}

export async function markJobRunning(prisma: PrismaClient, id: string) {
  return prisma.job.update({
    where: { id },
    // nextRetryAt is cleared here, not just left stale: once a handler is
    // actually about to run (first attempt or a matured retry), any prior
    // backoff deadline is no longer meaningful until/unless another
    // failure sets a new one.
    data: { status: JobStatus.RUNNING, startedAt: new Date(), nextRetryAt: null },
  });
}

export async function markJobSucceeded(prisma: PrismaClient, id: string) {
  return prisma.job.update({
    where: { id },
    data: { status: JobStatus.SUCCEEDED, finishedAt: new Date() },
  });
}

export async function markJobFailed(
  prisma: PrismaClient,
  id: string,
  data: { error: string; attempts: number; nextRetryAt: Date | null },
) {
  return prisma.job.update({
    where: { id },
    data: {
      status: JobStatus.FAILED,
      error: data.error,
      attempts: data.attempts,
      nextRetryAt: data.nextRetryAt,
      finishedAt: new Date(),
    },
  });
}

export async function markJobDeadLettered(
  prisma: PrismaClient,
  id: string,
  data: { error: string; attempts: number },
) {
  return prisma.job.update({
    where: { id },
    data: {
      status: JobStatus.DEAD_LETTERED,
      error: data.error,
      attempts: data.attempts,
      nextRetryAt: null,
      finishedAt: new Date(),
    },
  });
}
