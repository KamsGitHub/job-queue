import type Redis from 'ioredis';
import type { PrismaClient } from '../generated/prisma/client';
import type { Logger } from '../logger';
import { JOBS_STREAM_KEY, CONSUMER_GROUP } from './stream';
import { getJob, markJobRunning, markJobSucceeded, markJobFailed } from '../jobs/job.repository';
import { getHandler } from '../jobs/handlers';

export interface ProcessedJob {
  entryId: string;
  jobId: string;
  outcome: 'succeeded' | 'failed';
}

/**
 * ioredis types XREADGROUP's return as `unknown[]`, not `unknown[] | null`
 * — a typing gap, not real behavior. At runtime, a BLOCK timeout with no
 * new entries returns null, and the real shape when there IS data is
 * `[[streamKey, [[entryId, [field, value, ...]], ...]]]`. Parsed by hand
 * here (with noUncheckedIndexedAccess forcing explicit undefined checks
 * at every level) rather than trusting the loose library type.
 */
function parseFirstEntry(response: unknown): { entryId: string; fields: string[] } | null {
  if (!response) {
    return null;
  }
  const streams = response as [string, [string, string[]][]][];
  const firstStream = streams[0];
  if (!firstStream) {
    return null;
  }
  const [, entries] = firstStream;
  const firstEntry = entries[0];
  if (!firstEntry) {
    return null;
  }
  const [entryId, fields] = firstEntry;
  return { entryId, fields };
}

function extractField(fields: string[], name: string): string | undefined {
  const index = fields.indexOf(name);
  return index === -1 ? undefined : fields[index + 1];
}

/**
 * Reads and processes (at most) one job from the stream, as a single
 * consumer in CONSUMER_GROUP. Returns null if BLOCK timed out with nothing
 * new to read.
 *
 * Ack policy: only a successfully processed job is XACK'd. A job that
 * fails (unknown type, handler throws, or its Postgres row has vanished)
 * is deliberately left un-acked, sitting in the consumer's Pending Entries
 * List — Milestone 6 owns deciding what happens to it next (XCLAIM,
 * retry/backoff). The failure is still durably recorded in Postgres
 * (status FAILED + error) either way; leaving it un-acked is purely about
 * the stream's redelivery bookkeeping, not about losing the failure.
 */
export async function processNextJob(
  prisma: PrismaClient,
  redis: Redis,
  logger: Logger,
  consumerName: string,
  blockMs = 5000,
): Promise<ProcessedJob | null> {
  const response = await redis.xreadgroup(
    'GROUP',
    CONSUMER_GROUP,
    consumerName,
    'COUNT',
    1,
    'BLOCK',
    blockMs,
    'STREAMS',
    JOBS_STREAM_KEY,
    '>',
  );

  const entry = parseFirstEntry(response);
  if (!entry) {
    return null;
  }
  const { entryId, fields } = entry;

  const jobId = extractField(fields, 'jobId');
  if (!jobId) {
    logger.error({ entryId, fields }, 'Stream entry has no jobId field; leaving un-acked');
    return { entryId, jobId: '', outcome: 'failed' };
  }

  const job = await getJob(prisma, jobId);
  if (!job) {
    logger.error({ jobId, entryId }, 'Stream entry references a job that no longer exists in Postgres; leaving un-acked');
    return { entryId, jobId, outcome: 'failed' };
  }

  await markJobRunning(prisma, jobId);

  try {
    const handler = getHandler(job.type);
    if (!handler) {
      throw new Error(`No handler registered for job type "${job.type}"`);
    }
    // `job.payload` came back from Postgres as Prisma.JsonValue, but the
    // submission API's Zod schema already guarantees every stored payload
    // is a JSON object, not a bare array/primitive — same cast used when
    // writing it in job.repository.ts's createJob().
    await handler(job.payload as Record<string, unknown>, { logger });
    await markJobSucceeded(prisma, jobId);
    await redis.xack(JOBS_STREAM_KEY, CONSUMER_GROUP, entryId);
    logger.info({ jobId, entryId, type: job.type }, 'Job succeeded');
    return { entryId, jobId, outcome: 'succeeded' };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await markJobFailed(prisma, jobId, message);
    logger.error({ jobId, entryId, type: job.type, err }, 'Job failed; left un-acked pending Milestone 6');
    return { entryId, jobId, outcome: 'failed' };
  }
}
