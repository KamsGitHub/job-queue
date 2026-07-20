import type Redis from 'ioredis';
import type { PrismaClient } from '../generated/prisma/client';
import type { Logger } from '../logger';
import { env } from '../config/env';
import { JOBS_STREAM_KEY, CONSUMER_GROUP } from './stream';
import { getJob, markJobRunning, markJobSucceeded, markJobFailed } from '../jobs/job.repository';
import { getHandler } from '../jobs/handlers';
import { computeBackoffMs } from './backoff';

export interface ProcessedJob {
  entryId: string;
  jobId: string;
  outcome: 'succeeded' | 'failed' | 'deferred';
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
 * Processes one already-delivered stream entry, regardless of whether it
 * arrived via a fresh XREADGROUP read or was reclaimed from another
 * consumer's PEL by reclaimStaleEntries(). Redis Streams makes no protocol-
 * level distinction between "this consumer crashed" and "this entry is
 * un-acked for any other reason" — both just look like PEL entries idle
 * past some threshold — so both paths get identical treatment here: same
 * handler dispatch, same ack policy.
 *
 * Ack policy: only a successfully processed job is XACK'd. A job that
 * fails (unknown type, handler throws, or its Postgres row has vanished)
 * is deliberately left un-acked, to be picked up again by the next stale-
 * entry sweep — but see the backoff check below for what happens then.
 *
 * Retry backoff (Milestone 7): reclaimStaleEntries()'s XAUTOCLAIM sweep
 * still claims any PEL entry idle past WORKER_STALE_IDLE_MS, unchanged
 * from Milestone 6 — that mechanism doesn't need to know anything about
 * per-job backoff. Instead, Postgres's nextRetryAt is the actual retry
 * clock: if it's still in the future, processEntry declines to re-run the
 * handler at all and leaves the entry un-acked again, to be claimed (and
 * declined again) on however many subsequent sweeps it takes until the
 * deadline passes. That means claiming a job deep in backoff is cheap
 * busywork for a while — a deliberate tradeoff for keeping Postgres as
 * the single source of truth for "is this job allowed to run yet" rather
 * than teaching the Redis-side claim logic about per-job backoff state.
 */
async function processEntry(
  prisma: PrismaClient,
  redis: Redis,
  logger: Logger,
  entryId: string,
  fields: string[],
): Promise<ProcessedJob> {
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

  // Checked separately from (and before) the backoff-window check below:
  // once attempts reaches maxAttempts, nextRetryAt is set to null (see the
  // catch block), which would otherwise fall through this guard entirely
  // and let an exhausted job keep re-running its handler — and its
  // attempts counter keep climbing past maxAttempts — forever, on every
  // future sweep. Exhaustion must be a permanent stop, not just "no
  // backoff scheduled."
  if (job.attempts >= job.maxAttempts) {
    logger.error(
      { jobId, entryId, attempts: job.attempts, maxAttempts: job.maxAttempts },
      'Job has exhausted its retry budget; leaving un-acked and giving up',
    );
    return { entryId, jobId, outcome: 'deferred' };
  }

  if (job.nextRetryAt && job.nextRetryAt.getTime() > Date.now()) {
    logger.info({ jobId, entryId, nextRetryAt: job.nextRetryAt }, 'Job is still within its backoff window; deferring');
    return { entryId, jobId, outcome: 'deferred' };
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
    const attempts = job.attempts + 1;
    const exhausted = attempts >= job.maxAttempts;
    const nextRetryAt = exhausted
      ? null
      : new Date(Date.now() + computeBackoffMs(attempts, env.WORKER_RETRY_BASE_MS, env.WORKER_RETRY_MAX_MS));
    await markJobFailed(prisma, jobId, { error: message, attempts, nextRetryAt });
    logger.error(
      { jobId, entryId, type: job.type, err, attempts, maxAttempts: job.maxAttempts, nextRetryAt },
      exhausted ? 'Job failed; max attempts exhausted, giving up' : 'Job failed; will retry after backoff',
    );
    return { entryId, jobId, outcome: 'failed' };
  }
}

/**
 * Reads and processes (at most) one job from the stream, as a single
 * consumer in CONSUMER_GROUP. Returns null if BLOCK timed out with nothing
 * new to read.
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
  return processEntry(prisma, redis, logger, entry.entryId, entry.fields);
}

/**
 * ioredis types XAUTOCLAIM's return as `unknown[]`, same gap as
 * XREADGROUP. At runtime it's [nextCursor, claimedEntries, deletedIds] —
 * claimedEntries has the identical [entryId, fields][] shape XREADGROUP
 * returns, since a claimed entry IS a normal stream entry, just under new
 * ownership. deletedIds (entries claimed but whose underlying message was
 * since XDEL'd off the stream) is ignored — Redis already dropped those
 * from the PEL on our behalf, nothing left to process.
 */
function parseClaimedEntries(response: unknown): { entryId: string; fields: string[] }[] {
  const [, claimed] = response as [string, [string, string[]][], string[]];
  return claimed.map(([entryId, fields]) => ({ entryId, fields }));
}

/**
 * Sweeps CONSUMER_GROUP's PEL for entries idle at least idleMs — meaning
 * whatever consumer originally read them hasn't acked (or crashed instead
 * of acking) in at least that long — and reassigns them to consumerName
 * via XAUTOCLAIM, the atomic single-command replacement for the classic
 * "XPENDING to find stale entries, then XCLAIM each one" two-step (which
 * has a race: another consumer could claim the same entry between your
 * XPENDING read and your XCLAIM call).
 *
 * Always starts scanning from cursor '0' rather than persisting a cursor
 * across calls — if there are more stale entries than `count`, they're
 * simply picked up on the next sweep (idle time only grows), which avoids
 * needing any cursor state at all for a sweep that already runs every few
 * seconds.
 */
export async function reclaimStaleEntries(
  prisma: PrismaClient,
  redis: Redis,
  logger: Logger,
  consumerName: string,
  idleMs: number,
  count = 10,
): Promise<ProcessedJob[]> {
  const response = await redis.xautoclaim(
    JOBS_STREAM_KEY,
    CONSUMER_GROUP,
    consumerName,
    idleMs,
    '0',
    'COUNT',
    count,
  );

  const entries = parseClaimedEntries(response);
  if (entries.length === 0) {
    return [];
  }

  logger.info({ count: entries.length, consumerName }, 'Reclaimed stale PEL entries');

  const results: ProcessedJob[] = [];
  for (const { entryId, fields } of entries) {
    results.push(await processEntry(prisma, redis, logger, entryId, fields));
  }
  return results;
}
