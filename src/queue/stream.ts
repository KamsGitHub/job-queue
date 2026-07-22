import type Redis from 'ioredis';
import { JobPriority } from '../generated/prisma/client';

/**
 * Postgres is the source of truth for job state; these streams exist purely
 * to notify workers that a job is ready to be picked up. Entries carry only
 * the job ID, not `type`/`payload` — duplicating job data into the stream
 * would create a second copy that could drift from the Postgres row, for a
 * write the worker (Milestone 5+) can do itself with one extra read.
 *
 * Milestone 10: one dedicated stream per priority tier, not one shared
 * stream. Redis Streams only ever deliver in append order — there's no way
 * to reorder entries already on a single stream by priority — so giving
 * each tier its own stream is what makes "check the high-priority stream
 * before the low-priority one" possible at all. Each stream keeps its own
 * consumer group/PEL, which means every mechanism built for the single
 * stream in Milestones 5-9 (ack, XAUTOCLAIM reclaim, retry backoff,
 * dead-letter) applies unchanged per tier — nothing about those needed to
 * become priority-aware, only the dispatch loop that picks which stream to
 * read from next.
 */
export const JOBS_STREAM_KEYS: Record<JobPriority, string> = {
  [JobPriority.HIGH]: 'jobs:stream:high',
  [JobPriority.NORMAL]: 'jobs:stream:normal',
  [JobPriority.LOW]: 'jobs:stream:low',
};

/**
 * Highest to lowest. This is the order processNextJob checks streams in
 * (strict priority — a HIGH job always jumps ahead of a queued NORMAL/LOW
 * one) and the order reclaimStaleEntries sweeps them in. No anti-starvation
 * scheme (e.g. weighted round-robin) exists — a sustained flood of HIGH
 * jobs can delay LOW ones indefinitely. Named as this milestone's scope
 * boundary rather than solved speculatively; revisit if it's ever a real
 * problem.
 */
export const JOB_PRIORITY_ORDER: readonly JobPriority[] = [JobPriority.HIGH, JobPriority.NORMAL, JobPriority.LOW];

/**
 * A single consumer group name shared across all three streams (and across
 * multiple worker processes within each stream) — Redis load-balances each
 * stream's entries across whichever consumers have joined its group,
 * delivering each entry to exactly one consumer rather than broadcasting it
 * the way a plain XREAD would.
 */
export const CONSUMER_GROUP = 'workers';

async function ensureConsumerGroup(redis: Redis, streamKey: string): Promise<void> {
  try {
    // Start from '0', not '$': a worker starting up must still see every
    // job already sitting on the stream, not only ones added after the
    // group is created — skipping pre-existing jobs would be a silent
    // correctness bug for a job queue. MKSTREAM guards against the (edge
    // case) where a worker starts before any job has ever been submitted
    // to this tier and the stream doesn't exist yet.
    await redis.xgroup('CREATE', streamKey, CONSUMER_GROUP, '0', 'MKSTREAM');
  } catch (err) {
    // XGROUP CREATE is not naturally idempotent — recreating an existing
    // group is an error, not a no-op — so every worker startup calling
    // this needs to tolerate "already exists" specifically.
    const alreadyExists = err instanceof Error && err.message.includes('BUSYGROUP');
    if (!alreadyExists) {
      throw err;
    }
  }
}

export async function ensureConsumerGroups(redis: Redis): Promise<void> {
  for (const streamKey of Object.values(JOBS_STREAM_KEYS)) {
    await ensureConsumerGroup(redis, streamKey);
  }
}

export async function enqueueJob(redis: Redis, jobId: string, priority: JobPriority): Promise<string> {
  // ioredis types XADD's return as `string | null` because the `NOMKSTREAM`
  // option can return null when the stream doesn't exist yet — we never
  // pass that option, so a real entry ID is always returned here.
  const entryId = await redis.xadd(JOBS_STREAM_KEYS[priority], '*', 'jobId', jobId);
  if (entryId === null) {
    throw new Error('XADD returned null unexpectedly');
  }
  return entryId;
}

/**
 * Milestone 8: a separate append-only log for jobs that permanently
 * exhausted their retry budget — operational visibility/tooling can XREAD
 * this directly without a Postgres round-trip. Unlike jobs:stream, it's
 * safe to carry more than just the job ID here: a dead-lettered job is
 * final (no further Postgres writes will ever touch it in this
 * milestone — there's no requeue mechanism yet), so there's no "two
 * copies that could drift" risk the way there is for jobs:stream's still-
 * mutating rows.
 */
export const DEAD_LETTER_STREAM_KEY = 'jobs:dead-letter';

export async function sendToDeadLetter(
  redis: Redis,
  data: { jobId: string; type: string; error: string; attempts: number },
): Promise<string> {
  const entryId = await redis.xadd(
    DEAD_LETTER_STREAM_KEY,
    '*',
    'jobId',
    data.jobId,
    'type',
    data.type,
    'error',
    data.error,
    'attempts',
    String(data.attempts),
  );
  if (entryId === null) {
    throw new Error('XADD to dead-letter stream returned null unexpectedly');
  }
  return entryId;
}
