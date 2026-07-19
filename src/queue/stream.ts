import type Redis from 'ioredis';

/**
 * Postgres is the source of truth for job state; this stream exists purely
 * to notify workers that a job is ready to be picked up. Entries carry only
 * the job ID, not `type`/`payload` — duplicating job data into the stream
 * would create a second copy that could drift from the Postgres row, for a
 * write the worker (Milestone 5+) can do itself with one extra read.
 */
export const JOBS_STREAM_KEY = 'jobs:stream';

/**
 * A single consumer group for all workers. Multiple worker processes join
 * this same group with distinct consumer names — Redis then load-balances
 * stream entries across them, delivering each entry to exactly one
 * consumer, rather than broadcasting it to every reader the way a plain
 * XREAD would.
 */
export const CONSUMER_GROUP = 'workers';

export async function ensureConsumerGroup(redis: Redis): Promise<void> {
  try {
    // Start from '0', not '$': a worker starting up must still see every
    // job already sitting on the stream, not only ones added after the
    // group is created — skipping pre-existing jobs would be a silent
    // correctness bug for a job queue. MKSTREAM guards against the (edge
    // case) where a worker starts before any job has ever been submitted
    // and the stream doesn't exist yet.
    await redis.xgroup('CREATE', JOBS_STREAM_KEY, CONSUMER_GROUP, '0', 'MKSTREAM');
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

export async function enqueueJob(redis: Redis, jobId: string): Promise<string> {
  // ioredis types XADD's return as `string | null` because the `NOMKSTREAM`
  // option can return null when the stream doesn't exist yet — we never
  // pass that option, so a real entry ID is always returned here.
  const entryId = await redis.xadd(JOBS_STREAM_KEY, '*', 'jobId', jobId);
  if (entryId === null) {
    throw new Error('XADD returned null unexpectedly');
  }
  return entryId;
}
