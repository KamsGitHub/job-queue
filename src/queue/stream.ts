import type Redis from 'ioredis';

/**
 * Postgres is the source of truth for job state; this stream exists purely
 * to notify workers that a job is ready to be picked up. Entries carry only
 * the job ID, not `type`/`payload` — duplicating job data into the stream
 * would create a second copy that could drift from the Postgres row, for a
 * write the worker (Milestone 5+) can do itself with one extra read.
 */
export const JOBS_STREAM_KEY = 'jobs:stream';

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
