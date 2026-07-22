import type Redis from 'ioredis';
import type { PrismaClient } from '../generated/prisma/client';
import type { Logger } from '../logger';
import { enqueueJob } from './stream';
import { getJob } from '../jobs/job.repository';

/**
 * Milestone 11: Redis Streams have no "deliver later" primitive — an entry
 * is visible to XREADGROUP the instant it's XADD'd. So a job that isn't due
 * yet can't simply be enqueued onto its priority stream; it's held here, in
 * a sorted set scored by its due time (epoch ms), and only XADD'd onto the
 * real dispatch stream once a sweep (promoteDueJobs) notices it's due. One
 * shared set across all priority tiers — the tier itself is already on the
 * Postgres row and only needs to be known at the moment of promotion.
 */
export const SCHEDULED_JOBS_KEY = 'jobs:scheduled';

export async function scheduleJob(redis: Redis, jobId: string, scheduledAt: Date): Promise<void> {
  await redis.zadd(SCHEDULED_JOBS_KEY, scheduledAt.getTime(), jobId);
}

/**
 * Sweeps the scheduled set for jobs due by `now`, promoting each onto its
 * priority stream via the same enqueueJob() a normal (non-scheduled)
 * submission uses — from that point on it's indistinguishable from any
 * other dispatched job, no special-casing anywhere in processEntry.
 *
 * Claiming order is ZREM-then-XADD, not the reverse, and this is a real
 * tradeoff, not an oversight: this project runs multiple worker processes
 * concurrently as the normal case (see Milestones 5/10), so if XADD came
 * first, every worker's sweep would see the same due jobId on every tick
 * until its own ZREM eventually ran — routinely double-promoting (and
 * double-processing) it, not just in some rare crash window. ZREM-first
 * avoids that: ZREM's return value (1 vs 0) is the atomic claim — only the
 * sweep that actually removed the member proceeds to promote it, so
 * concurrent workers never race to promote the same job twice.
 *
 * The real cost of this ordering: if this process crashes between its
 * ZREM and its XADD, the job is removed from the scheduled set but never
 * reaches a priority stream — silently lost, permanently PENDING in
 * Postgres. Named here rather than solved: it's the same class of gap as
 * the still-open crash window between markJobSucceeded and XACK (see
 * Milestone 6/9's notes) that this project has consistently chosen to
 * document rather than paper over with unneeded machinery for a narrow
 * multi-command crash window.
 */
export async function promoteDueJobs(
  prisma: PrismaClient,
  redis: Redis,
  logger: Logger,
  now: Date = new Date(),
  limit = 100,
): Promise<number> {
  const dueJobIds = await redis.zrangebyscore(SCHEDULED_JOBS_KEY, '-inf', now.getTime(), 'LIMIT', 0, limit);
  let promoted = 0;

  for (const jobId of dueJobIds) {
    const removed = await redis.zrem(SCHEDULED_JOBS_KEY, jobId);
    if (removed === 0) {
      // Already claimed by another worker's concurrent sweep this tick.
      continue;
    }

    const job = await getJob(prisma, jobId);
    if (!job) {
      logger.error({ jobId }, 'Scheduled job no longer exists in Postgres; dropping');
      continue;
    }

    await enqueueJob(redis, job.id, job.priority);
    promoted++;
    logger.info({ jobId, priority: job.priority, scheduledAt: job.scheduledAt }, 'Promoted scheduled job onto its priority stream');
  }

  return promoted;
}
