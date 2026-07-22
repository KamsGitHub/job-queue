import { createPrismaClient } from '../db/client';
import { createRedisClient } from '../queue/redis';
import { createLogger } from '../logger';
import { createJob, deleteJob } from '../jobs/job.repository';
import { ensureConsumerGroups, JOBS_STREAM_KEYS, CONSUMER_GROUP } from './stream';
import { processNextJob } from './consumer';
import { scheduleJob, promoteDueJobs, SCHEDULED_JOBS_KEY } from './schedule';

describe('scheduleJob / promoteDueJobs', () => {
  const prisma = createPrismaClient();
  const redis = createRedisClient();
  const logger = createLogger();
  const consumerName = 'test-consumer';
  const createdJobIds: string[] = [];

  beforeAll(async () => {
    // Same rationale as consumer.test.ts: this is local dev/test-only
    // Redis, not shared production state — start from a clean scheduled
    // set and clean per-tier streams so tests can assume nothing stray is
    // already due/present.
    await redis.del(SCHEDULED_JOBS_KEY);
    for (const streamKey of Object.values(JOBS_STREAM_KEYS)) {
      await redis.del(streamKey);
    }
    await ensureConsumerGroups(redis);
  });

  afterAll(async () => {
    if (createdJobIds.length > 0) {
      await prisma.job.deleteMany({ where: { id: { in: createdJobIds } } });
    }
    await prisma.$disconnect();
    await redis.quit();
  });

  it('scheduleJob holds the job in the scheduled set without dispatching it onto any priority stream', async () => {
    const job = await createJob(prisma, { type: 'send-email', payload: { to: 'later@example.com' }, priority: 'NORMAL' });
    createdJobIds.push(job.id);

    const future = new Date(Date.now() + 60_000);
    await scheduleJob(redis, job.id, future);

    const score = await redis.zscore(SCHEDULED_JOBS_KEY, job.id);
    expect(Number(score)).toBe(future.getTime());

    for (const streamKey of Object.values(JOBS_STREAM_KEYS)) {
      const entries = await redis.xrange(streamKey, '-', '+');
      const match = entries.find(([, fields]) => fields[fields.indexOf('jobId') + 1] === job.id);
      expect(match).toBeUndefined();
    }

    await redis.zrem(SCHEDULED_JOBS_KEY, job.id);
  });

  it('promoteDueJobs leaves a job scheduled in the future untouched', async () => {
    const job = await createJob(prisma, { type: 'send-email', payload: { to: 'not-yet@example.com' }, priority: 'NORMAL' });
    createdJobIds.push(job.id);

    const future = new Date(Date.now() + 60_000);
    await scheduleJob(redis, job.id, future);

    const promoted = await promoteDueJobs(prisma, redis, logger);

    expect(promoted).toBe(0);
    expect(await redis.zscore(SCHEDULED_JOBS_KEY, job.id)).not.toBeNull();

    await redis.zrem(SCHEDULED_JOBS_KEY, job.id);
  });

  it('promoteDueJobs promotes a due job onto its priority stream, and it then processes normally', async () => {
    const job = await createJob(prisma, { type: 'send-email', payload: { to: 'due-now@example.com' }, priority: 'HIGH' });
    createdJobIds.push(job.id);

    // Already in the past — due immediately.
    await scheduleJob(redis, job.id, new Date(Date.now() - 1000));

    const promoted = await promoteDueJobs(prisma, redis, logger);
    expect(promoted).toBe(1);

    // Removed from the scheduled set...
    expect(await redis.zscore(SCHEDULED_JOBS_KEY, job.id)).toBeNull();

    // ...and now sitting on its priority stream, same as an immediately
    // dispatched job would be.
    const entries = await redis.xrange(JOBS_STREAM_KEYS.HIGH, '-', '+');
    const match = entries.find(([, fields]) => fields[fields.indexOf('jobId') + 1] === job.id);
    expect(match).toBeDefined();

    // From here it's indistinguishable from any other dispatched job.
    const result = await processNextJob(prisma, redis, logger, consumerName);
    expect(result?.jobId).toBe(job.id);
    expect(result?.outcome).toBe('succeeded');

    if (result) {
      await redis.xdel(JOBS_STREAM_KEYS.HIGH, result.entryId);
    }
  });

  it('is idempotent against a concurrent claim: a second promoteDueJobs call does not re-promote the same job', async () => {
    const job = await createJob(prisma, { type: 'send-email', payload: { to: 'race@example.com' }, priority: 'NORMAL' });
    createdJobIds.push(job.id);
    await scheduleJob(redis, job.id, new Date(Date.now() - 1000));

    // Simulates two workers' sweeps racing the same tick: the first call's
    // ZREM wins the claim, so a second call finds nothing left to promote.
    const [first, second] = await Promise.all([promoteDueJobs(prisma, redis, logger), promoteDueJobs(prisma, redis, logger)]);

    expect(first + second).toBe(1);

    const entries = await redis.xrange(JOBS_STREAM_KEYS.NORMAL, '-', '+');
    const matches = entries.filter(([, fields]) => fields[fields.indexOf('jobId') + 1] === job.id);
    expect(matches).toHaveLength(1);

    if (matches[0]) {
      await redis.xack(JOBS_STREAM_KEYS.NORMAL, CONSUMER_GROUP, matches[0][0]);
      await redis.xdel(JOBS_STREAM_KEYS.NORMAL, matches[0][0]);
    }
  });

  it('drops a due entry gracefully when its Postgres row no longer exists', async () => {
    const job = await createJob(prisma, { type: 'send-email', payload: { to: 'vanished@example.com' }, priority: 'NORMAL' });
    await scheduleJob(redis, job.id, new Date(Date.now() - 1000));
    await deleteJob(prisma, job.id);

    await expect(promoteDueJobs(prisma, redis, logger)).resolves.toBe(0);
    expect(await redis.zscore(SCHEDULED_JOBS_KEY, job.id)).toBeNull();
  });
});
