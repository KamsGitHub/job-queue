import { createPrismaClient } from '../db/client';
import { createRedisClient } from '../queue/redis';
import { createLogger } from '../logger';
import { createJob } from '../jobs/job.repository';
import { enqueueJob, ensureConsumerGroup, JOBS_STREAM_KEY, CONSUMER_GROUP } from './stream';
import { processNextJob, reclaimStaleEntries } from './consumer';

// ioredis types XPENDING's extended-form return as `unknown[]`; at runtime
// each entry is [entryId, consumer, idleTimeMs, deliveryCount].
type PendingEntry = [id: string, consumer: string, idleTimeMs: number, deliveryCount: number];

async function isStillPending(
  redis: ReturnType<typeof createRedisClient>,
  entryId: string,
): Promise<boolean> {
  const pending = (await redis.xpending(JOBS_STREAM_KEY, CONSUMER_GROUP, '-', '+', 10)) as PendingEntry[];
  return pending.some(([id]) => id === entryId);
}

describe('processNextJob', () => {
  const prisma = createPrismaClient();
  const redis = createRedisClient();
  const logger = createLogger();
  const consumerName = 'test-consumer';
  const createdJobIds: string[] = [];

  beforeAll(async () => {
    // This is a local dev/test-only Redis instance, not shared production
    // state — wipe any pre-existing entries (e.g. leftover manual
    // verification from earlier milestones) so tests can assume "the next
    // entry read is the one this test just enqueued" without racing
    // whatever else happens to be sitting on the stream. Deleting the
    // stream key also destroys its consumer group, so recreate it after.
    await redis.del(JOBS_STREAM_KEY);
    await ensureConsumerGroup(redis);
  });

  afterAll(async () => {
    if (createdJobIds.length > 0) {
      await prisma.job.deleteMany({ where: { id: { in: createdJobIds } } });
    }
    await prisma.$disconnect();
    await redis.quit();
  });

  it('processes a job with a registered handler: marks it SUCCEEDED and acks the stream entry', async () => {
    const job = await createJob(prisma, { type: 'send-email', payload: { to: 'test@example.com' } });
    createdJobIds.push(job.id);
    await enqueueJob(redis, job.id);

    const result = await processNextJob(prisma, redis, logger, consumerName);

    expect(result).not.toBeNull();
    expect(result?.jobId).toBe(job.id);
    expect(result?.outcome).toBe('succeeded');

    const row = await prisma.job.findUnique({ where: { id: job.id } });
    expect(row?.status).toBe('SUCCEEDED');
    expect(row?.finishedAt).not.toBeNull();

    // Acked entries leave the consumer's Pending Entries List entirely.
    expect(await isStillPending(redis, result?.entryId ?? '')).toBe(false);

    if (result) {
      await redis.xdel(JOBS_STREAM_KEY, result.entryId);
    }
  });

  it('processes a job with no registered handler: marks it FAILED and leaves it un-acked', async () => {
    const job = await createJob(prisma, { type: 'no-such-handler', payload: { foo: 'bar' } });
    createdJobIds.push(job.id);
    await enqueueJob(redis, job.id);

    const result = await processNextJob(prisma, redis, logger, consumerName);

    expect(result).not.toBeNull();
    expect(result?.jobId).toBe(job.id);
    expect(result?.outcome).toBe('failed');

    const row = await prisma.job.findUnique({ where: { id: job.id } });
    expect(row?.status).toBe('FAILED');
    expect(row?.error).toContain('no-such-handler');

    // Left un-acked on purpose (Milestone 6 owns retry/crash recovery) —
    // still present in the PEL.
    expect(await isStillPending(redis, result?.entryId ?? '')).toBe(true);

    // Clean up the PEL entry ourselves since this milestone doesn't have
    // any mechanism yet that would otherwise ever ack it.
    if (result) {
      await redis.xack(JOBS_STREAM_KEY, CONSUMER_GROUP, result.entryId);
      await redis.xdel(JOBS_STREAM_KEY, result.entryId);
    }
  });

  it('reclaimStaleEntries claims an entry abandoned by a dead consumer and reprocesses it: succeeds and acks', async () => {
    const job = await createJob(prisma, { type: 'send-email', payload: { to: 'stale@example.com' } });
    createdJobIds.push(job.id);
    await enqueueJob(redis, job.id);

    // Simulate a consumer that read the entry (so it's in the group's PEL,
    // owned by that consumer) then crashed before processing or acking it —
    // never call processEntry for it under this identity.
    await redis.xreadgroup('GROUP', CONSUMER_GROUP, 'dead-consumer', 'COUNT', 1, 'STREAMS', JOBS_STREAM_KEY, '>');

    // idleMs=0: claim immediately regardless of real elapsed time, so the
    // test doesn't need to sleep past a real staleness threshold.
    const results = await reclaimStaleEntries(prisma, redis, logger, consumerName, 0);

    expect(results).toHaveLength(1);
    const [result] = results;
    expect(result?.jobId).toBe(job.id);
    expect(result?.outcome).toBe('succeeded');

    const row = await prisma.job.findUnique({ where: { id: job.id } });
    expect(row?.status).toBe('SUCCEEDED');

    expect(await isStillPending(redis, result?.entryId ?? '')).toBe(false);

    if (result) {
      await redis.xdel(JOBS_STREAM_KEY, result.entryId);
    }
  });

  it('reclaimStaleEntries claims a stale entry, reprocesses it, and transfers PEL ownership on failure', async () => {
    const job = await createJob(prisma, { type: 'no-such-handler', payload: { foo: 'bar' } });
    createdJobIds.push(job.id);
    await enqueueJob(redis, job.id);

    await redis.xreadgroup('GROUP', CONSUMER_GROUP, 'dead-consumer', 'COUNT', 1, 'STREAMS', JOBS_STREAM_KEY, '>');

    const results = await reclaimStaleEntries(prisma, redis, logger, consumerName, 0);

    expect(results).toHaveLength(1);
    const [result] = results;
    expect(result?.jobId).toBe(job.id);
    expect(result?.outcome).toBe('failed');

    const row = await prisma.job.findUnique({ where: { id: job.id } });
    expect(row?.status).toBe('FAILED');

    // Still un-acked, but ownership moved from 'dead-consumer' to this
    // worker's own consumerName — proves XAUTOCLAIM actually reassigned it,
    // not just left it where it was.
    const pending = (await redis.xpending(JOBS_STREAM_KEY, CONSUMER_GROUP, '-', '+', 10)) as PendingEntry[];
    const owner = pending.find(([id]) => id === result?.entryId)?.[1];
    expect(owner).toBe(consumerName);

    if (result) {
      await redis.xack(JOBS_STREAM_KEY, CONSUMER_GROUP, result.entryId);
      await redis.xdel(JOBS_STREAM_KEY, result.entryId);
    }
  });
});
