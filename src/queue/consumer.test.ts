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
    // Milestone 7: first failure sets attempts=1 and schedules a future
    // retry (well under the default maxAttempts=5, so not exhausted yet).
    expect(row?.attempts).toBe(1);
    expect(row?.nextRetryAt).not.toBeNull();
    expect(row?.nextRetryAt?.getTime()).toBeGreaterThan(Date.now());

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

  it('defers a job whose nextRetryAt is still in the future, without touching its status', async () => {
    const job = await createJob(prisma, { type: 'no-such-handler', payload: {} });
    createdJobIds.push(job.id);
    // Simulate a job that already failed once and is mid-backoff.
    const future = new Date(Date.now() + 60_000);
    await prisma.job.update({
      where: { id: job.id },
      data: { status: 'FAILED', attempts: 1, nextRetryAt: future },
    });
    await enqueueJob(redis, job.id);

    const result = await processNextJob(prisma, redis, logger, consumerName);

    expect(result?.outcome).toBe('deferred');

    // Declining to run the handler must not touch status/attempts at all.
    const row = await prisma.job.findUnique({ where: { id: job.id } });
    expect(row?.status).toBe('FAILED');
    expect(row?.attempts).toBe(1);
    expect(row?.nextRetryAt?.getTime()).toBe(future.getTime());

    // Deferred entries are left un-acked too, same as a real failure —
    // they'll be claimed (and declined) again until nextRetryAt passes.
    expect(await isStillPending(redis, result?.entryId ?? '')).toBe(true);

    if (result) {
      await redis.xack(JOBS_STREAM_KEY, CONSUMER_GROUP, result.entryId);
      await redis.xdel(JOBS_STREAM_KEY, result.entryId);
    }
  });

  it('stops scheduling retries once attempts reaches maxAttempts, and stays stopped on a later attempt', async () => {
    const job = await createJob(prisma, { type: 'no-such-handler', payload: {} });
    createdJobIds.push(job.id);
    // One failure away from the default maxAttempts=5.
    await prisma.job.update({ where: { id: job.id }, data: { attempts: 4 } });
    await enqueueJob(redis, job.id);

    const result = await processNextJob(prisma, redis, logger, consumerName);

    expect(result?.outcome).toBe('failed');

    const row = await prisma.job.findUnique({ where: { id: job.id } });
    expect(row?.status).toBe('FAILED');
    expect(row?.attempts).toBe(5);
    expect(row?.maxAttempts).toBe(5);
    // Exhausted: no further retry is scheduled.
    expect(row?.nextRetryAt).toBeNull();

    // The exhausting failure alone isn't proof the job is *permanently*
    // stopped — nextRetryAt being null could otherwise be misread by a
    // later check as "no backoff, free to run now." The entry is still
    // un-acked (in this consumer's own PEL) from the call above; simulate
    // the next sweep reclaiming it and confirm it declines to re-run the
    // handler at all: attempts must not climb past 5.
    const reclaimed = await reclaimStaleEntries(prisma, redis, logger, consumerName, 0);
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0]?.outcome).toBe('deferred');

    const rowAfter = await prisma.job.findUnique({ where: { id: job.id } });
    expect(rowAfter?.attempts).toBe(5);

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
