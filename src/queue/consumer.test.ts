import { createPrismaClient } from '../db/client';
import { createRedisClient } from '../queue/redis';
import { createLogger } from '../logger';
import { createJob } from '../jobs/job.repository';
import { enqueueJob, ensureConsumerGroup, JOBS_STREAM_KEY, CONSUMER_GROUP, DEAD_LETTER_STREAM_KEY } from './stream';
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

  it('moves a job to DEAD_LETTERED and acks its entry once attempts reaches maxAttempts, recording it on the dead-letter stream', async () => {
    const job = await createJob(prisma, { type: 'no-such-handler', payload: {} });
    createdJobIds.push(job.id);
    // One failure away from the default maxAttempts=5.
    await prisma.job.update({ where: { id: job.id }, data: { attempts: 4 } });
    await enqueueJob(redis, job.id);

    const result = await processNextJob(prisma, redis, logger, consumerName);

    expect(result?.outcome).toBe('dead-lettered');

    const row = await prisma.job.findUnique({ where: { id: job.id } });
    expect(row?.status).toBe('DEAD_LETTERED');
    expect(row?.attempts).toBe(5);
    expect(row?.maxAttempts).toBe(5);
    expect(row?.nextRetryAt).toBeNull();

    // Unlike a plain (retryable) failure, exhaustion acks the entry —
    // it must never be reclaimed and reprocessed again.
    expect(await isStillPending(redis, result?.entryId ?? '')).toBe(false);

    // Recorded on the separate dead-letter stream, with enough context to
    // inspect it without a Postgres round-trip.
    const dlqEntries = await redis.xrange(DEAD_LETTER_STREAM_KEY, '-', '+');
    const match = dlqEntries.find(([, fields]) => fields[fields.indexOf('jobId') + 1] === job.id);
    expect(match).toBeDefined();
    expect(match?.[1][match[1].indexOf('type') + 1]).toBe('no-such-handler');

    if (match) {
      await redis.xdel(DEAD_LETTER_STREAM_KEY, match[0]);
    }
    if (result) {
      await redis.xdel(JOBS_STREAM_KEY, result.entryId);
    }
  });

  it('is idempotent when reclaiming a stray already-DEAD_LETTERED job: acks without duplicating the dead-letter entry', async () => {
    const job = await createJob(prisma, { type: 'no-such-handler', payload: {} });
    createdJobIds.push(job.id);
    // Simulate a job already dead-lettered by a previous run, whose entry
    // is somehow still un-acked (e.g. a stray left over from before this
    // logic existed).
    await prisma.job.update({
      where: { id: job.id },
      data: { status: 'DEAD_LETTERED', attempts: 5, nextRetryAt: null, error: 'already dead-lettered' },
    });
    await enqueueJob(redis, job.id);
    await redis.xreadgroup('GROUP', CONSUMER_GROUP, 'dead-consumer', 'COUNT', 1, 'STREAMS', JOBS_STREAM_KEY, '>');

    const results = await reclaimStaleEntries(prisma, redis, logger, consumerName, 0);

    expect(results).toHaveLength(1);
    const [result] = results;
    expect(result?.outcome).toBe('dead-lettered');

    // Not re-written: attempts/error stay exactly what they already were.
    const row = await prisma.job.findUnique({ where: { id: job.id } });
    expect(row?.attempts).toBe(5);
    expect(row?.error).toBe('already dead-lettered');

    expect(await isStillPending(redis, result?.entryId ?? '')).toBe(false);

    // No duplicate dead-letter entry was created for this job.
    const dlqEntries = await redis.xrange(DEAD_LETTER_STREAM_KEY, '-', '+');
    const matches = dlqEntries.filter(([, fields]) => fields[fields.indexOf('jobId') + 1] === job.id);
    expect(matches).toHaveLength(0);

    if (result) {
      await redis.xdel(JOBS_STREAM_KEY, result.entryId);
    }
  });

  it('is idempotent when reclaiming a stale entry for a job already SUCCEEDED: acks without re-running the handler', async () => {
    const job = await createJob(prisma, { type: 'send-email', payload: { to: 'already-done@example.com' } });
    createdJobIds.push(job.id);
    // Simulate the Milestone 6-named gap directly: the handler already ran
    // and Postgres already recorded success, but the entry is still
    // sitting un-acked (as if the process crashed between markJobSucceeded
    // and XACK).
    await prisma.job.update({ where: { id: job.id }, data: { status: 'SUCCEEDED', finishedAt: new Date() } });
    await enqueueJob(redis, job.id);
    await redis.xreadgroup('GROUP', CONSUMER_GROUP, 'dead-consumer', 'COUNT', 1, 'STREAMS', JOBS_STREAM_KEY, '>');

    const results = await reclaimStaleEntries(prisma, redis, logger, consumerName, 0);

    expect(results).toHaveLength(1);
    const [result] = results;
    expect(result?.outcome).toBe('succeeded');

    // Untouched — no second run recorded a new finishedAt/startedAt.
    const row = await prisma.job.findUnique({ where: { id: job.id } });
    expect(row?.status).toBe('SUCCEEDED');
    expect(row?.startedAt).toBeNull();

    // Acked, so it will never be reclaimed and reprocessed again.
    expect(await isStillPending(redis, result?.entryId ?? '')).toBe(false);

    if (result) {
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
