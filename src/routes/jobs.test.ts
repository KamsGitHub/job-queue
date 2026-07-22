import { buildApp } from '../app';
import { createPrismaClient } from '../db/client';
import { createRedisClient } from '../queue/redis';
import { JOBS_STREAM_KEYS } from '../queue/stream';
import { SCHEDULED_JOBS_KEY } from '../queue/schedule';

describe('POST /jobs', () => {
  // Shared across tests in this file, not per-test: buildApp() only closes
  // clients it created itself (see app.ts), so it's this file's job to
  // dispose of them once, after every test that uses them has finished.
  const prisma = createPrismaClient();
  const redis = createRedisClient();
  const createdJobIds: string[] = [];

  afterAll(async () => {
    if (createdJobIds.length > 0) {
      await prisma.job.deleteMany({ where: { id: { in: createdJobIds } } });
    }
    await prisma.$disconnect();
    await redis.quit();
  });

  it('creates a job, persists it, and enqueues it onto the Redis stream', async () => {
    const app = buildApp({ prisma, redis });

    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { type: 'send-email', payload: { to: 'test@example.com' } },
    });

    expect(response.statusCode).toBe(201);
    const body = response.json();
    expect(body.type).toBe('send-email');
    expect(body.payload).toEqual({ to: 'test@example.com' });
    expect(body.status).toBe('PENDING');
    expect(body.id).toEqual(expect.any(String));
    createdJobIds.push(body.id);

    // The response alone doesn't prove it was actually persisted — check
    // the row exists via a fresh read, not just what the handler returned.
    const row = await prisma.job.findUnique({ where: { id: body.id } });
    expect(row).not.toBeNull();
    expect(row?.type).toBe('send-email');

    // Same logic for the stream side: prove the job was actually announced
    // on the stream, not just that the handler didn't throw.
    const entries = await redis.xrevrange(JOBS_STREAM_KEYS.NORMAL, '+', '-', 'COUNT', 20);
    const match = entries.find(([, fields]) => fields[fields.indexOf('jobId') + 1] === body.id);
    expect(match).toBeDefined();
    if (match) {
      await redis.xdel(JOBS_STREAM_KEYS.NORMAL, match[0]);
    }

    await app.close();
  });

  it('returns the existing job for a repeat submission with the same idempotencyKey, without enqueueing again', async () => {
    const app = buildApp({ prisma, redis });
    const idempotencyKey = `idem-${Date.now()}`;

    const first = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { type: 'send-email', payload: { to: 'test@example.com' }, idempotencyKey },
    });
    expect(first.statusCode).toBe(201);
    const firstBody = first.json();
    createdJobIds.push(firstBody.id);

    const second = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { type: 'send-email', payload: { to: 'ignored@example.com' }, idempotencyKey },
    });

    expect(second.statusCode).toBe(200);
    const secondBody = second.json();
    expect(secondBody.id).toBe(firstBody.id);
    // Proves the second request's (different) payload was never persisted —
    // the original row is returned untouched, not updated.
    expect(secondBody.payload).toEqual({ to: 'test@example.com' });

    // Only one row exists for this key.
    const rows = await prisma.job.findMany({ where: { idempotencyKey } });
    expect(rows).toHaveLength(1);

    // Only one stream entry was ever produced — the second request must
    // not have enqueued a duplicate.
    const entries = await redis.xrevrange(JOBS_STREAM_KEYS.NORMAL, '+', '-', 'COUNT', 20);
    const matches = entries.filter(([, fields]) => fields[fields.indexOf('jobId') + 1] === firstBody.id);
    expect(matches).toHaveLength(1);
    if (matches[0]) {
      await redis.xdel(JOBS_STREAM_KEYS.NORMAL, matches[0][0]);
    }

    await app.close();
  });

  it('defaults priority to NORMAL, and honors an explicit priority by enqueueing onto that tier\'s stream', async () => {
    const app = buildApp({ prisma, redis });

    const defaultResponse = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { type: 'send-email', payload: { to: 'default-priority@example.com' } },
    });
    expect(defaultResponse.statusCode).toBe(201);
    const defaultBody = defaultResponse.json();
    expect(defaultBody.priority).toBe('NORMAL');
    createdJobIds.push(defaultBody.id);

    const highResponse = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { type: 'send-email', payload: { to: 'high-priority@example.com' }, priority: 'HIGH' },
    });
    expect(highResponse.statusCode).toBe(201);
    const highBody = highResponse.json();
    expect(highBody.priority).toBe('HIGH');
    createdJobIds.push(highBody.id);

    // Proves it was actually dispatched onto the HIGH stream, not just
    // stamped in Postgres.
    const highEntries = await redis.xrevrange(JOBS_STREAM_KEYS.HIGH, '+', '-', 'COUNT', 20);
    const highMatch = highEntries.find(([, fields]) => fields[fields.indexOf('jobId') + 1] === highBody.id);
    expect(highMatch).toBeDefined();
    if (highMatch) {
      await redis.xdel(JOBS_STREAM_KEYS.HIGH, highMatch[0]);
    }

    const normalEntries = await redis.xrevrange(JOBS_STREAM_KEYS.NORMAL, '+', '-', 'COUNT', 20);
    const normalMatch = normalEntries.find(([, fields]) => fields[fields.indexOf('jobId') + 1] === defaultBody.id);
    expect(normalMatch).toBeDefined();
    if (normalMatch) {
      await redis.xdel(JOBS_STREAM_KEYS.NORMAL, normalMatch[0]);
    }

    await app.close();
  });

  it('holds a job with a future scheduledAt in the scheduled set instead of dispatching it immediately', async () => {
    const app = buildApp({ prisma, redis });
    const scheduledAt = new Date(Date.now() + 60_000).toISOString();

    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { type: 'send-email', payload: { to: 'scheduled@example.com' }, scheduledAt },
    });

    expect(response.statusCode).toBe(201);
    const body = response.json();
    expect(body.status).toBe('PENDING');
    expect(new Date(body.scheduledAt).toISOString()).toBe(scheduledAt);
    createdJobIds.push(body.id);

    // Not dispatched onto any priority stream yet...
    for (const streamKey of Object.values(JOBS_STREAM_KEYS)) {
      const entries = await redis.xrange(streamKey, '-', '+');
      expect(entries.find(([, fields]) => fields[fields.indexOf('jobId') + 1] === body.id)).toBeUndefined();
    }
    // ...but sitting in the scheduled set, due at exactly the requested time.
    const score = await redis.zscore(SCHEDULED_JOBS_KEY, body.id);
    expect(Number(score)).toBe(new Date(scheduledAt).getTime());

    await redis.zrem(SCHEDULED_JOBS_KEY, body.id);
    await app.close();
  });

  it('resolves delaySeconds to an equivalent scheduledAt and holds the job the same way', async () => {
    const app = buildApp({ prisma, redis });
    const before = Date.now();

    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { type: 'send-email', payload: { to: 'delayed@example.com' }, delaySeconds: 60 },
    });

    expect(response.statusCode).toBe(201);
    const body = response.json();
    createdJobIds.push(body.id);

    const scheduledAtMs = new Date(body.scheduledAt).getTime();
    expect(scheduledAtMs).toBeGreaterThanOrEqual(before + 60_000);
    expect(scheduledAtMs).toBeLessThan(before + 65_000);

    expect(await redis.zscore(SCHEDULED_JOBS_KEY, body.id)).not.toBeNull();

    await redis.zrem(SCHEDULED_JOBS_KEY, body.id);
    await app.close();
  });

  it('rejects a request providing both scheduledAt and delaySeconds', async () => {
    const app = buildApp({ prisma, redis });

    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: {
        type: 'send-email',
        payload: { to: 'ambiguous@example.com' },
        scheduledAt: new Date(Date.now() + 60_000).toISOString(),
        delaySeconds: 60,
      },
    });

    expect(response.statusCode).toBe(400);

    await app.close();
  });

  it('dispatches immediately when scheduledAt is already in the past', async () => {
    const app = buildApp({ prisma, redis });
    const scheduledAt = new Date(Date.now() - 1000).toISOString();

    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { type: 'send-email', payload: { to: 'already-due@example.com' }, scheduledAt },
    });

    expect(response.statusCode).toBe(201);
    const body = response.json();
    createdJobIds.push(body.id);

    const entries = await redis.xrevrange(JOBS_STREAM_KEYS.NORMAL, '+', '-', 'COUNT', 20);
    const match = entries.find(([, fields]) => fields[fields.indexOf('jobId') + 1] === body.id);
    expect(match).toBeDefined();
    expect(await redis.zscore(SCHEDULED_JOBS_KEY, body.id)).toBeNull();

    if (match) {
      await redis.xdel(JOBS_STREAM_KEYS.NORMAL, match[0]);
    }
    await app.close();
  });

  it('rejects a request missing "type"', async () => {
    const app = buildApp({ prisma, redis });

    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { payload: { foo: 'bar' } },
    });

    expect(response.statusCode).toBe(400);
    expect(response.json().error).toBe('Validation failed');

    await app.close();
  });

  it('rejects a non-object payload', async () => {
    const app = buildApp({ prisma, redis });

    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { type: 'send-email', payload: 'not-an-object' },
    });

    expect(response.statusCode).toBe(400);

    await app.close();
  });
});
