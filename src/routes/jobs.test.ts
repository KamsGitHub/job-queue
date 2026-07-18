import { buildApp } from '../app';
import { createPrismaClient } from '../db/client';
import { createRedisClient } from '../queue/redis';
import { JOBS_STREAM_KEY } from '../queue/stream';

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
    const entries = await redis.xrevrange(JOBS_STREAM_KEY, '+', '-', 'COUNT', 20);
    const match = entries.find(([, fields]) => fields[fields.indexOf('jobId') + 1] === body.id);
    expect(match).toBeDefined();
    if (match) {
      await redis.xdel(JOBS_STREAM_KEY, match[0]);
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
