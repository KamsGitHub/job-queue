import { buildApp } from '../app';
import { createPrismaClient } from '../db/client';

describe('POST /jobs', () => {
  // Shared across tests in this file, not per-test: buildApp() only closes
  // a Prisma client it created itself (see app.ts), so it's this file's job
  // to disconnect it once, after every test that uses it has finished.
  const prisma = createPrismaClient();
  const createdJobIds: string[] = [];

  afterAll(async () => {
    if (createdJobIds.length > 0) {
      await prisma.job.deleteMany({ where: { id: { in: createdJobIds } } });
    }
    await prisma.$disconnect();
  });

  it('creates a job and persists it', async () => {
    const app = buildApp({ prisma });

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

    await app.close();
  });

  it('rejects a request missing "type"', async () => {
    const app = buildApp({ prisma });

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
    const app = buildApp({ prisma });

    const response = await app.inject({
      method: 'POST',
      url: '/jobs',
      payload: { type: 'send-email', payload: 'not-an-object' },
    });

    expect(response.statusCode).toBe(400);

    await app.close();
  });
});
