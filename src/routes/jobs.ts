import { FastifyInstance } from 'fastify';
import type Redis from 'ioredis';
import { PrismaClient } from '../generated/prisma/client';
import { createJobSchema } from '../jobs/job.schema';
import { createJob, deleteJob } from '../jobs/job.repository';
import { enqueueJob } from '../queue/stream';

export async function jobRoutes(
  app: FastifyInstance,
  opts: { prisma: PrismaClient; redis: Redis },
): Promise<void> {
  app.post('/jobs', async (req, reply) => {
    const body = createJobSchema.parse(req.body);
    const job = await createJob(opts.prisma, body);

    try {
      await enqueueJob(opts.redis, job.id);
    } catch (err) {
      // The Postgres row and the Redis stream aren't written atomically —
      // if the stream write fails, a PENDING row with nothing to ever
      // dispatch it is worse than no row at all, so we undo the insert.
      // This isn't true cross-system atomicity (the delete itself could in
      // principle fail too), but it closes the common-case gap.
      req.log.error({ err, jobId: job.id }, 'Failed to enqueue job onto Redis stream; rolling back insert');
      await deleteJob(opts.prisma, job.id);
      return reply.code(500).send({ error: 'Failed to enqueue job' });
    }

    reply.code(201).send(job);
  });
}
