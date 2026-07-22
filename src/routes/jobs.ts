import { FastifyInstance } from 'fastify';
import type Redis from 'ioredis';
import { PrismaClient } from '../generated/prisma/client';
import { createJobSchema } from '../jobs/job.schema';
import { findOrCreateJob, deleteJob } from '../jobs/job.repository';
import { enqueueJob } from '../queue/stream';
import { scheduleJob } from '../queue/schedule';

export async function jobRoutes(
  app: FastifyInstance,
  opts: { prisma: PrismaClient; redis: Redis },
): Promise<void> {
  app.post('/jobs', async (req, reply) => {
    const body = createJobSchema.parse(req.body);
    const { job, created } = await findOrCreateJob(opts.prisma, body);

    // A repeat submission with the same idempotencyKey: the job is already
    // persisted and was already enqueued the first time it was created, so
    // re-enqueueing here would just duplicate the stream entry. 200, not
    // 201 — nothing new was created by this request.
    if (!created) {
      return reply.code(200).send(job);
    }

    // Milestone 11: a scheduledAt in the future holds the job in the
    // scheduled set instead of dispatching it immediately — the promotion
    // sweep (queue/schedule.ts) enqueues it onto its priority stream once
    // due. A scheduledAt that's already passed (or wasn't given at all)
    // behaves exactly as before this milestone.
    const isScheduledForLater = job.scheduledAt !== null && job.scheduledAt.getTime() > Date.now();

    try {
      if (isScheduledForLater) {
        await scheduleJob(opts.redis, job.id, job.scheduledAt as Date);
      } else {
        await enqueueJob(opts.redis, job.id, job.priority);
      }
    } catch (err) {
      // The Postgres row and the Redis write aren't written atomically —
      // if it fails, a PENDING row with nothing to ever dispatch it is
      // worse than no row at all, so we undo the insert. This isn't true
      // cross-system atomicity (the delete itself could in principle fail
      // too), but it closes the common-case gap.
      req.log.error({ err, jobId: job.id }, 'Failed to enqueue/schedule job; rolling back insert');
      await deleteJob(opts.prisma, job.id);
      return reply.code(500).send({ error: 'Failed to enqueue job' });
    }

    reply.code(201).send(job);
  });
}
