import { FastifyInstance } from 'fastify';
import { PrismaClient } from '../generated/prisma/client';
import { createJobSchema } from '../jobs/job.schema';
import { createJob } from '../jobs/job.repository';

export async function jobRoutes(app: FastifyInstance, opts: { prisma: PrismaClient }): Promise<void> {
  app.post('/jobs', async (req, reply) => {
    const body = createJobSchema.parse(req.body);
    const job = await createJob(opts.prisma, body);
    reply.code(201).send(job);
  });
}
