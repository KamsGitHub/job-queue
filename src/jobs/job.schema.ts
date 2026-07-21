import { z } from 'zod';

export const createJobSchema = z.object({
  type: z.string().min(1),
  payload: z.record(z.string(), z.unknown()),
  // Milestone 9: optional client-supplied dedup key. A repeat submission
  // with the same key returns the existing job instead of creating a
  // duplicate — see findOrCreateJob in job.repository.ts.
  idempotencyKey: z.string().min(1).optional(),
});

export type CreateJobInput = z.infer<typeof createJobSchema>;
