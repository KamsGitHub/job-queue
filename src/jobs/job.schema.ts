import { z } from 'zod';

export const createJobSchema = z.object({
  type: z.string().min(1),
  payload: z.record(z.string(), z.unknown()),
  // Milestone 9: optional client-supplied dedup key. A repeat submission
  // with the same key returns the existing job instead of creating a
  // duplicate — see findOrCreateJob in job.repository.ts.
  idempotencyKey: z.string().min(1).optional(),
  // Milestone 10: which per-tier stream this job dispatches onto — see
  // queue/stream.ts. Defaulted here (not left undefined) so it's always a
  // concrete value by the time it reaches the repository/enqueue layer.
  priority: z.enum(['LOW', 'NORMAL', 'HIGH']).default('NORMAL'),
});

export type CreateJobInput = z.infer<typeof createJobSchema>;
