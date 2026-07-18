import { z } from 'zod';

export const createJobSchema = z.object({
  type: z.string().min(1),
  payload: z.record(z.string(), z.unknown()),
});

export type CreateJobInput = z.infer<typeof createJobSchema>;
