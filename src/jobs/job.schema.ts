import { z } from 'zod';

/**
 * Milestone 11: two caller-facing ways to express the same underlying
 * concept (a single absolute point in time before which the job isn't
 * eligible for dispatch) — an explicit `scheduledAt`, or a `delaySeconds`
 * relative to submission time. Mutually exclusive (refine below) and
 * resolved into one `scheduledAt: Date | undefined` (transform below) so
 * every downstream consumer (repository, route) only ever has to deal with
 * one field, not "check both."
 */
export const createJobSchema = z
  .object({
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
    scheduledAt: z.coerce.date().optional(),
    delaySeconds: z.number().positive().optional(),
  })
  .refine((data) => !(data.scheduledAt && data.delaySeconds !== undefined), {
    message: 'Provide at most one of scheduledAt or delaySeconds',
    path: ['scheduledAt'],
  })
  .transform(({ delaySeconds, scheduledAt, ...rest }) => {
    const resolvedScheduledAt = scheduledAt ?? (delaySeconds !== undefined ? new Date(Date.now() + delaySeconds * 1000) : undefined);
    // Conditional spread, not `scheduledAt: resolvedScheduledAt` directly —
    // same exactOptionalPropertyTypes reasoning as job.repository.ts: this
    // keeps the field truly absent (not present-with-value-undefined) when
    // no scheduling was requested, so every direct CreateJobInput literal
    // (tests bypassing the HTTP layer included) can omit it entirely.
    return { ...rest, ...(resolvedScheduledAt ? { scheduledAt: resolvedScheduledAt } : {}) };
  });

export type CreateJobInput = z.infer<typeof createJobSchema>;
