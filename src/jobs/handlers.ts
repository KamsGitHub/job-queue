import type { Logger } from '../logger';

export type JobHandler = (payload: Record<string, unknown>, ctx: { logger: Logger }) => Promise<void>;

/**
 * Maps a job's `type` to the function that does the actual work. This
 * project is a demonstration of queue mechanics, not a real task-execution
 * platform, so `send-email` is a stand-in — no email is actually sent.
 * Real handlers would register here the same way, one per job type.
 */
const handlers: Record<string, JobHandler> = {
  'send-email': async (payload, { logger }) => {
    logger.info({ to: payload.to }, '[send-email] would send email (stand-in, no real send)');
  },
};

export function getHandler(type: string): JobHandler | undefined {
  return handlers[type];
}
