import os from 'node:os';
import { createPrismaClient } from './db/client';
import { createRedisClient } from './queue/redis';
import { ensureConsumerGroup } from './queue/stream';
import { processNextJob } from './queue/consumer';
import { createLogger } from './logger';

const logger = createLogger();
const prisma = createPrismaClient();
const redis = createRedisClient();

// Unique per process, not per host: multiple workers can run on the same
// machine (or in the same container replica set), and Redis needs a
// distinct consumer name per worker to track separate PELs.
const consumerName = `${os.hostname()}:${process.pid}`;

let running = true;

async function loop(): Promise<void> {
  await ensureConsumerGroup(redis);
  logger.info({ consumerName }, 'Worker started, listening on jobs:stream');

  while (running) {
    try {
      await processNextJob(prisma, redis, logger, consumerName);
    } catch (err) {
      // A failure inside processNextJob's own try/catch already means a
      // bad *job*. Reaching here means something broke in the read/dispatch
      // machinery itself (e.g. Redis connection drop) — log and keep the
      // loop alive rather than taking the whole worker down over one
      // failed read.
      logger.error({ err }, 'Unexpected error in worker loop; continuing');
    }
  }
}

/**
 * Mirrors server.ts's SIGTERM/SIGINT handling: flip a flag rather than
 * exiting immediately. The loop notices on its next iteration — at most
 * one BLOCK timeout (up to 5s) after the signal, plus however long any
 * job already being processed takes to finish — instead of abandoning a
 * job mid-work.
 */
async function shutdown(signal: string): Promise<void> {
  logger.info(`Received ${signal}, stopping worker after the current read...`);
  running = false;
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

loop()
  .then(async () => {
    logger.info('Worker loop exited, shutting down');
    await prisma.$disconnect();
    await redis.quit();
    process.exit(0);
  })
  .catch(async (err) => {
    logger.error({ err }, 'Worker crashed');
    await prisma.$disconnect();
    await redis.quit();
    process.exit(1);
  });
