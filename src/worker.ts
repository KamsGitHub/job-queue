import os from 'node:os';
import { createPrismaClient } from './db/client';
import { createRedisClient } from './queue/redis';
import { ensureConsumerGroups } from './queue/stream';
import { processNextJob, reclaimStaleEntries } from './queue/consumer';
import { createLogger } from './logger';
import { env } from './config/env';

const logger = createLogger();
const prisma = createPrismaClient();
const redis = createRedisClient();

// Redis processes a single client connection's commands strictly in
// order — while `redis` has a BLOCK 5000 read outstanding in the main
// loop below, any command sent on that SAME connection (e.g. the sweep's
// XAUTOCLAIM/XACK) queues behind it and can't get a response until the
// blocking read finally resolves. Confirmed directly: a PING sent right
// after a BLOCK 5000 read on one connection took ~5s to resolve, not
// ~1ms. `sweepRedis` is a separate connection (ioredis's .duplicate(),
// same connection options) used only by the sweep, so its commands are
// never stuck behind the main loop's blocking reads.
const sweepRedis = redis.duplicate();

// Unique per process, not per host: multiple workers can run on the same
// machine (or in the same container replica set), and Redis needs a
// distinct consumer name per worker to track separate PELs.
const consumerName = `${os.hostname()}:${process.pid}`;

let running = true;

/**
 * Crash recovery: every worker also sweeps the shared consumer group's PEL
 * on its own timer, independent of (and running concurrently with) the
 * main read loop below — coupling the sweep to the loop's BLOCK cadence
 * would mean a single slow job delays the sweep by however long that job
 * takes. Every live worker doing this means the group self-heals as long
 * as at least one worker is up; there's no dedicated reaper role/process.
 * `sweeping` guards against a sweep still running when the next interval
 * tick fires (e.g. Postgres briefly slow) — skip rather than overlap.
 */
let sweeping: Promise<void> | null = null;

function startStaleEntrySweep(): NodeJS.Timeout {
  return setInterval(() => {
    if (sweeping) {
      return;
    }
    sweeping = reclaimStaleEntries(prisma, sweepRedis, logger, consumerName, env.WORKER_STALE_IDLE_MS)
      .then(() => undefined)
      .catch((err: unknown) => {
        logger.error({ err }, 'Error while sweeping for stale PEL entries');
      })
      .finally(() => {
        sweeping = null;
      });
  }, env.WORKER_STALE_SWEEP_INTERVAL_MS);
}

async function loop(): Promise<void> {
  await ensureConsumerGroups(redis);
  const sweepTimer = startStaleEntrySweep();
  logger.info(
    { consumerName, sweepIntervalMs: env.WORKER_STALE_SWEEP_INTERVAL_MS, idleMs: env.WORKER_STALE_IDLE_MS },
    'Worker started, listening on jobs:stream:high / :normal / :low',
  );

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

  clearInterval(sweepTimer);
  // The interval is stopped, but a sweep it already kicked off may still be
  // running (reclaiming + reprocessing several entries can take a moment) —
  // wait for it rather than disconnecting Postgres/Redis out from under it.
  if (sweeping) {
    await sweeping;
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
    await sweepRedis.quit();
    process.exit(0);
  })
  .catch(async (err) => {
    logger.error({ err }, 'Worker crashed');
    await prisma.$disconnect();
    await redis.quit();
    await sweepRedis.quit();
    process.exit(1);
  });
