import { buildApp } from './app';
import { env } from './config/env';

const app = buildApp();

async function start(): Promise<void> {
  try {
    // Bind 0.0.0.0, not localhost/127.0.0.1. Inside a container, "localhost"
    // refers to the container's own loopback interface — binding there
    // makes the server unreachable from outside the container, including
    // from Docker's own port mapping. This bites people constantly.
    await app.listen({ port: env.PORT, host: '0.0.0.0' });
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

/**
 * When Docker stops a container, or Kubernetes terminates a pod, it sends
 * SIGTERM and then waits a grace period before SIGKILL. If we don't handle
 * SIGTERM, in-flight requests get dropped mid-response and connections are
 * severed abruptly. app.close() drains Fastify's plugins and lets active
 * requests finish before the process exits. This matters far more once we
 * have workers mid-job — killing a worker mid-job is exactly the crash
 * scenario Milestone 6 (crash recovery) exists to handle, but we shouldn't
 * manufacture unnecessary crashes on ordinary deploys.
 */
async function shutdown(signal: string): Promise<void> {
  app.log.info(`Received ${signal}, shutting down gracefully...`);
  try {
    await app.close();
    app.log.info('Shutdown complete.');
    process.exit(0);
  } catch (err) {
    app.log.error(err, 'Error during shutdown');
    process.exit(1);
  }
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

void start();
