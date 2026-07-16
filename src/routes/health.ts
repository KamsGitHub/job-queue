import { FastifyInstance } from 'fastify';

/**
 * A Fastify "plugin" is just an async function that takes the app instance
 * and registers routes/hooks on it. This is Fastify's encapsulation model —
 * every future domain (jobs, workers, queues) will be its own plugin,
 * registered independently. It keeps routing logic out of app.ts and makes
 * each domain testable and reason-about-able in isolation.
 *
 * This is a liveness check only: "is the process up and responding?"
 * It intentionally does NOT check Postgres or Redis connectivity yet —
 * we don't have clients for either. Once we do (Milestone 2+), we'll add
 * a separate /health/ready endpoint. Liveness and readiness answer
 * different questions and orchestrators (k8s, ECS) treat them differently:
 * a failed liveness check gets the container restarted; a failed readiness
 * check just gets it pulled out of the load balancer rotation.
 */
export async function healthRoutes(app: FastifyInstance) {
  app.get('/health', async () => {
    return {
      status: 'ok',
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
    };
  });
}
