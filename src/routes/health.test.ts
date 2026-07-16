import { buildApp } from '../app';

describe('GET /health', () => {
  it('returns 200 with a status of ok', async () => {
    const app = buildApp();

    // app.inject() simulates an HTTP request in-process — no real socket,
    // no real port. This is the pattern we'll reuse for every route in
    // this project.
    const response = await app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(200);

    const body = response.json();
    expect(body.status).toBe('ok');
    expect(typeof body.uptime).toBe('number');
    expect(typeof body.timestamp).toBe('string');

    await app.close();
  });
});
