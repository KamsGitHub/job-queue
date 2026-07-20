import { computeBackoffMs } from './backoff';

describe('computeBackoffMs', () => {
  it('stays within [0, base * 2^(attempts-1)] while under the cap', () => {
    const base = 1000;
    const cap = 60000;

    for (const attempts of [1, 2, 3, 4]) {
      const ceiling = base * 2 ** (attempts - 1);
      for (let i = 0; i < 50; i++) {
        const delay = computeBackoffMs(attempts, base, cap);
        expect(delay).toBeGreaterThanOrEqual(0);
        expect(delay).toBeLessThan(ceiling);
      }
    }
  });

  it('never exceeds the cap once the exponential ceiling would surpass it', () => {
    const base = 1000;
    const cap = 5000;

    // 2^9 * 1000 = 512000, far past the 5000ms cap.
    for (let i = 0; i < 50; i++) {
      const delay = computeBackoffMs(10, base, cap);
      expect(delay).toBeGreaterThanOrEqual(0);
      expect(delay).toBeLessThan(cap);
    }
  });

  it('grows on average across attempts (full jitter, not a fixed value)', () => {
    const base = 1000;
    const cap = 60000;
    const samples = 200;

    const average = (attempts: number): number => {
      let total = 0;
      for (let i = 0; i < samples; i++) {
        total += computeBackoffMs(attempts, base, cap);
      }
      return total / samples;
    };

    expect(average(3)).toBeGreaterThan(average(1));
  });
});
