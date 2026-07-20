/**
 * Full jitter exponential backoff (AWS's well-known algorithm): the delay
 * ceiling doubles with each attempt, capped, and the actual delay is chosen
 * uniformly at random between 0 and that ceiling — spreading retries out
 * instead of every failed job waking up at exactly the same moment.
 */
export function computeBackoffMs(attempts: number, baseMs: number, capMs: number): number {
  const ceiling = Math.min(capMs, baseMs * 2 ** (attempts - 1));
  return Math.floor(Math.random() * ceiling);
}
