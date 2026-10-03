// @instantdb/core's e2e project (LEGACY/client/packages/core/vitest.config.ts)
// pointed at a LOCAL server. run-suite.sh copies this file into the core
// package directory (vitest resolves the imports below from the config's own
// location, and the include globs from its root) and runs it with
// SDK_SERVER_PORT set.
//
// The upstream config sets __DEV_LOCAL_PORT__ to 0 whenever CI is set, and
// __tests__/src/utils/e2e.ts then falls back to https://api.instantdb.com.
// This config never does that: a missing or invalid port is a hard error.
import { playwright } from '@vitest/browser-playwright';
import { defineConfig } from 'vitest/config';

const raw = process.env.SDK_SERVER_PORT;
const port = Number(raw);
if (!raw || !Number.isInteger(port) || port <= 0 || port > 65535) {
  throw new Error(
    `SDK_SERVER_PORT must be the local server's port (got ${JSON.stringify(raw)}); ` +
      'refusing to run: without it the core e2e suite would hit production',
  );
}

// infiniteQuery.e2e.test.ts races the server on both servers alike: a test
// transacts again ~35 ms after the first transact, on the strength of the
// optimistic result, and passes only if the first transact's refresh arrives
// in between. When that refresh already includes the second write, the
// infinite query bootstraps at the new first item and the last item lands on
// a page the test never loads (`[-1, 0, 1, 2]` for `[-1, 0, 1, 2, 3]`). In a
// local replica it failed 26/260 against this server and 11/100 against
// legacy, with the same frames. That file alone gets retries; every other
// file still fails on its first failure.
const RACY = ['**/infiniteQuery.e2e.test.ts'];

export default defineConfig({
  define: {
    __DEV_LOCAL_PORT__: port,
  },
  test: {
    // `include` stays out of the root: projects that extend it merge arrays
    projects: [
      {
        extends: true,
        test: { name: 'e2e', include: ['**/**.e2e.test.ts'], exclude: [...RACY, '**/node_modules/**'] },
      },
      { extends: true, test: { name: 'e2e-retried', include: RACY, retry: 2 } },
    ],
    expect: {
      poll: {
        timeout: 10_000,
      },
    },
    browser: {
      enabled: true,
      provider: playwright(),
      screenshotFailures: false,
      headless: true,
      instances: [{ browser: 'chromium' }],
    },
  },
});
