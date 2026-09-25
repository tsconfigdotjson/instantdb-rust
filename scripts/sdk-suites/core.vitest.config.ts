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

export default defineConfig({
  define: {
    __DEV_LOCAL_PORT__: port,
  },
  test: {
    name: 'e2e',
    include: ['**/**.e2e.test.ts'],
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
