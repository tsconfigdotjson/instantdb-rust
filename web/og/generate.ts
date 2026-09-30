// Renders og/card.html to public/og.jpg (1200x630) with Playwright.
//
//   npm run og            # write public/og.jpg
//   npm run og -- --open  # also leave a headed browser on the card to tweak it
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { chromium } from "playwright";

const root = fileURLToPath(new URL("..", import.meta.url));
const out = fileURLToPath(new URL("../public/og.jpg", import.meta.url));
const headed = process.argv.includes("--open");

const server = await createServer({ root, logLevel: "warn", server: { port: 0 } });
await server.listen();
const url = new URL("og/card.html", server.resolvedUrls!.local[0]).href;

const browser = await chromium.launch({ headless: !headed });
try {
  const page = await browser.newPage({ viewport: { width: 1200, height: 630 }, deviceScaleFactor: 1 });
  page.on("pageerror", (err) => {
    throw err;
  });
  await page.goto(url);
  await page.waitForSelector("html[data-ready]", { timeout: 15_000 });
  await page.locator(".card").screenshot({ path: out, type: "jpeg", quality: 90 });
  console.log(`wrote ${out}`);
  if (headed) {
    console.log(`card is open at ${url}; Ctrl-C to quit`);
    await new Promise(() => {});
  }
} finally {
  await browser.close();
  await server.close();
}
