# InstantDB Rust — marketing site

React 19 + Vite + Tailwind 4. Hand-drawn strokes come from
[rough.js](https://roughjs.com) (sketched once, then animated with
[Motion](https://motion.dev) as SVG path length), so every doodle draws itself
in as it scrolls into view.

```sh
npm install
npm run dev        # http://localhost:5173
npm run build      # static site in dist/
```

`VITE_DASH_URL` overrides where "Try the demo" points (defaults to the public
demo dashboard). Append `?still` to any URL to render every animation at its end
state, for screenshots.

The parity number is not typed in by hand: `src/sections/Parity.tsx` imports
`scripts/differential/coverage-baseline.json`, so it tracks the harness. The
performance figures come from `docs/PERF.md`; update both together.

Layout: `src/components/` holds the drawing primitives (`rough.tsx`), text
effects (`type.tsx`) and the sunset scene (`Horizon.tsx`); `src/sections/` is
one file per page section, in page order in `App.tsx`.

The social card lives in `og/`: `card.html` + `card.css` lay it out at
1200x630 and mount the site's own drawings (sun, stars, crab) from
`src/components/`. `npm run og` screenshots it with Playwright to
`public/og.jpg`, which `index.html`'s Open Graph and Twitter tags point at.
Re-run it after changing the headline or palette (first time:
`npx playwright install chromium`).
