import { useMemo } from "react";
import { motion } from "motion/react";
import baseline from "../../../scripts/differential/coverage-baseline.json";
import { Heading } from "../components/Section";
import { INK, RoughOverlay, Sketch } from "../components/rough";
import { CountUp, Scrawl } from "../components/type";
import { doc } from "../config";

const covered: string[] = baseline.covered;

const groups = [
  { key: "http", label: "HTTP routes" },
  { key: "iq", label: "InstaQL operators" },
  { key: "ws", label: "websocket ops" },
  { key: "err", label: "error types" },
  { key: "tx", label: "transaction steps" },
  { key: "cel", label: "CEL functions" },
];

const steps = [
  {
    n: "1",
    title: "Boot both servers",
    body: "The official legacy Instant server runs in Docker next to the Rust one, each on its own Postgres.",
  },
  {
    n: "2",
    title: "Replay the same script",
    body: "Byte-identical op sequences hit both: queries, transacts, permissions, presence, streams, OAuth errors.",
  },
  {
    n: "3",
    title: "Diff what clients see",
    body: "Frames are folded into exactly what the SDK's reactor reads. Any difference fails the run unless it is listed with a citation.",
  },
];

export function Parity() {
  const counts = useMemo(
    () =>
      groups.map((g) => ({
        ...g,
        n: covered.filter((c) => c.startsWith(`${g.key}:`)).length,
      })),
    [],
  );
  const half = Math.ceil(covered.length / 2);
  const rows = [covered.slice(0, half), covered.slice(half)];

  return (
    <section id="parity" className="relative scroll-mt-24 py-28 sm:py-36">
      <div className="mx-auto max-w-7xl px-4 sm:px-8">
        <div className="grid gap-10 lg:grid-cols-[1.2fr_1fr] lg:items-end">
          <Heading kicker="the parity harness" title="We don't assume parity." accent="We diff it." />
          <p className="max-w-xl text-lg leading-relaxed text-peach-200/85">
            "Compatible" is easy to claim. The differential harness checks it against the real thing: every counted
            surface a client can touch runs on both servers, and the results have to match.
          </p>
        </div>

        <ol className="mt-20 grid gap-10 md:grid-cols-3">
          {steps.map((s, i) => (
            <motion.li
              key={s.n}
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, amount: 0.5 }}
              transition={{ duration: 0.8, delay: i * 0.15 }}
              className="relative"
            >
              <div className="relative inline-flex h-16 w-16 items-center justify-center">
                <Sketch
                  viewBox="0 0 64 64"
                  className="absolute inset-0 h-full w-full"
                  delay={0.2 + i * 0.15}
                  duration={0.8}
                  build={(g) => g.circle(32, 32, 54, { stroke: INK.ember, strokeWidth: 2.2, roughness: 1.8, seed: 200 + i })}
                />
                <span className="font-hand text-4xl font-bold text-ember-400">{s.n}</span>
              </div>
              <h3 className="display mt-5 text-2xl font-medium text-cream-100">{s.title}</h3>
              <p className="mt-2 leading-relaxed text-peach-200/75">{s.body}</p>
            </motion.li>
          ))}
        </ol>

        <div className="mt-24 grid items-center gap-16 lg:grid-cols-[auto_1fr] lg:gap-24">
          <div className="relative text-center lg:text-left">
            <div className="relative inline-block px-4">
              <span className="display text-[clamp(5rem,13vw,10rem)] leading-none font-semibold text-ember-400">
                <CountUp to={covered.length} duration={2.4} />
                <span className="ml-1 text-[0.5em] text-ember-300">/{covered.length}</span>
              </span>
              <RoughOverlay
                className="-inset-x-8 -inset-y-10"
                delay={2}
                duration={1.3}
                stagger={0.3}
                build={(g, w, h) => g.ellipse(w / 2, h / 2, w, h, { stroke: INK.ember, strokeWidth: 3, roughness: 2.2, bowing: 2, seed: 13 })}
              />
            </div>
            <p className="mt-8 max-w-sm text-lg text-peach-200/85 lg:max-w-xs">
              counted surface items exercised by the harness, each one diffed against legacy
            </p>
            <Scrawl className="mt-3 -rotate-3 text-2xl text-ember-300" delay={2.6}>
              gaps are written down, too →
            </Scrawl>
          </div>

          <div>
            <div className="grid grid-cols-2 gap-x-8 gap-y-10 sm:grid-cols-3">
              {counts.map((c, i) => (
                <motion.div
                  key={c.key}
                  initial={{ opacity: 0, y: 24 }}
                  whileInView={{ opacity: 1, y: 0 }}
                  viewport={{ once: true, amount: 0.8 }}
                  transition={{ duration: 0.7, delay: 0.1 + i * 0.08 }}
                >
                  <span className="relative inline-block">
                    <span className="display text-5xl font-semibold text-ember-400 sm:text-6xl">
                      <CountUp to={c.n} duration={1.4} />
                    </span>
                    <Tally delay={0.6 + i * 0.1} seed={300 + i} />
                  </span>
                  <p className="mt-6 text-peach-200/80">{c.label}</p>
                </motion.div>
              ))}
            </div>
            <p className="mt-12 max-w-xl text-sm leading-relaxed text-peach-200/55">
              Known gaps, like bring-your-own-Postgres proxy tables and the hosted service's billing routes, are tracked
              openly in{" "}
              <a className="underline decoration-dashed underline-offset-4 hover:text-ember-300" href={doc("docs/PARITY.md")} target="_blank" rel="noopener">
                docs/PARITY.md
              </a>
              .
            </p>
          </div>
        </div>
      </div>

      <div className="mt-24 space-y-4 overflow-hidden py-2 [mask-image:linear-gradient(90deg,transparent,black_12%,black_88%,transparent)]" aria-hidden="true">
        {rows.map((row, r) => (
          <div key={r} className={`flex w-max gap-3 ${r ? "marquee-reverse" : "marquee"}`}>
            {[...row, ...row].map((item, i) => (
              <span
                key={i}
                className="flex items-center gap-2 rounded-full border border-dashed border-plum-500/70 px-4 py-1.5 font-mono text-[13px] whitespace-nowrap text-peach-200/75"
              >
                <span className="text-ember-400">✓</span>
                {item}
              </span>
            ))}
          </div>
        ))}
      </div>
    </section>
  );
}

/** A five-bar tally gate under each count: "checked off". */
function Tally({ delay, seed }: { delay: number; seed: number }) {
  return (
    <RoughOverlay
      className="left-0 -bottom-4 h-4 w-full"
      delay={delay}
      duration={0.35}
      stagger={0.05}
      amount={0.5}
      build={(g, _w, h) =>
        [
          ...[0, 1, 2, 3].map((j) =>
            g.line(j * 6 + 3, 1, j * 6 + 2, h - 1, { stroke: INK.ember3, strokeWidth: 1.8, roughness: 0.6, seed: seed + j }),
          ),
          g.line(-2, h - 3, 24, 3, { stroke: INK.ember, strokeWidth: 2, roughness: 0.6, seed: seed + 9 }),
        ]
      }
    />
  );
}
