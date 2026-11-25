import { useEffect, useRef, useState } from "react";
import { motion, useInView } from "motion/react";
import { Heading } from "../components/Section";
import { INK, RoughOverlay, Sketch } from "../components/rough";
import { CountUp, Scrawl } from "../components/type";
import { doc } from "../config";

// Figures from the test deployments on Cloudflare (docs/CLOUDFLARE.md and
// deploy-test-logs/); update them together.
const stats = [
  {
    value: 0,
    unit: "",
    label: "servers to run. The engine sleeps a minute after the last client leaves and wakes in about a second",
  },
  {
    value: 122,
    unit: "ms",
    label: "for a new query to answer, with Postgres one region away from the client",
  },
  {
    value: 339,
    unit: "ms",
    label: "for a write to reach another connected client as a live update",
  },
  {
    value: 2000,
    unit: "",
    label: "idle subscribed clients held open by a single Durable Object",
  },
];

export function Serverless() {
  return (
    <section id="serverless" className="relative scroll-mt-24 pt-28 pb-10 sm:pt-36 sm:pb-16">
      <div className="mx-auto max-w-7xl px-4 sm:px-8">
        <div className="grid items-center gap-14 lg:grid-cols-[1.05fr_1fr] lg:gap-20">
          <div>
            <div className="relative inline-block">
              <Heading kicker="or run no server at all" title="Serverless, on" accent="Cloudflare Workers." />
              <span className="absolute -top-3 right-0 hidden translate-x-[35%] sm:block">
                <span className="relative inline-block px-3 py-1">
                  <Scrawl className="rotate-6 text-2xl text-peach-200" delay={0.9}>
                    experimental
                  </Scrawl>
                  <RoughOverlay
                    className="-inset-x-1 -inset-y-1 rotate-6"
                    delay={1.6}
                    duration={0.9}
                    build={(g, w, h) =>
                      g.ellipse(w / 2, h / 2, w, h, { stroke: INK.plum400, strokeWidth: 1.8, roughness: 1.8, seed: 41 })
                    }
                  />
                </span>
              </span>
            </div>
            <div className="mt-4 sm:hidden">
              <Scrawl className="-rotate-2 text-2xl text-peach-200" delay={0.9}>
                (experimental)
              </Scrawl>
            </div>
            <p className="mt-8 max-w-xl text-lg leading-relaxed text-peach-200/85">
              The same Rust server, compiled to WebAssembly, runs inside a Cloudflare Durable Object and reaches your
              Postgres through Hyperdrive. Your <code className="font-mono text-ember-300">@instantdb</code> apps connect
              to it unmodified. There is no machine to keep alive, and nothing to pay for while nobody is connected.
            </p>
            <div className="mt-8 inline-block max-w-full overflow-x-auto rounded-xl bg-plum-950/70 px-5 py-4 font-mono text-[15px] leading-relaxed text-peach-200/90">
              <div>
                <span className="text-plum-400">$</span> cd crates/instant-worker
              </div>
              <div>
                <span className="text-plum-400">$</span> npx wrangler <span className="text-ember-300">deploy</span>
              </div>
            </div>
            <div className="mt-6">
              <Scrawl className="rotate-[-2deg] text-2xl text-ember-300" delay={0.4}>
                your Postgres stays yours.
              </Scrawl>
            </div>
          </div>
          <Diagram />
        </div>

        <div className="mt-20 grid gap-x-10 gap-y-16 sm:grid-cols-2 lg:grid-cols-4">
          {stats.map((s, i) => (
            <motion.div
              key={s.label}
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, amount: 0.6 }}
              transition={{ duration: 0.8, delay: i * 0.12 }}
            >
              <div className="relative inline-block">
                <span className="display text-[clamp(3.5rem,6vw,5rem)] leading-none font-semibold text-ember-400">
                  <CountUp to={s.value} />
                  {s.unit ? <span className="ml-1 text-[0.45em] font-medium text-ember-300">{s.unit}</span> : null}
                </span>
                {i === 0 ? (
                  <RoughOverlay
                    className="-inset-x-6 -inset-y-4"
                    delay={1}
                    duration={1.2}
                    build={(g, w, h) => g.ellipse(w / 2, h / 2, w, h, { stroke: INK.ember, strokeWidth: 2.2, roughness: 2, seed: 48 })}
                  />
                ) : (
                  <RoughOverlay
                    className="inset-x-0 -bottom-3 h-3"
                    delay={1 + i * 0.1}
                    duration={0.7}
                    build={(g, w, h) =>
                      g.curve(
                        [
                          [0, h * 0.6],
                          [w * 0.35, h * 0.2],
                          [w * 0.7, h * 0.8],
                          [w, h * 0.3],
                        ],
                        { stroke: INK.ember, strokeWidth: 2.6, roughness: 1, seed: 49 + i },
                      )
                    }
                  />
                )}
              </div>
              <p className="mt-6 max-w-[16rem] leading-relaxed text-peach-200/80">{s.label}</p>
            </motion.div>
          ))}
        </div>
        <p className="mt-14 max-w-3xl text-sm leading-relaxed text-peach-200/50">
          Experimental. Measured on real deployments with the client in Toronto and Postgres in AWS us-east-2: the
          wire-conformance suite, sign-in and webhooks pass, and a three-hour soak dropped nothing. It builds with a
          Cloudflare preview toolchain, one Durable Object serves every app, and speed depends on where Cloudflare
          places it, so the self-hosted server is still the dependable way to run this. Design, limits and the test
          reports are in{" "}
          <a className="underline decoration-dashed underline-offset-4 hover:text-ember-300" href={doc("docs/CLOUDFLARE.md")} target="_blank" rel="noopener">
            docs/CLOUDFLARE.md
          </a>
          .
        </p>
      </div>
    </section>
  );
}

const CLIENTS = [110, 260, 410];

function Diagram() {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref, { once: true, amount: 0.35 });
  const [pulsing, setPulsing] = useState(false);
  useEffect(() => {
    if (!inView) return;
    const t = setTimeout(() => setPulsing(true), 2600);
    return () => clearTimeout(t);
  }, [inView]);

  const box = { stroke: INK.ember4, strokeWidth: 2, roughness: 1.3 };
  const arrow = { stroke: INK.peach, strokeWidth: 1.6, roughness: 0.8, disableMultiStroke: true };
  const label = (show: boolean, delay: number) => ({
    initial: { opacity: 0 },
    animate: show ? { opacity: 1 } : undefined,
    transition: { duration: 0.6, delay },
  });

  return (
    <div ref={ref} className="relative mx-auto w-full max-w-[560px]">
      <svg
        viewBox="0 0 520 660"
        className="w-full"
        aria-label="Clients connect to a Cloudflare Worker, which hands every request to one Durable Object running the whole sync engine. The Durable Object reaches your Postgres database through Hyperdrive."
      >
        <Sketch
          viewBox="0 0 520 660"
          show={inView}
          stagger={0.1}
          duration={0.8}
          build={(g) => [
            ...CLIENTS.map((cx, i) => g.rectangle(cx - 55, 14, 110, 54, { ...box, seed: 200 + i })),
            g.rectangle(110, 138, 300, 50, { ...box, stroke: INK.plum400, seed: 210 }),
            g.rectangle(70, 252, 380, 116, { ...box, fill: INK.plum800, fillStyle: "solid", seed: 220 }),
            g.rectangle(150, 428, 220, 46, { ...box, stroke: INK.plum400, seed: 230 }),
            g.ellipse(260, 540, 280, 44, { ...box, seed: 240 }),
            g.line(120, 540, 120, 618, { ...box, seed: 241 }),
            g.line(400, 540, 400, 618, { ...box, seed: 242 }),
            g.arc(260, 618, 280, 44, 0, Math.PI, false, { ...box, seed: 243 }),
            // a crescent moon by the engine: it sleeps when nobody is connected
            g.arc(468, 232, 46, 46, Math.PI * 0.35, Math.PI * 1.75, false, { ...box, stroke: INK.peach, strokeWidth: 1.8, seed: 250 }),
            g.arc(480, 224, 36, 36, Math.PI * 0.5, Math.PI * 1.55, false, { ...box, stroke: INK.peach, strokeWidth: 1.8, seed: 251 }),
          ]}
        />
        <Sketch
          viewBox="0 0 520 660"
          show={inView}
          delay={1.4}
          stagger={0.06}
          duration={0.5}
          build={(g) => [
            ...CLIENTS.map((cx, i) => g.line(cx, 72, 200 + i * 60, 134, { ...arrow, seed: 260 + i })),
            g.line(260, 192, 260, 246, { ...arrow, seed: 270 }),
            g.line(260, 372, 260, 422, { ...arrow, stroke: INK.ember3, seed: 271 }),
            g.line(260, 478, 260, 514, { ...arrow, stroke: INK.ember3, seed: 272 }),
          ]}
        />

        <g className="font-mono" fontSize="17" fill={INK.cream} textAnchor="middle">
          {["react", "core", "admin"].map((t, i) => (
            <motion.text key={t} x={CLIENTS[i]} y={47} {...label(inView, 0.6 + i * 0.08)}>
              {t}
            </motion.text>
          ))}
          <motion.text x={260} y={169} fill={INK.peach} {...label(inView, 1)}>
            Worker, at the edge
          </motion.text>
          <motion.g {...label(inView, 1.2)}>
            <text x={260} y={300} className="display" fontFamily="Fraunces Variable" fontSize="28" fill={INK.ember}>
              Durable Object
            </text>
            <text x={260} y={328} fill={INK.ember4} fontSize="15">
              the whole sync engine
            </text>
            <text x={260} y={350} fill={INK.peach} fontSize="13" opacity={0.7}>
              rust → wasm · sessions · live queries
            </text>
          </motion.g>
          <motion.text x={260} y={457} fill={INK.peach} {...label(inView, 1.6)}>
            Hyperdrive
          </motion.text>
          <motion.g {...label(inView, 1.8)}>
            <text x={260} y={592} className="display" fontFamily="Fraunces Variable" fontSize="28" fill={INK.ember}>
              your Postgres
            </text>
          </motion.g>
        </g>

        {pulsing ? (
          <g>
            {CLIENTS.map((cx, i) => (
              <circle key={`c${cx}`} r="3.5" fill={INK.peach}>
                <animateMotion dur="1.8s" repeatCount="indefinite" begin={`${i * 0.4}s`} path={`M${cx},72 L${200 + i * 60},134`} />
              </circle>
            ))}
            <circle r="3.5" fill={INK.peach}>
              <animateMotion dur="1.4s" repeatCount="indefinite" begin="0.5s" path="M260,192 L260,246" />
            </circle>
            <circle r="4.5" fill={INK.ember}>
              <animateMotion dur="1.6s" repeatCount="indefinite" begin="0.2s" path="M260,514 L260,478" />
            </circle>
            <circle r="4.5" fill={INK.ember}>
              <animateMotion dur="1.6s" repeatCount="indefinite" begin="0.9s" path="M260,422 L260,372" />
            </circle>
          </g>
        ) : null}
      </svg>
      <div className="pointer-events-none absolute right-[16%] top-[29.5%]">
        <Scrawl className="rotate-3 text-lg text-ember-300 sm:text-xl" delay={2.4}>
          asleep until needed
        </Scrawl>
      </div>
    </div>
  );
}
