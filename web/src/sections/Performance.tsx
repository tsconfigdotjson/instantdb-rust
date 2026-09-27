import { useEffect, useRef, useState } from "react";
import { motion, useInView } from "motion/react";
import { Heading } from "../components/Section";
import { INK, RoughOverlay, Sketch } from "../components/rough";
import { CountUp, Scrawl } from "../components/type";
import { CrabWalk } from "../components/CrabWalk";
import { doc } from "../config";

const stats = [
  {
    value: 48,
    unit: "KB",
    label: "of server memory per live websocket session, measured with 5,000 connected",
  },
  {
    value: 133,
    unit: "k/s",
    label: "query updates pushed to clients by one node, on about two CPU cores",
  },
  {
    value: 1362,
    unit: "tx/s",
    label: "transactions on one node, where Postgres became the bottleneck, not the server",
  },
  {
    value: 69,
    unit: "",
    label: "queries computed when 5,000 clients with 15,000 subscriptions reconnect at once",
  },
];

export function Performance() {
  return (
    <section id="speed" className="relative scroll-mt-24 overflow-hidden bg-plum-850 py-28 sm:py-36">
      <TornEdge className="absolute inset-x-0 top-0 -translate-y-px" />
      <div className="mx-auto max-w-7xl px-4 sm:px-8">
        <div className="grid gap-10 lg:grid-cols-[1.2fr_1fr] lg:items-end">
          <Heading kicker="why rust?" title="Rewritten in Rust." accent="Cheap to run." />
          <p className="max-w-xl text-lg leading-relaxed text-peach-200/85">
            Legacy Instant is a Clojure service on the JVM. This is one Rust binary built on tokio and axum. The costly
            part of a sync engine is re-running queries when data changes, so each result is computed once and shared
            with every subscriber who asked for it.
          </p>
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
                  <span className="ml-1 text-[0.45em] font-medium text-ember-300">{s.unit}</span>
                </span>
                {i === 0 ? (
                  <RoughOverlay
                    className="-inset-x-5 -inset-y-4"
                    delay={1}
                    duration={1.2}
                    build={(g, w, h) => g.ellipse(w / 2, h / 2, w, h, { stroke: INK.ember, strokeWidth: 2.2, roughness: 2, seed: 8 })}
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
                        { stroke: INK.ember, strokeWidth: 2.6, roughness: 1, seed: 9 + i },
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
          Measured with the repo's own load generator against one server node with Postgres 17 on an 8-core Apple M1;
          fan-out and transaction ceilings are loopback runs. Methodology, raw tables and how to reproduce them are in{" "}
          <a className="underline decoration-dashed underline-offset-4 hover:text-ember-300" href={doc("docs/PERF.md")} target="_blank" rel="noopener">
            docs/PERF.md
          </a>
          .
        </p>

        <CrabWalk />
        <Architecture />
      </div>
      <TornEdge className="absolute inset-x-0 bottom-0 translate-y-px rotate-180" />
    </section>
  );
}

function Architecture() {
  return (
    <div className="grid items-center gap-14 lg:grid-cols-[1fr_1.05fr] lg:gap-20">
      <div>
        <Heading kicker="no sticky sessions" title="Stateless, so it" accent="scales sideways." />
        <p className="mt-8 max-w-xl text-lg leading-relaxed text-peach-200/85">
          Nodes keep nothing between requests. Queries are computed from Postgres, and invalidations, presence and
          broadcasts fan out over <code className="font-mono text-ember-300">LISTEN/NOTIFY</code>. Any node can serve any
          client, so you add replicas behind any websocket-capable load balancer and scale the database.
        </p>
        <div className="mt-8 inline-block rounded-xl bg-plum-950/70 px-5 py-4 font-mono text-[15px] text-peach-200/90">
          <span className="text-plum-400">$</span> docker compose up <span className="text-ember-300">--scale server=3</span>
        </div>
        <div className="mt-6">
          <Scrawl className="rotate-[-2deg] text-2xl text-ember-300" delay={0.4}>
            Postgres is the only thing you operate.
          </Scrawl>
        </div>
      </div>
      <Diagram />
    </div>
  );
}

const NODES = [90, 260, 430];

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
      <svg viewBox="0 0 520 640" className="w-full" aria-label="Clients connect through a load balancer to any of several stateless Rust nodes, which share one Postgres database.">
        <Sketch
          viewBox="0 0 520 640"
          show={inView}
          stagger={0.1}
          duration={0.8}
          build={(g) => [
            ...[10, 140, 270, 400].map((x, i) => g.rectangle(x, 14, 110, 54, { ...box, seed: 100 + i })),
            g.rectangle(60, 140, 400, 52, { ...box, stroke: INK.plum400, seed: 110 }),
            ...NODES.map((cx, i) =>
              g.rectangle(cx - 72, 262, 144, 78, { ...box, fill: INK.plum800, fillStyle: "solid", seed: 120 + i }),
            ),
            g.ellipse(260, 460, 320, 50, { ...box, seed: 130 }),
            g.line(100, 460, 100, 572, { ...box, seed: 131 }),
            g.line(420, 460, 420, 572, { ...box, seed: 132 }),
            g.arc(260, 572, 320, 50, 0, Math.PI, false, { ...box, seed: 133 }),
            g.arc(260, 510, 320, 44, 0, Math.PI, false, { ...box, stroke: INK.plum400, strokeWidth: 1.4, seed: 134 }),
          ]}
        />
        <Sketch
          viewBox="0 0 520 640"
          show={inView}
          delay={1.4}
          stagger={0.06}
          duration={0.5}
          build={(g) => [
            ...[65, 195, 325, 455].map((x, i) => g.line(x, 72, 180 + i * 53, 136, { ...arrow, seed: 140 + i })),
            ...NODES.map((cx, i) => g.line(200 + i * 60, 196, cx, 256, { ...arrow, seed: 150 + i })),
            ...NODES.map((cx, i) => g.line(cx, 344, 200 + i * 60, 432, { ...arrow, stroke: INK.ember3, seed: 160 + i })),
          ]}
        />

        <g className="font-mono" fontSize="17" fill={INK.cream} textAnchor="middle">
          {["react", "core", "admin", "cli"].map((t, i) => (
            <motion.text key={t} x={65 + i * 130} y={47} {...label(inView, 0.6 + i * 0.08)}>
              {t}
            </motion.text>
          ))}
          <motion.text x={260} y={172} fill={INK.peach} {...label(inView, 1)}>
            any load balancer
          </motion.text>
          {NODES.map((cx, i) => (
            <motion.g key={cx} {...label(inView, 1.2 + i * 0.1)}>
              <text x={cx} y={296} fill={INK.ember4} fontSize="18">
                node {i + 1}
              </text>
              <text x={cx} y={320} fill={INK.peach} fontSize="13" opacity={0.7}>
                stateless
              </text>
            </motion.g>
          ))}
          <motion.g {...label(inView, 1.8)}>
            <text x={260} y={540} className="display" fontFamily="Fraunces Variable" fontSize="30" fill={INK.ember}>
              Postgres
            </text>
            <text x={260} y={596} fontSize="13" fill={INK.peach} opacity={0.75}>
              data · LISTEN/NOTIFY · presence · blobs
            </text>
          </motion.g>
        </g>

        {pulsing ? (
          <g>
            {[65, 195, 325, 455].map((x, i) => (
              <circle key={`c${x}`} r="3.5" fill={INK.peach}>
                <animateMotion dur="1.8s" repeatCount="indefinite" begin={`${i * 0.35}s`} path={`M${x},72 L${180 + i * 53},136`} />
              </circle>
            ))}
            {NODES.map((cx, i) => (
              <circle key={`n${cx}`} r="4.5" fill={INK.ember}>
                <animateMotion dur="1.6s" repeatCount="indefinite" begin={`${0.3 + i * 0.5}s`} path={`M${200 + i * 60},432 L${cx},344`} />
              </circle>
            ))}
            {NODES.map((cx, i) => (
              <circle key={`l${cx}`} r="3.5" fill={INK.peach}>
                <animateMotion dur="1.7s" repeatCount="indefinite" begin={`${0.6 + i * 0.4}s`} path={`M${200 + i * 60},196 L${cx},256`} />
              </circle>
            ))}
          </g>
        ) : null}
      </svg>
      <div className="pointer-events-none absolute -right-2 top-[60%] hidden sm:block">
        <Scrawl className="rotate-6 text-xl text-ember-300" delay={2.4}>
          ← every node hears every change
        </Scrawl>
      </div>
    </div>
  );
}

/** A ragged paper edge between sections. */
function TornEdge({ className = "" }: { className?: string }) {
  return (
    <svg viewBox="0 0 1440 24" preserveAspectRatio="none" className={`h-6 w-full text-plum-900 ${className}`} aria-hidden="true">
      <path
        d="M0 0h1440v10c-40 6-80-4-120 3s-70 8-110 1-90 6-140 4-60-9-110-3-80 9-130 2-70-6-120 1-90 7-140 2-60-7-110-1-80 8-130 3-70-6-120 0-80 6-110 2V0Z"
        fill="currentColor"
      />
    </svg>
  );
}
