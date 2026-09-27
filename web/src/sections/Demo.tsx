import { useRef } from "react";
import { motion, useInView, useScroll, useTransform } from "motion/react";
import { Horizon } from "../components/Horizon";
import { Stars } from "../components/Sky";
import { Heading } from "../components/Section";
import { HandButton } from "../components/doodles";
import { DASH_URL } from "../config";

const notes = [
  { big: "3", small: "apps per account", rotate: -4, tape: 8 },
  { big: "10 MB", small: "per app", rotate: 3, tape: -6 },
  { big: "$0", small: "every paid feature switched on", rotate: -2, tape: 4 },
];

/** The closing act: the sun comes back up over the demo server. */
export function Demo() {
  const ref = useRef<HTMLElement>(null);
  const { scrollYProgress } = useScroll({ target: ref, offset: ["start end", "end end"] });
  const sunY = useTransform(scrollYProgress, [0.25, 1], [240, 0]);
  const glow = useTransform(scrollYProgress, [0.25, 1], [0.1, 1]);
  const reflect = useTransform(scrollYProgress, [0.6, 1], [0, 1]);
  const stars = useTransform(scrollYProgress, [0.3, 1], [1, 0.3]);
  const sceneRef = useRef<HTMLDivElement>(null);
  const sceneInView = useInView(sceneRef, { once: true, amount: 0.1 });

  return (
    <section
      id="demo"
      ref={ref}
      className="relative isolate flex min-h-svh scroll-mt-16 flex-col overflow-hidden bg-[linear-gradient(180deg,#1c0932_0%,#2a0f4a_35%,#4a1a6c_62%,#7a2a68_86%)]"
    >
      <Stars opacity={stars} seed={21} count={24} className="absolute inset-x-0 top-0 -z-10 h-[60%] w-full" />

      <div className="mx-auto w-full max-w-5xl px-4 pt-28 pb-16 text-center sm:px-8 sm:pt-36 sm:pb-20">
        <Heading kicker="good morning" title="Kick the tires on the" accent="demo server." center />
        <p className="mx-auto mt-8 max-w-2xl text-lg leading-relaxed text-peach-200/90 sm:text-xl">
          We run a public InstantDB Rust server so you can try it before moving anything real. Sign in to the
          dashboard, create an app, and point a client at it.
        </p>

        <div className="mt-14 flex flex-wrap items-start justify-center gap-x-4 gap-y-6 sm:gap-10">
          {notes.map((n, i) => (
            <motion.div
              key={n.small}
              initial={{ opacity: 0, y: 40, rotate: 0 }}
              whileInView={{ opacity: 1, y: 0, rotate: n.rotate }}
              viewport={{ once: true, amount: 0.6 }}
              transition={{ type: "spring", stiffness: 120, damping: 12, delay: 0.2 + i * 0.12 }}
              whileHover={{ rotate: 0, y: -6 }}
              className="relative w-[9.5rem] bg-ember-300 px-4 pt-7 pb-5 sm:w-48 sm:px-5 text-plum-950 shadow-[0_18px_40px_-12px_rgba(0,0,0,0.55)] sm:w-48"
            >
              <span
                className="absolute -top-3 left-1/2 h-6 w-20 -translate-x-1/2 bg-peach-200/60"
                style={{ transform: `translateX(-50%) rotate(${n.tape}deg)` }}
              />
              <p className="display text-4xl font-semibold sm:text-5xl">{n.big}</p>
              <p className="mt-1 font-hand text-2xl leading-tight">{n.small}</p>
            </motion.div>
          ))}
        </div>

        <div className="mt-14 flex flex-col items-center gap-5">
          <HandButton href={DASH_URL} delay={0.6}>
            Open the demo dashboard
          </HandButton>
          <p className="max-w-md text-sm text-peach-200/65">
            It's a sandbox: no SLA, and data can be wiped. Self-host anything you care about.
          </p>
        </div>
      </div>

      <div ref={sceneRef} className="mt-auto">
        <Horizon
          sunY={sunY}
          glow={glow}
          reflect={reflect}
          show={sceneInView}
          className="pointer-events-none relative -z-10 -mb-px block h-[clamp(260px,42svh,480px)] w-full overflow-visible"
        />
      </div>
    </section>
  );
}
