import { useEffect, useRef, useState } from "react";
import { motion, useScroll, useTransform } from "motion/react";
import { Horizon } from "../components/Horizon";
import { Gulls, Stars } from "../components/Sky";
import { HandButton, PenArrow } from "../components/doodles";
import { RustStamp } from "../components/RustStamp";
import { RevealWords, Scrawl, Underlined } from "../components/type";
import { DASH_URL, GITHUB_URL } from "../config";

export function Hero() {
  const ref = useRef<HTMLElement>(null);
  const { scrollYProgress } = useScroll({ target: ref, offset: ["start start", "end start"] });
  const sunY = useTransform(scrollYProgress, [0, 0.7], [0, 240]);
  const glow = useTransform(scrollYProgress, [0, 0.7], [1, 0.1]);
  const reflect = useTransform(scrollYProgress, [0, 0.4], [1, 0]);
  const stars = useTransform(scrollYProgress, [0, 0.6], [0.45, 1]);
  const dusk = useTransform(scrollYProgress, [0, 0.8], [0, 0.6]);
  const textY = useTransform(scrollYProgress, [0, 1], [0, -140]);
  const textOpacity = useTransform(scrollYProgress, [0.35, 0.8], [1, 0]);

  const [ready, setReady] = useState(false);
  useEffect(() => {
    const t = setTimeout(() => setReady(true), 250);
    return () => clearTimeout(t);
  }, []);

  return (
    <section
      id="top"
      ref={ref}
      className="relative isolate flex min-h-svh flex-col overflow-hidden bg-[linear-gradient(180deg,#12051f_0%,#1c0932_26%,#34145c_52%,#5a1d6e_72%,#7a2a68_86%)]"
    >
      <Stars opacity={stars} className="absolute inset-x-0 top-0 -z-10 h-[70%] w-full" />
      <motion.div style={{ opacity: dusk }} className="absolute inset-0 -z-10 bg-plum-950" />
      <Gulls className="top-[30%]" />

      <motion.div
        style={{ y: textY, opacity: textOpacity }}
        className="relative z-10 mx-auto flex w-full max-w-6xl flex-col items-center px-4 pt-32 text-center sm:px-8 sm:pt-40"
      >
        <div className="relative mb-6 sm:mb-8">
          <Scrawl className="-rotate-3 text-2xl text-ember-300 sm:text-3xl" delay={0.9}>
            psst: Instant Cloud shuts down Aug&nbsp;31,&nbsp;2027
          </Scrawl>
          <PenArrow
            className="absolute -right-16 top-6 hidden h-20 w-32 rotate-6 sm:block"
            delay={1.9}
            d="M10 12C60 4 120 18 150 60s-10 50-30 44"
            head={[[132, 88], [118, 104], [138, 110]]}
          />
        </div>

        <RustStamp
          show={ready}
          delay={1.5}
          className="absolute top-[9.5rem] right-[1%] hidden h-52 w-52 lg:block xl:right-[4%]"
        />
        <h1 className="display text-[clamp(3.5rem,11vw,9.75rem)] leading-[0.86] font-medium text-ember-400">
          <RevealWords text="Instant," show={ready} delay={0.1} />
          <br />
          <RevealWords text="after the" show={ready} delay={0.25} />{" "}
          <span className="inline-block overflow-hidden pb-[0.14em] -mb-[0.14em] pr-[0.08em] align-bottom">
            <motion.span
              className="inline-block"
              initial={{ y: "115%", rotate: 4 }}
              animate={ready ? { y: "0%", rotate: 0 } : undefined}
              transition={{ duration: 0.9, delay: 0.45, ease: [0.2, 0.75, 0.2, 1] }}
            >
              <Underlined delay={1.3} width={4} className="display-italic text-ember-500">
                sunset.
              </Underlined>
            </motion.span>
          </span>
        </h1>

        <motion.p
          initial={{ opacity: 0, y: 24 }}
          animate={ready ? { opacity: 1, y: 0 } : undefined}
          transition={{ duration: 0.9, delay: 0.8 }}
          className="mt-8 max-w-2xl text-lg leading-relaxed text-peach-200/90 sm:text-xl"
        >
          The Instant sync engine, rewritten from scratch in <strong className="font-semibold text-ember-300">Rust</strong>{" "}
          and open source. Your existing{" "}
          <code className="rounded bg-plum-800/70 px-1.5 py-0.5 font-mono text-[0.85em] text-ember-300">@instantdb</code>{" "}
          clients connect unmodified: to your own Postgres, on your own servers.
        </motion.p>

        <motion.div
          initial={{ opacity: 0, y: 24 }}
          animate={ready ? { opacity: 1, y: 0 } : undefined}
          transition={{ duration: 0.9, delay: 1 }}
          className="mt-10 flex flex-col items-center gap-5 sm:flex-row sm:gap-7"
        >
          <HandButton href={DASH_URL} delay={1.5}>
            Try the demo server
          </HandButton>
          <HandButton href={GITHUB_URL} variant="ghost" delay={1.7}>
            Read the source
          </HandButton>
        </motion.div>
        <RustStamp show={ready} delay={1.8} className="mt-10 h-36 w-36 lg:hidden" />
      </motion.div>

      <Horizon
        sunY={sunY}
        glow={glow}
        reflect={reflect}
        show={ready}
        className="pointer-events-none relative -z-10 mt-auto -mb-px block h-[clamp(300px,48svh,560px)] w-full overflow-visible sm:-mt-8"
      />
    </section>
  );
}
