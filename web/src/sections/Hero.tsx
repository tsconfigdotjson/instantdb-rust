import { useEffect, useRef, useState } from "react";
import { motion, useScroll, useTransform } from "motion/react";
import { Horizon } from "../components/Horizon";
import { Gulls, Stars } from "../components/Sky";
import { HandButton, PenArrow } from "../components/doodles";
import { Crab } from "../components/Crab";
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
        <Scrawl className="mb-6 -rotate-3 text-2xl text-ember-300 sm:mb-8 sm:text-3xl" delay={0.9}>
          psst: Instant Cloud shuts down Aug&nbsp;31,&nbsp;2027
        </Scrawl>

        <div className="relative w-full">
          <h1 className="display text-[clamp(3.5rem,11vw,9.75rem)] leading-[0.86] font-medium text-ember-400">
            <RevealWords text="Instant," show={ready} delay={0.1} />
            <br />
            <RevealWords text="after the" show={ready} delay={0.25} />{" "}
            <span className="relative inline-block">
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
              {/* the crab drops onto the end of the underline once it's drawn */}
              <motion.span
                className="absolute right-[-0.46em] bottom-[-0.06em] w-[0.52em]"
                initial={{ y: "-160%", opacity: 0, rotate: -25 }}
                animate={ready ? { y: "0%", opacity: 1, rotate: 0 } : undefined}
                transition={{ type: "spring", stiffness: 260, damping: 13, delay: 2.1 }}
                aria-hidden="true"
              >
                <Crab className="w-full" wiggle show={ready} />
              </motion.span>
            </span>
          </h1>
          <div className="pointer-events-none absolute top-[1.25rem] left-[calc(50%+11rem)] hidden rotate-[5deg] text-left lg:block xl:left-[calc(50%+16rem)]">
            <Scrawl className="text-[3.1rem] leading-[0.95] text-ember-300" delay={1.7}>
              rewritten
              <br />
              in{" "}
              <Underlined delay={2.6} width={2.6} color="#ff9a55">
                Rust!
              </Underlined>
            </Scrawl>
            <PenArrow
              className="-mt-1 -ml-10 h-20 w-32"
              delay={2.4}
              d="M150 8C146 56 110 88 44 100"
              head={[[62, 86], [44, 100], [64, 112]]}
            />
          </div>
        </div>
        <Scrawl className="mt-5 -rotate-2 text-4xl text-ember-300 lg:hidden" delay={1.7}>
          rewritten in{" "}
          <Underlined delay={2.6} width={2.4} color="#ff9a55">
            Rust!
          </Underlined>
        </Scrawl>

        <motion.p
          initial={{ opacity: 0, y: 24 }}
          animate={ready ? { opacity: 1, y: 0 } : undefined}
          transition={{ duration: 0.9, delay: 0.8 }}
          className="mt-8 max-w-2xl text-lg leading-relaxed text-peach-200/90 sm:text-xl"
        >
          The Instant sync engine, rebuilt from scratch and open source. Your existing{" "}
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
