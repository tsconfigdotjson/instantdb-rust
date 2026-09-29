import { useMemo, useRef } from "react";
import { motion, useScroll, useTransform } from "motion/react";
import { Heading } from "../components/Section";
import { INK, Sketch, sketch } from "../components/rough";
import { SUNSET_ESSAY_URL } from "../config";

const stops = [
  {
    when: "Aug 2026",
    title: "The team joins OpenAI",
    body: "New Instant Cloud signups close. Subscriptions started after July 31 are refunded.",
  },
  {
    when: "Aug 31, 2027",
    title: "Cloud apps shut down",
    body: "Every app still hosted on Instant Cloud stops serving traffic.",
  },
  {
    when: "Aug 31, 2028",
    title: "Backups are deleted",
    body: "The last cloud backups go away with the service.",
  },
  {
    when: "and after",
    title: "Your app keeps syncing",
    body: "Same clients, same queries, same permission rules, on InstantDB Rust and hardware you control.",
    hope: true,
  },
];

export function Timeline() {
  const ref = useRef<HTMLDivElement>(null);
  const { scrollYProgress } = useScroll({ target: ref, offset: ["start 0.85", "end 0.55"] });
  const draw = useTransform(scrollYProgress, [0, 1], [0, 1]);

  const line = useMemo(
    () =>
      sketch((g) =>
        g.curve(
          [
            [10, 30], [180, 22], [330, 36], [480, 26], [640, 34], [800, 24], [960, 34], [1110, 28], [1190, 30],
          ],
          { stroke: INK.ember3, strokeWidth: 2.4, roughness: 0.8, disableMultiStroke: true, seed: 4 },
        ),
      )[0],
    [],
  );

  return (
    <section id="why" className="relative mx-auto max-w-7xl scroll-mt-24 px-4 py-28 sm:px-8 sm:py-36">
      <div className="grid gap-10 lg:grid-cols-[1.25fr_1fr] lg:items-end">
        <Heading kicker="so, what happened?" title="Instant Cloud is" accent="going dark." />
        <p className="max-w-xl text-lg leading-relaxed text-peach-200/85">
          In August 2026 the Instant team{" "}
          <a href={SUNSET_ESSAY_URL} target="_blank" rel="noopener" className="text-ember-300 underline decoration-dashed underline-offset-4 hover:text-ember-400">
            joined OpenAI
          </a>
          . The hosted service is winding down on a fixed schedule. Instant's code stays open source, and this project
          makes self-hosting it light enough to run anywhere.
        </p>
      </div>

      <div ref={ref} className="relative mt-20">
        {/* desktop: a pen line that draws left to right as you scroll */}
        <svg viewBox="0 0 1200 60" preserveAspectRatio="none" className="absolute inset-x-0 top-0 hidden h-[60px] w-full md:block" aria-hidden="true">
          <motion.path d={line.d} stroke={line.stroke} strokeWidth={line.strokeWidth} fill="none" strokeLinecap="round" style={{ pathLength: draw }} />
        </svg>
        {/* mobile: the same idea, top to bottom */}
        <motion.div
          style={{ scaleY: draw }}
          className="absolute top-2 bottom-8 left-[19px] w-[2.5px] origin-top rounded-full bg-ember-300/80 md:hidden"
        />

        <ol className="relative grid gap-12 md:grid-cols-4 md:gap-8">
          {stops.map((s, i) => (
            <motion.li
              key={s.when}
              className="relative pl-14 md:pl-0 md:pt-20"
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, amount: 0.5 }}
              transition={{ duration: 0.8, delay: i * 0.12, ease: [0.2, 0.75, 0.2, 1] }}
            >
              <Marker hope={!!s.hope} delay={0.2 + i * 0.15} />
              <p className={`font-hand text-3xl ${s.hope ? "text-ember-400" : "text-ember-300"}`}>{s.when}</p>
              <h3 className={`display mt-1 text-2xl font-medium ${s.hope ? "text-ember-400" : "text-cream-100"}`}>{s.title}</h3>
              <p className="mt-2 leading-relaxed text-peach-200/75">{s.body}</p>
            </motion.li>
          ))}
        </ol>
      </div>
    </section>
  );
}

function Marker({ hope, delay }: { hope: boolean; delay: number }) {
  return (
    <Sketch
      viewBox="0 0 60 60"
      delay={delay}
      duration={0.8}
      stagger={0.2}
      className="absolute top-0 left-0 h-10 w-10 md:top-2 md:left-[-6px] md:h-14 md:w-14"
      build={(g) =>
        hope
          ? [
              g.circle(30, 30, 34, { fill: INK.ember, fillStyle: "solid", stroke: "none", seed: 2 }),
              g.circle(30, 30, 34, { fill: INK.ember3, fillStyle: "hachure", hachureGap: 5, stroke: INK.ember4, strokeWidth: 2.2, seed: 3 }),
              ...[0, 1, 2, 3, 4, 5, 6, 7].map((k) => {
                const a = (k / 8) * Math.PI * 2;
                return g.line(30 + Math.cos(a) * 21, 30 + Math.sin(a) * 21, 30 + Math.cos(a) * 28, 30 + Math.sin(a) * 28, {
                  stroke: INK.ember4,
                  strokeWidth: 2.2,
                  roughness: 0.5,
                  seed: 10 + k,
                });
              }),
            ]
          : [
              g.circle(30, 30, 26, { fill: INK.plum900, fillStyle: "solid", stroke: "none", seed: 5 }),
              g.circle(30, 30, 26, { stroke: INK.ember3, strokeWidth: 2.4, roughness: 1.6, seed: 6 }),
            ]
      }
    />
  );
}
