// Section break: a crab scuttles sideways along a pen line as you scroll,
// legs moving only while the page moves, then splits into three replicas.
import { useMemo, useRef } from "react";
import { motion, useScroll, useTransform } from "motion/react";
import { Crab } from "./Crab";
import { INK, sketch } from "./rough";

export function CrabWalk() {
  const ref = useRef<HTMLDivElement>(null);
  const { scrollYProgress } = useScroll({ target: ref, offset: ["start 0.95", "end 0.4"] });
  const walk = useTransform(scrollYProgress, [0, 0.72], [0, 1], { clamp: true });
  const left = useTransform(walk, (w) => `${w * 100}%`);
  const trail = useTransform(walk, (w) => `inset(0 ${100 - w * 100}% 0 0)`);
  const legsA = useTransform(scrollYProgress, (p) => Math.sin(p * 90) * 13 * (p < 0.72 ? 1 : 0));
  const legsB = useTransform(legsA, (v) => -v);
  const bob = useTransform(scrollYProgress, (p) => Math.abs(Math.sin(p * 90)) * -2.5 * (p < 0.72 ? 1 : 0));
  const split = useTransform(scrollYProgress, [0.74, 0.92], [0, 1], { clamp: true });
  const cloneOne = useTransform(split, [0, 1], ["0%", "-105%"]);
  const cloneTwo = useTransform(split, [0, 1], ["0%", "-210%"]);
  const cloneFade = useTransform(split, [0, 0.4], [0, 1]);
  const note = useTransform(scrollYProgress, [0.86, 0.98], [0, 1]);

  const ground = useMemo(
    () =>
      sketch((g) =>
        g.curve(
          [
            [0, 14], [160, 10], [340, 17], [520, 11], [700, 16], [880, 10], [1060, 16], [1200, 12],
          ],
          { stroke: INK.plum400, strokeWidth: 2, roughness: 1.2, seed: 91 },
        ),
      ),
    [],
  );

  return (
    <div ref={ref} className="relative my-24 h-40 sm:my-32 sm:h-44" aria-hidden="true">
      <svg viewBox="0 0 1200 28" preserveAspectRatio="none" className="absolute inset-x-0 bottom-3 h-7 w-full">
        {ground.map((p, i) => (
          <path key={i} d={p.d} stroke={p.stroke} strokeWidth={p.strokeWidth} fill="none" strokeLinecap="round" />
        ))}
      </svg>
      {/* footprints, revealed up to wherever the crab has got to */}
      <motion.div
        style={{ clipPath: trail }}
        className="absolute inset-x-0 bottom-7 h-3.5 bg-[radial-gradient(circle_at_20%_25%,#ffb77f_1.8px,transparent_2.4px),radial-gradient(circle_at_70%_75%,#ffb77f_1.8px,transparent_2.4px)] bg-[length:30px_14px] opacity-55"
      />
      <div className="absolute inset-y-0 left-0 right-[90px] sm:right-[120px]">
        <motion.div style={{ left }} className="absolute bottom-6 w-[90px] sm:w-[120px]">
          <motion.div style={{ x: cloneTwo, opacity: cloneFade }} className="absolute inset-0">
            <Crab className="w-full opacity-70" legsA={legsA} legsB={legsB} />
          </motion.div>
          <motion.div style={{ x: cloneOne, opacity: cloneFade }} className="absolute inset-0">
            <Crab className="w-full opacity-85" legsA={legsA} legsB={legsB} />
          </motion.div>
          <Crab className="relative w-full" legsA={legsA} legsB={legsB} bob={bob} />
        </motion.div>
      </div>
      <motion.p
        style={{ opacity: note }}
        className="absolute top-0 right-2 rotate-[-3deg] font-hand text-2xl text-ember-300 sm:right-10 sm:text-3xl"
      >
        one binary, as many copies as you like
      </motion.p>
    </div>
  );
}
