// A hand-drawn crab (Rust's unofficial mascot is one). Legs are separate
// groups so callers can drive them: a CSS wiggle, or a scroll-linked stride.
import { useMemo } from "react";
import { motion, type MotionValue } from "motion/react";
import { Drawn, INK, sketch } from "./rough";

export function Crab({
  className = "",
  legsA,
  legsB,
  bob,
  wiggle = false,
  show = true,
}: {
  className?: string;
  /** Leg swing in degrees for each alternating set of legs. */
  legsA?: MotionValue<number>;
  legsB?: MotionValue<number>;
  bob?: MotionValue<number>;
  /** Idle CSS wiggle instead of a driven stride. */
  wiggle?: boolean;
  show?: boolean;
}) {
  const art = useMemo(() => {
    const ink = { stroke: INK.ember4, strokeWidth: 2.6, roughness: 0.9 };
    const leg = (x1: number, y1: number, x2: number, y2: number, x3: number, y3: number, seed: number) =>
      sketch((g) => g.linearPath([[x1, y1], [x2, y2], [x3, y3]], { ...ink, seed, disableMultiStroke: true }));
    return {
      legsA: [
        leg(38, 52, 22, 54, 14, 66, 1),
        leg(42, 60, 28, 66, 24, 78, 2),
        leg(82, 56, 98, 60, 106, 72, 3),
      ],
      legsB: [
        leg(40, 56, 24, 60, 18, 72, 4),
        leg(78, 52, 96, 54, 104, 66, 5),
        leg(80, 60, 92, 66, 98, 78, 6),
      ],
      body: sketch((g) => [
        g.ellipse(60, 50, 64, 40, { fill: INK.ember, fillStyle: "solid", stroke: "none", roughness: 0.8, seed: 7 }),
        g.ellipse(60, 50, 64, 40, {
          ...ink,
          fill: INK.ember3,
          fillStyle: "hachure",
          hachureGap: 5,
          hachureAngle: -40,
          fillWeight: 1.4,
          seed: 8,
        }),
        g.arc(60, 54, 16, 10, 0.2, Math.PI - 0.2, false, { ...ink, stroke: INK.plum950, strokeWidth: 2, seed: 9 }),
      ]),
      eyes: sketch((g) => [
        g.line(50, 34, 46, 20, { ...ink, seed: 10 }),
        g.line(70, 34, 74, 20, { ...ink, seed: 11 }),
        g.circle(45, 17, 11, { fill: INK.cream, fillStyle: "solid", stroke: INK.ember4, strokeWidth: 2, roughness: 0.6, seed: 12 }),
        g.circle(75, 17, 11, { fill: INK.cream, fillStyle: "solid", stroke: INK.ember4, strokeWidth: 2, roughness: 0.6, seed: 13 }),
        g.circle(46, 18, 4, { fill: INK.plum950, fillStyle: "solid", stroke: "none", seed: 14 }),
        g.circle(76, 18, 4, { fill: INK.plum950, fillStyle: "solid", stroke: "none", seed: 15 }),
      ]),
      claws: sketch((g) => [
        g.linearPath([[32, 44], [22, 38], [16, 28]], { ...ink, seed: 16, disableMultiStroke: true }),
        g.arc(14, 22, 18, 18, Math.PI * 0.55, Math.PI * 1.95, false, { ...ink, fill: INK.ember, fillStyle: "solid", seed: 17 }),
        g.linearPath([[88, 44], [98, 38], [104, 28]], { ...ink, seed: 18, disableMultiStroke: true }),
        g.arc(106, 22, 18, 18, Math.PI * 1.05, Math.PI * 2.45, false, { ...ink, fill: INK.ember, fillStyle: "solid", seed: 19 }),
      ]),
    };
  }, []);

  const pivot = { transformBox: "view-box", transformOrigin: "60px 52px" } as const;
  return (
    <svg viewBox="0 -2 120 84" className={className} aria-hidden="true">
      <motion.g
        className={wiggle ? "crab-legs-a" : undefined}
        style={{ ...pivot, rotate: legsA }}
      >
        {art.legsA.map((p, i) => (
          <Drawn key={i} paths={p} show={show} duration={0.3} delay={0.1 * i} />
        ))}
      </motion.g>
      <motion.g
        className={wiggle ? "crab-legs-b" : undefined}
        style={{ ...pivot, rotate: legsB }}
      >
        {art.legsB.map((p, i) => (
          <Drawn key={i} paths={p} show={show} duration={0.3} delay={0.1 * i + 0.05} />
        ))}
      </motion.g>
      <motion.g style={{ y: bob }}>
        <Drawn paths={art.claws} show={show} duration={0.4} stagger={0.08} />
        <Drawn paths={art.body} show={show} duration={0.6} stagger={0.1} />
        <Drawn paths={art.eyes} show={show} duration={0.3} stagger={0.05} delay={0.3} />
      </motion.g>
    </svg>
  );
}
