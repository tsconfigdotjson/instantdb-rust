// Twinkling sparkles and a couple of gulls for the dusk sky.
import { useMemo } from "react";
import { motion, type MotionValue } from "motion/react";
import { INK } from "./rough";

function prng(seed: number) {
  return () => {
    seed = (seed * 1664525 + 1013904223) % 4294967296;
    return seed / 4294967296;
  };
}

export function Stars({
  count = 34,
  seed = 9,
  opacity,
  className = "",
}: {
  count?: number;
  seed?: number;
  opacity?: MotionValue<number>;
  className?: string;
}) {
  const stars = useMemo(() => {
    const r = prng(seed);
    return Array.from({ length: count }, () => ({
      x: r() * 1440,
      y: r() * 420,
      s: 0.35 + r() * 0.9,
      d: r() * 4,
      c: r() > 0.72 ? INK.ember3 : INK.peach,
    }));
  }, [count, seed]);
  return (
    <motion.svg
      viewBox="0 0 1440 460"
      preserveAspectRatio="xMidYMin slice"
      className={className}
      style={opacity ? { opacity } : undefined}
      aria-hidden="true"
    >
      {stars.map((s, i) => (
        <g key={i} transform={`translate(${s.x} ${s.y}) scale(${s.s})`}>
          <path
            className="twinkle"
            style={{ animationDelay: `${-s.d}s` }}
            d="M0-9C.6-3 3-.7 9 0 3 .7.6 3 0 9-.6 3-3 .7-9 0-3-.7-.6-3 0-9Z"
            fill={s.c}
          />
        </g>
      ))}
    </motion.svg>
  );
}

export function Gulls({ className = "" }: { className?: string }) {
  return (
    <div className={`pointer-events-none absolute inset-x-0 ${className}`} aria-hidden="true">
      <div className="drift" style={{ animationDelay: "-12s" }}>
        <svg viewBox="0 0 80 40" className="h-8 w-16 text-peach-200/80">
          <path className="flap" d="M4 20c6-8 12-8 16 0 4-8 10-8 16 0" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" />
          <path className="flap" style={{ animationDelay: "-.3s" }} d="M44 30c4-6 9-6 12 0 3-6 8-6 12 0" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
        </svg>
      </div>
    </div>
  );
}
