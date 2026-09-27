// A sticker-seal that thumps down onto the hero: "rewritten in Rust".
// Solid shapes and plain vector type keep it crisp next to the headline;
// only the crab in the middle is hand-drawn.
import { useId } from "react";
import { motion } from "motion/react";
import { Crab } from "./Crab";
import { INK } from "./rough";

/** A scalloped disc edge: `bumps` rounded lobes between radii `inner` and `outer`. */
function scallop(cx: number, cy: number, inner: number, outer: number, bumps: number) {
  const at = (a: number, r: number) => `${(cx + Math.cos(a) * r).toFixed(2)},${(cy + Math.sin(a) * r).toFixed(2)}`;
  const step = (Math.PI * 2) / bumps;
  let d = `M${at(0, inner)}`;
  for (let i = 0; i < bumps; i++) {
    d += ` Q${at(i * step + step / 2, outer + (outer - inner))} ${at((i + 1) * step, inner)}`;
  }
  return `${d}Z`;
}

const EDGE = scallop(100, 100, 90, 95, 24);

export function RustStamp({ show, delay = 0, className = "" }: { show: boolean; delay?: number; className?: string }) {
  const id = useId().replace(/:/g, "");
  return (
    <motion.div
      className={`pointer-events-none ${className}`}
      // Motion's automatic will-change would keep the badge rasterized at
      // its mid-animation scale; let it re-render crisply once it lands.
      style={{ willChange: "auto" }}
      initial={{ scale: 2.4, opacity: 0, rotate: -40 }}
      animate={show ? { scale: 1, opacity: 1, rotate: -10 } : undefined}
      transition={{ type: "spring", stiffness: 320, damping: 17, mass: 1.1, delay }}
      aria-hidden="true"
    >
      <div className="relative h-full w-full">
        <svg viewBox="-4 -4 212 212" className="absolute inset-0 h-full w-full">
          <defs>
            <path id={`${id}-ring`} d="M100,100 m-67,0 a67,67 0 1,1 134,0 a67,67 0 1,1 -134,0" />
          </defs>
          <path d={EDGE} transform="translate(4 6)" fill={INK.plum950} opacity="0.55" />
          <path d={EDGE} fill={INK.ember} />
          <circle cx="100" cy="100" r="81" fill="none" stroke={INK.plum950} strokeWidth="2" />
          <circle cx="100" cy="100" r="56" fill={INK.plum900} stroke={INK.plum950} strokeWidth="2.5" />
          <text
            fill={INK.plum950}
            fontFamily="DM Sans Variable, sans-serif"
            fontWeight={800}
            fontSize="14.5"
            letterSpacing="1.5"
          >
            <textPath href={`#${id}-ring`} textLength="416" lengthAdjust="spacing">
              REWRITTEN IN RUST ★ REWRITTEN IN RUST ★
            </textPath>
          </text>
        </svg>
        <div className="absolute inset-[29%] flex items-center justify-center">
          <Crab className="w-full" wiggle show={show} />
        </div>
      </div>
    </motion.div>
  );
}
