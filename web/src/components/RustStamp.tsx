// A rubber stamp that thumps down onto the hero: "rewritten in Rust".
import { useId, useMemo } from "react";
import { motion } from "motion/react";
import { Crab } from "./Crab";
import { Drawn, INK, sketch } from "./rough";

export function RustStamp({ show, delay = 0, className = "" }: { show: boolean; delay?: number; className?: string }) {
  const id = useId().replace(/:/g, "");
  const rings = useMemo(
    () =>
      sketch((g) => [
        g.circle(100, 100, 188, { stroke: INK.ember, strokeWidth: 4, roughness: 1.3, seed: 51 }),
        g.circle(100, 100, 120, { stroke: INK.ember, strokeWidth: 3, roughness: 1.1, seed: 52 }),
      ]),
    [],
  );
  return (
    <motion.div
      className={`pointer-events-none ${className}`}
      initial={{ scale: 2.6, opacity: 0, rotate: -40 }}
      animate={show ? { scale: 1, opacity: 1, rotate: -12 } : undefined}
      transition={{ type: "spring", stiffness: 320, damping: 17, mass: 1.1, delay }}
      aria-hidden="true"
    >
      <div className="relative h-full w-full">
        <svg viewBox="0 0 200 200" className="absolute inset-0 h-full w-full">
          <defs>
            <path id={`${id}-ring`} d="M100,100 m-75,0 a75,75 0 1,1 150,0 a75,75 0 1,1 -150,0" />
            {/* Worn-ink texture: noise punches small holes in the print. */}
            <filter id={`${id}-worn`}>
              <feTurbulence type="fractalNoise" baseFrequency="0.95" numOctaves="2" seed="4" result="noise" />
              <feColorMatrix in="noise" type="matrix" values="0 0 0 0 0  0 0 0 0 0  0 0 0 0 0  -2.2 0 0 0 2.05" result="mask" />
              <feComposite in="SourceGraphic" in2="mask" operator="in" />
            </filter>
          </defs>
          <circle cx="100" cy="100" r="92" fill={INK.ember} opacity="0.1" />
          <g filter={`url(#${id}-worn)`}>
            <Drawn paths={rings} show={show} duration={0.01} delay={delay} />
            <g className="spin-slow" style={{ transformBox: "view-box", transformOrigin: "100px 100px" }}>
              <text
                fill={INK.ember}
                fontFamily="Fraunces Variable, serif"
                fontWeight={800}
                fontSize="16.5"
              >
                <textPath href={`#${id}-ring`} textLength="468" lengthAdjust="spacing">
                  REWRITTEN IN RUST • REWRITTEN IN RUST •
                </textPath>
              </text>
            </g>
          </g>
        </svg>
        <div className="absolute inset-[27%] flex items-center justify-center">
          <Crab className="w-full" wiggle show={show} />
        </div>
      </div>
    </motion.div>
  );
}
