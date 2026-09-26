// Typographic motion: words that rise into place, handwriting that "writes"
// itself left to right, pen underlines and numbers that count up.
import { useEffect, useRef, useState, type ReactNode } from "react";
import { animate, motion, useInView } from "motion/react";
import { INK, RoughOverlay } from "./rough";

const rise = [0.2, 0.75, 0.2, 1] as const;

/** Splits a phrase into words that slide up out of a mask, one after another. */
export function RevealWords({
  text,
  className = "",
  wordClassName = "",
  delay = 0,
  stagger = 0.06,
  show,
}: {
  text: string;
  className?: string;
  wordClassName?: string;
  delay?: number;
  stagger?: number;
  show?: boolean;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  const inView = useInView(ref, { once: true, amount: 0.5 });
  const on = show ?? inView;
  const words = text.split(" ");
  return (
    <span ref={ref} className={className}>
      {words.map((w, i) => (
        <span key={i}>
          <span className="inline-block overflow-hidden pb-[0.14em] -mb-[0.14em] pr-[0.06em] -mr-[0.06em] align-bottom">
            <motion.span
              className={`inline-block ${wordClassName}`}
              initial={{ y: "115%", rotate: 4 }}
              animate={on ? { y: "0%", rotate: 0 } : undefined}
              transition={{ duration: 0.9, delay: delay + i * stagger, ease: rise }}
            >
              {w}
            </motion.span>
          </span>
          {i < words.length - 1 ? " " : null}
        </span>
      ))}
    </span>
  );
}

/** Handwritten note that wipes in like it is being written. */
export function Scrawl({
  children,
  className = "",
  delay = 0,
  duration = 1.2,
}: {
  children: ReactNode;
  className?: string;
  delay?: number;
  duration?: number;
}) {
  // Observe an unclipped wrapper: Chrome counts the element's own clip-path
  // when deciding visibility, so a fully clipped span never "enters" view.
  const ref = useRef<HTMLSpanElement>(null);
  const inView = useInView(ref, { once: true, amount: 0.8 });
  return (
    <span ref={ref} className={`inline-block ${className}`}>
      <motion.span
        data-wipe
        className="inline-block px-1 py-1 font-hand"
        initial={{ clipPath: "inset(0 100% 0 0)" }}
        animate={inView ? { clipPath: "inset(0 0% 0 0)" } : undefined}
        transition={{ duration, delay, ease: "easeInOut" }}
      >
        {children}
      </motion.span>
    </span>
  );
}

/** Wavy pen underline drawn under its children. */
export function Underlined({
  children,
  className = "",
  color = INK.ember,
  delay = 0.6,
  width = 3,
}: {
  children: ReactNode;
  className?: string;
  color?: string;
  delay?: number;
  width?: number;
}) {
  return (
    <span className={`relative inline-block ${className}`}>
      {children}
      <RoughOverlay
        className="left-[-2%] right-[-2%] bottom-[-0.12em] h-[0.3em]"
        delay={delay}
        duration={0.8}
        build={(g, w, h) =>
          g.curve(
            [
              [0, h * 0.55],
              [w * 0.22, h * 0.3],
              [w * 0.5, h * 0.62],
              [w * 0.78, h * 0.28],
              [w, h * 0.5],
            ],
            { stroke: color, strokeWidth: width, roughness: 1.1, bowing: 2 },
          )
        }
      />
    </span>
  );
}

/** Counts up to a number the first time it scrolls into view. */
export function CountUp({
  to,
  decimals = 0,
  className = "",
  duration = 1.8,
}: {
  to: number;
  decimals?: number;
  className?: string;
  duration?: number;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  const inView = useInView(ref, { once: true, amount: 0.8 });
  const [value, setValue] = useState(0);
  useEffect(() => {
    if (!inView) return;
    const controls = animate(0, to, {
      duration,
      ease: [0.16, 1, 0.3, 1],
      onUpdate: setValue,
    });
    return () => controls.stop();
  }, [inView, to, duration]);
  return (
    <span ref={ref} className={`tabular-nums ${className}`}>
      {value.toLocaleString("en-US", { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}
    </span>
  );
}
