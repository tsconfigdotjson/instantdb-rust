// Hand-drawn strokes. rough.js turns geometry into wobbly, double-stroked
// pen lines; motion animates each path's length so it draws itself in.
import { useLayoutEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { motion, useInView } from "motion/react";
import rough from "roughjs";
import type { Drawable, PathInfo } from "roughjs/bin/core";
import type { RoughGenerator } from "roughjs/bin/generator";

export const INK = {
  ember: "#ff7a2f",
  ember4: "#ff9a55",
  ember3: "#ffb77f",
  peach: "#ffd6b0",
  cream: "#fff1e2",
  plum950: "#12051f",
  plum900: "#1c0932",
  plum800: "#2d1150",
  plum700: "#3e1a6b",
  plum600: "#552689",
  plum500: "#7340ad",
  plum400: "#9a6fd0",
  dusk: "#6b2172",
} as const;

const gen = rough.generator({
  options: { stroke: INK.ember, strokeWidth: 2, roughness: 1.3, bowing: 1.1 },
});

export type Build = (g: RoughGenerator) => Drawable | Drawable[];

export function sketch(build: Build): PathInfo[] {
  const out = build(gen);
  return (Array.isArray(out) ? out : [out]).flatMap((d) => gen.toPaths(d));
}

export const ease = [0.65, 0, 0.35, 1] as const;

type DrawProps = {
  paths: PathInfo[];
  show: boolean;
  duration?: number;
  delay?: number;
  stagger?: number;
};

/** Renders sketched paths; each strokes itself in once `show` flips on. */
export function Drawn({ paths, show, duration = 0.9, delay = 0, stagger = 0.06 }: DrawProps) {
  return (
    <>
      {paths.map((p, i) => {
        const at = delay + i * stagger;
        const solid = p.fill && p.fill !== "none";
        if (solid) {
          return (
            <motion.path
              key={i}
              d={p.d}
              fill={p.fill}
              stroke="none"
              initial={{ opacity: 0 }}
              animate={show ? { opacity: 1 } : { opacity: 0 }}
              transition={{ duration: duration * 0.8, delay: at }}
            />
          );
        }
        return (
          <motion.path
            key={i}
            d={p.d}
            fill="none"
            stroke={p.stroke}
            strokeWidth={p.strokeWidth}
            strokeLinecap="round"
            strokeLinejoin="round"
            initial={{ pathLength: 0, opacity: 0 }}
            animate={show ? { pathLength: 1, opacity: 1 } : { pathLength: 0, opacity: 0 }}
            transition={{
              pathLength: { duration, delay: at, ease },
              opacity: { duration: 0.01, delay: at },
            }}
          />
        );
      })}
    </>
  );
}

type SketchProps = {
  viewBox: string;
  build: Build;
  className?: string;
  style?: CSSProperties;
  duration?: number;
  delay?: number;
  stagger?: number;
  /** Fraction of the element that must be visible before it draws. */
  amount?: number;
  /** Force the draw state instead of waiting for the element to scroll in. */
  show?: boolean;
  children?: ReactNode;
};

/** A fixed-viewBox doodle that draws itself when scrolled into view. */
export function Sketch({
  viewBox,
  build,
  className,
  style,
  duration,
  delay,
  stagger,
  amount = 0.5,
  show,
  children,
}: SketchProps) {
  const ref = useRef<SVGSVGElement>(null);
  const inView = useInView(ref, { once: true, amount });
  // Doodles are static art: build once per mount.
  const paths = useMemo(() => sketch(build), []);
  return (
    <svg ref={ref} viewBox={viewBox} className={className} style={style} fill="none" aria-hidden="true">
      <Drawn paths={paths} show={show ?? inView} duration={duration} delay={delay} stagger={stagger} />
      {children}
    </svg>
  );
}

function useBoxSize<T extends Element>() {
  const ref = useRef<T>(null);
  const [size, setSize] = useState<{ w: number; h: number } | null>(null);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([entry]) => {
      const w = Math.round(entry.contentRect.width);
      const h = Math.round(entry.contentRect.height);
      setSize((s) => (s && Math.abs(s.w - w) < 4 && Math.abs(s.h - h) < 4 ? s : { w, h }));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, size] as const;
}

type OverlayProps = {
  /** Geometry in the overlay's own pixel space. */
  build: (g: RoughGenerator, w: number, h: number) => Drawable | Drawable[];
  /** Positioning classes; the overlay is absolutely placed over its parent. */
  className?: string;
  show?: boolean;
  duration?: number;
  delay?: number;
  stagger?: number;
  amount?: number;
};

/** A doodle sized to whatever it overlays: underlines, loops, frames. */
export function RoughOverlay({
  build,
  className = "inset-0",
  show,
  duration,
  delay,
  stagger,
  amount = 0.6,
}: OverlayProps) {
  const [ref, size] = useBoxSize<HTMLSpanElement>();
  const inView = useInView(ref, { once: true, amount });
  const paths = useMemo(
    () => (size && size.w > 0 && size.h > 0 ? sketch((g) => build(g, size.w, size.h)) : []),
    [size],
  );
  // An absolutely placed <svg> won't stretch between its insets (it is a
  // replaced element), so a span takes the insets and the svg fills it.
  return (
    <span ref={ref} className={`pointer-events-none absolute block ${className}`} aria-hidden="true">
      <svg
        className="absolute inset-0 block h-full w-full overflow-visible"
        viewBox={size ? `0 0 ${size.w} ${size.h}` : undefined}
        fill="none"
      >
        <Drawn
          key={size ? `${size.w}x${size.h}` : "none"}
          paths={paths}
          show={show ?? inView}
          duration={duration}
          delay={delay}
          stagger={stagger}
        />
      </svg>
    </span>
  );
}

