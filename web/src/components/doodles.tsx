// Small reusable marks: the logo, pen arrows and the hand-drawn button.
import { useId, type ReactNode } from "react";
import { INK, RoughOverlay, Sketch } from "./rough";

export function LogoMark({ className = "" }: { className?: string }) {
  const clip = `logo-${useId().replace(/:/g, "")}`;
  return (
    <svg viewBox="0 0 64 48" className={className} aria-hidden="true">
      <clipPath id={clip}>
        <rect x="0" y="0" width="64" height="30" />
      </clipPath>
      <circle cx="32" cy="30" r="17" fill={INK.ember} clipPath={`url(#${clip})`} />
      <path d="M13 22.5l-4-2M51 22.5l4-2M32 7.5V3M19 13l-3-3M45 13l3-3" stroke={INK.ember4} strokeWidth="2.6" strokeLinecap="round" />
      <path d="M3 30.5c9-1.2 18 .8 29-.3s19-.9 29 .2" stroke={INK.peach} strokeWidth="3" fill="none" strokeLinecap="round" />
      <path d="M20 37.5c4-.6 8 .3 12-.2M28 43c3-.4 5 .2 8-.1" stroke={INK.ember4} strokeWidth="2.5" fill="none" strokeLinecap="round" />
    </svg>
  );
}

/** A looping pen arrow. `d` is drawn in a 200x120 box; the head sits at its end. */
export function PenArrow({
  d,
  head,
  className = "",
  delay = 0,
  color = INK.ember3,
}: {
  d: string;
  head: [number, number][];
  className?: string;
  delay?: number;
  color?: string;
}) {
  return (
    <Sketch
      viewBox="0 0 200 120"
      className={className}
      delay={delay}
      duration={0.9}
      stagger={0.35}
      build={(g) => [
        g.path(d, { stroke: color, strokeWidth: 2.4, roughness: 0.9, disableMultiStroke: true }),
        g.linearPath(head, { stroke: color, strokeWidth: 2.4, roughness: 0.6, disableMultiStroke: true }),
      ]}
    />
  );
}

/** Pill-free button: a flat ember slab with a pen outline that slips on hover. */
export function HandButton({
  href,
  children,
  variant = "solid",
  className = "",
  delay = 0.4,
}: {
  href: string;
  children: ReactNode;
  variant?: "solid" | "ghost";
  className?: string;
  delay?: number;
}) {
  const solid = variant === "solid";
  const external = href.startsWith("http");
  return (
    <a
      href={href}
      {...(external ? { target: "_blank", rel: "noopener" } : {})}
      className={`group relative inline-flex items-center gap-3 px-7 py-4 text-lg font-semibold tracking-tight ${
        solid ? "text-plum-950" : "text-ember-300"
      } ${className}`}
    >
      <span
        aria-hidden="true"
        className={`absolute inset-0 rounded-[18px_12px_20px_10px] transition-transform duration-300 ease-out group-hover:-translate-x-1 group-hover:-translate-y-1 ${
          solid ? "bg-ember-500 group-hover:bg-ember-400" : "bg-plum-800/60 group-hover:bg-plum-700/70"
        }`}
      />
      <RoughOverlay
        className="-inset-[3px] translate-x-[5px] translate-y-[5px] transition-transform duration-300 group-hover:translate-x-[7px] group-hover:translate-y-[7px]"
        delay={delay}
        duration={1}
        stagger={0.3}
        amount={0.2}
        build={(g, w, h) =>
          g.rectangle(2, 2, w - 4, h - 4, {
            stroke: solid ? INK.peach : INK.ember,
            strokeWidth: 1.8,
            roughness: 1.6,
            bowing: 1.5,
          })
        }
      />
      <span className="relative">{children}</span>
      <svg
        viewBox="0 0 32 16"
        className="relative h-4 w-8 transition-transform duration-300 group-hover:translate-x-1.5"
        aria-hidden="true"
      >
        <path
          d="M2 8.5c7-.8 15 .6 26-.4M21 2.5c2.5 2 4.6 3.6 7.2 5.6-2.4 2-4.5 3.6-7 5.5"
          fill="none"
          stroke="currentColor"
          strokeWidth="2.4"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </a>
  );
}
