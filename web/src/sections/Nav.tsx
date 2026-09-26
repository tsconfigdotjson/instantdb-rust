import { useState } from "react";
import { motion, useMotionValueEvent, useScroll } from "motion/react";
import { LogoMark } from "../components/doodles";
import { DASH_URL, GITHUB_URL } from "../config";

const links = [
  { href: "#why", label: "Why" },
  { href: "#compatible", label: "Compatibility" },
  { href: "#speed", label: "Performance" },
  { href: "#parity", label: "Parity" },
  { href: "#self-host", label: "Self-host" },
];

export function Nav() {
  const { scrollY } = useScroll();
  const [solid, setSolid] = useState(false);
  useMotionValueEvent(scrollY, "change", (y) => setSolid(y > 40));

  return (
    <motion.header
      initial={{ y: -80, opacity: 0 }}
      animate={{ y: 0, opacity: 1 }}
      transition={{ duration: 0.8, delay: 0.2, ease: [0.2, 0.75, 0.2, 1] }}
      className={`fixed inset-x-0 top-0 z-50 transition-colors duration-500 ${
        solid ? "bg-plum-950/75 backdrop-blur-md shadow-[0_1px_0_rgba(255,214,176,0.08)]" : ""
      }`}
    >
      <nav className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-3 sm:px-8">
        <a href="#top" className="flex items-center gap-2.5" aria-label="InstantDB Rust, home">
          <LogoMark className="h-8 w-11" />
          <span className="display text-xl font-semibold text-ember-400">
            instantdb
            <span className="ml-1 inline-block -rotate-6 font-hand text-2xl font-bold text-ember-300">rust</span>
          </span>
        </a>
        <ul className="hidden items-center gap-7 text-[15px] text-peach-200/80 lg:flex">
          {links.map((l) => (
            <li key={l.href}>
              <a href={l.href} className="transition-colors hover:text-ember-300">
                {l.label}
              </a>
            </li>
          ))}
        </ul>
        <div className="flex items-center gap-2 sm:gap-4">
          <a
            href={GITHUB_URL}
            target="_blank"
            rel="noopener"
            className="hidden text-[15px] text-peach-200/80 transition-colors hover:text-ember-300 sm:inline"
          >
            GitHub
          </a>
          <a
            href={DASH_URL}
            target="_blank"
            rel="noopener"
            className="rounded-[12px_9px_13px_8px] bg-ember-500 px-4 py-2 text-[15px] font-semibold text-plum-950 transition-all hover:-translate-y-0.5 hover:bg-ember-400"
          >
            Try the demo
          </a>
        </div>
      </nav>
    </motion.header>
  );
}
