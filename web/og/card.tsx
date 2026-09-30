// Mounts the site's hand-drawn pieces into the card's [data-slot]s with every
// animation at its end state, then flags <html data-ready> for the generator.
import { createElement, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { flushSync } from "react-dom";
import { MotionGlobalConfig, motionValue } from "motion/react";
import { Horizon } from "../src/components/Horizon";
import { Stars } from "../src/components/Sky";
import { Crab } from "../src/components/Crab";
import { LogoMark } from "../src/components/doodles";
import { Underlined } from "../src/components/type";

MotionGlobalConfig.skipAnimations = true;
document.documentElement.classList.add("still");

const still = (v: number) => motionValue(v);

const slots: Record<string, (text: string) => ReactNode> = {
  stars: () => createElement(Stars, { count: 30, seed: 9 }),
  horizon: () =>
    createElement(Horizon, {
      sunY: still(0),
      glow: still(1),
      reflect: still(1),
      show: true,
    }),
  logo: () => createElement(LogoMark),
  crab: () => createElement(Crab, { show: true }),
  underline: (text) => createElement(Underlined, { width: 4, children: text }),
  "underline-small": (text) => createElement(Underlined, { width: 2.4, color: "#ff9a55", children: text }),
};

for (const el of document.querySelectorAll<HTMLElement>("[data-slot]")) {
  const render = slots[el.dataset.slot!];
  const text = el.textContent ?? "";
  flushSync(() => createRoot(el).render(render(text)));
}

await document.fonts.ready;
// Two frames: rough overlays measure their boxes after the first layout.
requestAnimationFrame(() => requestAnimationFrame(() => (document.documentElement.dataset.ready = "")));
