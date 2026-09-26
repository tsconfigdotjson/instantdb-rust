import { MotionConfig } from "motion/react";
import { Nav } from "./sections/Nav";
import { Hero } from "./sections/Hero";
import { Timeline } from "./sections/Timeline";
import { Compatible } from "./sections/Compatible";
import { Features } from "./sections/Features";
import { Performance } from "./sections/Performance";
import { Parity } from "./sections/Parity";
import { SelfHost } from "./sections/SelfHost";
import { Demo } from "./sections/Demo";
import { Footer } from "./sections/Footer";

export function App() {
  return (
    <MotionConfig reducedMotion="user">
      <div className="grain">
        <Nav />
        <main>
          <Hero />
          <Timeline />
          <Compatible />
          <Features />
          <Performance />
          <Parity />
          <SelfHost />
          <Demo />
        </main>
        <Footer />
      </div>
    </MotionConfig>
  );
}
