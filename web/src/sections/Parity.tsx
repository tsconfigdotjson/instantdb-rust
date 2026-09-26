import baseline from "../../../scripts/differential/coverage-baseline.json";
import { Heading } from "../components/Section";
import { INK, RoughOverlay } from "../components/rough";
import { CountUp } from "../components/type";
import { doc } from "../config";

const covered: string[] = baseline.covered;

export function Parity() {
  const half = Math.ceil(covered.length / 2);
  const rows = [covered.slice(0, half), covered.slice(half)];

  return (
    <section id="parity" className="relative scroll-mt-24 py-28 sm:py-36">
      <div className="mx-auto grid max-w-7xl items-center gap-16 px-4 sm:px-8 lg:grid-cols-[1.3fr_1fr] lg:gap-24">
        <div>
          <Heading kicker="the parity harness" title="We don't assume parity." accent="We diff it." />
          <p className="mt-8 max-w-xl text-lg leading-relaxed text-peach-200/85">
            The official legacy server boots beside the Rust one, both get the exact same traffic, and every frame is
            compared the way the client SDK reads it. Any difference nobody has documented fails the run.
          </p>
        </div>

        <div className="text-center lg:text-left">
          <div className="relative inline-block px-4">
            <span className="display text-[clamp(5rem,13vw,10rem)] leading-none font-semibold text-ember-400">
              <CountUp to={covered.length} duration={2.4} />
              <span className="ml-1 text-[0.5em] text-ember-300">/{covered.length}</span>
            </span>
            <RoughOverlay
              className="-inset-x-8 -inset-y-10"
              delay={2}
              duration={1.3}
              stagger={0.3}
              build={(g, w, h) => g.ellipse(w / 2, h / 2, w, h, { stroke: INK.ember, strokeWidth: 3, roughness: 2.2, bowing: 2, seed: 13 })}
            />
          </div>
          <p className="mx-auto mt-10 max-w-xs text-lg text-peach-200/85 lg:mx-0">
            routes, ops, operators and error types diffed against legacy.{" "}
            <a className="text-ember-300 underline decoration-dashed underline-offset-4 hover:text-ember-400" href={doc("docs/PARITY.md")} target="_blank" rel="noopener">
              Gaps are listed.
            </a>
          </p>
        </div>
      </div>

      <div className="mt-24 space-y-4 overflow-hidden py-2 [mask-image:linear-gradient(90deg,transparent,black_12%,black_88%,transparent)]" aria-hidden="true">
        {rows.map((row, r) => (
          <div key={r} className={`flex w-max gap-3 ${r ? "marquee-reverse" : "marquee"}`}>
            {[...row, ...row].map((item, i) => (
              <span
                key={i}
                className="flex items-center gap-2 rounded-full border border-dashed border-plum-500/70 px-4 py-1.5 font-mono text-[13px] whitespace-nowrap text-peach-200/75"
              >
                <span className="text-ember-400">✓</span>
                {item}
              </span>
            ))}
          </div>
        ))}
      </div>
    </section>
  );
}
