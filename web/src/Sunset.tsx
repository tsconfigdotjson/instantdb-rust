import { useEffect, useRef, useState } from "react";
import { MotionConfig, useMotionValue } from "motion/react";
import { Horizon } from "./components/Horizon";
import { Stars } from "./components/Sky";
import { Heading, Kicker } from "./components/Section";
import { HandButton, LogoMark } from "./components/doodles";
import { GITHUB_URL } from "./config";
import { SELF_HOST_PROMPT, SUNSET } from "./sunset";

/** /dashboard: where the old demo links land now that the hosted demo is gone. */
export function Sunset() {
  // The sun sits most of the way down: the demo has set, the project hasn't.
  const sunY = useMotionValue(170);
  const glow = useMotionValue(0.35);
  const reflect = useMotionValue(0.4);
  const [ready, setReady] = useState(false);
  useEffect(() => {
    const t = setTimeout(() => setReady(true), 250);
    return () => clearTimeout(t);
  }, []);

  return (
    <MotionConfig reducedMotion="user">
      <div className="grain">
        <main className="relative isolate flex min-h-svh flex-col overflow-hidden bg-[linear-gradient(180deg,#12051f_0%,#1c0932_30%,#34145c_60%,#5a1d6e_82%,#7a2a68_100%)]">
          <Stars className="absolute inset-x-0 top-0 -z-10 h-[70%] w-full" />

          <header className="mx-auto flex w-full max-w-5xl items-center px-4 py-3 sm:px-8">
            <a href="/" className="flex items-center gap-2.5" aria-label="InstantDB Rust, home">
              <LogoMark className="h-8 w-11" />
              <span className="display text-xl font-semibold text-ember-400">
                instantdb
                <span className="ml-1 inline-block -rotate-6 font-hand text-2xl font-bold text-ember-300">rust</span>
              </span>
            </a>
          </header>

          <div className="mx-auto w-full max-w-3xl px-4 pt-16 pb-12 text-center sm:px-8 sm:pt-24">
            <Heading kicker={SUNSET.kicker} title={SUNSET.title} accent={SUNSET.accent} center />
            {SUNSET.body.map((p) => (
              <p key={p} className="mx-auto mt-6 max-w-2xl text-lg leading-relaxed text-peach-200/90 sm:text-xl">
                {p}
              </p>
            ))}

            <div className="mt-14 text-left">
              <Kicker>{SUNSET.promptKicker}</Kicker>
              <p className="mt-2 text-peach-200/65">{SUNSET.promptHint}</p>
              <PromptBlock text={SELF_HOST_PROMPT} />
            </div>

            <div className="mt-12 flex flex-col items-center justify-center gap-5 sm:flex-row sm:gap-7">
              <HandButton href="/" delay={0.6}>
                {SUNSET.home}
              </HandButton>
              <HandButton href={GITHUB_URL} variant="ghost" delay={0.8}>
                {SUNSET.github}
              </HandButton>
            </div>
          </div>

          <Horizon
            sunY={sunY}
            glow={glow}
            reflect={reflect}
            show={ready}
            className="pointer-events-none relative -z-10 mt-auto -mb-px block h-[clamp(220px,36svh,420px)] w-full overflow-visible"
          />
        </main>
      </div>
    </MotionConfig>
  );
}

function PromptBlock({ text }: { text: string }) {
  const pre = useRef<HTMLPreElement>(null);
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    if (!copied) return;
    const t = setTimeout(() => setCopied(false), 2000);
    return () => clearTimeout(t);
  }, [copied]);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
    } catch {
      // No clipboard access (insecure context, denied permission): select the
      // text so a plain Ctrl/Cmd+C still works.
      const range = document.createRange();
      range.selectNodeContents(pre.current!);
      getSelection()?.removeAllRanges();
      getSelection()?.addRange(range);
    }
  };

  return (
    <div className="relative mt-5 rounded-[18px_12px_20px_10px] bg-plum-950/80 shadow-[0_18px_40px_-12px_rgba(0,0,0,0.55)] ring-1 ring-peach-200/10">
      <button
        type="button"
        onClick={copy}
        className="absolute top-3 right-3 rounded-[12px_9px_13px_8px] bg-ember-500 px-4 py-2 text-[15px] font-semibold text-plum-950 transition-all hover:-translate-y-0.5 hover:bg-ember-400"
      >
        <span aria-live="polite">{copied ? SUNSET.copied : SUNSET.copy}</span>
      </button>
      <pre
        ref={pre}
        className="max-h-[28rem] overflow-auto px-5 pt-16 pb-5 font-mono text-[13px] leading-relaxed whitespace-pre text-peach-200/85 sm:px-6"
      >
        <code>{text}</code>
      </pre>
    </div>
  );
}
