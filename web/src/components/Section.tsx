import type { ReactNode } from "react";
import { RevealWords, Scrawl } from "./type";

export function Kicker({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <Scrawl className={`-rotate-2 text-2xl text-ember-300 sm:text-[1.7rem] ${className}`}>{children}</Scrawl>;
}

/** Section title: a handwritten kicker over a big display line that rises in. */
export function Heading({
  kicker,
  title,
  accent,
  className = "",
  center = false,
}: {
  kicker: string;
  title: string;
  /** Optional trailing phrase rendered in italic ember. */
  accent?: string;
  className?: string;
  center?: boolean;
}) {
  return (
    <div className={`${center ? "text-center" : ""} ${className}`}>
      <Kicker>{kicker}</Kicker>
      <h2 className="display mt-3 text-[clamp(2.5rem,6.2vw,5.25rem)] leading-[0.95] font-medium text-ember-400">
        <RevealWords text={title} />
        {accent ? (
          <>
            {" "}
            <RevealWords text={accent} delay={0.25} wordClassName="display-italic text-ember-500" />
          </>
        ) : null}
      </h2>
    </div>
  );
}
