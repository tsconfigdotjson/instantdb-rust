import { useMemo, type CSSProperties } from "react";
import { LogoMark } from "../components/doodles";
import { Scrawl } from "../components/type";
import { DASH_URL, doc, GITHUB_URL, SUNSET_ESSAY_URL } from "../config";

const columns = [
  {
    title: "Project",
    links: [
      { label: "Source on GitHub", href: GITHUB_URL },
      { label: "Parity status", href: doc("docs/PARITY.md") },
      { label: "Performance notes", href: doc("docs/PERF.md") },
      { label: "Demo dashboard", href: DASH_URL },
    ],
  },
  {
    title: "Docs",
    links: [
      { label: "Migrating from Instant Cloud", href: doc("docs/MIGRATION.md") },
      { label: "Deploying", href: doc("docs/DEPLOY.md") },
      { label: "Wire protocol", href: doc("docs/PROTOCOL.md") },
      { label: "Permissions", href: doc("docs/PERMS.md") },
    ],
  },
];

export function Footer() {
  return (
    <footer className="relative isolate overflow-hidden bg-plum-950 px-4 pt-20 pb-12 sm:px-8">
      <Embers />
      <div className="relative mx-auto grid max-w-7xl gap-14 lg:grid-cols-[1.4fr_1fr_1fr]">
        <div>
          <div className="flex items-center gap-3">
            <LogoMark className="h-10 w-14" />
            <span className="display text-2xl font-semibold text-ember-400">
              instantdb
              <span className="ml-1 inline-block -rotate-6 font-hand text-3xl font-bold text-ember-300">rust</span>
            </span>
          </div>
          <Scrawl className="mt-6 -rotate-2 text-3xl text-ember-300" duration={1.4}>
            see you on the other side of the sunset.
          </Scrawl>
          <p className="mt-6 max-w-md text-sm leading-relaxed text-peach-200/55">
            An independent open-source project, not affiliated with or endorsed by Instant or OpenAI. Background on the
            shutdown is in{" "}
            <a className="underline decoration-dashed underline-offset-4 hover:text-ember-300" href={SUNSET_ESSAY_URL} target="_blank" rel="noopener">
              Instant's announcement
            </a>
            .
          </p>
        </div>
        {columns.map((c) => (
          <div key={c.title}>
            <h3 className="font-hand text-2xl text-ember-300">{c.title}</h3>
            <ul className="mt-4 space-y-3">
              {c.links.map((l) => (
                <li key={l.label}>
                  <a href={l.href} target="_blank" rel="noopener" className="text-peach-200/75 transition-colors hover:text-ember-300">
                    {l.label}
                  </a>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </footer>
  );
}

/** Sparks drifting up through the footer, like the last embers of a campfire. */
function Embers() {
  const embers = useMemo(() => {
    let seed = 17;
    const r = () => ((seed = (seed * 1664525 + 1013904223) % 4294967296) / 4294967296);
    return Array.from({ length: 28 }, () => ({
      left: `${r() * 100}%`,
      size: 2 + r() * 4,
      "--dur": `${7 + r() * 8}s`,
      "--delay": `${-r() * 15}s`,
      "--sway": `${(r() - 0.5) * 120}px`,
    }));
  }, []);
  return (
    <div className="pointer-events-none absolute inset-0 -z-10" aria-hidden="true">
      {embers.map(({ left, size, ...vars }, i) => (
        <span key={i} className="ember" style={{ left, width: size, height: size, ...(vars as CSSProperties) }} />
      ))}
    </div>
  );
}
