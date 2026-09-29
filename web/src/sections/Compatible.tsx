import { motion } from "motion/react";
import { Heading } from "../components/Section";
import { INK, RoughOverlay, Sketch } from "../components/rough";
import { PenArrow } from "../components/doodles";
import { Scrawl } from "../components/type";
import { doc } from "../config";

const checks = [
  <>
    The official <Code>@instantdb/react</Code>, <Code>/core</Code> and <Code>/admin</Code> clients, unmodified
  </>,
  <>
    <Code>instant-cli</Code> login, <Code>push</Code> and <Code>pull</Code> for schema and permissions
  </>,
  <>Instant's own Postgres schema, so an exported app restores directly</>,
  <>Instant's dashboard, self-hosted and pointed at your server</>,
];

export function Compatible() {
  return (
    <section id="compatible" className="relative mx-auto max-w-7xl scroll-mt-24 px-4 py-28 sm:px-8 sm:py-36">
      <div className="grid items-center gap-16 lg:grid-cols-[1fr_1.1fr] lg:gap-20">
        <div>
          <Heading kicker="the migration guide, basically" title="Moving is a" accent="URL change." />
          <p className="mt-8 max-w-xl text-lg leading-relaxed text-peach-200/85">
            InstantDB Rust speaks Instant's wire protocol down to the error shapes. Point your app at a new{" "}
            <Code>apiURI</Code> and keep <Code>useQuery</Code>, <Code>transact</Code>, presence and magic codes
            exactly as they are.
          </p>
          <ul className="mt-10 space-y-5">
            {checks.map((c, i) => (
              <li key={i} className="flex items-start gap-4 text-[17px] leading-snug text-peach-200/90">
                <Sketch
                  viewBox="0 0 40 40"
                  className="mt-[-4px] h-8 w-8 flex-none"
                  delay={0.15 * i}
                  duration={0.5}
                  stagger={0.2}
                  amount={1}
                  build={(g) => [
                    g.rectangle(5, 7, 28, 27, { stroke: INK.plum400, strokeWidth: 1.6, roughness: 1.4, seed: 30 + i }),
                    g.linearPath(
                      [
                        [9, 20],
                        [17, 29],
                        [37, 3],
                      ],
                      { stroke: INK.ember, strokeWidth: 3, roughness: 0.7, disableMultiStroke: true, seed: 40 + i },
                    ),
                  ]}
                />
                <span>{c}</span>
              </li>
            ))}
          </ul>
          <a
            href={doc("docs/MIGRATION.md")}
            target="_blank"
            rel="noopener"
            className="mt-10 inline-block font-hand text-2xl text-ember-300 underline decoration-dashed underline-offset-8 hover:text-ember-400"
          >
            read the full migration notes →
          </a>
        </div>

        <CodeCard />
      </div>
    </section>
  );
}

function CodeCard() {
  const lines: { t: React.ReactNode; add?: boolean }[] = [
    { t: <><K>import</K> {"{ init }"} <K>from</K> <S>"@instantdb/react"</S>;</> },
    { t: "" },
    { t: <><K>const</K> db = <F>init</F>({"{"}</> },
    { t: <>  appId: <S>"your-app-id"</S>,</> },
    { t: <>  schema,</> },
    { t: <>  apiURI: <S>"https://api.example.com"</S>,</>, add: true },
    { t: <>  websocketURI: <S>"wss://api.example.com/runtime/session"</S>,</>, add: true },
    { t: <>{"});"}</> },
    { t: "" },
    { t: <><C>// everything below is unchanged</C></> },
    { t: <><K>const</K> {"{ data }"} = db.<F>useQuery</F>({"{ todos: {} }"});</> },
  ];
  return (
    <motion.div
      initial={{ opacity: 0, y: 40, rotate: 2 }}
      whileInView={{ opacity: 1, y: 0, rotate: -1 }}
      viewport={{ once: true, amount: 0.4 }}
      transition={{ duration: 1, ease: [0.2, 0.75, 0.2, 1] }}
      className="relative min-w-0"
    >
      <div className="relative rounded-[22px_16px_24px_14px] bg-plum-950/80 p-6 shadow-[0_30px_80px_-20px_rgba(0,0,0,0.6)] sm:p-8">
        <RoughOverlay
          className="-inset-2"
          delay={0.3}
          duration={1.4}
          stagger={0.4}
          build={(g, w, h) => g.rectangle(3, 3, w - 6, h - 6, { stroke: INK.ember4, strokeWidth: 2, roughness: 1.8, bowing: 2 })}
        />
        <div className="mb-5 flex items-center gap-2">
          <span className="h-3 w-3 rounded-full bg-ember-500" />
          <span className="h-3 w-3 rounded-full bg-ember-300/70" />
          <span className="h-3 w-3 rounded-full bg-plum-500" />
          <span className="ml-3 font-mono text-sm text-peach-200/50">src/db.ts</span>
        </div>
        <pre className="overflow-x-auto font-mono text-[12.5px] leading-7 text-peach-200/90 [scrollbar-width:thin] [scrollbar-color:#552689_transparent] sm:text-[14px]">
          {lines.map((l, i) => (
            <div
              key={i}
              className={`-mx-3 flex rounded-md px-3 ${l.add ? "bg-ember-500/15" : ""}`}
            >
              <span className={`mr-4 w-3 select-none ${l.add ? "text-ember-400" : "text-plum-500"}`}>{l.add ? "+" : " "}</span>
              <span className="whitespace-pre">{l.t}</span>
            </div>
          ))}
        </pre>
      </div>

      <div className="pointer-events-none mt-8 flex items-end justify-end gap-1 sm:absolute sm:-bottom-24 sm:right-4 sm:mt-0">
        <PenArrow
          className="hidden h-20 w-28 sm:block"
          delay={1.2}
          d="M170 104C120 100 76 78 56 22"
          head={[[44, 40], [56, 22], [72, 36]]}
        />
        <Scrawl className="rotate-[-4deg] text-3xl text-ember-300" delay={1.6}>
          that's the whole migration.
        </Scrawl>
      </div>
    </motion.div>
  );
}

function Code({ children }: { children: React.ReactNode }) {
  return <code className="rounded bg-plum-800/70 px-1.5 py-0.5 font-mono text-[0.85em] text-ember-300">{children}</code>;
}
const K = ({ children }: { children: React.ReactNode }) => <span className="text-plum-400">{children}</span>;
const S = ({ children }: { children: React.ReactNode }) => <span className="text-ember-300">{children}</span>;
const F = ({ children }: { children: React.ReactNode }) => <span className="text-cream-100">{children}</span>;
const C = ({ children }: { children: React.ReactNode }) => <span className="text-peach-200/40 italic">{children}</span>;
