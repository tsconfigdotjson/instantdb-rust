import { useEffect, useRef, useState } from "react";
import { motion, useInView } from "motion/react";
import { Heading } from "../components/Section";
import { INK, RoughOverlay } from "../components/rough";
import { doc, GITHUB_URL } from "../config";

const script: { kind: "cmd" | "note"; text: string }[] = [
  { kind: "cmd", text: `git clone ${GITHUB_URL}` },
  { kind: "cmd", text: "cd instantdb-rust && docker compose up --build" },
  { kind: "note", text: "# Postgres + the server, listening on :8888" },
  { kind: "cmd", text: './scripts/create-app.sh "my app"' },
  { kind: "note", text: "# prints your app_id and admin_token" },
];

const facts = [
  { title: "Any Postgres", body: "Stock Postgres 16, 17 or 18. Instant's migrations replay cleanly onto it." },
  { title: "Your blobs, your call", body: "Files live in Postgres by default, or on disk, S3, R2 or MinIO." },
  { title: "Observable", body: "Prometheus /metrics: sessions, query timings, NOTIFY lag, pool use." },
  { title: "Instant's CLI & dashboard", body: "instant-cli and Instant's dashboard work against your own server." },
];

export function SelfHost() {
  return (
    <section id="self-host" className="relative mx-auto max-w-7xl scroll-mt-24 px-4 py-28 sm:px-8 sm:py-36">
      <div className="grid items-start gap-16 lg:grid-cols-[1fr_1.15fr] lg:gap-20">
        <div>
          <Heading kicker="bring it home" title="Your servers." accent="Three commands." />
          <p className="mt-8 max-w-xl text-lg leading-relaxed text-peach-200/85">
            The server speaks plain HTTP and websockets behind any TLS proxy. Clone it, bring it up with Docker or run
            the binary on bare metal, then create an app and hand its id to your client.
          </p>
          <div className="mt-10 grid gap-x-8 gap-y-7 sm:grid-cols-2">
            {facts.map((f, i) => (
              <motion.div
                key={f.title}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true, amount: 0.6 }}
                transition={{ duration: 0.6, delay: i * 0.1 }}
              >
                <h3 className="display text-xl font-medium text-ember-400">{f.title}</h3>
                <p className="mt-1.5 leading-relaxed text-peach-200/75">{f.body}</p>
              </motion.div>
            ))}
          </div>
          <div className="mt-10 flex flex-wrap gap-x-8 gap-y-3 font-hand text-2xl text-ember-300">
            <a className="underline decoration-dashed underline-offset-8 hover:text-ember-400" href={doc("docs/DEPLOY.md")} target="_blank" rel="noopener">
              production checklist →
            </a>
            <a className="underline decoration-dashed underline-offset-8 hover:text-ember-400" href={`${GITHUB_URL}#environment`} target="_blank" rel="noopener">
              every env var →
            </a>
          </div>
        </div>
        <Terminal />
      </div>
    </section>
  );
}

function Terminal() {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref, { once: true, amount: 0.5 });
  const total = script.reduce((n, l) => n + l.text.length, 0);
  const [typed, setTyped] = useState(0);

  useEffect(() => {
    if (!inView) return;
    if (matchMedia("(prefers-reduced-motion: reduce)").matches || document.documentElement.classList.contains("still")) {
      setTyped(total);
      return;
    }
    const id = setInterval(() => setTyped((t) => (t >= total ? t : t + 2)), 28);
    return () => clearInterval(id);
  }, [inView, total]);

  let left = typed;
  return (
    <motion.div
      ref={ref}
      initial={{ opacity: 0, y: 40, rotate: -2 }}
      whileInView={{ opacity: 1, y: 0, rotate: 1 }}
      viewport={{ once: true, amount: 0.3 }}
      transition={{ duration: 1, ease: [0.2, 0.75, 0.2, 1] }}
      className="relative min-w-0 lg:mt-24"
    >
      <div className="relative rounded-[16px_24px_14px_22px] bg-plum-950/85 p-6 shadow-[0_30px_80px_-20px_rgba(0,0,0,0.6)] sm:p-8">
        <RoughOverlay
          className="-inset-2"
          delay={0.3}
          duration={1.4}
          stagger={0.4}
          build={(g, w, h) => g.rectangle(3, 3, w - 6, h - 6, { stroke: INK.plum400, strokeWidth: 2, roughness: 1.8, bowing: 2, seed: 17 })}
        />
        <div className="mb-5 flex items-center gap-2">
          <span className="h-3 w-3 rounded-full bg-ember-500" />
          <span className="h-3 w-3 rounded-full bg-ember-300/70" />
          <span className="h-3 w-3 rounded-full bg-plum-500" />
          <span className="ml-3 font-mono text-sm text-peach-200/50">~/code</span>
        </div>
        <pre className="min-h-[15rem] overflow-x-auto font-mono text-[12.5px] leading-8 whitespace-pre text-peach-200/90 [scrollbar-color:#552689_transparent] [scrollbar-width:thin] sm:text-[14px]">
          {script.map((l, i) => {
            const shown = l.text.slice(0, Math.max(0, left));
            const done = left >= l.text.length;
            const active = left > 0 && !done;
            left -= l.text.length;
            if (!shown && i > 0) return null;
            return (
              <div key={i} className={l.kind === "note" ? "text-plum-400" : ""}>
                {l.kind === "cmd" ? <span className="mr-3 text-ember-400">$</span> : null}
                {shown}
                {active || (done && i === script.length - 1) ? (
                  <span className="ml-0.5 inline-block h-[1.1em] w-[0.55em] translate-y-[3px] animate-pulse bg-ember-400" />
                ) : null}
              </div>
            );
          })}
        </pre>
      </div>
    </motion.div>
  );
}
