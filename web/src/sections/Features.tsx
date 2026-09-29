import { motion } from "motion/react";
import type { RoughGenerator } from "roughjs/bin/generator";
import type { Drawable } from "roughjs/bin/core";
import { Heading } from "../components/Section";
import { INK, RoughOverlay, Sketch } from "../components/rough";

type Feature = {
  title: string;
  body: string;
  icon: (g: RoughGenerator) => Drawable[];
};

const o = (seed: number, extra = {}) => ({ stroke: INK.ember4, strokeWidth: 2.2, roughness: 1.1, seed, ...extra });

const features: Feature[] = [
  {
    title: "The sync protocol",
    body: "Websockets with the SSE fallback, add-query, transact, refresh-ok pushes and batched frames, shaped exactly the way the client's reactor reads them.",
    icon: (g) => [
      g.arc(32, 32, 40, 40, Math.PI * 1.1, Math.PI * 1.95, false, o(1)),
      g.arc(32, 32, 40, 40, Math.PI * 0.1, Math.PI * 0.95, false, o(2)),
      g.linearPath([[46, 10], [51, 18], [43, 20]], o(3)),
      g.linearPath([[18, 54], [13, 46], [21, 44]], o(4)),
    ],
  },
  {
    title: "InstaQL",
    body: "Nested links, dot-path where clauses, $in, $like, typed comparisons, ordering, cursor pagination, field projection and counts.",
    icon: (g) => [g.circle(27, 27, 28, o(5)), g.line(37, 37, 54, 54, o(6, { strokeWidth: 3.2 })), g.line(20, 27, 34, 27, o(7))],
  },
  {
    title: "InstaML",
    body: "Lookups, deep merges, cascading deletes, unique and required checks, schemaless attrs and create or update modes.",
    icon: (g) => [
      g.rectangle(12, 14, 30, 38, o(8)),
      g.line(18, 24, 36, 24, o(9)),
      g.line(18, 32, 32, 32, o(10)),
      g.line(40, 46, 56, 12, o(11, { strokeWidth: 3 })),
      g.line(36, 52, 40, 46, o(12)),
    ],
  },
  {
    title: "Permissions",
    body: "Instant's CEL rules engine: bind, data.ref, auth.ref, ruleParams, field rules, request.* bindings and rate limits.",
    icon: (g) => [
      g.rectangle(14, 28, 36, 26, o(13)),
      g.arc(32, 28, 24, 26, Math.PI, Math.PI * 2, false, o(14)),
      g.circle(32, 40, 6, o(15, { fill: INK.ember, fillStyle: "solid" })),
    ],
  },
  {
    title: "Auth",
    body: "Magic codes, guest users, refresh tokens, and OAuth or OIDC sign-in with PKCE. Google works out of the box.",
    icon: (g) => [
      g.circle(20, 32, 20, o(16)),
      g.line(30, 32, 56, 32, o(17)),
      g.line(46, 32, 46, 40, o(18)),
      g.line(53, 32, 53, 42, o(19)),
    ],
  },
  {
    title: "Rooms & presence",
    body: "Join, presence patches and broadcasts that reach every node through Postgres, so peers find each other anywhere.",
    icon: (g) => [
      g.circle(22, 22, 14, o(20)),
      g.arc(22, 48, 26, 24, Math.PI, Math.PI * 2, false, o(21)),
      g.circle(44, 26, 12, o(22)),
      g.arc(44, 50, 22, 20, Math.PI, Math.PI * 2, false, o(23)),
    ],
  },
  {
    title: "Storage",
    body: "$files uploads and deletes with blobs in Postgres, on disk, or in any S3-compatible bucket (S3, R2, MinIO), in the legacy layout.",
    icon: (g) => [
      g.ellipse(32, 16, 36, 12, o(24)),
      g.line(14, 16, 14, 48, o(25)),
      g.line(50, 16, 50, 48, o(26)),
      g.arc(32, 48, 36, 12, 0, Math.PI, false, o(27)),
      g.arc(32, 32, 36, 12, 0, Math.PI, false, o(28)),
    ],
  },
  {
    title: "Streams & sync tables",
    body: "Resumable byte streams with live tailing, and admin sync tables with their change feed and forced resyncs.",
    icon: (g) => [
      g.curve([[8, 20], [20, 12], [32, 22], [44, 12], [56, 20]], o(29)),
      g.curve([[8, 34], [20, 26], [32, 36], [44, 26], [56, 34]], o(30)),
      g.curve([[8, 48], [20, 40], [32, 50], [44, 40], [56, 48]], o(31)),
    ],
  },
  {
    title: "Admin API, CLI & webhooks",
    body: "@instantdb/admin queries and transacts, impersonation, users, instant-cli push and pull, and signed webhooks.",
    icon: (g) => [
      g.rectangle(8, 12, 48, 40, o(32)),
      g.linearPath([[16, 26], [24, 32], [16, 38]], o(33)),
      g.line(28, 40, 40, 40, o(34)),
    ],
  },
];

export function Features() {
  return (
    <section className="relative mx-auto max-w-7xl px-4 pb-28 sm:px-8 sm:pb-36">
      <Heading kicker="all of it. really." title="Everything your app" accent="already uses." />
      <div className="mt-16 grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
        {features.map((f, i) => (
          <motion.article
            key={f.title}
            initial={{ opacity: 0, y: 40 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, amount: 0.3 }}
            transition={{ duration: 0.7, delay: (i % 3) * 0.1, ease: [0.2, 0.75, 0.2, 1] }}
            whileHover={{ y: -6, rotate: i % 2 ? 0.8 : -0.8 }}
            className="group relative rounded-[20px_14px_22px_12px] bg-plum-800/55 p-7 transition-colors duration-300 hover:bg-plum-800"
          >
            <RoughOverlay
              className="inset-0"
              delay={0.2 + (i % 3) * 0.1}
              duration={1.2}
              stagger={0.3}
              amount={0.3}
              build={(g, w, h) =>
                g.rectangle(2, 2, w - 4, h - 4, { stroke: INK.plum500, strokeWidth: 1.5, roughness: 1.4, bowing: 1.5, seed: 70 + i })
              }
            />
            <Sketch viewBox="0 0 64 64" className="h-14 w-14" build={f.icon} delay={0.35 + (i % 3) * 0.1} duration={0.7} stagger={0.15} amount={0.8} />
            <h3 className="display mt-5 text-2xl font-medium text-ember-400">{f.title}</h3>
            <p className="mt-3 leading-relaxed text-peach-200/75">{f.body}</p>
          </motion.article>
        ))}
      </div>
    </section>
  );
}
