// The hand-drawn sunset: a hatched sun over two ridges and a still lake.
// The hero sinks it as you scroll; the closing section raises it again.
import { useId, useMemo } from "react";
import { motion, type MotionValue } from "motion/react";
import { Drawn, INK, sketch } from "./rough";

const W = 1440;
const HORIZON = 380;
const SUN = { x: 720, y: 360, r: 130 };

const backRidge: [number, number][] = [
  [-20, HORIZON], [-20, 262], [90, 236], [190, 270], [300, 214], [420, 282], [520, 300],
  [610, 352], [720, 374], [830, 350], [930, 296], [1040, 262], [1150, 214], [1260, 250],
  [1360, 226], [1460, 258], [1460, HORIZON],
];
const frontRidge: [number, number][] = [
  [-20, HORIZON], [-20, 316], [120, 300], [240, 334], [360, 308], [470, 352], [560, 374],
  [880, 374], [980, 344], [1090, 310], [1210, 336], [1330, 298], [1460, 318], [1460, HORIZON],
];
const reflections = [
  { y: 398, w: 250 },
  { y: 414, w: 206 },
  { y: 432, w: 168 },
  { y: 452, w: 126 },
  { y: 475, w: 86 },
  { y: 500, w: 46 },
];

export function Horizon({
  sunY,
  glow,
  reflect,
  show,
  className = "",
}: {
  sunY: MotionValue<number>;
  glow: MotionValue<number>;
  reflect: MotionValue<number>;
  show: boolean;
  className?: string;
}) {
  const id = useId().replace(/:/g, "");

  const art = useMemo(() => {
    const rays = Array.from({ length: 20 }, (_, i) => {
      const a = (i / 20) * Math.PI * 2;
      const r0 = SUN.r + 20;
      const r1 = SUN.r + (i % 2 ? 38 : 56);
      return [SUN.x + Math.cos(a) * r0, SUN.y + Math.sin(a) * r0, SUN.x + Math.cos(a) * r1, SUN.y + Math.sin(a) * r1];
    });
    return {
      sun: sketch((g) => [
        g.circle(SUN.x, SUN.y, SUN.r * 2, { fill: INK.ember, fillStyle: "solid", stroke: "none", roughness: 0.8, seed: 3 }),
        g.circle(SUN.x, SUN.y, SUN.r * 2 - 6, {
          fill: INK.ember3,
          fillStyle: "hachure",
          hachureAngle: -38,
          hachureGap: 9,
          fillWeight: 2,
          stroke: INK.ember4,
          strokeWidth: 3,
          roughness: 1.4,
          seed: 7,
        }),
      ]),
      rays: sketch((g) =>
        rays.map(([x1, y1, x2, y2], i) =>
          g.line(x1, y1, x2, y2, { stroke: INK.ember4, strokeWidth: 3, roughness: 0.9, seed: 20 + i }),
        ),
      ),
      back: sketch((g) =>
        g.polygon(backRidge, {
          fill: INK.plum800,
          fillStyle: "solid",
          stroke: INK.plum500,
          strokeWidth: 2,
          roughness: 1.6,
          seed: 11,
        }),
      ),
      front: sketch((g) => [
        g.polygon(frontRidge, { fill: "#240d40", fillStyle: "solid", stroke: "none", roughness: 1.2, seed: 12 }),
        g.polygon(frontRidge, {
          fill: INK.plum700,
          fillStyle: "hachure",
          hachureAngle: 60,
          hachureGap: 11,
          fillWeight: 1.4,
          stroke: INK.plum400,
          strokeWidth: 2,
          roughness: 1.8,
          seed: 13,
        }),
      ]),
      horizon: sketch((g) =>
        g.line(-20, HORIZON + 1, W + 20, HORIZON + 1, { stroke: INK.peach, strokeWidth: 2.4, roughness: 1.2, seed: 5 }),
      ),
      reflections: reflections.map((r, i) =>
        sketch((g) =>
          g.line(SUN.x - r.w / 2, r.y, SUN.x + r.w / 2, r.y, {
            stroke: i < 2 ? INK.ember4 : INK.ember,
            strokeWidth: 3.2 - i * 0.3,
            roughness: 1.4,
            seed: 40 + i,
          }),
        ),
      ),
      ripples: sketch((g) =>
        [
          [140, 430, 260], [980, 420, 1120], [300, 470, 380], [1080, 488, 1220], [520, 530, 600], [880, 540, 940],
        ].map(([x1, y, x2], i) =>
          g.line(x1, y, x2, y, { stroke: INK.plum500, strokeWidth: 1.6, roughness: 1.3, seed: 60 + i }),
        ),
      ),
    };
  }, []);

  return (
    <svg
      viewBox={`0 0 ${W} 560`}
      preserveAspectRatio="xMidYMax slice"
      className={className}
      aria-hidden="true"
    >
      <defs>
        <radialGradient id={`${id}-glow`}>
          <stop offset="0%" stopColor={INK.ember} stopOpacity="0.75" />
          <stop offset="35%" stopColor="#ff5d73" stopOpacity="0.28" />
          <stop offset="70%" stopColor={INK.dusk} stopOpacity="0.12" />
          <stop offset="100%" stopColor={INK.dusk} stopOpacity="0" />
        </radialGradient>
        <linearGradient id={`${id}-lake`} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor="#2a0d45" />
          <stop offset="100%" stopColor={INK.plum900} />
        </linearGradient>
        <clipPath id={`${id}-sky`}>
          <rect x="-20" y="-400" width={W + 40} height={HORIZON + 402} />
        </clipPath>
      </defs>

      <g clipPath={`url(#${id}-sky)`}>
        <motion.g style={{ y: sunY, opacity: glow }}>
          <circle cx={SUN.x} cy={SUN.y} r={560} fill={`url(#${id}-glow)`} />
        </motion.g>
        <motion.g style={{ y: sunY }}>
          <g className="spin-slow" style={{ transformOrigin: `${SUN.x}px ${SUN.y}px`, transformBox: "view-box" }}>
            <Drawn paths={art.rays} show={show} duration={0.5} delay={0.9} stagger={0.03} />
          </g>
          <Drawn paths={art.sun} show={show} duration={1.6} delay={0.2} stagger={0.4} />
        </motion.g>
        <Drawn paths={art.back} show={show} duration={1.8} delay={0.3} />
      </g>

      <rect x="-20" y={HORIZON} width={W + 40} height={200} fill={`url(#${id}-lake)`} />
      <Drawn paths={art.front} show={show} duration={2} delay={0.6} stagger={0.2} />
      <Drawn paths={art.horizon} show={show} duration={1.4} delay={0.1} />

      <motion.g style={{ opacity: reflect }}>
        {art.reflections.map((paths, i) => (
          <g key={i} className="shimmer" style={{ animationDelay: `${-i * 0.7}s` }}>
            <Drawn paths={paths} show={show} duration={0.6} delay={1.2 + i * 0.12} />
          </g>
        ))}
      </motion.g>
      <Drawn paths={art.ripples} show={show} duration={0.8} delay={1.6} stagger={0.1} />
    </svg>
  );
}
