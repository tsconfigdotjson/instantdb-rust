// Entry for /dashboard (dashboard/index.html, a second Vite page), so static
// hosts serve it as a real file with no router or SPA fallback.
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MotionGlobalConfig } from "motion/react";
import "./index.css";
import { Sunset } from "./Sunset";

if (new URLSearchParams(location.search).has("still")) {
  MotionGlobalConfig.skipAnimations = true;
  document.documentElement.classList.add("still");
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Sunset />
  </StrictMode>,
);
