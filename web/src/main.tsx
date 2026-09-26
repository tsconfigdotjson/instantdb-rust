import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MotionGlobalConfig } from "motion/react";
import "./index.css";
import { App } from "./App";

// `?still` renders every animation at its end state (screenshots, print).
if (new URLSearchParams(location.search).has("still")) {
  MotionGlobalConfig.skipAnimations = true;
  document.documentElement.classList.add("still");
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
