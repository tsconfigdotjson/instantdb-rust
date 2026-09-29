// Links the site points at. The demo dashboard's origin can be overridden at
// build time (VITE_DASH_URL), e.g. for a staging stack.
export const DASH_URL: string =
  import.meta.env.VITE_DASH_URL ?? "https://dash.instantdbrust.com/dash";

export const GITHUB_URL = "https://github.com/tsconfigdotjson/instantdb-rust";

export const doc = (path: string) => `${GITHUB_URL}/blob/main/${path}`;

export const SUNSET_ESSAY_URL = "https://www.instantdb.com/essays/instant_team_joins_openai";
