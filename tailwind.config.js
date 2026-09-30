/** Tailwind theme — the AEMO dashboard family language.
 *
 *  Every colour is a CSS variable, so dark/light is one attribute flip on <html>.
 *  The token values live in assets/css/tailwind.src.css (the single source of truth).
 *  No raw hex in index.html or design/*.html — use these names.
 *
 *  Frozen copy of the family language as built for aemo-generator-credit-dashboard
 *  (2026-09-30). Update the token VALUES only with a deliberate family-wide decision —
 *  a per-page tweak here is how three dashboards end up as three designs.
 */
module.exports = {
  // The page (its inline <script> builds class strings too) and the unpublished design pages.
  content: ["./index.html", "./design/**/*.html"],
  // Preflight ON: the page's own inline reset (`* { margin:0; padding:0 }`, the body font) is
  // removed in step 1 of the brief, so this is the only reset.
  corePlugins: { preflight: true },
  theme: {
    extend: {
      colors: {
        bg: "var(--bg)",
        canvas: "var(--canvas)",
        surface: "var(--surface)",
        "surface-2": "var(--surface-2)",
        "surface-3": "var(--surface-3)",
        line: "var(--line)",
        "line-strong": "var(--line-strong)",
        "line-soft": "var(--line-soft)",
        ink: "var(--text)",
        muted: "var(--muted)",
        faint: "var(--faint)",
        accent: "var(--accent)",
        "accent-soft": "var(--accent-soft)",
        good: "var(--good)",
        warn: "var(--warn)",
        bad: "var(--bad)",
        info: "var(--info)",
        neutral: "var(--neutral)",
        "accent-wash": "var(--accent-wash)",
        "good-wash": "var(--good-wash)",
        "warn-wash": "var(--warn-wash)",
        "bad-wash": "var(--bad-wash)",
        "neutral-wash": "var(--neutral-wash)",
      },
      // Preflight gives every element a border colour; make a bare `border` a hairline, not gray-200.
      borderColor: { DEFAULT: "var(--line)" },
      borderRadius: { control: "8px", card: "12px", panel: "14px" },
      boxShadow: { pop: "var(--shadow-pop)" },
      spacing: { 18: "4.5rem" },
      fontFamily: {
        sans: ['-apple-system', 'BlinkMacSystemFont', '"SF Pro Text"', '"Segoe UI"', 'Inter', 'system-ui', 'sans-serif'],
        mono: ['ui-monospace', '"SF Mono"', '"JetBrains Mono"', 'Menlo', 'Consolas', 'monospace'],
      },
    },
  },
  plugins: [],
};
