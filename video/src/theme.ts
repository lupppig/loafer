/*
 * The demo shares its palette with the site, verbatim.
 *
 * Kept as a plain object rather than imported from web/src/tokens.css because
 * the two projects install separately; the values are copied and the source of
 * truth is named here so a palette change is a two-file edit, not a hunt.
 *
 * Source: web/src/tokens.css
 */
export const theme = {
  inkBase: '#0d0f0e',
  inkSurface: '#131615',
  inkRaised: '#1a1e1d',
  steelSubtle: '#222726',
  steel: '#333a38',
  steelStrong: '#646d69',
  paper: '#ece9e1',
  paperDim: '#a6ada7',
  paperMute: '#828b86',
  signal: '#d95f2a',
  signalBright: '#ef7a44',
  ok: '#58a06d',
  warn: '#c8922e',
} as const

export const font = {
  sans: '"Geist", ui-sans-serif, system-ui, sans-serif',
  mono: '"Geist Mono", ui-monospace, SFMono-Regular, monospace',
} as const

export const FPS = 30

/*
 * The cut list. Each scene declares its own length here so the timeline is
 * readable in one place and the total duration is derived rather than
 * maintained by hand.
 */
export const SCENES = {
  title: 3.2,
  install: 4.0,
  declare: 13.5,
  validate: 4.5,
  run: 16.0,
  reconcile: 6.0,
  end: 4.0,
} as const

export const sceneFrames = (seconds: number) => Math.round(seconds * FPS)

export const TOTAL_FRAMES = Object.values(SCENES).reduce(
  (total, seconds) => total + sceneFrames(seconds),
  0,
)
