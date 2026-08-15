# Loafer demo video

Remotion source for the product demo embedded on the home page.

The rendered artifacts are committed under `web/public/media/`, so a normal
`web` build needs nothing from this directory. You only need to come here to
change the video.

## Working on it

```bash
cd video
npm install
npm run studio     # live preview at localhost:3000
```

## Re-rendering

```bash
npm run build      # renders the mp4 and the poster into web/public/media/
```

`npm run build` writes:

- `web/public/media/loafer-demo.mp4`
- `web/public/media/loafer-demo-poster.jpg`

Commit both. The first render downloads a headless Chrome shell (~150 MB) into
Remotion's cache; subsequent renders reuse it.

## Structure

- `src/theme.ts` — palette copied from `web/src/tokens.css`, plus the cut list.
  Scene durations live here and the timeline is derived from them.
- `src/components/` — the stamped-plate chrome, the frame-derived typewriter,
  and the font loader that blocks rendering until Geist is ready.
- `src/scenes/` — one file per scene, in cut order: title, install, declare,
  validate, run, reconcile, end.

Every figure in the run and reconcile scenes comes from
`benchmarks/results/30m-row-local.json`. If that benchmark is re-run, update
`src/scenes/Run.tsx` and `src/scenes/Reconcile.tsx` to match.
