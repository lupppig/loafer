import { useEffect, useState } from 'react'
import { continueRender, delayRender, staticFile } from 'remotion'

/*
 * Geist, loaded from the files in public/fonts.
 *
 * Rendering has to block until the faces are actually ready. Remotion will
 * happily screenshot a frame mid-fallback otherwise, and the first second of
 * the video comes out in a different typeface than the rest of it — which is
 * exactly the kind of defect that only shows up after the render finishes.
 */

const FACES = [
  { family: 'Geist', weight: 400, file: 'fonts/geist-sans-latin-400-normal.woff2' },
  { family: 'Geist', weight: 600, file: 'fonts/geist-sans-latin-600-normal.woff2' },
  { family: 'Geist', weight: 700, file: 'fonts/geist-sans-latin-700-normal.woff2' },
  { family: 'Geist Mono', weight: 400, file: 'fonts/geist-mono-latin-400-normal.woff2' },
  { family: 'Geist Mono', weight: 500, file: 'fonts/geist-mono-latin-500-normal.woff2' },
] as const

export function useGeist() {
  const [handle] = useState(() => delayRender('Loading Geist'))

  useEffect(() => {
    let cancelled = false

    const loaded = FACES.map(async (face) => {
      const font = new FontFace(face.family, `url(${staticFile(face.file)}) format("woff2")`, {
        weight: String(face.weight),
        style: 'normal',
      })
      await font.load()
      document.fonts.add(font)
    })

    Promise.all(loaded)
      .then(() => document.fonts.ready)
      .then(() => {
        if (!cancelled) continueRender(handle)
      })
      .catch(() => {
        // A missing font must not wedge the render; the CSS stack still has a
        // usable monospace and sans fallback behind it.
        if (!cancelled) continueRender(handle)
      })

    return () => {
      cancelled = true
    }
  }, [handle])
}
