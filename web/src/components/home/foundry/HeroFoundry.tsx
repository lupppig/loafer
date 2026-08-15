'use client'

import { useEffect, useRef, useState } from 'react'
import dynamic from 'next/dynamic'
import { FoundryPoster } from './Poster'

/*
 * The gate in front of the 3D scene.
 *
 * The visual direction is explicit that meaningful HTML and the primary calls
 * to action render before 3D initializes, that WebGL loads only where it is
 * used, and that work pauses when off-screen or backgrounded. This component
 * is where all of that is enforced, so the scene itself can be about geometry.
 *
 * Order of events:
 *   1. The poster ships in the server HTML and is what the crawler indexes.
 *   2. On mount we decide whether a canvas is appropriate at all.
 *   3. Only then is the three.js bundle requested, and only once the section
 *      has actually been scrolled near.
 */

const FoundryScene = dynamic(() => import('./scene'), {
  ssr: false,
  loading: () => null,
})

type Verdict = 'deciding' | 'poster' | 'canvas'

/**
 * Whether this device should be asked to run a WebGL scene at all.
 *
 * Deliberately conservative. A hero that stutters is worse than a hero that is
 * a clean static drawing, and the drawing says the same thing.
 */
function shouldRenderCanvas(): boolean {
  if (typeof window === 'undefined') return false

  // Explicit user and carrier signals win outright.
  const connection = (
    navigator as Navigator & { connection?: { saveData?: boolean; effectiveType?: string } }
  ).connection
  if (connection?.saveData) return false
  if (connection?.effectiveType && /(^|-)2g$/.test(connection.effectiveType)) return false

  // Low-core and low-memory devices get the poster.
  if (typeof navigator.hardwareConcurrency === 'number' && navigator.hardwareConcurrency <= 2) {
    return false
  }
  const memory = (navigator as Navigator & { deviceMemory?: number }).deviceMemory
  if (typeof memory === 'number' && memory <= 2) return false

  // Finally, does WebGL actually work here? Probed on a throwaway canvas that
  // is released immediately, so this costs one context and no memory.
  try {
    const probe = document.createElement('canvas')
    const gl = probe.getContext('webgl2') ?? probe.getContext('webgl')
    if (!gl) return false
    const lose = (gl as WebGLRenderingContext).getExtension('WEBGL_lose_context')
    lose?.loseContext()
    return true
  } catch {
    return false
  }
}

export function HeroFoundry() {
  const [verdict, setVerdict] = useState<Verdict>('deciding')
  const [animate, setAnimate] = useState(true)
  const [active, setActive] = useState(false)
  const [near, setNear] = useState(false)
  const frame = useRef<HTMLDivElement>(null)

  // Reduced motion, tracked live rather than read once: people toggle it.
  useEffect(() => {
    const query = window.matchMedia('(prefers-reduced-motion: reduce)')
    const sync = () => setAnimate(!query.matches)
    sync()
    query.addEventListener('change', sync)
    return () => query.removeEventListener('change', sync)
  }, [])

  /*
   * Two observers, one purpose: the GPU should be idle whenever the yard is
   * not being looked at. `near` triggers the code-split fetch slightly before
   * the section arrives; `active` runs the frame loop only while it is on
   * screen and the tab is foregrounded.
   */
  useEffect(() => {
    const node = frame.current
    if (!node) return

    /*
     * The capability probe rides along with the first intersection rather than
     * running in its own effect on mount. Two reasons: a device that never
     * scrolls the hero into view is never asked to spin up a WebGL context at
     * all, and the verdict is set from a callback instead of synchronously
     * during an effect, which would cascade a second render on every load.
     */
    const prefetch = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setVerdict(shouldRenderCanvas() ? 'canvas' : 'poster')
          setNear(true)
          prefetch.disconnect()
        }
      },
      { rootMargin: '400px' },
    )
    const visibility = new IntersectionObserver(
      ([entry]) => setActive(entry.isIntersecting && !document.hidden),
      { threshold: 0.05 },
    )

    prefetch.observe(node)
    visibility.observe(node)

    const onVisibilityChange = () => {
      if (document.hidden) setActive(false)
    }
    document.addEventListener('visibilitychange', onVisibilityChange)

    return () => {
      prefetch.disconnect()
      visibility.disconnect()
      document.removeEventListener('visibilitychange', onVisibilityChange)
    }
  }, [])

  const showCanvas = verdict === 'canvas' && near

  return (
    <div
      ref={frame}
      className="material-registration relative aspect-[16/10] w-full border border-steel bg-ink-surface"
    >
      {/* The poster stays mounted underneath and simply fades out, so there is
          never an empty frame between the HTML and the first rendered pixel. */}
      <FoundryPoster
        className={`absolute inset-0 h-full w-full transition-opacity duration-500 ${
          showCanvas ? 'opacity-0' : 'opacity-100'
        }`}
      />

      {showCanvas ? (
        <div className="absolute inset-0" aria-hidden="true">
          <FoundryScene animate={animate} active={active} />
        </div>
      ) : null}

      <div className="pointer-events-none absolute inset-x-0 bottom-0 flex items-center justify-between border-t border-steel-subtle bg-ink-base/70 px-4 py-2.5 backdrop-blur-sm">
        <span className="stamp">routing yard</span>
        <span className="stamp text-paper-mute">
          {animate ? 'batch size 4' : 'motion reduced'}
        </span>
      </div>
    </div>
  )
}
