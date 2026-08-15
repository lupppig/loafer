/*
 * The Loafer mark.
 *
 * The silhouette is the original routing arrow: material enters wide at the
 * top, is folded through two turns, and leaves as a single sealed point. It
 * kept its shape through this revision; what it lost was the purple-to-violet
 * gradient and the blur stack behind it, neither of which belonged to a
 * palette of graphite, steel, and one safety-orange signal.
 *
 * Drawn with hard edges and flat fills only, so it survives being rasterized
 * to a 16px favicon.
 */

const ARROW_PATH =
  'M25.946 44.938c-.664.845-2.021.375-2.021-.698V33.937a2.26 2.26 0 0 0-2.262-2.262H10.287' +
  'c-.92 0-1.456-1.04-.92-1.788l7.48-10.471c1.07-1.497 0-3.578-1.842-3.578H1.237' +
  'c-.92 0-1.456-1.04-.92-1.788L10.013.474c.214-.297.556-.474.92-.474h28.894' +
  'c.92 0 1.456 1.04.92 1.788l-7.48 10.471c-1.07 1.498 0 3.579 1.842 3.579h11.377' +
  'c.943 0 1.473 1.088.89 1.83L25.947 44.94z'

interface LoaferMarkProps {
  /** Rendered edge length in pixels. The mark is square. */
  size?: number
  className?: string
}

/**
 * The mark on its stamped plate, for the topbar, footer, and app icons.
 */
export function LoaferMark({ size = 22, className }: LoaferMarkProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      aria-hidden="true"
      focusable="false"
    >
      <rect x="0.5" y="0.5" width="31" height="31" rx="3.5" fill="#1a1e1d" stroke="#333a38" />
      <g transform="translate(4.25 5) scale(0.49)">
        <path d={ARROW_PATH} fill="#d95f2a" />
      </g>
    </svg>
  )
}

/**
 * The bare arrow with no plate, for placements that already sit on a surface.
 */
export function LoaferGlyph({ size = 16, className }: LoaferMarkProps) {
  return (
    <svg
      width={size}
      height={(size * 46) / 48}
      viewBox="0 0 48 46"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      aria-hidden="true"
      focusable="false"
    >
      <path d={ARROW_PATH} fill="currentColor" />
    </svg>
  )
}
