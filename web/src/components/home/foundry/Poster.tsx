/*
 * The static representation of the routing yard.
 *
 * This is not a "sorry, no WebGL" placeholder. It is the same four stations
 * drawn as a blueprint elevation, and it is what gets served to crawlers, to
 * print, to Save-Data connections, to devices without WebGL, and to anyone who
 * has asked their system for reduced motion. It ships in the HTML, so it is
 * also what is on screen during the moments before the canvas initializes.
 */

const STEEL = '#333a38'
const STEEL_STRONG = '#646d69'
const INK_RAISED = '#1a1e1d'
const SIGNAL = '#d95f2a'
const PARCEL_RAW = '#5b6562'

/** Parcels on the rail. Anything past the press at x=392 is sealed. */
const PARCELS = [104, 128, 152, 176, 268, 292, 316, 452, 476, 500]

export function FoundryPoster({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 640 380"
      className={className}
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      role="img"
      aria-label="Schematic of a Loafer run: a source dock feeds batches of rows onto a rail, through a validation gate, through a transform press that seals them, and into a target that receives the finished output."
    >
      {/* Blueprint rule. */}
      <defs>
        <pattern id="foundry-grid" width="44" height="44" patternUnits="userSpaceOnUse">
          <path d="M44 0H0v44" stroke="#222726" strokeWidth="1" fill="none" />
        </pattern>
      </defs>
      <rect width="640" height="380" fill="url(#foundry-grid)" />

      {/* Rail and sleepers. */}
      <rect x="30" y="286" width="580" height="8" fill="#131615" stroke={STEEL} />
      {Array.from({ length: 24 }, (_, i) => (
        <rect key={i} x={36 + i * 24} y="294" width="3" height="9" fill={STEEL} />
      ))}

      {/* Source dock. */}
      <g>
        <rect x="46" y="196" width="76" height="90" fill={INK_RAISED} stroke={STEEL_STRONG} />
        <path d="M46 224h76M46 252h76" stroke={STEEL} />
        <rect x="62" y="182" width="44" height="14" fill={INK_RAISED} stroke={STEEL_STRONG} />
      </g>

      {/* Validation gate. */}
      <g>
        <rect x="210" y="176" width="12" height="110" fill={INK_RAISED} stroke={STEEL_STRONG} />
        <rect x="298" y="176" width="12" height="110" fill={INK_RAISED} stroke={STEEL_STRONG} />
        <rect x="210" y="176" width="100" height="12" fill={INK_RAISED} stroke={STEEL_STRONG} />
        <path d="M222 262h76" stroke={SIGNAL} strokeWidth="2" strokeDasharray="5 4" />
      </g>

      {/* Transform press. */}
      <g>
        <rect x="352" y="168" width="8" height="118" fill={INK_RAISED} stroke={STEEL} />
        <rect x="424" y="168" width="8" height="118" fill={INK_RAISED} stroke={STEEL} />
        <rect x="356" y="196" width="72" height="18" fill={INK_RAISED} stroke={STEEL_STRONG} />
        <rect x="356" y="298" width="72" height="12" fill={INK_RAISED} stroke={STEEL_STRONG} />
        <path d="M392 214v40" stroke={SIGNAL} strokeWidth="2" />
        <path d="M386 248l6 8 6-8" stroke={SIGNAL} strokeWidth="2" fill="none" />
      </g>

      {/* Target bin, with sealed output already settled. */}
      <g>
        <path d="M534 232v62h84v-62" stroke={STEEL_STRONG} fill={INK_RAISED} />
        <rect x="546" y="272" width="18" height="18" fill={SIGNAL} />
        <rect x="568" y="272" width="18" height="18" fill={SIGNAL} />
        <rect x="590" y="272" width="18" height="18" fill={SIGNAL} />
        <rect x="557" y="252" width="18" height="18" fill={SIGNAL} />
        <rect x="579" y="252" width="18" height="18" fill={SIGNAL} />
      </g>

      {/* Batches in flight. Four to a batch, with a gap, because that is what
          bounded execution actually looks like. */}
      {PARCELS.map((x) => (
        <rect
          key={x}
          x={x}
          y="268"
          width="18"
          height="18"
          fill={x > 392 ? SIGNAL : PARCEL_RAW}
          stroke={x > 392 ? '#f0a583' : STEEL_STRONG}
        />
      ))}

      {/* Station stamps. */}
      {[
        [84, 'extract'],
        [260, 'validate'],
        [392, 'transform'],
        [576, 'load'],
      ].map(([x, label]) => (
        <text
          key={label}
          x={x as number}
          y="338"
          textAnchor="middle"
          fill="#828b86"
          fontFamily="ui-monospace, monospace"
          fontSize="11"
          letterSpacing="1.8"
        >
          {(label as string).toUpperCase()}
        </text>
      ))}
    </svg>
  )
}
