import type { CSSProperties, ReactNode } from 'react'
import { AbsoluteFill, interpolate, useCurrentFrame } from 'remotion'
import { font, theme } from '../theme'

/*
 * The ground every scene sits on: graphite, the blueprint rule, and a stamped
 * caption strip along the bottom that names the stage the viewer is watching.
 * The caption is not decoration — it is what makes the video legible with the
 * sound off, which is how it will be watched.
 */

export function Grid({ opacity = 0.34 }: { opacity?: number }) {
  return (
    <AbsoluteFill
      style={{
        opacity,
        backgroundImage: `linear-gradient(to right, ${theme.steelSubtle} 1px, transparent 1px), linear-gradient(to bottom, ${theme.steelSubtle} 1px, transparent 1px)`,
        backgroundSize: '96px 96px',
      }}
    />
  )
}

export function Stamp({ children, style }: { children: ReactNode; style?: CSSProperties }) {
  return (
    <div
      style={{
        fontFamily: font.mono,
        fontSize: 20,
        fontWeight: 500,
        letterSpacing: '0.18em',
        textTransform: 'uppercase',
        color: theme.paperMute,
        ...style,
      }}
    >
      {children}
    </div>
  )
}

/**
 * The bottom caption strip. `step` is 1-indexed and rendered against the total
 * so the viewer always knows where they are in the run.
 */
export function Caption({
  step,
  total,
  label,
  note,
}: {
  step: number
  total: number
  label: string
  note: string
}) {
  const frame = useCurrentFrame()
  const enter = interpolate(frame, [0, 14], [0, 1], { extrapolateRight: 'clamp' })

  return (
    <div
      style={{
        position: 'absolute',
        left: 0,
        right: 0,
        bottom: 0,
        display: 'flex',
        alignItems: 'center',
        gap: 28,
        padding: '26px 72px',
        borderTop: `1px solid ${theme.steelSubtle}`,
        background: 'rgba(13,15,14,0.82)',
        opacity: enter,
      }}
    >
      <div
        style={{
          fontFamily: font.mono,
          fontSize: 19,
          color: theme.signal,
          fontVariantNumeric: 'tabular-nums',
        }}
      >
        {String(step).padStart(2, '0')} / {String(total).padStart(2, '0')}
      </div>
      <Stamp style={{ color: theme.paper }}>{label}</Stamp>
      <div style={{ fontFamily: font.sans, fontSize: 21, color: theme.paperDim }}>{note}</div>
    </div>
  )
}

/** A stamped plate: the bordered surface the YAML and terminal live on. */
export function Plate({
  title,
  right,
  children,
  style,
}: {
  title: string
  right?: string
  children: ReactNode
  style?: CSSProperties
}) {
  return (
    <div
      style={{
        border: `1px solid ${theme.steel}`,
        background: theme.inkSurface,
        display: 'flex',
        flexDirection: 'column',
        ...style,
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '16px 24px',
          borderBottom: `1px solid ${theme.steelSubtle}`,
        }}
      >
        <Stamp>{title}</Stamp>
        {right ? <Stamp>{right}</Stamp> : null}
      </div>
      <div style={{ flex: 1, padding: '24px 28px', minHeight: 0 }}>{children}</div>
    </div>
  )
}

/** The Loafer routing arrow, at whatever size a scene needs. */
export function Mark({ size = 64 }: { size?: number }) {
  return (
    <svg width={size} height={(size * 46) / 48} viewBox="0 0 48 46" fill="none">
      <path
        fill={theme.signal}
        d="M25.946 44.938c-.664.845-2.021.375-2.021-.698V33.937a2.26 2.26 0 0 0-2.262-2.262H10.287c-.92 0-1.456-1.04-.92-1.788l7.48-10.471c1.07-1.497 0-3.578-1.842-3.578H1.237c-.92 0-1.456-1.04-.92-1.788L10.013.474c.214-.297.556-.474.92-.474h28.894c.92 0 1.456 1.04.92 1.788l-7.48 10.471c-1.07 1.498 0 3.579 1.842 3.579h11.377c.943 0 1.473 1.088.89 1.83L25.947 44.94z"
      />
    </svg>
  )
}
