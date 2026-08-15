import { AbsoluteFill, interpolate, useCurrentFrame } from 'remotion'
import { Grid, Mark, Stamp } from '../components/Frame'
import { font, theme } from '../theme'

/*
 * One instruction to leave with. The install command is the call to action,
 * not a "learn more" — the whole point of the preceding fifty seconds is that
 * there is nothing between reading this and running it.
 */
export function End() {
  const frame = useCurrentFrame()
  const enter = interpolate(frame, [0, 18], [0, 1], { extrapolateRight: 'clamp' })
  const commandIn = interpolate(frame, [14, 34], [0, 1], { extrapolateRight: 'clamp' })
  const footIn = interpolate(frame, [30, 50], [0, 1], { extrapolateRight: 'clamp' })

  return (
    <AbsoluteFill style={{ background: theme.inkBase }}>
      <Grid />
      <AbsoluteFill
        style={{
          alignItems: 'center',
          justifyContent: 'center',
          flexDirection: 'column',
          gap: 44,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 22, opacity: enter }}>
          <Mark size={54} />
          <div
            style={{
              fontFamily: font.sans,
              fontSize: 62,
              fontWeight: 700,
              letterSpacing: '-0.04em',
              color: theme.paper,
            }}
          >
            loafer
          </div>
        </div>

        <div
          style={{
            border: `1px solid ${theme.steel}`,
            background: theme.inkSurface,
            padding: '26px 46px',
            opacity: commandIn,
            transform: `translateY(${(1 - commandIn) * 10}px)`,
          }}
        >
          <div style={{ fontFamily: font.mono, fontSize: 42, color: theme.paper }}>
            <span style={{ color: theme.signal }}>$ </span>
            pip install loafer-etl
          </div>
        </div>

        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 40,
            opacity: footIn,
            marginTop: 10,
          }}
        >
          <Stamp>github.com/lupppig/loafer</Stamp>
          <div style={{ width: 1, height: 20, background: theme.steel }} />
          <Stamp>mit licensed</Stamp>
        </div>
      </AbsoluteFill>
    </AbsoluteFill>
  )
}
