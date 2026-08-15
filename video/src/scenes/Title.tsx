import { AbsoluteFill, interpolate, spring, useCurrentFrame, useVideoConfig } from 'remotion'
import { Grid, Mark, Stamp } from '../components/Frame'
import { font, theme } from '../theme'

export function Title() {
  const frame = useCurrentFrame()
  const { fps } = useVideoConfig()

  const markIn = spring({ frame, fps, config: { damping: 200, mass: 0.6 } })
  const wordIn = interpolate(frame, [8, 26], [0, 1], { extrapolateRight: 'clamp' })
  const lineIn = interpolate(frame, [22, 44], [0, 1], { extrapolateRight: 'clamp' })
  const ruleIn = interpolate(frame, [18, 46], [0, 1], { extrapolateRight: 'clamp' })

  return (
    <AbsoluteFill style={{ background: theme.inkBase }}>
      <Grid />
      <AbsoluteFill
        style={{
          alignItems: 'center',
          justifyContent: 'center',
          flexDirection: 'column',
          gap: 34,
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 24,
            transform: `scale(${0.9 + markIn * 0.1})`,
          }}
        >
          <div style={{ opacity: markIn }}>
            <Mark size={72} />
          </div>
          <div
            style={{
              fontFamily: font.sans,
              fontSize: 84,
              fontWeight: 700,
              letterSpacing: '-0.04em',
              color: theme.paper,
              opacity: wordIn,
            }}
          >
            loafer
          </div>
        </div>

        <div
          style={{
            width: 520 * ruleIn,
            height: 1,
            background: theme.steel,
          }}
        />

        <div
          style={{
            fontFamily: font.sans,
            fontSize: 38,
            fontWeight: 400,
            color: theme.paperDim,
            opacity: lineIn,
            letterSpacing: '-0.01em',
          }}
        >
          Open-source ETL and ELT, declared in{' '}
          <span style={{ color: theme.signal, fontWeight: 600 }}>YAML</span>.
        </div>

        <Stamp style={{ opacity: lineIn, marginTop: 14 }}>self-hosted · mit licensed</Stamp>
      </AbsoluteFill>
    </AbsoluteFill>
  )
}
