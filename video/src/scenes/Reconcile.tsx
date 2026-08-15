import { AbsoluteFill, interpolate, useCurrentFrame } from 'remotion'
import { Caption, Grid, Plate, Stamp } from '../components/Frame'
import { font, theme } from '../theme'

/*
 * The ending the product actually earns.
 *
 * Most pipeline tools finish by telling you the job exited zero. This scene is
 * about the stronger claim: the digest of what was read and the digest of what
 * was written are the same string, so "it worked" is a checkable statement
 * rather than an absence of errors.
 *
 * The two hashes are shown stacked and aligned deliberately — the equality is
 * meant to be visible without reading the characters.
 */

const DIGEST = '9f3c1a7e6d2b48c05fae3719b8d6c2417ae5f0938c1de6b47a2905fc3e8d1b64'

function Digest({ label, value, at }: { label: string; value: string; at: number }) {
  const frame = useCurrentFrame()
  const reveal = interpolate(frame, [at, at + 22], [0, 1], {
    extrapolateLeft: 'clamp',
    extrapolateRight: 'clamp',
  })
  const shown = Math.floor(reveal * value.length)

  return (
    <div style={{ opacity: reveal > 0 ? 1 : 0 }}>
      <Stamp>{label}</Stamp>
      <div
        style={{
          marginTop: 12,
          fontFamily: font.mono,
          fontSize: 30,
          letterSpacing: '0.02em',
          color: theme.paper,
          wordBreak: 'break-all',
        }}
      >
        <span style={{ color: theme.paperMute }}>sha256:</span>
        {value.slice(0, shown)}
      </div>
    </div>
  )
}

function Verdict({ at }: { at: number }) {
  const frame = useCurrentFrame()
  const enter = interpolate(frame, [at, at + 18], [0, 1], {
    extrapolateLeft: 'clamp',
    extrapolateRight: 'clamp',
  })

  return (
    <div
      style={{
        marginTop: 52,
        display: 'flex',
        alignItems: 'center',
        gap: 22,
        opacity: enter,
        transform: `translateY(${(1 - enter) * 12}px)`,
      }}
    >
      <div
        style={{
          width: 44,
          height: 44,
          border: `2px solid ${theme.ok}`,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: theme.ok,
          fontSize: 28,
          fontFamily: font.mono,
        }}
      >
        ✓
      </div>
      <div
        style={{
          fontFamily: font.sans,
          fontSize: 42,
          fontWeight: 600,
          letterSpacing: '-0.02em',
          color: theme.paper,
        }}
      >
        Identical. The run is published.
      </div>
    </div>
  )
}

export function Reconcile() {
  return (
    <AbsoluteFill style={{ background: theme.inkBase }}>
      <Grid />
      <AbsoluteFill style={{ padding: '110px 120px 170px' }}>
        <Plate title="reconciliation" right="run 7c1f4a" style={{ flex: 1 }}>
          <div style={{ display: 'flex', gap: 96, marginBottom: 46 }}>
            {[
              ['rows read', '30,000,000'],
              ['rows written', '30,000,000'],
              ['rejected', '0'],
              ['temp files', '0'],
            ].map(([label, value]) => (
              <div key={label}>
                <Stamp>{label}</Stamp>
                <div
                  style={{
                    marginTop: 12,
                    fontFamily: font.mono,
                    fontSize: 40,
                    color: theme.paper,
                    fontVariantNumeric: 'tabular-nums',
                    letterSpacing: '-0.02em',
                  }}
                >
                  {value}
                </div>
              </div>
            ))}
          </div>

          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              gap: 34,
              borderTop: `1px solid ${theme.steelSubtle}`,
              paddingTop: 40,
            }}
          >
            <Digest label="input digest" value={DIGEST} at={10} />
            <Digest label="output digest" value={DIGEST} at={44} />
          </div>

          <Verdict at={86} />
        </Plate>
      </AbsoluteFill>
      <Caption
        step={5}
        total={5}
        label="reconcile"
        note="What was read and what was written hash to the same value."
      />
    </AbsoluteFill>
  )
}
