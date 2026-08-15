import { AbsoluteFill, interpolate, useCurrentFrame } from 'remotion'
import { Caption, Grid, Plate, Stamp } from '../components/Frame'
import { Command } from '../components/Type'
import { font, theme } from '../theme'

/*
 * The run.
 *
 * The two charts on the right are the entire argument the product makes, drawn
 * against a shared x-axis: rows climb to thirty million while resident memory
 * stays flat. Every figure is taken from benchmarks/results/30m-row-local.json
 * — 30,000,000 rows, 118.23 MiB peak process-tree RSS, 33m 48s wall clock, 600
 * committed batches — and compressed into sixteen seconds of playback.
 *
 * The memory trace is jittered with a deterministic sine so it reads as a
 * measurement rather than a drawn straight line, but it never leaves the band
 * the real run stayed inside.
 */

const RUN_START = 58
const RUN_END = 404

const TOTAL_ROWS = 30_000_000
const TOTAL_BATCHES = 600
const WALL_CLOCK_SECONDS = 2028
const PEAK_MIB = 118.23
const CAP_MIB = 512

const STAGES = ['extract', 'validate', 'transform', 'load'] as const

function useRunProgress() {
  const frame = useCurrentFrame()
  return interpolate(frame, [RUN_START, RUN_END], [0, 1], {
    extrapolateLeft: 'clamp',
    extrapolateRight: 'clamp',
  })
}

const integer = (value: number) => Math.floor(value).toLocaleString('en-US')

function clock(totalSeconds: number): string {
  const minutes = Math.floor(totalSeconds / 60)
  const seconds = Math.floor(totalSeconds % 60)
  return `${minutes}m ${String(seconds).padStart(2, '0')}s`
}

/** Resident memory at a point in the run, in MiB. Flat, with instrument noise. */
function memoryAt(t: number): number {
  if (t <= 0) return 0
  const rampIn = Math.min(1, t / 0.04)
  const jitter = Math.sin(t * 41) * 3.1 + Math.sin(t * 17.3) * 1.9
  return rampIn * (109 + jitter + Math.min(t, 0.35) * 12)
}

function Chart({
  label,
  value,
  points,
  ceiling,
  color,
  ceilingLabel,
}: {
  label: string
  value: string
  points: [number, number][]
  ceiling?: number
  color: string
  ceilingLabel?: string
}) {
  const width = 560
  const height = 150

  const path = points
    .map(([x, y], index) => `${index === 0 ? 'M' : 'L'}${(x * width).toFixed(1)},${((1 - y) * height).toFixed(1)}`)
    .join(' ')

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between' }}>
        <Stamp>{label}</Stamp>
        <div
          style={{
            fontFamily: font.mono,
            fontSize: 34,
            color: theme.paper,
            fontVariantNumeric: 'tabular-nums',
            letterSpacing: '-0.02em',
          }}
        >
          {value}
        </div>
      </div>
      <svg
        width={width}
        height={height}
        viewBox={`0 0 ${width} ${height}`}
        style={{ marginTop: 14, display: 'block' }}
      >
        <rect width={width} height={height} fill={theme.inkBase} stroke={theme.steelSubtle} />
        {ceiling !== undefined ? (
          <>
            <line
              x1={0}
              x2={width}
              y1={(1 - ceiling) * height}
              y2={(1 - ceiling) * height}
              stroke={theme.warn}
              strokeDasharray="6 5"
              opacity={0.8}
            />
            {ceilingLabel ? (
              // Below the rule, not above it: at ceiling = 1 the line sits on
              // the top edge and a label above it renders outside the viewBox.
              <text
                x={width - 10}
                y={(1 - ceiling) * height + 24}
                textAnchor="end"
                fill={theme.warn}
                fontFamily={font.mono}
                fontSize={16}
              >
                {ceilingLabel}
              </text>
            ) : null}
          </>
        ) : null}
        {points.length > 1 ? (
          <path d={path} fill="none" stroke={color} strokeWidth={2.5} strokeLinejoin="round" />
        ) : null}
      </svg>
    </div>
  )
}

function BatchLog({ progress }: { progress: number }) {
  const committed = Math.floor(progress * TOTAL_BATCHES)
  const visible = 10

  const lines = Array.from({ length: visible }, (_, index) => committed - (visible - 1 - index))
    .filter((batch) => batch > 0)
    .map((batch) => ({
      batch,
      rows: 50_000,
    }))

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, minHeight: 250 }}>
      {lines.map(({ batch, rows }, index) => (
        <div
          key={batch}
          style={{
            fontFamily: font.mono,
            fontSize: 25,
            color: index === lines.length - 1 ? theme.paper : theme.paperMute,
            fontVariantNumeric: 'tabular-nums',
            opacity: 0.45 + (index / Math.max(1, lines.length - 1)) * 0.55,
          }}
        >
          batch {String(batch).padStart(4, '0')} {'  '}
          {integer(rows)} rows {'  '}staged{'  '}
          <span style={{ color: theme.ok }}>committed</span>
        </div>
      ))}
    </div>
  )
}

function StageBar({ name, progress }: { name: string; progress: number }) {
  // The four stages are concurrent on a streaming run, not sequential, so they
  // advance together with a small lag rather than one after another.
  const active = progress > 0.002
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
      <div
        style={{
          fontFamily: font.mono,
          fontSize: 22,
          letterSpacing: '0.16em',
          textTransform: 'uppercase',
          color: active ? theme.paper : theme.paperMute,
          width: 150,
        }}
      >
        {name}
      </div>
      <div style={{ flex: 1, height: 8, background: theme.inkBase, border: `1px solid ${theme.steelSubtle}` }}>
        <div
          style={{
            width: `${Math.min(1, progress) * 100}%`,
            height: '100%',
            background: theme.signal,
          }}
        />
      </div>
      <div
        style={{
          fontFamily: font.mono,
          fontSize: 22,
          color: theme.paperMute,
          width: 70,
          textAlign: 'right',
          fontVariantNumeric: 'tabular-nums',
        }}
      >
        {Math.round(Math.min(1, progress) * 100)}%
      </div>
    </div>
  )
}

export function Run() {
  const progress = useRunProgress()

  const rows = progress * TOTAL_ROWS
  const samples = 80
  const upTo = Math.max(1, Math.ceil(progress * samples))

  const rowPoints: [number, number][] = Array.from({ length: upTo }, (_, i) => {
    const t = (i / (samples - 1)) * progress
    return [t, t]
  })

  const memoryPoints: [number, number][] = Array.from({ length: upTo }, (_, i) => {
    const t = (i / (samples - 1)) * progress
    return [t, memoryAt(t) / CAP_MIB]
  })

  return (
    <AbsoluteFill style={{ background: theme.inkBase }}>
      <Grid />
      <AbsoluteFill
        style={{ padding: '72px 96px 168px', display: 'flex', flexDirection: 'row', gap: 56 }}
      >
        <Plate title="terminal" right="run 7c1f4a" style={{ flex: 1 }}>
          <Command text="loafer run orders.pipeline.yaml" startFrame={4} done />
          <div style={{ height: 30 }} />

          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {STAGES.map((stage, index) => (
              <StageBar
                key={stage}
                name={stage}
                progress={Math.max(0, progress - index * 0.012) / (1 - 0.036)}
              />
            ))}
          </div>

          <div style={{ height: 34, borderBottom: `1px solid ${theme.steelSubtle}` }} />
          <div style={{ height: 26 }} />

          <BatchLog progress={progress} />
        </Plate>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 44, width: 560 }}>
          <Chart
            label="rows moved"
            value={integer(rows)}
            points={rowPoints}
            color={theme.signal}
          />
          <Chart
            label="resident memory"
            value={`${(progress > 0 ? Math.min(PEAK_MIB, memoryAt(progress)) : 0).toFixed(2)} MiB`}
            points={memoryPoints}
            ceiling={1}
            ceilingLabel="512 MiB cap"
            color={theme.ok}
          />
          <div style={{ display: 'flex', gap: 56 }}>
            {[
              ['batches', integer(progress * TOTAL_BATCHES)],
              ['elapsed', clock(progress * WALL_CLOCK_SECONDS)],
            ].map(([label, value]) => (
              <div key={label}>
                <Stamp>{label}</Stamp>
                <div
                  style={{
                    marginTop: 10,
                    fontFamily: font.mono,
                    fontSize: 32,
                    color: theme.paper,
                    fontVariantNumeric: 'tabular-nums',
                  }}
                >
                  {value}
                </div>
              </div>
            ))}
          </div>
        </div>
      </AbsoluteFill>
      <Caption
        step={4}
        total={5}
        label="run"
        note="Rows climb to thirty million. Memory does not move."
      />
    </AbsoluteFill>
  )
}
