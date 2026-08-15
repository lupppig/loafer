import { AbsoluteFill, interpolate, useCurrentFrame } from 'remotion'
import { Caption, Grid, Plate, Stamp } from '../components/Frame'
import { CodeType, type Line } from '../components/Type'
import { font, theme } from '../theme'

/*
 * The pipeline gets written in front of the viewer, one block at a time, and
 * each block is explained the moment it finishes typing rather than in a
 * voiceover nobody will hear.
 *
 * The annotation frames below are derived from the typing rate: at 34
 * characters per second, the `source` block completes around frame 91, and so
 * on. They are constants rather than a computation because the snippet is
 * fixed, but they are why the typing speed is not a free parameter — changing
 * it means re-deriving these.
 */

const K = theme.signal
const V = theme.paper
const D = theme.paperDim

const YAML: Line[] = [
  [{ t: 'name', c: K }, { t: ': ' }, { t: 'daily_orders', c: V }],
  [{ t: 'mode', c: K }, { t: ': ' }, { t: 'etl', c: V }],
  [],
  [{ t: 'source', c: K }, { t: ':' }],
  [{ t: '  url', c: K }, { t: ': ' }, { t: '${DATABASE_URL}', c: V }],
  [{ t: '  query', c: K }, { t: ': ' }, { t: 'SELECT * FROM orders', c: V }],
  [],
  [{ t: 'transform', c: K }, { t: ':' }],
  [{ t: '  instruction', c: K }, { t: ': ' }, { t: 'Normalize currency to USD', c: V }],
  [],
  [{ t: 'target', c: K }, { t: ':' }],
  [{ t: '  path', c: K }, { t: ': ' }, { t: './output/orders.json', c: V }],
  [{ t: '  write_mode', c: K }, { t: ': ' }, { t: 'overwrite', c: V }],
  [],
  [{ t: 'execution', c: K }, { t: ':' }],
  [{ t: '  transform_class', c: K }, { t: ': ' }, { t: 'row_local', c: V }],
  [{ t: '  schema_drift', c: K }, { t: ': ' }, { t: 'fail', c: V }],
]

const NOTES = [
  {
    at: 95,
    key: 'source',
    title: 'Any source, inferred',
    body: 'The connector is read from the URL scheme. Postgres, MySQL, Mongo, REST, CSV, Excel, PDF.',
  },
  {
    at: 142,
    key: 'transform',
    title: 'SQL, Python, or plain English',
    body: 'The field you set picks the mode. This one asks an LLM for an artifact, then validates it.',
  },
  {
    at: 197,
    key: 'target',
    title: 'Written atomically',
    body: 'Staged first, moved into place in one step. A failed run leaves no half-written file.',
  },
  {
    at: 251,
    key: 'execution',
    title: 'Bounded, and strict',
    body: 'row_local streams in batches at flat memory. Drift fails the run instead of corrupting it.',
  },
] as const

function Note({ note, index }: { note: (typeof NOTES)[number]; index: number }) {
  const frame = useCurrentFrame()
  const progress = interpolate(frame, [note.at, note.at + 16], [0, 1], {
    extrapolateLeft: 'clamp',
    extrapolateRight: 'clamp',
  })

  return (
    <div
      style={{
        opacity: progress,
        transform: `translateX(${(1 - progress) * 26}px)`,
        borderLeft: `2px solid ${progress > 0.5 ? theme.signal : theme.steel}`,
        paddingLeft: 24,
        paddingTop: 4,
        paddingBottom: 4,
      }}
    >
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 14 }}>
        <span
          style={{
            fontFamily: font.mono,
            fontSize: 20,
            color: theme.signal,
            fontVariantNumeric: 'tabular-nums',
          }}
        >
          {String(index + 1).padStart(2, '0')}
        </span>
        <span style={{ fontFamily: font.mono, fontSize: 24, color: theme.paperMute }}>
          {note.key}:
        </span>
        <span
          style={{
            fontFamily: font.sans,
            fontSize: 28,
            fontWeight: 600,
            color: theme.paper,
            letterSpacing: '-0.01em',
          }}
        >
          {note.title}
        </span>
      </div>
      <div
        style={{
          marginTop: 10,
          fontFamily: font.sans,
          fontSize: 24,
          lineHeight: 1.5,
          color: D,
          maxWidth: 640,
        }}
      >
        {note.body}
      </div>
    </div>
  )
}

export function Declare() {
  return (
    <AbsoluteFill style={{ background: theme.inkBase }}>
      <Grid />
      <AbsoluteFill
        style={{
          padding: '84px 110px 170px',
          display: 'flex',
          flexDirection: 'row',
          gap: 64,
          alignItems: 'stretch',
        }}
      >
        <Plate title="orders.pipeline.yaml" right="etl" style={{ width: 860 }}>
          <CodeType lines={YAML} startFrame={12} charsPerSecond={34} fontSize={29} />
        </Plate>

        <div
          style={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            gap: 40,
          }}
        >
          <Stamp>one file, four blocks</Stamp>
          {NOTES.map((note, index) => (
            <Note key={note.key} note={note} index={index} />
          ))}
        </div>
      </AbsoluteFill>
      <Caption
        step={2}
        total={5}
        label="declare"
        note="A source, a transformation, a target. That is the whole pipeline."
      />
    </AbsoluteFill>
  )
}
