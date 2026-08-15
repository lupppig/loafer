import { Fragment } from 'react'
import { useCurrentFrame } from 'remotion'
import { font, theme, FPS } from '../theme'

/*
 * Typing, as a pure function of the frame.
 *
 * Nothing here holds state or runs a timer: at frame N the same characters are
 * always visible. That is what makes the render deterministic and what lets
 * any frame be re-rendered out of order by a distributed render without the
 * text desynchronising.
 */

export type Segment = { t: string; c?: string }
export type Line = Segment[]

function visibleCharCount(frame: number, startFrame: number, charsPerSecond: number): number {
  return Math.max(0, Math.floor(((frame - startFrame) / FPS) * charsPerSecond))
}

/** A blinking block cursor at 2Hz, the same rate a real terminal uses. */
function Cursor({ visible }: { visible: boolean }) {
  const frame = useCurrentFrame()
  const on = Math.floor(frame / (FPS / 4)) % 2 === 0
  if (!visible) return null
  return (
    <span
      style={{
        display: 'inline-block',
        width: '0.55em',
        height: '1.05em',
        background: on ? theme.signal : 'transparent',
        verticalAlign: 'text-bottom',
        marginLeft: 2,
      }}
    />
  )
}

/**
 * Types a block of syntax-coloured lines. Colour is carried per segment rather
 * than re-derived from a highlighter, because the snippets are fixed and a
 * tokenizer would be a dependency doing no work.
 */
export function CodeType({
  lines,
  startFrame = 0,
  charsPerSecond = 34,
  fontSize = 30,
  showCursor = true,
}: {
  lines: Line[]
  startFrame?: number
  charsPerSecond?: number
  fontSize?: number
  showCursor?: boolean
}) {
  const frame = useCurrentFrame()

  const total = lines.reduce(
    (sum, line, index) => sum + line.reduce((s, seg) => s + seg.t.length, 0) + (index > 0 ? 1 : 0),
    0,
  )
  const budget = visibleCharCount(frame, startFrame, charsPerSecond)
  const done = budget >= total

  let remaining = budget

  return (
    <pre
      style={{
        margin: 0,
        fontFamily: font.mono,
        fontSize,
        lineHeight: 1.65,
        color: theme.paperDim,
        whiteSpace: 'pre-wrap',
      }}
    >
      {lines.map((line, lineIndex) => {
        if (lineIndex > 0) {
          if (remaining <= 0) return null
          remaining -= 1
        }

        const rendered = line.map((segment, segmentIndex) => {
          if (remaining <= 0) return null
          const take = Math.min(remaining, segment.t.length)
          remaining -= take
          return (
            <span key={segmentIndex} style={{ color: segment.c ?? theme.paperDim }}>
              {segment.t.slice(0, take)}
            </span>
          )
        })

        return (
          <Fragment key={lineIndex}>
            {lineIndex > 0 ? '\n' : null}
            {rendered}
          </Fragment>
        )
      })}
      <Cursor visible={showCursor && !done} />
    </pre>
  )
}

/** A single typed command line, prefixed with a prompt. */
export function Command({
  text,
  startFrame = 0,
  charsPerSecond = 26,
  fontSize = 30,
  done = false,
}: {
  text: string
  startFrame?: number
  charsPerSecond?: number
  fontSize?: number
  done?: boolean
}) {
  const frame = useCurrentFrame()
  const shown = Math.min(text.length, visibleCharCount(frame, startFrame, charsPerSecond))

  return (
    <div
      style={{
        fontFamily: font.mono,
        fontSize,
        lineHeight: 1.6,
        color: theme.paper,
        display: 'flex',
        alignItems: 'baseline',
        gap: '0.6em',
      }}
    >
      <span style={{ color: theme.signal }}>$</span>
      <span>
        {text.slice(0, shown)}
        <Cursor visible={!done && shown < text.length} />
      </span>
    </div>
  )
}

/**
 * Terminal output lines that appear one at a time. Real command output does
 * not type itself in character by character; it lands whole.
 */
export function Output({
  lines,
  startFrame,
  framesPerLine = 7,
  fontSize = 27,
}: {
  lines: { text: string; color?: string }[]
  startFrame: number
  framesPerLine?: number
  fontSize?: number
}) {
  const frame = useCurrentFrame()
  const shown = Math.max(0, Math.floor((frame - startFrame) / framesPerLine))

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      {lines.slice(0, shown).map((line, index) => (
        <div
          key={index}
          style={{
            fontFamily: font.mono,
            fontSize,
            lineHeight: 1.55,
            color: line.color ?? theme.paperDim,
            fontVariantNumeric: 'tabular-nums',
          }}
        >
          {line.text}
        </div>
      ))}
    </div>
  )
}
