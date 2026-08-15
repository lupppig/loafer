import { AbsoluteFill, Sequence } from 'remotion'
import { useGeist } from './components/Fonts'
import { Title } from './scenes/Title'
import { Install } from './scenes/Install'
import { Declare } from './scenes/Declare'
import { Validate } from './scenes/Validate'
import { Run } from './scenes/Run'
import { Reconcile } from './scenes/Reconcile'
import { End } from './scenes/End'
import { SCENES, sceneFrames, theme } from './theme'

/*
 * The cut.
 *
 * Scenes are laid end to end from the durations in theme.ts rather than from
 * hand-written frame offsets, so re-timing one scene does not require
 * recalculating every scene after it.
 *
 * There are no crossfades. The demo is about a tool that does discrete,
 * verifiable steps, and hard cuts between stamped plates suit it better than
 * dissolves — which would also cost render time for no comprehension gain.
 */

const ORDER = [
  ['title', Title],
  ['install', Install],
  ['declare', Declare],
  ['validate', Validate],
  ['run', Run],
  ['reconcile', Reconcile],
  ['end', End],
] as const

export function Demo() {
  useGeist()

  let cursor = 0

  return (
    <AbsoluteFill style={{ background: theme.inkBase }}>
      {ORDER.map(([key, Scene]) => {
        const duration = sceneFrames(SCENES[key])
        const from = cursor
        cursor += duration
        return (
          <Sequence key={key} from={from} durationInFrames={duration} name={key}>
            <Scene />
          </Sequence>
        )
      })}
    </AbsoluteFill>
  )
}
