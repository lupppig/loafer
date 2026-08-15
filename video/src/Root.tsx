import { Composition } from 'remotion'
import { Demo } from './Demo'
import { FPS, TOTAL_FRAMES } from './theme'

/*
 * 1920x1080 because the YAML and terminal output have to survive being scaled
 * into a 16:9 figure on the home page and still be readable on a laptop.
 */
export function RemotionRoot() {
  return (
    <Composition
      id="Demo"
      component={Demo}
      durationInFrames={TOTAL_FRAMES}
      fps={FPS}
      width={1920}
      height={1080}
    />
  )
}
