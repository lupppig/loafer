import { AbsoluteFill } from 'remotion'
import { Caption, Grid, Plate } from '../components/Frame'
import { Command, Output } from '../components/Type'
import { theme } from '../theme'

/*
 * Validation is its own step in the video because it is its own step in the
 * product: the config, the connections, and the schema are all checked before
 * a single row is read. Getting a typo back in under a second is a different
 * experience from getting it back forty minutes into a load.
 */
export function Validate() {
  return (
    <AbsoluteFill style={{ background: theme.inkBase }}>
      <Grid />
      <AbsoluteFill style={{ padding: '96px 120px 160px' }}>
        <Plate title="terminal" right="~/analytics" style={{ flex: 1 }}>
          <Command text="loafer validate orders.pipeline.yaml" startFrame={4} done />
          <div style={{ height: 22 }} />
          <Output
            startFrame={48}
            framesPerLine={11}
            lines={[
              { text: '✓  config parsed          17 keys, 0 unknown', color: theme.ok },
              { text: '✓  source reachable       postgresql://…/orders', color: theme.ok },
              { text: '✓  schema resolved        4 columns, 0 drift', color: theme.ok },
              { text: '✓  transform compiled     row_local, sha256:9f3c1a…', color: theme.ok },
              { text: '✓  target writable        ./output/orders.json', color: theme.ok },
              { text: '' },
              { text: 'pipeline is valid — 0 errors, 0 warnings', color: theme.paper },
            ]}
          />
        </Plate>
      </AbsoluteFill>
      <Caption
        step={3}
        total={5}
        label="validate"
        note="Config, connections, and schema are checked before any row is read."
      />
    </AbsoluteFill>
  )
}
