import { AbsoluteFill } from 'remotion'
import { Caption, Grid, Plate } from '../components/Frame'
import { Command, Output } from '../components/Type'
import { theme } from '../theme'

/*
 * Step one is deliberately unglamorous: it is one pip install and no account.
 * Showing the version echo matters more than it looks — it is the proof that
 * "installed" and "working" are the same event here.
 */
export function Install() {
  return (
    <AbsoluteFill style={{ background: theme.inkBase }}>
      <Grid />
      <AbsoluteFill style={{ padding: '96px 120px 160px' }}>
        <Plate title="terminal" right="~/analytics" style={{ flex: 1 }}>
          <Command text="pip install loafer-etl" startFrame={6} />
          <div style={{ height: 18 }} />
          <Output
            startFrame={52}
            framesPerLine={8}
            lines={[
              { text: 'Collecting loafer-etl' },
              { text: 'Successfully installed loafer-etl', color: theme.ok },
            ]}
          />
          <div style={{ height: 24 }} />
          <Command text="loafer --version" startFrame={76} done />
          <div style={{ height: 14 }} />
          <Output
            startFrame={104}
            lines={[{ text: 'loafer 0.4.0', color: theme.paper }]}
          />
        </Plate>
      </AbsoluteFill>
      <Caption step={1} total={5} label="install" note="One package. No account, no hosted service." />
    </AbsoluteFill>
  )
}
