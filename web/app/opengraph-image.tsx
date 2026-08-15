import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { ImageResponse } from 'next/og'

/*
 * The social card.
 *
 * A link to Loafer gets pasted into Slack, Hacker News, and Bluesky far more
 * often than the homepage gets visited directly, so this image is doing real
 * top-of-funnel work. It says the category in words, shows the mark, and
 * carries the one measured number — the same three things the hero says.
 *
 * Drawn on the brand palette rather than a screenshot, because a screenshot of
 * a dark terminal is illegible at the size these unfurls actually render.
 */

export const alt = 'Loafer — open-source ETL and ELT engine you self-host'
export const size = { width: 1200, height: 630 }
export const contentType = 'image/png'

const ARROW_PATH =
  'M25.946 44.938c-.664.845-2.021.375-2.021-.698V33.937a2.26 2.26 0 0 0-2.262-2.262H10.287c-.92 0-1.456-1.04-.92-1.788l7.48-10.471c1.07-1.497 0-3.578-1.842-3.578H1.237c-.92 0-1.456-1.04-.92-1.788L10.013.474c.214-.297.556-.474.92-.474h28.894c.92 0 1.456 1.04.92 1.788l-7.48 10.471c-1.07 1.498 0 3.579 1.842 3.579h11.377c.943 0 1.473 1.088.89 1.83L25.947 44.94z'

const MARK_DATA_URI = `data:image/svg+xml;base64,${Buffer.from(
  `<svg xmlns="http://www.w3.org/2000/svg" width="48" height="46" viewBox="0 0 48 46"><path fill="#d95f2a" d="${ARROW_PATH}"/></svg>`,
).toString('base64')}`

/*
 * Read off disk rather than through `fetch(new URL(..., import.meta.url))`.
 * Under webpack that pattern is rewritten to a `/_next/static/media/…` asset
 * path, which has no origin and so cannot be fetched during prerender. The
 * files are kept in the build output by `outputFileTracingIncludes` in
 * next.config.mjs.
 *
 * The files are TTF builds of @fontsource's Geist with the layout tables
 * stripped, because satori cannot parse woff2 and rejects Geist's GSUB. See
 * app/_og/README.md for why, and for how to regenerate them.
 */
const loadFont = (file: string) => readFile(join(process.cwd(), 'app', '_og', file))

export default async function OpenGraphImage() {
  const [sans700, sans400, mono500] = await Promise.all([
    loadFont('geist-sans-700.ttf'),
    loadFont('geist-sans-400.ttf'),
    loadFont('geist-mono-500.ttf'),
  ])

  return new ImageResponse(
    (
      <div
        style={{
          width: '100%',
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'space-between',
          backgroundColor: '#0d0f0e',
          // The blueprint rule, drawn as two repeating gradients so it costs
          // nothing to rasterize.
          backgroundImage:
            'linear-gradient(to right, #222726 1px, transparent 1px), linear-gradient(to bottom, #222726 1px, transparent 1px)',
          backgroundSize: '88px 88px',
          padding: '72px 80px',
          fontFamily: 'Geist',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 18 }}>
            <img src={MARK_DATA_URI} width={42} height={40} alt="" />
          <div
            style={{
              fontSize: 34,
              fontWeight: 700,
              color: '#ece9e1',
              letterSpacing: '-0.02em',
            }}
          >
            loafer
          </div>
          <div
            style={{
              marginLeft: 'auto',
              fontFamily: 'Geist Mono',
              fontSize: 19,
              letterSpacing: '0.16em',
              textTransform: 'uppercase',
              color: '#828b86',
            }}
          >
            mit licensed
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column' }}>
          <div
            style={{
              fontSize: 74,
              fontWeight: 700,
              lineHeight: 1.05,
              letterSpacing: '-0.035em',
              color: '#ece9e1',
              display: 'flex',
              flexDirection: 'column',
            }}
          >
            <span>Open-source ETL and ELT</span>
            {/* A non-breaking space: satori trims the trailing whitespace of a
                text node that sits directly before an element, which glues
                "you" to "self-host". */}
            <span>
              you{' '}
              <span style={{ color: '#d95f2a' }}>self-host</span>.
            </span>
          </div>
          <div
            style={{
              marginTop: 26,
              fontSize: 28,
              lineHeight: 1.45,
              color: '#a6ada7',
              maxWidth: 880,
            }}
          >
            Declare a pipeline in YAML. Run it on your own machines. Get a checksum that proves
            what landed.
          </div>
        </div>

        <div
          style={{
            display: 'flex',
            alignItems: 'stretch',
            borderTop: '1px solid #333a38',
            paddingTop: 26,
            gap: 56,
            fontFamily: 'Geist Mono',
          }}
        >
          {[
            ['30,000,000', 'rows moved'],
            ['118.23 MiB', 'peak memory'],
            ['sha-256', 'reconciled exact'],
          ].map(([value, label]) => (
            <div key={label} style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <div style={{ fontSize: 34, color: '#ece9e1', letterSpacing: '-0.02em' }}>{value}</div>
              <div
                style={{
                  fontSize: 17,
                  letterSpacing: '0.16em',
                  textTransform: 'uppercase',
                  color: '#828b86',
                }}
              >
                {label}
              </div>
            </div>
          ))}
        </div>
      </div>
    ),
    {
      ...size,
      fonts: [
        { name: 'Geist', data: sans700, style: 'normal', weight: 700 },
        { name: 'Geist', data: sans400, style: 'normal', weight: 400 },
        { name: 'Geist Mono', data: mono500, style: 'normal', weight: 500 },
      ],
    },
  )
}
