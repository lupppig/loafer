import { ImageResponse } from 'next/og'

/*
 * iOS home-screen icon. Apple ignores SVG favicons, so this is the one place
 * the mark has to be rasterized. Generated from the same arrow path as
 * `LoaferMark` rather than a second hand-maintained file.
 */

export const size = { width: 180, height: 180 }
export const contentType = 'image/png'

const ARROW_PATH =
  'M25.946 44.938c-.664.845-2.021.375-2.021-.698V33.937a2.26 2.26 0 0 0-2.262-2.262H10.287c-.92 0-1.456-1.04-.92-1.788l7.48-10.471c1.07-1.497 0-3.578-1.842-3.578H1.237c-.92 0-1.456-1.04-.92-1.788L10.013.474c.214-.297.556-.474.92-.474h28.894c.92 0 1.456 1.04.92 1.788l-7.48 10.471c-1.07 1.498 0 3.579 1.842 3.579h11.377c.943 0 1.473 1.088.89 1.83L25.947 44.94z'

const MARK_DATA_URI = `data:image/svg+xml;base64,${Buffer.from(
  `<svg xmlns="http://www.w3.org/2000/svg" width="48" height="46" viewBox="0 0 48 46"><path fill="#d95f2a" d="${ARROW_PATH}"/></svg>`,
).toString('base64')}`

export default function AppleIcon() {
  return new ImageResponse(
    (
      <div
        style={{
          width: '100%',
          height: '100%',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          backgroundColor: '#131615',
        }}
      >
        <img src={MARK_DATA_URI} width={104} height={100} alt="" />
      </div>
    ),
    size,
  )
}
