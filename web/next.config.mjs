import createMDX from '@next/mdx'

const withMDX = createMDX({
  extension: /\.mdx?$/,
  options: {
    remarkPlugins: ['remark-gfm'],
    rehypePlugins: [
      [
        'rehype-pretty-code',
        {
          theme: 'github-dark',
          keepBackground: false,
        },
      ],
    ],
  },
})

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  pageExtensions: ['js', 'jsx', 'md', 'mdx', 'ts', 'tsx'],
  poweredByHeader: false,

  // The social card renders Geist through satori, which needs the font files
  // themselves. They are read from disk, so tracing cannot infer them.
  outputFileTracingIncludes: {
    '/opengraph-image': ['./app/_og/**'],
  },
}

export default withMDX(nextConfig)
