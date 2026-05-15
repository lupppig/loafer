import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import mdx from '@mdx-js/rollup'
import rehypePrettyCode from 'rehype-pretty-code'
import remarkGfm from 'remark-gfm'

const prettyCodeOptions = {
  theme: 'github-dark',
  keepBackground: false,
};

export default defineConfig({
  plugins: [
    { 
      enforce: 'pre', 
      ...mdx({
        providerImportSource: '@mdx-js/react',
        remarkPlugins: [remarkGfm],
        rehypePlugins: [[rehypePrettyCode, prettyCodeOptions]]
      })
    },
    react(),
    tailwindcss(),
  ],
})
