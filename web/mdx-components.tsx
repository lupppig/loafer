import type { MDXComponents as MDXComponentMap } from 'mdx/types'
import { MDXComponents } from './src/components/docs/MDXComponents'

export function useMDXComponents(components: MDXComponentMap): MDXComponentMap {
  return {
    ...MDXComponents,
    ...components,
  }
}
