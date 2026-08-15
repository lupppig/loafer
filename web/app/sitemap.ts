import type { MetadataRoute } from 'next'
import { docSlugs } from '@/src/content/docs'
import { absoluteUrl } from '@/src/lib/site'

/*
 * The docs are the long tail: twelve pages that each answer a specific
 * configuration question, which is what people actually search for. They are
 * listed individually rather than left to be discovered through the nav.
 */
export default function sitemap(): MetadataRoute.Sitemap {
  const lastModified = new Date()

  return [
    {
      url: absoluteUrl('/'),
      lastModified,
      changeFrequency: 'weekly',
      priority: 1,
    },
    {
      url: absoluteUrl('/docs/introduction'),
      lastModified,
      changeFrequency: 'weekly',
      priority: 0.9,
    },
    ...docSlugs
      .filter((slug) => slug !== 'introduction')
      .map((slug) => ({
        url: absoluteUrl(`/docs/${slug}`),
        lastModified,
        changeFrequency: 'weekly' as const,
        priority: 0.7,
      })),
    {
      url: absoluteUrl('/changelog'),
      lastModified,
      changeFrequency: 'weekly',
      priority: 0.5,
    },
  ]
}
