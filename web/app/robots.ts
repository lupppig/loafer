import type { MetadataRoute } from 'next'
import { absoluteUrl } from '@/src/lib/site'

/*
 * /api is the auth boundary and the control-plane proxy; neither has anything
 * to index and both would return errors to a crawler.
 */
export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      {
        userAgent: '*',
        allow: '/',
        disallow: ['/api/'],
      },
    ],
    sitemap: absoluteUrl('/sitemap.xml'),
    host: absoluteUrl('/'),
  }
}
