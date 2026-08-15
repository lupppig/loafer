/*
 * Canonical site identity.
 *
 * Every absolute URL the site emits — canonical tags, Open Graph, sitemap,
 * robots, JSON-LD — resolves from `siteUrl`, so the deployment never has to be
 * told its own address twice.
 *
 * Resolution order:
 *   1. NEXT_PUBLIC_SITE_URL, for self-hosters serving from their own domain.
 *   2. VERCEL_PROJECT_PRODUCTION_URL, injected on Vercel builds. This is the
 *      stable production domain, not the per-deployment preview URL, so
 *      previews still emit canonicals pointing at production, which is what
 *      you want: previews must not compete for the same queries.
 *   3. localhost, for `next dev`.
 */

function resolveSiteUrl(): string {
  const explicit = process.env.NEXT_PUBLIC_SITE_URL?.trim()
  if (explicit) return explicit.replace(/\/$/, '')

  const vercel = process.env.VERCEL_PROJECT_PRODUCTION_URL?.trim()
  if (vercel) return `https://${vercel.replace(/\/$/, '')}`

  return 'http://localhost:3000'
}

export const siteUrl = resolveSiteUrl()

export const site = {
  url: siteUrl,
  name: 'Loafer',
  /* The one-line answer to "what is this". Used as the Open Graph description
     and as the JSON-LD abstract, so it has to stand alone with no page around
     it. */
  tagline: 'Open-source ETL and ELT engine you self-host.',
  description:
    'Loafer is an open-source ETL and ELT engine. Declare a data pipeline in YAML, ' +
    'extract from PostgreSQL, MySQL, MongoDB, REST APIs, CSV, Excel, or PDF, transform ' +
    'with SQL, Python, or an LLM, and load into PostgreSQL, MongoDB, JSON, or CSV — ' +
    'running entirely on your own infrastructure, with a checksum reconciling what landed ' +
    'against what was read.',
  repository: 'https://github.com/lupppig/loafer',
  packageIndex: 'https://pypi.org/project/loafer-etl/',
  license: 'MIT',
  author: 'Darasimi Kelani',
} as const

export function absoluteUrl(path: string): string {
  return new URL(path, siteUrl).toString()
}
