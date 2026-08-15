import type { Metadata } from 'next'
import type { ReactNode } from 'react'
import { site, siteUrl } from '@/src/lib/site'
import '../src/index.css'

export const metadata: Metadata = {
  metadataBase: new URL(siteUrl),

  /*
   * The default title has to answer "what is this" inside the ~60 characters a
   * result actually renders. "Loafer" alone is a shoe; the category words are
   * what make the brand name resolvable at all.
   */
  title: {
    default: 'Loafer — Open-source ETL and ELT engine you self-host',
    template: '%s | Loafer',
  },
  description: site.description,
  applicationName: site.name,
  authors: [{ name: site.author }],
  creator: site.author,
  publisher: site.author,
  category: 'technology',

  keywords: [
    'ETL tool',
    'ELT tool',
    'open source ETL',
    'self-hosted ETL',
    'data pipeline',
    'YAML data pipeline',
    'Python ETL',
    'PostgreSQL ETL',
    'MongoDB ETL',
    'CSV to PostgreSQL',
    'incremental extraction',
    'data engineering',
    'change data capture alternative',
    'loafer-etl',
  ],

  alternates: {
    canonical: '/',
  },

  openGraph: {
    type: 'website',
    siteName: site.name,
    locale: 'en_US',
    url: '/',
    title: 'Loafer — Open-source ETL and ELT engine you self-host',
    description: site.description,
  },

  twitter: {
    card: 'summary_large_image',
    title: 'Loafer — Open-source ETL and ELT engine you self-host',
    description: site.tagline,
  },

  icons: {
    icon: [{ url: '/favicon.svg', type: 'image/svg+xml' }],
  },

  robots: {
    index: true,
    follow: true,
    googleBot: {
      index: true,
      follow: true,
      'max-image-preview': 'large',
      'max-snippet': -1,
      'max-video-preview': -1,
    },
  },
}

/*
 * Structured data.
 *
 * SoftwareApplication is the type that makes an open-source developer tool
 * eligible for a software result, and it is the only place the price of zero
 * can be stated in a machine-readable way. WebSite carries the docs search
 * endpoint so a sitelinks searchbox is at least possible.
 *
 * Everything asserted here is verifiable from the repository. No aggregate
 * ratings, no invented install counts.
 */
function StructuredData() {
  const graph = {
    '@context': 'https://schema.org',
    '@graph': [
      {
        '@type': 'SoftwareApplication',
        '@id': `${siteUrl}/#software`,
        name: 'Loafer',
        alternateName: 'loafer-etl',
        applicationCategory: 'DeveloperApplication',
        applicationSubCategory: 'ETL and ELT engine',
        operatingSystem: 'Linux, macOS',
        description: site.description,
        url: siteUrl,
        codeRepository: site.repository,
        downloadUrl: site.packageIndex,
        installUrl: site.packageIndex,
        license: 'https://opensource.org/licenses/MIT',
        programmingLanguage: 'Python',
        softwareRequirements: 'Python 3.11 or newer',
        offers: {
          '@type': 'Offer',
          price: '0',
          priceCurrency: 'USD',
        },
        author: {
          '@type': 'Person',
          name: site.author,
        },
        featureList: [
          'Declarative ETL and ELT pipelines defined in YAML',
          'PostgreSQL, MySQL, SQLite, MongoDB, REST, CSV, Excel, and PDF sources',
          'PostgreSQL, MongoDB, JSON, and CSV targets',
          'SQL, Python, LLM-generated, and multi-step transforms',
          'Bounded row-local batches with constant memory',
          'SHA-256 run reconciliation and atomic publication',
          'Durable runs with leases, fencing tokens, and checkpoint replay',
          'Cursor-based incremental extraction',
          'Self-hosted HTTPS control plane with workspace roles',
        ],
      },
      {
        '@type': 'WebSite',
        '@id': `${siteUrl}/#website`,
        name: site.name,
        url: siteUrl,
        description: site.tagline,
        inLanguage: 'en',
        license: 'https://opensource.org/licenses/MIT',
        about: { '@id': `${siteUrl}/#software` },
      },
    ],
  }

  return (
    <script
      type="application/ld+json"
      // The payload is a literal built above; there is no user input in it.
      dangerouslySetInnerHTML={{ __html: JSON.stringify(graph) }}
    />
  )
}

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <body>
        {children}
        <StructuredData />
      </body>
    </html>
  )
}
