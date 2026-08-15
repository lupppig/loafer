import type { Metadata } from 'next'
import { Changelog } from '@/src/screens/Changelog'

export const metadata: Metadata = {
  title: 'Changelog',
  description:
    'Release notes for Loafer, the open-source ETL and ELT engine: connectors, transform modes, bounded execution, durability, and control-plane changes by version.',
  alternates: { canonical: '/changelog' },
  openGraph: {
    type: 'article',
    url: '/changelog',
    title: 'Changelog | Loafer',
    description: 'Release notes for the Loafer ETL and ELT engine, by version.',
  },
}

export default function ChangelogPage() {
  return <Changelog />
}
