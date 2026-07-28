import type { Metadata } from 'next'
import { Changelog } from '@/src/screens/Changelog'

export const metadata: Metadata = {
  title: 'Changelog',
  description: 'Release notes and improvements for the Loafer data platform.',
}

export default function ChangelogPage() {
  return <Changelog />
}
