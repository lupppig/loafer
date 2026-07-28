import type { Metadata } from 'next'
import { Home } from '@/src/screens/Home'

export const metadata: Metadata = {
  title: 'Your data pipeline in plain English',
  description:
    'Connect a source, describe your transformation, and load clean data with Loafer.',
}

export default function HomePage() {
  return <Home />
}
