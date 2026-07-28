import type { Metadata } from 'next'
import { Studio } from '@/src/screens/Studio'

export const metadata: Metadata = {
  title: 'Studio — Control plane preview',
  description: 'A preview of Loafer pipeline authoring and operations.',
}

export default function StudioPage() {
  return <Studio />
}
