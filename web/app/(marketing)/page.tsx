import type { Metadata } from 'next'
import { Home } from '@/src/screens/Home'
import { site } from '@/src/lib/site'

/*
 * The home page owns the primary query. The title states the category, the
 * licence, and the deployment model, because "Loafer" on its own competes with
 * footwear and wins nothing.
 */
export const metadata: Metadata = {
  // `absolute` so the parent "%s | Loafer" template does not append a second
  // "Loafer" to a title that already starts with it.
  title: { absolute: 'Loafer — Open-source ETL and ELT engine you self-host' },
  description: site.description,
  alternates: { canonical: '/' },
  openGraph: {
    url: '/',
    title: 'Loafer — Open-source ETL and ELT engine you self-host',
    description: site.description,
  },
}

export default function HomePage() {
  return <Home />
}
