import type { Metadata } from 'next'
import { notFound } from 'next/navigation'
import { docs, docSlugs, isDocSlug } from '@/src/content/docs'

type DocPageProps = {
  params: Promise<{ slug: string }>
}

export function generateStaticParams() {
  return docSlugs.map((slug) => ({ slug }))
}

export async function generateMetadata({ params }: DocPageProps): Promise<Metadata> {
  const { slug } = await params
  if (!isDocSlug(slug)) return {}

  const doc = docs[slug]

  /*
   * The docs are the long tail — twelve pages that each answer one
   * configuration question. Each needs its own canonical, or the marketing
   * layout's inherited canonical points every one of them at the homepage.
   */
  return {
    title: doc.title,
    description: doc.description,
    alternates: { canonical: `/docs/${slug}` },
    openGraph: {
      type: 'article',
      url: `/docs/${slug}`,
      title: `${doc.title} | Loafer`,
      description: doc.description,
    },
  }
}

export default async function DocPage({ params }: DocPageProps) {
  const { slug } = await params
  if (!isDocSlug(slug)) notFound()

  const Content = docs[slug].component
  return <Content />
}
