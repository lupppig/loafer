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
  return {
    title: docs[slug].title,
    description: docs[slug].description,
  }
}

export default async function DocPage({ params }: DocPageProps) {
  const { slug } = await params
  if (!isDocSlug(slug)) notFound()

  const Content = docs[slug].component
  return <Content />
}
