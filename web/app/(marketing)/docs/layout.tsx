import type { Metadata } from 'next'
import type { ReactNode } from 'react'
import { DocsShell } from '@/src/components/docs/DocsShell'

export const metadata: Metadata = {
  title: {
    default: 'Documentation',
    template: '%s | Loafer Docs',
  },
  description: 'Learn how to install, configure, and operate Loafer pipelines.',
}

export default function DocsLayout({ children }: Readonly<{ children: ReactNode }>) {
  return <DocsShell>{children}</DocsShell>
}
