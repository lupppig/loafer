import type { Metadata } from 'next'
import type { ReactNode } from 'react'
import '../src/index.css'

export const metadata: Metadata = {
  title: {
    default: 'Loafer | Self-hosted ETL and ELT with verifiable runs',
    template: '%s | Loafer',
  },
  description:
    'Open-source ETL and ELT engine. Declare a pipeline in YAML, run it on your own infrastructure, and reconcile what landed against what was read.',
  icons: {
    icon: '/favicon.svg',
  },
}

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
