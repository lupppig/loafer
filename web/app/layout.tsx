import type { Metadata } from 'next'
import type { ReactNode } from 'react'
import '../src/index.css'

export const metadata: Metadata = {
  title: {
    default: 'Loafer | Your data pipeline in plain English',
    template: '%s | Loafer',
  },
  description:
    'Open-source ETL/ELT tooling for authoring, running, and observing reliable data pipelines.',
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
