import type { ReactNode } from 'react'
import { Footer } from '@/src/components/layout/Footer'
import { Topbar } from '@/src/components/layout/Topbar'

export default function MarketingLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <div className="min-h-screen flex flex-col antialiased">
      <Topbar />
      <main className="flex-1 mt-[52px] flex flex-col w-full relative">{children}</main>
      <Footer />
    </div>
  )
}
