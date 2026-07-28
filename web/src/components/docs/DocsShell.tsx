'use client'

import { useEffect, useState, type ReactNode } from 'react'
import { usePathname } from 'next/navigation'
import { ChevronRight, Menu } from 'lucide-react'
import { LeftNav } from './LeftNav'
import { SearchModal } from './SearchModal'
import { TableOfContents } from './TableOfContents'
import { cn } from '../../utils/cn'

export function DocsShell({ children }: Readonly<{ children: ReactNode }>) {
  const [searchOpen, setSearchOpen] = useState(false)
  const [mobileNavOpen, setMobileNavOpen] = useState(false)
  const pathname = usePathname()

  useEffect(() => {
    const handleDown = (event: KeyboardEvent) => {
      if (event.key === 'k' && (event.metaKey || event.ctrlKey)) {
        event.preventDefault()
        setSearchOpen(true)
      }
    }
    document.addEventListener('keydown', handleDown)
    return () => document.removeEventListener('keydown', handleDown)
  }, [])

  const pageTitle = pathname.split('/').pop()?.replace(/-/g, ' ') || 'Docs'

  return (
    <>
      <div className="lg:hidden sticky top-[52px] z-30 flex items-center justify-between w-full h-10 px-4 bg-bg-surface border-b border-border-subtle backdrop-blur-md bg-bg-surface/90">
        <button
          type="button"
          onClick={() => setMobileNavOpen(true)}
          className="flex items-center gap-2 text-[13px] font-medium text-text-secondary hover:text-text-primary transition-colors outline-none"
        >
          <Menu className="w-4 h-4" />
          <span>Menu</span>
        </button>
        <div className="flex items-center gap-1.5 text-[12px] text-text-muted font-medium capitalize">
          Docs <ChevronRight className="w-3 h-3 opacity-50" /> {pageTitle}
        </div>
      </div>

      <div className="flex w-full max-w-[1440px] mx-auto items-start relative">
        <div
          className={cn(
            'fixed inset-0 z-50 bg-bg-base/60 backdrop-blur-sm lg:hidden transition-opacity duration-300',
            mobileNavOpen ? 'opacity-100 pointer-events-auto' : 'opacity-0 pointer-events-none',
          )}
          onClick={() => setMobileNavOpen(false)}
        />

        <LeftNav
          onSearchClick={() => setSearchOpen(true)}
          isMobileOpen={mobileNavOpen}
          onClose={() => setMobileNavOpen(false)}
        />

        <main className="flex-1 min-w-0 flex justify-center px-6 md:px-12 pt-8 lg:pt-12 pb-24 relative">
          <article className="w-full max-w-[680px]">{children}</article>
        </main>

        <TableOfContents key={pathname} />
      </div>

      <SearchModal isOpen={searchOpen} onClose={() => setSearchOpen(false)} />
    </>
  )
}
