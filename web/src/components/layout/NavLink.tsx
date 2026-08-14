'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import type { AnchorHTMLAttributes } from 'react'
import { cn } from '../../utils/cn'

export interface NavLinkProps
  extends Omit<AnchorHTMLAttributes<HTMLAnchorElement>, 'href'> {
  href: string
  activeClassName?: string
}

export function NavLink({ className, activeClassName, href, ...props }: NavLinkProps) {
  const pathname = usePathname()
  const isActive = pathname === href || (href !== '/' && pathname.startsWith(`${href}/`))

  return (
    <Link
      {...props}
      href={href}
      className={cn(
        'text-[13px] text-text-secondary transition-colors hover:text-text-primary hover:underline underline-offset-4 decoration-border-strong rounded-sm outline-none focus-visible:ring-2 focus-visible:ring-signal',
        isActive && cn('text-text-primary', activeClassName),
        className,
      )}
    />
  )
}
