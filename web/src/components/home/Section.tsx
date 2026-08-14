import type { ReactNode } from 'react'
import { cn } from '../../utils/cn'

interface SectionProps {
  id?: string
  children: ReactNode
  className?: string
  /** Ink surface instead of the base ground, for alternating weight. */
  raised?: boolean
}

export function Section({ id, children, className, raised = false }: SectionProps) {
  return (
    <section
      id={id}
      className={cn(
        'w-full border-t border-steel-subtle px-6 py-20 md:py-28 lg:px-10',
        raised ? 'bg-ink-surface' : 'bg-ink-base',
        className,
      )}
    >
      <div className="mx-auto w-full max-w-[1180px]">{children}</div>
    </section>
  )
}

interface SectionHeadingProps {
  stamp?: string
  title: string
  lede?: string
  className?: string
}

export function SectionHeading({ stamp, title, lede, className }: SectionHeadingProps) {
  return (
    <div className={cn('max-w-[62ch]', className)}>
      {stamp ? <p className="stamp mb-5">{stamp}</p> : null}
      <h2 className="text-balance text-[28px] font-semibold leading-[1.15] tracking-[-0.02em] text-paper md:text-[38px]">
        {title}
      </h2>
      {lede ? (
        <p className="mt-4 max-w-[54ch] text-[15px] leading-[1.65] text-paper-dim md:text-[16px]">
          {lede}
        </p>
      ) : null}
    </div>
  )
}
