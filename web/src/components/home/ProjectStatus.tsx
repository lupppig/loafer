import Link from 'next/link'
import { Section, SectionHeading } from './Section'
import { cn } from '../../utils/cn'

/*
 * The status ledger.
 *
 * Loafer's own readiness assessment refuses to claim a property it has not
 * measured. This section is that document, condensed: it is the last thing a
 * reader sees, and it is the reason to trust everything above it.
 */
type State = 'shipping' | 'building' | 'planned'

const LEDGER: { area: string; state: State; detail: string }[] = [
  {
    area: 'CLI engine and bounded data plane',
    state: 'shipping',
    detail: 'Declarative ETL and ELT, row-local batches, atomic publication, incremental cursors.',
  },
  {
    area: 'Durable runs and crash recovery',
    state: 'shipping',
    detail: 'Leases, fencing tokens, batch checkpoints, replay from the last committed position.',
  },
  {
    area: 'Authenticated control plane',
    state: 'shipping',
    detail: 'HTTPS /api/v1, workspace roles, audit events, idempotent commands, SSE run events.',
  },
  {
    area: 'Distributed workers and job transport',
    state: 'building',
    detail: 'Queue port and adapters landed. Role-isolated pools and the outbox relay are next.',
  },
  {
    area: 'Web operations dashboard',
    state: 'building',
    detail: 'The Studio route is a preview on fixture data and is labelled as one.',
  },
  {
    area: 'Crawling, OCR, and further connectors',
    state: 'planned',
    detail: 'Specified in the roadmap. No implementation yet, and no claims made for them.',
  },
]

const STATE_LABEL: Record<State, string> = {
  shipping: 'Shipping',
  building: 'Building',
  planned: 'Planned',
}

const STATE_STYLE: Record<State, string> = {
  shipping: 'border-state-ok text-state-ok',
  building: 'border-state-warn text-state-warn',
  planned: 'border-steel-strong text-paper-mute',
}

export function ProjectStatus() {
  return (
    <Section id="status">
      <SectionHeading
        title="What ships, what does not, and how you can tell."
        lede="Loafer publishes the runs that failed alongside the ones that passed. If a capability is not on the shipping line below, it is not something you should plan around yet."
      />

      <table className="mt-14 w-full border-collapse text-left">
        <caption className="sr-only">Loafer capability status by area</caption>
        <thead>
          <tr className="border-b border-steel">
            <th scope="col" className="stamp py-3 pr-6 font-medium">
              Area
            </th>
            <th scope="col" className="stamp hidden py-3 pr-6 font-medium md:table-cell">
              Detail
            </th>
            <th scope="col" className="stamp py-3 text-right font-medium">
              Status
            </th>
          </tr>
        </thead>
        <tbody>
          {LEDGER.map((row) => (
            <tr key={row.area} className="border-b border-steel-subtle align-top">
              <td className="py-5 pr-6">
                <span className="text-[15px] font-medium leading-snug tracking-[-0.01em] text-paper">
                  {row.area}
                </span>
                <span className="mt-2 block text-[13px] leading-[1.6] text-paper-dim md:hidden">
                  {row.detail}
                </span>
              </td>
              <td className="hidden max-w-[48ch] py-5 pr-6 text-[13.5px] leading-[1.6] text-paper-dim md:table-cell">
                {row.detail}
              </td>
              <td className="py-5 text-right">
                <span
                  className={cn(
                    'stamp inline-block whitespace-nowrap border px-2.5 py-1',
                    STATE_STYLE[row.state],
                  )}
                >
                  {STATE_LABEL[row.state]}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <div className="mt-14 flex flex-col gap-6 border-t border-steel pt-8 md:flex-row md:items-center md:justify-between">
        <p className="max-w-[46ch] text-[14.5px] leading-[1.65] text-paper-dim">
          MIT licensed. Connector, engine, deployment, and reliability contributions are welcome.
        </p>
        <nav aria-label="Project resources" className="flex flex-wrap items-center gap-x-7 gap-y-3">
          {[
            { href: '/docs', label: 'Documentation', external: false },
            { href: '/changelog', label: 'Changelog', external: false },
            {
              href: 'https://github.com/lupppig/loafer/blob/main/PRODUCTION_READINESS.md',
              label: 'Readiness assessment',
              external: true,
            },
            { href: 'https://pypi.org/project/loafer-etl/', label: 'PyPI', external: true },
          ].map((item) =>
            item.external ? (
              <a
                key={item.label}
                href={item.href}
                target="_blank"
                rel="noopener noreferrer"
                className="text-[14px] font-medium text-paper-dim transition-colors hover:text-signal"
              >
                {item.label}
              </a>
            ) : (
              <Link
                key={item.label}
                href={item.href}
                className="text-[14px] font-medium text-paper-dim transition-colors hover:text-signal"
              >
                {item.label}
              </Link>
            ),
          )}
        </nav>
      </div>
    </Section>
  )
}
