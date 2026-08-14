import { Section, SectionHeading } from './Section'

/*
 * Exactly the connectors returned by loafer.connectors.registry. No roadmap
 * entries, no "coming soon" filler: the visual direction requires sources and
 * targets without unsupported claims.
 */
const SOURCES = [
  { name: 'PostgreSQL', note: 'cursor pushdown' },
  { name: 'MySQL', note: 'cursor pushdown' },
  { name: 'SQLite', note: 'cursor pushdown' },
  { name: 'MongoDB', note: 'filter documents' },
  { name: 'REST API', note: 'paginated, cursor param' },
  { name: 'CSV', note: 'streamed by chunk' },
  { name: 'Excel', note: 'sheet selection' },
  { name: 'PDF', note: 'page and table provenance' },
] as const

const TARGETS = [
  { name: 'PostgreSQL', note: 'append, replace, upsert, create-once' },
  { name: 'MongoDB', note: 'append, replace, upsert' },
  { name: 'JSON', note: 'atomic publication' },
  { name: 'CSV', note: 'atomic publication' },
] as const

export function SourcesTargets() {
  return (
    <Section id="connectors" raised>
      <SectionHeading
        title="Eight sources in. Four targets out."
        lede="Types are inferred from the URL scheme or file extension, so most pipelines never declare one. Every connector listed here ships today."
      />

      <div className="reveal mt-14 grid gap-x-16 gap-y-12 md:grid-cols-2">
        <Column heading="Sources" items={SOURCES} />
        <Column heading="Targets" items={TARGETS} />
      </div>

      <p className="mt-12 max-w-[68ch] text-[13.5px] leading-[1.65] text-paper-mute">
        MongoDB is rejected at config validation for bounded row-local runs until a tested staging
        and merge protocol exists. ClickHouse, MariaDB, TiDB, TimescaleDB, CouchDB, and TigerGraph
        are planned as separately tested connectors, not protocol aliases.
      </p>
    </Section>
  )
}

function Column({
  heading,
  items,
}: {
  heading: string
  items: readonly { name: string; note: string }[]
}) {
  return (
    <div>
      <h3 className="stamp border-b border-steel pb-3 text-paper">{heading}</h3>
      <dl className="mt-1">
        {items.map((item) => (
          <div
            key={item.name}
            className="flex items-baseline justify-between gap-6 border-b border-steel-subtle py-3.5"
          >
            <dt className="text-[15px] font-medium tracking-[-0.01em] text-paper">{item.name}</dt>
            <dd className="text-right font-mono text-[12px] text-paper-mute">{item.note}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}
