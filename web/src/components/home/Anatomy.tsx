import { Section, SectionHeading } from './Section'

/*
 * The explainer the page did not have.
 *
 * Everything else on this page assumes you already know what an ETL engine is
 * and what Loafer's opinion about it is. This section makes no such
 * assumption: it states the category in prose, then walks the four blocks of a
 * real pipeline file. It is also the only body copy dense enough to answer a
 * search query, which is the other job it is doing.
 *
 * Every line in the plate is valid configuration.
 */

const BLOCKS = [
  {
    key: 'source',
    title: 'Where the rows come from',
    body: 'A URL and, for databases, a query. The connector type is inferred from the scheme or the file extension, so you almost never declare it. Credentials come from the environment or a secret reference, never from the file.',
  },
  {
    key: 'transform',
    title: 'What happens on the way through',
    body: 'Use `query` for SQL, `path` for a Python file, `instruction` for an LLM-generated artifact, or a list for multiple steps. Loafer infers which from the field you set, validates the result, and versions it by content hash.',
  },
  {
    key: 'target',
    title: 'Where it lands',
    body: 'A destination and a write mode: append, overwrite, upsert, or create-once. File targets are written to a staging path and moved into place in one step, so a failed run never leaves a half-written file behind.',
  },
  {
    key: 'execution',
    title: 'How strictly it runs',
    body: 'Declaring `transform_class: row_local` opts the run into bounded batches, where memory stays flat regardless of dataset size. `schema_drift` decides what a changed column does: fail, evolve, quarantine, or coerce.',
  },
] as const

export function Anatomy() {
  return (
    <Section id="what-is-loafer">
      <SectionHeading
        stamp="anatomy"
        title="What Loafer is, and what a pipeline looks like"
        lede="Loafer is a command-line ETL and ELT engine. You describe a pipeline as a single YAML file — a source, a transformation, a target — and Loafer validates it, runs it, and reports what it moved. There is no hosted service in the path: the CLI runs on your laptop, in a container, or under the scheduler you already use, and your data never leaves your infrastructure."
      />

      <p className="mt-5 max-w-[62ch] text-[15px] leading-[1.7] text-paper-dim md:text-[16px]">
        ETL and ELT are both supported and are a one-word change. In <code className="font-mono text-[0.92em] text-paper">etl</code>{' '}
        mode the transformation runs inside Loafer before the load; in{' '}
        <code className="font-mono text-[0.92em] text-paper">elt</code> mode the rows are loaded
        first and the transformation is pushed down to the warehouse that received them.
      </p>

      <div className="reveal mt-14 grid gap-10 lg:grid-cols-[0.95fr_1.05fr] lg:gap-16">
        <figure className="material-registration relative m-0 self-start border border-steel bg-ink-surface lg:sticky lg:top-[72px]">
          <figcaption className="flex items-center justify-between border-b border-steel-subtle px-4 py-2.5">
            <span className="stamp">orders.pipeline.yaml</span>
            <span className="stamp text-paper-mute">etl</span>
          </figcaption>
          <pre className="overflow-x-auto px-4 py-4 font-mono text-[12.5px] leading-[1.75] text-paper-dim md:text-[13px]">
            <code>
              <Key>name</Key>: daily_orders{'\n'}
              <Key>mode</Key>: etl{'\n'}
              {'\n'}
              <Key>source</Key>:{'\n'}
              {'  '}
              <Key>url</Key>: <Val>{'${DATABASE_URL}'}</Val>
              {'\n'}
              {'  '}
              <Key>query</Key>: <Val>SELECT * FROM orders</Val>
              {'\n'}
              {'\n'}
              <Key>transform</Key>:{'\n'}
              {'  '}
              <Key>instruction</Key>: <Val>Normalize currency to USD</Val>
              {'\n'}
              {'\n'}
              <Key>target</Key>:{'\n'}
              {'  '}
              <Key>path</Key>: <Val>./output/orders.json</Val>
              {'\n'}
              {'  '}
              <Key>write_mode</Key>: <Val>overwrite</Val>
              {'\n'}
              {'\n'}
              <Key>execution</Key>:{'\n'}
              {'  '}
              <Key>transform_class</Key>: <Val>row_local</Val>
              {'\n'}
              {'  '}
              <Key>schema_drift</Key>: <Val>fail</Val>
            </code>
          </pre>
          <div className="flex flex-wrap items-center gap-x-6 gap-y-1 border-t border-steel-subtle px-4 py-3">
            <span className="stamp">run</span>
            <span className="figure text-[12px] text-paper">loafer run orders.pipeline.yaml</span>
          </div>
        </figure>

        <dl className="flex flex-col border-t border-steel-subtle">
          {BLOCKS.map((block, index) => (
            <div key={block.key} className="border-b border-steel-subtle py-6">
              <dt className="flex items-baseline gap-3">
                <span className="figure text-[12px] text-signal">
                  {String(index + 1).padStart(2, '0')}
                </span>
                <span className="font-mono text-[14px] font-medium text-paper">{block.key}:</span>
                <span className="text-[15px] font-semibold tracking-[-0.01em] text-paper">
                  {block.title}
                </span>
              </dt>
              <dd className="mt-3 pl-[30px] text-[14px] leading-[1.7] text-paper-dim">
                <Body text={block.body} />
              </dd>
            </div>
          ))}
        </dl>
      </div>
    </Section>
  )
}

/** Renders `backticked` spans in the block copy as inline code. */
function Body({ text }: { text: string }) {
  return (
    <>
      {text.split(/`([^`]+)`/g).map((chunk, index) =>
        index % 2 === 1 ? (
          <code key={index} className="font-mono text-[0.92em] text-paper">
            {chunk}
          </code>
        ) : (
          <span key={index}>{chunk}</span>
        ),
      )}
    </>
  )
}

function Key({ children }: { children: string }) {
  return <span className="text-signal">{children}</span>
}

function Val({ children }: { children: string }) {
  return <span className="text-paper">{children}</span>
}
