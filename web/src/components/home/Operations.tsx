import { Section, SectionHeading } from './Section'

/*
 * Offset rows on a hairline rail. Each entry describes behaviour that exists
 * in the engine today, phrased as what happens rather than as a feature name.
 */
const BEHAVIOURS = [
  {
    title: 'A killed worker does not lose the run',
    body: 'Batches are staged to object storage and checkpointed before the target sees them. When another worker picks the run up, it replays what was committed and resumes at the next source position.',
    figure: 'resume at last checkpoint',
  },
  {
    title: 'A stale worker cannot write',
    body: 'Every claim issues a monotonic fencing token. A worker that stalls past its lease and wakes up mid-write is rejected, so two processes can never both believe they own a run.',
    figure: 'fencing token + lease',
  },
  {
    title: 'The same command twice is still one run',
    body: 'Runs are keyed by an idempotency key scoped to the workspace. Retrying an enqueue returns the run that already exists rather than starting a second one.',
    figure: 'idempotent by command key',
  },
  {
    title: 'Drifting schemas fail loudly, or by your rule',
    body: 'Declare fail, evolve, quarantine, or coerce. Rejected rows are written out with the batch and reason that produced them instead of disappearing into a log line.',
    figure: 'four drift policies',
  },
] as const

export function Operations() {
  return (
    <Section id="operations">
      <SectionHeading
        stamp="durability"
        title="Built for the run that fails halfway."
        lede="Most of a pipeline engine is what happens when something breaks. Loafer keeps authoritative state in PostgreSQL or SQLite and treats the worker as replaceable."
      />

      <div className="mt-14 border-t border-steel">
        {BEHAVIOURS.map((item, index) => (
          <article
            key={item.title}
            className="reveal grid gap-4 border-b border-steel-subtle py-8 md:grid-cols-[auto_1fr_auto] md:items-baseline md:gap-10"
          >
            <span className="figure text-[12px] text-steel-strong md:pt-1">
              {String(index + 1).padStart(2, '0')}
            </span>
            <div className="max-w-[58ch]">
              <h3 className="text-[17px] font-semibold tracking-[-0.015em] text-paper md:text-[19px]">
                {item.title}
              </h3>
              <p className="mt-3 text-[14.5px] leading-[1.7] text-paper-dim">{item.body}</p>
            </div>
            <span className="stamp shrink-0 text-signal md:pt-1 md:text-right">{item.figure}</span>
          </article>
        ))}
      </div>
    </Section>
  )
}
