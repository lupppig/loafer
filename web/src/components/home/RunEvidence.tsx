import { Section, SectionHeading } from './Section'
import { BenchmarkChart } from './BenchmarkChart'
import {
  headlineEnvironment,
  headlineRun,
  formatDuration,
  formatMib,
  formatRows,
} from '../../content/benchmarks'

/*
 * Execution proof.
 *
 * Every figure in this section is read from benchmarks/results/*.json rather
 * than typed in. The visual direction bans fabricated metrics, and a number
 * transcribed by hand is one benchmark re-run away from being fabricated.
 *
 * The section states the measured case, then shows all three published runs
 * against their caps — including the one that was killed. Publishing the
 * failure next to the success is the whole point: it is what makes the success
 * mean anything.
 */

const STAGES = [
  { name: 'extract', detail: 'Source streams in bounded batches' },
  { name: 'validate', detail: 'Every row checked against the declared schema' },
  { name: 'transform', detail: 'One artifact, prepared once, reused per batch' },
  { name: 'load', detail: 'Staged, then published in a single atomic step' },
] as const

const MEASURED = [
  {
    label: 'rows',
    value: formatRows(headlineRun.rowsOutput ?? headlineRun.rowsRequested),
    note: 'four-column row-local workload',
  },
  {
    label: 'peak memory',
    value: formatMib(headlineRun.peakMib),
    note: `process tree, under a ${headlineRun.capMib} MiB cap`,
  },
  {
    label: 'wall clock',
    value: formatDuration(headlineRun.wallSeconds),
    note: `${headlineEnvironment.containerCpus} vCPU, ${headlineEnvironment.containerMemoryMb / 1024} GiB container`,
  },
  {
    label: 'reconciliation',
    value: headlineRun.reconciled ? 'exact' : 'unverified',
    note: 'input and output SHA-256 identical',
  },
] as const

export function RunEvidence() {
  return (
    <Section id="execution" raised>
      <SectionHeading
        stamp="execution"
        title="Bounded batches, and a checksum at the end."
        lede="Declare a transform as row-local and Loafer stops materializing your dataset. Rows flow through in batches, nothing reaches the target until every batch succeeds, and the run reconciles what went in against what came out."
      />

      <ol className="reveal mt-14 grid gap-px border border-steel bg-steel md:grid-cols-4">
        {STAGES.map((stage, index) => (
          <li key={stage.name} className="bg-ink-base p-5">
            <div className="flex items-baseline gap-3">
              <span className="figure text-[12px] text-signal">{index + 1}</span>
              <span className="stamp text-paper">{stage.name}</span>
            </div>
            <p className="mt-3 text-[13.5px] leading-[1.6] text-paper-dim">{stage.detail}</p>
          </li>
        ))}
      </ol>

      <div className="reveal mt-14 grid gap-10 lg:grid-cols-[0.9fr_1.1fr] lg:gap-16">
        <div>
          <h3 className="text-[19px] font-semibold tracking-[-0.01em] text-paper">
            One measured run, published in full
          </h3>
          <p className="mt-4 text-[14.5px] leading-[1.7] text-paper-dim">
            A four-column identity workload completed {formatRows(headlineRun.rowsRequested)} rows
            inside a {headlineRun.capMib} MiB limit, wrote no temporary output, and produced an
            output digest identical to its input. The report and its environment provenance live in
            the repository.
          </p>
          <p className="mt-4 text-[14.5px] leading-[1.7] text-paper-dim">
            Wider rows, other transform classes, and concurrent runs each need their own capped
            benchmark. Loafer does not claim them until they pass.
          </p>
          <a
            href="https://github.com/lupppig/loafer/blob/main/benchmarks/results/30m-row-local.json"
            target="_blank"
            rel="noopener noreferrer"
            className="mt-6 inline-flex items-center gap-2 border-b border-signal pb-0.5 text-[14px] font-medium text-signal transition-colors hover:border-signal-bright hover:text-signal-bright"
          >
            Read the benchmark report
          </a>
        </div>

        <dl className="material-registration relative grid grid-cols-1 gap-px border border-steel bg-steel sm:grid-cols-2">
          {MEASURED.map((item) => (
            <div key={item.label} className="bg-ink-surface p-5">
              <dt className="stamp">{item.label}</dt>
              <dd className="figure mt-3 text-[24px] leading-none tracking-[-0.02em] text-paper md:text-[27px]">
                {item.value}
              </dd>
              <dd className="mt-2.5 text-[12.5px] leading-[1.55] text-paper-mute">{item.note}</dd>
            </div>
          ))}
        </dl>
      </div>

      <div className="reveal mt-16">
        <h3 className="text-[19px] font-semibold tracking-[-0.01em] text-paper">
          Every published run, including the one that died
        </h3>
        <p className="mt-4 max-w-[68ch] text-[14.5px] leading-[1.7] text-paper-dim">
          Bounded execution is opt-in, because applying a global transform to independent chunks
          changes its meaning. Undeclared transforms still run through the materialized path, where
          memory grows with the dataset until something stops it.
        </p>

        <div className="mt-8">
          <BenchmarkChart />
        </div>
      </div>
    </Section>
  )
}
