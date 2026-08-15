import type { ReactNode } from 'react'
import {
  benchmarkRuns,
  formatDuration,
  formatMibExact,
  formatRows,
  formatThroughput,
  type BenchmarkRun,
} from '../../content/benchmarks'

/*
 * Peak memory against the cap, for all three published runs.
 *
 * The form is a horizontal bar on a shared absolute axis rather than a line
 * over row count. A line would imply a measured curve between the points, and
 * there isn't one: the row-local path has exactly one published run. Bars
 * compare magnitudes without inventing anything in between.
 *
 * Colour does one job. Identity is carried by the label on every row, so the
 * only hue is emphasis: the row-local run is the subject and wears the signal
 * orange; the materialized runs are the recessive baseline in steel. The
 * terminated run is hatched — a texture, not a third colour, because the
 * palette check put the obvious red at ΔE 3.7 against the orange, which is
 * indistinguishable even with full colour vision.
 *
 * Termination is stated in words next to an icon, never encoded by colour
 * alone. The 10M run overshot its cap by 8.7 MiB, so the breach is legible as
 * the bar crossing the cap rule, not as a dramatic overflow that would have to
 * be exaggerated to be seen.
 */

/* Headroom past the largest cap so a breaching bar has somewhere to go. */
const DOMAIN_MIB = 2176
const TICKS_MIB = [0, 512, 1024, 1536, 2048]

const percent = (mib: number) => `${(mib / DOMAIN_MIB) * 100}%`

export function BenchmarkChart() {
  return (
    <figure className="material-registration relative m-0 border border-steel bg-ink-surface">
      <figcaption className="flex flex-wrap items-center justify-between gap-x-6 gap-y-2 border-b border-steel-subtle px-5 py-3">
        <span className="stamp">peak process-tree rss · mib</span>
        <Legend />
      </figcaption>

      <div className="px-5 pt-7 pb-2">
        <ol className="flex flex-col gap-7">
          {benchmarkRuns.map((run) => (
            <RunRow key={run.id} run={run} />
          ))}
        </ol>

        <Axis />
      </div>

      <p className="border-t border-steel-subtle px-5 py-3.5 text-[12.5px] leading-[1.6] text-paper-mute">
        The two materialized runs are the v0.4.0 baseline; the row-local run is a later revision
        measured inside a pinned 4 vCPU, 2 GiB container. They are published side by side because
        they bound the same workload shape, not because they were run on identical hosts. Every
        value here is read from{' '}
        <code className="font-mono text-[0.94em] text-paper-dim">benchmarks/results/</code>.
      </p>
    </figure>
  )
}

function Legend() {
  return (
    <div className="flex flex-wrap items-center gap-x-5 gap-y-2">
      <LegendKey label="row-local">
        <span className="block h-2.5 w-5 rounded-[2px] bg-signal" />
      </LegendKey>
      <LegendKey label="materialized">
        <span className="block h-2.5 w-5 rounded-[2px] bg-steel-strong" />
      </LegendKey>
      <LegendKey label="terminated">
        <span className="hatch block h-2.5 w-5 rounded-[2px] bg-steel-strong" />
      </LegendKey>
    </div>
  )
}

function LegendKey({ label, children }: { label: string; children: ReactNode }) {
  return (
    <span className="flex items-center gap-2">
      {children}
      <span className="stamp text-[10px]">{label}</span>
    </span>
  )
}

function RunRow({ run }: { run: BenchmarkRun }) {
  const subject = run.path === 'row-local'

  return (
    <li>
      <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
        <div className="flex items-baseline gap-3">
          <span className="figure text-[15px] tracking-[-0.01em] text-paper">
            {formatRows(run.rowsRequested)}
          </span>
          <span className="text-[13px] text-paper-mute">rows</span>
          <span
            className={`stamp text-[10px] ${subject ? 'text-signal' : 'text-paper-mute'}`}
          >
            {run.path}
          </span>
        </div>
        <span className="figure text-[15px] tracking-[-0.01em] text-paper">
          {formatMibExact(run.peakMib)}
        </span>
      </div>

      <div className="relative mt-2.5 h-[14px] w-full bg-ink-base">
        {/* Recessive grid, drawn behind the mark. */}
        {TICKS_MIB.slice(1).map((tick) => (
          <span
            key={tick}
            aria-hidden="true"
            className="absolute inset-y-0 w-px bg-steel-subtle"
            style={{ left: percent(tick) }}
          />
        ))}

        <span
          className={[
            'absolute inset-y-0 left-0 rounded-r-[4px]',
            subject ? 'bg-signal' : 'bg-steel-strong',
            run.terminated ? 'hatch' : '',
          ]
            .filter(Boolean)
            .join(' ')}
          style={{ width: percent(run.peakMib) }}
          /* Native tooltip: the exact figure behind the rounded label. */
          title={`${run.transformClass} — ${run.peakMib.toFixed(3)} MiB peak against a ${run.capMib} MiB cap`}
        />

        {/* The cap this particular run was given. */}
        <span
          aria-hidden="true"
          className="absolute -inset-y-[5px] w-px border-l border-dashed border-paper-dim"
          style={{ left: percent(run.capMib) }}
        />
      </div>

      <p className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 font-mono text-[11.5px] text-paper-mute">
        <span>{run.capMib} MiB cap</span>
        <Dot />
        <span>chunk {formatRows(run.chunkSize)}</span>
        <Dot />
        <span>{formatDuration(run.wallSeconds)}</span>
        <Dot />
        <span>{formatThroughput(run.throughputRowsPerSecond)}</span>
        <Dot />
        <Outcome run={run} />
      </p>
    </li>
  )
}

function Dot() {
  return (
    <span aria-hidden="true" className="text-steel-strong">
      ·
    </span>
  )
}

/*
 * The outcome always ships as an icon plus words. A reader who cannot separate
 * the two state colours still gets "terminated: rss_limit_exceeded".
 */
function Outcome({ run }: { run: BenchmarkRun }) {
  if (run.terminated) {
    return (
      <span className="flex items-center gap-1.5 text-state-fail">
        <svg viewBox="0 0 12 12" className="h-3 w-3 shrink-0" aria-hidden="true">
          <path
            d="M3 3l6 6M9 3l-6 6"
            stroke="currentColor"
            strokeWidth="1.6"
            strokeLinecap="round"
          />
        </svg>
        terminated: {run.terminationReason}
      </span>
    )
  }

  return (
    <span className="flex items-center gap-1.5 text-state-ok">
      <svg viewBox="0 0 12 12" className="h-3 w-3 shrink-0" aria-hidden="true">
        <path
          d="M2.5 6.4l2.4 2.4L9.5 3.6"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
      {run.reconciled ? 'published, digest match' : 'published'}
    </span>
  )
}

function Axis() {
  return (
    <div className="relative mt-6 h-5 border-t border-steel-subtle">
      {TICKS_MIB.map((tick) => (
        <span
          key={tick}
          className="absolute top-1.5 -translate-x-1/2 whitespace-nowrap font-mono text-[10.5px] text-paper-mute first:translate-x-0"
          style={{ left: percent(tick) }}
        >
          {tick === 0 ? '0' : tick}
        </span>
      ))}
    </div>
  )
}
