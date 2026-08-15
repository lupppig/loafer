import oneMillion from '../../../benchmarks/results/1m.json'
import tenMillion from '../../../benchmarks/results/10m.json'
import thirtyMillion from '../../../benchmarks/results/30m-row-local.json'

/*
 * The published benchmark reports, read straight from the files the benchmark
 * harness writes.
 *
 * These are imported rather than transcribed on purpose. The visual direction
 * forbids fabricated metrics, and the surest way to end up with one is a
 * hand-copied number that nobody updates after the next run. Webpack inlines
 * the JSON at build time, so there is no runtime file dependency.
 *
 * Source of truth: benchmarks/results/*.json (schema_version 1).
 */

/** The two execution paths the reports distinguish. */
export type TransformPath = 'materialized' | 'row-local'

export interface BenchmarkRun {
  id: string
  /** Rows the run was asked for, which is not always what it delivered. */
  rowsRequested: number
  rowsOutput: number | null
  path: TransformPath
  transformClass: string
  peakMib: number
  capMib: number
  /** True when the process was killed rather than finishing. */
  terminated: boolean
  terminationReason: string | null
  status: string
  outputPublished: boolean
  /** Whether the input and output digests matched. Null when nothing landed. */
  reconciled: boolean | null
  throughputRowsPerSecond: number | null
  wallSeconds: number
  chunkSize: number
  loaferVersion: string
}

function classify(transformClass: string): TransformPath {
  return transformClass.includes('row_local') ? 'row-local' : 'materialized'
}

function toRun(
  id: string,
  report: (typeof thirtyMillion | typeof tenMillion | typeof oneMillion) & {
    input_sha256: string | null
    output_sha256: string | null
  },
): BenchmarkRun {
  return {
    id,
    rowsRequested: report.rows_requested,
    rowsOutput: report.rows_output,
    path: classify(report.transform_class),
    transformClass: report.transform_class,
    peakMib: report.peak_process_tree_rss_mb,
    capMib: report.rss_limit_mb,
    terminated: report.termination_reason !== null,
    terminationReason: report.termination_reason,
    status: report.status,
    outputPublished: report.output_published,
    reconciled:
      report.output_sha256 === null ? null : report.output_sha256 === report.input_sha256,
    throughputRowsPerSecond: report.throughput_rows_per_second,
    wallSeconds: report.wall_seconds,
    chunkSize: report.chunk_size,
    loaferVersion: report.environment.loafer_version,
  }
}

/* Ordered by row count, which is also the order the argument reads in. */
export const benchmarkRuns: BenchmarkRun[] = [
  toRun('1m', oneMillion),
  toRun('10m', tenMillion),
  toRun('30m', thirtyMillion),
]

/** The run the page leads with. */
export const headlineRun = benchmarkRuns[benchmarkRuns.length - 1]

export const headlineEnvironment = {
  containerCpus: thirtyMillion.environment.container_cpus,
  containerMemoryMb: thirtyMillion.environment.container_memory_mb,
  image: thirtyMillion.environment.container_image,
  python: thirtyMillion.environment.python,
  platform: thirtyMillion.environment.platform,
  sourceRevision: thirtyMillion.environment.source_revision,
  inputBytes: thirtyMillion.input_bytes,
  digest: thirtyMillion.input_sha256,
}

/*
 * Formatting. Every figure on the page goes through one of these, so a number
 * is never rendered two different ways in two different sections.
 */

export const formatRows = (rows: number) => rows.toLocaleString('en-US')

/** Prose and stat tiles, where GiB reads better once past a gigabyte. */
export const formatMib = (mib: number) =>
  mib >= 1024 ? `${(mib / 1024).toFixed(2)} GiB` : `${mib.toFixed(2)} MiB`

/**
 * Charts, where the axis and the caps are both in MiB. Switching a bar label
 * to GiB forces the reader to convert in their head before they can see that
 * 2056.69 is past a 2048 cap, which is exactly the comparison the chart is for.
 */
export const formatMibExact = (mib: number) => `${mib.toFixed(2)} MiB`

export function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds.toFixed(1)}s`
  const minutes = Math.floor(seconds / 60)
  const remainder = Math.round(seconds % 60)
  return `${minutes}m ${String(remainder).padStart(2, '0')}s`
}

export const formatThroughput = (rowsPerSecond: number | null) =>
  rowsPerSecond === null ? '—' : `${Math.round(rowsPerSecond).toLocaleString('en-US')} rows/s`
