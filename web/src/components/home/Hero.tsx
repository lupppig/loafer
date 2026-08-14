import Link from 'next/link'
import { InstallCommand } from './InstallCommand'

/*
 * Asymmetric split hero. The right column carries the actual pipeline
 * definition rather than a decorated screenshot: for a YAML-first tool the
 * config is the product, and showing the real thing is the proof.
 */
export function Hero() {
  return (
    <section className="relative overflow-hidden border-b border-steel-subtle px-6 pt-16 pb-20 md:pt-24 md:pb-28 lg:px-10">
      <div
        aria-hidden="true"
        className="material-grid pointer-events-none absolute inset-0 opacity-[0.35]"
      />
      <div
        aria-hidden="true"
        className="pointer-events-none absolute inset-0 bg-gradient-to-b from-transparent via-ink-base/60 to-ink-base"
      />

      <div className="relative mx-auto grid w-full max-w-[1180px] items-start gap-12 lg:grid-cols-[1.05fr_0.95fr] lg:gap-16">
        <div className="max-w-[36rem]">
          <h1 className="text-balance text-[38px] font-extrabold leading-[1.05] tracking-[-0.035em] text-paper md:text-[56px]">
            Move 30 million rows
            <br />
            in 118 megabytes.
          </h1>

          <p className="mt-6 max-w-[46ch] text-[16px] leading-[1.65] text-paper-dim md:text-[17px]">
            Loafer is an open-source ETL and ELT engine. Declare a pipeline in YAML, run it on your
            own machines, and get a checksum that proves what landed.
          </p>

          <div className="mt-9 flex flex-col gap-3 sm:flex-row sm:items-center">
            <Link
              href="/docs/quickstart"
              className="inline-flex h-11 items-center justify-center rounded-sm bg-signal px-6 text-[14px] font-semibold text-ink-base transition-colors hover:bg-signal-bright active:translate-y-px"
            >
              Get started
            </Link>
            <a
              href="https://github.com/lupppig/loafer"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex h-11 items-center justify-center rounded-sm border border-steel px-6 text-[14px] font-medium text-paper-dim transition-colors hover:border-steel-strong hover:text-paper active:translate-y-px"
            >
              Read the source
            </a>
          </div>

          <div className="mt-8 max-w-[26rem]">
            <InstallCommand />
          </div>
        </div>

        <PipelinePlate />
      </div>
    </section>
  )
}

/*
 * The work order: a real, runnable pipeline definition presented as a stamped
 * plate. Every value here is valid Loafer configuration.
 */
function PipelinePlate() {
  return (
    <figure className="material-registration relative m-0 border border-steel bg-ink-surface">
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
        <span className="stamp">delivery</span>
        <span className="figure text-[12px] text-paper">atomic_run_publication</span>
      </div>
    </figure>
  )
}

function Key({ children }: { children: string }) {
  return <span className="text-signal">{children}</span>
}

function Val({ children }: { children: string }) {
  return <span className="text-paper">{children}</span>
}
