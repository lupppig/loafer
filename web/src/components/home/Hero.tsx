import Link from 'next/link'
import { InstallCommand } from './InstallCommand'
import { HeroFoundry } from './foundry/HeroFoundry'

/*
 * Asymmetric split hero.
 *
 * The previous version led with "Move 30 million rows in 118 megabytes." It is
 * the better sentence, and it is still on the page — but as an H1 it asked the
 * reader to already know what Loafer was. Nobody searches for a memory figure;
 * they search for an ETL tool. The heading now names the category and the
 * deployment model, and the number does the work it is actually good at, which
 * is being the first proof underneath.
 *
 * The right column carries the routing yard rather than a screenshot. The
 * pipeline definition moved down into "Anatomy of a pipeline", where it gets
 * the annotation it deserves instead of sitting decoratively beside the fold.
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

      <div className="relative mx-auto grid w-full max-w-[1180px] items-center gap-12 lg:grid-cols-[1fr_1fr] lg:gap-14">
        <div className="max-w-[36rem]">
          <p className="stamp mb-6">self-hosted · mit licensed · python 3.11+</p>

          <h1 className="text-balance text-[38px] font-extrabold leading-[1.05] tracking-[-0.035em] text-paper md:text-[54px]">
            Open-source ETL and ELT,
            <br />
            declared in <span className="text-signal">YAML</span>.
          </h1>

          <p className="mt-6 max-w-[48ch] text-[16px] leading-[1.65] text-paper-dim md:text-[17px]">
            Loafer moves data between PostgreSQL, MySQL, MongoDB, REST APIs, CSV, Excel and PDF,
            transforming it with SQL, Python, or an LLM on the way through. It runs on your own
            machines, streams in bounded batches, and ends every run with a checksum that proves
            what landed.
          </p>

          <p className="mt-4 max-w-[48ch] text-[16px] leading-[1.65] text-paper md:text-[17px]">
            One measured run moved <strong className="font-semibold">30 million rows</strong> in{' '}
            <strong className="figure font-semibold">118 MiB</strong> of memory.
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

        <HeroFoundry />
      </div>
    </section>
  )
}
