import { Section, SectionHeading } from './Section'

/*
 * The questions that get asked before anyone installs anything.
 *
 * The visible copy and the FAQPage structured data are generated from the same
 * array, so the two can never drift — which matters, because an answer in
 * JSON-LD that does not appear on the page is a structured-data violation as
 * well as a lie.
 *
 * Every answer here is checkable against the repository. Where the honest
 * answer is "not yet", it says so.
 */

const QUESTIONS = [
  {
    q: 'What is Loafer?',
    a: 'Loafer is an open-source ETL and ELT engine for moving and transforming data. You define a pipeline as a YAML file — a source, a transformation, and a target — and run it with the loafer CLI on your own machine, in Docker, or from a scheduler. It is MIT licensed and installs from PyPI as loafer-etl.',
  },
  {
    q: 'Do I need to run a server or sign up for anything?',
    a: 'No. The CLI is the whole product for a single-machine pipeline: pip install loafer-etl, write a YAML file, and run it. There is an optional self-hosted HTTPS control plane called loaferd for teams that want workspaces, roles, audit events, and durable run history, but nothing is sent anywhere unless you deploy it yourself.',
  },
  {
    q: 'Does my data leave my infrastructure?',
    a: 'No. Loafer runs where you run it, and reads and writes only the sources and targets you declare. The one exception is opt-in: if you use an AI transform, a bounded sample of your schema — not your rows — is sent to the LLM provider you configured, so it can generate a transformation artifact that is then validated and executed locally.',
  },
  {
    q: 'Which databases and file formats does it support?',
    a: 'Sources: PostgreSQL, MySQL, SQLite, MongoDB, REST APIs, CSV, Excel, and PDF. Targets: PostgreSQL, MongoDB, JSON, and CSV, with append, overwrite, upsert, and create-once write modes. The connector type is inferred from the URL scheme or file extension, so most pipelines never declare one.',
  },
  {
    q: 'How much memory does a large run need?',
    a: 'It depends on whether the run is bounded. A pipeline that declares transform_class: row_local streams in batches and holds memory flat: a four-column identity workload completed 30 million rows at 118.23 MiB peak process-tree memory under a 512 MiB cap. Pipelines that do not declare a row-local transform use the materialized path, where memory grows with the dataset — a 10 million row run on that path crossed a 2 GiB budget and was terminated.',
  },
  {
    q: 'Is the AI transform required?',
    a: 'No, and it is off unless you configure a provider. Transformations can be plain SQL executed through DuckDB, a Python file run in a resource-limited subprocess, an ordered list of steps, or an LLM-generated artifact. All four are validated, versioned by content hash, and executed under the same limits.',
  },
  {
    q: 'How does Loafer compare to Airflow, dbt, or Airbyte?',
    a: 'Different layer. Airflow is a scheduler and orchestrator for tasks you write; dbt transforms data already in a warehouse; Airbyte is a connector platform for moving it. Loafer is the engine for a single pipeline end to end — extract, transform, load, and verify in one declarative file — and it is designed to be invoked by whatever scheduler you already run, including Airflow.',
  },
  {
    q: 'Is it ready for production?',
    a: 'Parts of it. The CLI engine, bounded data plane, durable runs with crash recovery, and the authenticated control plane are shipping. Distributed worker pools and the web operations dashboard are still being built, and crawling and OCR are specified but not implemented. The readiness assessment in the repository lists the verified limits and release gates rather than a general claim.',
  },
] as const

export function Faq() {
  const structuredData = {
    '@context': 'https://schema.org',
    '@type': 'FAQPage',
    mainEntity: QUESTIONS.map((item) => ({
      '@type': 'Question',
      name: item.q,
      acceptedAnswer: { '@type': 'Answer', text: item.a },
    })),
  }

  return (
    <Section id="faq">
      <SectionHeading
        stamp="questions"
        title="Before you install it"
        lede="The things worth knowing up front, including the parts that are not finished."
      />

      <div className="reveal mt-12 border-t border-steel-subtle">
        {QUESTIONS.map((item) => (
          <details key={item.q} className="group border-b border-steel-subtle">
            <summary className="flex cursor-pointer list-none items-center justify-between gap-6 py-5 text-[15.5px] font-medium tracking-[-0.01em] text-paper transition-colors hover:text-signal [&::-webkit-details-marker]:hidden">
              <h3 className="text-[15.5px] font-medium">{item.q}</h3>
              <span
                aria-hidden="true"
                className="relative h-3 w-3 shrink-0 text-steel-strong transition-transform duration-200 group-open:rotate-45"
              >
                <span className="absolute left-1/2 top-0 h-3 w-px -translate-x-1/2 bg-current" />
                <span className="absolute top-1/2 left-0 h-px w-3 -translate-y-1/2 bg-current" />
              </span>
            </summary>
            <p className="max-w-[76ch] pb-6 pr-10 text-[14.5px] leading-[1.75] text-paper-dim">
              {item.a}
            </p>
          </details>
        ))}
      </div>

      <script
        type="application/ld+json"
        // Generated from QUESTIONS above; the same array renders the visible
        // copy, so the markup cannot claim an answer the page does not show.
        dangerouslySetInnerHTML={{ __html: JSON.stringify(structuredData) }}
      />
    </Section>
  )
}
