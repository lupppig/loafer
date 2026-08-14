import { Section, SectionHeading } from './Section'

/*
 * Topology, drawn as inline SVG because it depicts a mechanism rather than
 * decorating the page: which process holds which responsibility, and which
 * boundary a credential never crosses.
 */
export function SelfHosting() {
  return (
    <Section id="self-hosting" raised>
      <div className="grid gap-12 lg:grid-cols-[0.95fr_1.05fr] lg:gap-16">
        <div>
          <SectionHeading
            title="One host to start. The same shape when it grows."
            lede="A pinned Compose profile brings up PostgreSQL, the control plane, a scheduler, and a worker as separate non-root processes on a read-only filesystem. Scaling out adds workers; it does not change the architecture."
          />

          <dl className="mt-10 border-t border-steel-subtle">
            {[
              ['Control plane', 'Issues commands over HTTPS. Never executes pipeline work.'],
              ['Scheduler', 'Turns due schedules into durable run commands.'],
              ['Worker', 'The only process that reads sources or writes targets.'],
              ['Metadata', 'Authoritative run, batch, and checkpoint state.'],
            ].map(([term, detail]) => (
              <div key={term} className="border-b border-steel-subtle py-3.5">
                <dt className="text-[14px] font-semibold text-paper">{term}</dt>
                <dd className="mt-1 text-[13.5px] leading-[1.6] text-paper-dim">{detail}</dd>
              </div>
            ))}
          </dl>
        </div>

        <figure className="m-0 self-start">
          <TopologyDiagram />
          <figcaption className="mt-4 text-[13px] leading-[1.6] text-paper-mute">
            Secrets stay on the server. The browser and the CLI both receive a short-lived token and
            see connection references, never credentials.
          </figcaption>
        </figure>
      </div>
    </Section>
  )
}

function TopologyDiagram() {
  return (
    <svg
      viewBox="0 0 520 300"
      role="img"
      aria-label="Browser and CLI send authenticated HTTPS commands to the control plane, which records them in PostgreSQL. A scheduler creates due runs, and workers claim them under a lease before reading sources and writing targets."
      className="h-auto w-full border border-steel bg-ink-base"
    >
      <defs>
        <marker
          id="topology-arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="6"
          markerHeight="6"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--steel-strong)" />
        </marker>
      </defs>

      <g fill="none" stroke="var(--steel)" strokeWidth="1">
        <rect x="24" y="28" width="132" height="42" />
        <rect x="24" y="88" width="132" height="42" />
        <rect x="212" y="58" width="132" height="42" />
        <rect x="212" y="150" width="132" height="42" />
        <rect x="212" y="228" width="132" height="42" />
        <rect x="392" y="228" width="104" height="42" />
      </g>

      <g
        fill="var(--paper)"
        fontFamily="var(--font-mono)"
        fontSize="11"
        textAnchor="middle"
        letterSpacing="0.08em"
      >
        <text x="90" y="54">BROWSER</text>
        <text x="90" y="114">CLI</text>
        <text x="278" y="84">loaferd</text>
        <text x="278" y="176">SCHEDULER</text>
        <text x="278" y="254">WORKER</text>
        <text x="444" y="254">TARGET</text>
      </g>

      <g
        stroke="var(--steel-strong)"
        strokeWidth="1"
        fill="none"
        markerEnd="url(#topology-arrow)"
      >
        <path d="M 156 49 L 184 49 L 184 74 L 206 74" />
        <path d="M 156 109 L 184 109 L 184 84 L 206 84" />
        <path d="M 278 100 L 278 144" />
        <path d="M 278 192 L 278 222" />
        <path d="M 344 249 L 386 249" />
      </g>

      <g fill="var(--paper-mute)" fontFamily="var(--font-mono)" fontSize="9.5">
        <text x="164" y="42">HTTPS</text>
        <text x="288" y="126">run command</text>
        <text x="288" y="214">claim + lease</text>
      </g>

      <g fill="none" stroke="var(--signal)" strokeWidth="1" strokeDasharray="3 3">
        <rect x="196" y="14" width="164" height="272" />
      </g>
      <text
        x="278"
        y="8"
        fill="var(--signal)"
        fontFamily="var(--font-mono)"
        fontSize="9.5"
        textAnchor="middle"
        letterSpacing="0.12em"
      >
        YOUR INFRASTRUCTURE
      </text>
    </svg>
  )
}
