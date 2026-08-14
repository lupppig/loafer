'use client'

import { useState } from 'react'
import { Section, SectionHeading } from './Section'
import { cn } from '../../utils/cn'

/*
 * Four real transform declarations. Each snippet is valid configuration, not
 * an illustration: the transform type is inferred from which field is present.
 */
const MODES = [
  {
    id: 'sql',
    label: 'SQL',
    summary: 'Runs through DuckDB. The incoming dataset is addressed as {{source}}.',
    code: `transform:
  query: |
    SELECT id, email, total_usd
    FROM {{source}}
    WHERE status = 'paid'`,
  },
  {
    id: 'python',
    label: 'Python',
    summary:
      'Your file, executed in a subprocess with CPU and address-space limits and a restricted builtin set.',
    code: `transform:
  path: ./transforms/normalize.py

sandbox:
  timeout: 60
  max_memory_mb: 512`,
  },
  {
    id: 'ai',
    label: 'AI',
    summary:
      'A bounded schema sample goes to the provider. What comes back is validated, then executed like any other artifact.',
    code: `transform:
  instruction: Convert totals to USD and drop cancelled orders
  review: true

llm:
  provider: gemini
  model: gemini-3.6-flash`,
  },
  {
    id: 'multi',
    label: 'Multi-step',
    summary: 'Steps run in order, each receiving the previous step output.',
    code: `transform:
  - name: tag_region
    path: ./transforms/tag_region.py
  - name: keep_active
    query: SELECT * FROM {{source}} WHERE is_active`,
  },
] as const

export function Authoring() {
  const [active, setActive] = useState<(typeof MODES)[number]['id']>('sql')
  const mode = MODES.find((item) => item.id === active) ?? MODES[0]

  return (
    <Section id="authoring">
      <div className="grid gap-12 lg:grid-cols-[0.85fr_1.15fr] lg:gap-16">
        <div>
          <SectionHeading
            title="Four ways to write a transform. One way to run it."
            lede="Loafer infers the transform type from the field you use. Whichever you pick, the engine validates it, versions the artifact by content hash, and executes it under the same limits."
          />

          <div className="mt-10 flex flex-col border-t border-steel-subtle">
            {MODES.map((item) => {
              const selected = item.id === mode.id
              return (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => setActive(item.id)}
                  aria-pressed={selected}
                  className={cn(
                    'group border-b border-steel-subtle py-4 text-left transition-colors',
                    selected ? 'text-paper' : 'text-paper-mute hover:text-paper-dim',
                  )}
                >
                  <span className="flex items-center gap-3">
                    <span
                      aria-hidden="true"
                      className={cn(
                        'h-4 w-0.5 transition-colors',
                        selected ? 'bg-signal' : 'bg-steel group-hover:bg-steel-strong',
                      )}
                    />
                    <span className="text-[15px] font-semibold tracking-[-0.01em]">
                      {item.label}
                    </span>
                  </span>
                  <span
                    className={cn(
                      'mt-2 block pl-[18px] text-[13px] leading-[1.6]',
                      selected ? 'text-paper-dim' : 'text-paper-mute',
                    )}
                  >
                    {item.summary}
                  </span>
                </button>
              )
            })}
          </div>
        </div>

        <figure className="material-registration relative m-0 self-start border border-steel bg-ink-surface">
          <figcaption className="flex items-center justify-between border-b border-steel-subtle px-4 py-2.5">
            <span className="stamp">pipeline.yaml</span>
            <span className="stamp text-paper">{mode.label}</span>
          </figcaption>
          <pre className="overflow-x-auto px-4 py-5 font-mono text-[12.5px] leading-[1.8] text-paper md:text-[13px]">
            <code>{mode.code}</code>
          </pre>
        </figure>
      </div>
    </Section>
  )
}
