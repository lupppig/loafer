'use client'

import { useState } from 'react'
import { Check, Copy } from 'lucide-react'
import { cn } from '../../utils/cn'

const COMMANDS = {
  pip: 'pip install loafer-etl',
  docker: 'docker pull ghcr.io/lupppig/loafer',
} as const

type Channel = keyof typeof COMMANDS

export function InstallCommand() {
  const [channel, setChannel] = useState<Channel>('pip')
  const [copied, setCopied] = useState(false)

  async function copy() {
    try {
      await navigator.clipboard.writeText(COMMANDS[channel])
      setCopied(true)
      window.setTimeout(() => setCopied(false), 2000)
    } catch {
      setCopied(false)
    }
  }

  return (
    <div className="border border-steel bg-ink-surface">
      <div
        role="tablist"
        aria-label="Install channel"
        className="flex items-center border-b border-steel-subtle"
      >
        {(Object.keys(COMMANDS) as Channel[]).map((key) => (
          <button
            key={key}
            role="tab"
            type="button"
            aria-selected={channel === key}
            onClick={() => setChannel(key)}
            className={cn(
              'stamp border-r border-steel-subtle px-4 py-2.5 transition-colors',
              channel === key
                ? 'bg-ink-raised text-paper'
                : 'text-paper-mute hover:text-paper-dim',
            )}
          >
            {key}
          </button>
        ))}
      </div>
      <div className="flex items-center justify-between gap-3 px-4 py-3">
        <code className="truncate font-mono text-[13px] text-paper">{COMMANDS[channel]}</code>
        <button
          type="button"
          onClick={copy}
          aria-label={`Copy ${COMMANDS[channel]}`}
          className="shrink-0 text-paper-mute transition-colors hover:text-paper"
        >
          {copied ? (
            <Check className="h-4 w-4 text-state-ok" aria-hidden="true" />
          ) : (
            <Copy className="h-4 w-4" aria-hidden="true" />
          )}
        </button>
      </div>
      <p aria-live="polite" className="sr-only">
        {copied ? 'Command copied to clipboard' : ''}
      </p>
    </div>
  )
}
