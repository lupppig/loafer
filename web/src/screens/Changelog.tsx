import type { ReactNode } from 'react'
import { Sparkles, Zap, Bug, Package } from 'lucide-react';

const releases = [
  {
    version: 'Unreleased',
    date: 'Phase 0',
    new: [
      'A deterministic full-pipeline benchmark now enforces memory and timeout limits while verifying row counts and SHA-256 output checksums',
      'Provider-aware model defaults: <code>gemini-3.6-flash</code>, <code>claude-sonnet-5</code>, <code>gpt-5.6-terra</code>, and <code>qwen3.7-plus</code>'
    ],
    improved: [
      'CSV and JSON targets publish completed output atomically and preserve existing files when a run fails',
      'PostgreSQL target and ELT identifiers now use driver-native composition, including schema-qualified table names',
      'Omitting <code>llm.model</code> now selects the default for the configured provider instead of inheriting Gemini\u2019s model'
    ],
    fixed: [
      'Retired Claude Sonnet 4 and Gemini 2.0 defaults no longer break newly configured AI pipelines',
      'Failed target contexts no longer finalize partial CSV or JSON output',
      '<code>write_mode: error</code> no longer overwrites a file created concurrently by another process'
    ]
  },
  {
    version: 'v0.3.1',
    date: 'June 2026',
    new: [
      '<code>loafer --version</code> (<code>-V</code>) prints the installed version'
    ],
    improved: [
      'AI transforms that coerce a column\u2019s type no longer trip the destructive-operation guard \u2014 the change you asked for runs without <code>--yes</code>, while row drops and column removals still require confirmation',
      'An invalid LLM API key now surfaces a clear authentication message and stops immediately instead of dumping raw provider JSON and burning through the retry/backoff loop',
      '<code>list-schedules</code> now reports each job\u2019s last run time and last status'
    ],
    fixed: [
      'Clean <code>pip</code>/<code>pipx</code> installs and the Docker image crashed on every command with <code>ModuleNotFoundError</code> \u2014 <code>click</code> is now a declared dependency instead of relying on an older <code>typer</code> to pull it in',
      'AI transforms silently wrote zero rows on any streaming source (Postgres always, plus any source above <code>streaming_threshold</code>) \u2014 the AI runner now consumes the stream instead of an empty in-memory list',
      'ELT mode looped forever on <code>Generating and executing SQL</code> \u2014 the graph retry counter is now tracked correctly and bounded, with a recursion limit as a backstop',
      'ELT <code>write_mode: replace</code> is now honored \u2014 the target table is dropped before recreation, so re-runs no longer fail on an existing table',
      'Scheduled jobs never executed and <code>start -d</code> exited without leaving a daemon \u2014 the jobstore now uses an absolute path, the daemon stays running, and execution and errors are written to <code>~/.loafer/scheduler.log</code>',
      'A false <code>Source returned 0 rows</code> warning appeared on every successful streaming extract \u2014 the warning now fires only after the stream is drained and the real count is known',
      '<code>loafer init</code> crashed with an <code>UnboundLocalError</code> when scaffolding any non-custom transform or non-CSV source',
      '<code>loafer validate</code> printed the <code>Config validation failed:</code> prefix twice'
    ]
  },
  {
    version: 'v0.3.0',
    date: 'June 2026',
    new: [
      'Incremental loading \u2014 cursor/watermark extraction for Postgres, MySQL, SQLite, and REST sources',
      '<code>--full-refresh</code> flag to bypass the saved cursor and re-extract everything',
      'Upsert write mode for Postgres and MongoDB targets \u2014 idempotent loads keyed on one or more columns',
      'Transform code now runs in a resource-limited sandbox subprocess (CPU/memory/timeout enforced on Linux & macOS)',
      'Multi-step pipeline transforms with per-step result tracking',
      'New source connectors: SQLite, Excel, PDF, and REST API (with pagination)',
      'JSON target connector with incremental array writing',
      'Cron and interval scheduling with a background daemon (start/stop/status/logs)',
      'Claude, OpenAI, and Qwen LLM providers alongside Gemini',
      'Auto-detection of source, target, and transform types from URLs and file extensions',
      '<code>loafer init</code> scaffolder for new pipeline projects',
      'Official Docker images on GitHub Container Registry'
    ],
    improved: [
      'Pipeline state persists between runs next to the config file',
      'Friendlier error messages for LLM, connection, and config failures'
    ],
    fixed: [
      'Postgres target now honors <code>replace</code> and <code>error</code> write modes (previously ignored)',
      '<code>IndexError</code> during LLM error parsing',
      'Markdown tables now render via remark in the docs site'
    ]
  },
  {
    version: 'v0.2.0',
    date: 'March 2025',
    new: [
      'SQL transform mode with sqlglot validation',
      'Custom Python transform file support',
      'Human-in-the-loop destructive operation detection'
    ],
    improved: [
      'Token usage now shown in run summary',
      'Retry count shown per agent in live progress view'
    ],
    fixed: [
      'MongoDB <code>ObjectId</code> serialisation in streaming mode'
    ]
  },
  {
    version: 'v0.1.0',
    date: 'February 2025',
    new: [
      'Initial public release',
      'AI transform mode with Gemini 1.5 Flash',
      'PostgreSQL, CSV, MongoDB, MySQL streaming connectors',
      'Rich animated terminal CLI traces'
    ],
    improved: [],
    fixed: []
  }
];

type Category = {
  key: 'new' | 'improved' | 'fixed';
  label: string;
  icon: ReactNode;
  accentClass: string;
  borderClass: string;
  dotClass: string;
};

const categories: Category[] = [
  {
    key: 'new',
    label: 'New',
    icon: <Sparkles className="w-3.5 h-3.5" />,
    accentClass: 'text-indigo-400 bg-indigo-500/10 border-indigo-500/25',
    borderClass: 'border-l-indigo-500/40',
    dotClass: 'bg-indigo-500',
  },
  {
    key: 'improved',
    label: 'Improved',
    icon: <Zap className="w-3.5 h-3.5" />,
    accentClass: 'text-emerald-400 bg-emerald-500/10 border-emerald-500/25',
    borderClass: 'border-l-emerald-500/40',
    dotClass: 'bg-emerald-500',
  },
  {
    key: 'fixed',
    label: 'Fixed',
    icon: <Bug className="w-3.5 h-3.5" />,
    accentClass: 'text-amber-400 bg-amber-500/10 border-amber-500/25',
    borderClass: 'border-l-amber-500/40',
    dotClass: 'bg-amber-500',
  },
];

function RichText({ html }: { html: string }) {
  return (
    <span
      dangerouslySetInnerHTML={{ __html: html }}
      className="[&>code]:font-mono [&>code]:text-[12px] [&>code]:px-1.5 [&>code]:py-0.5 [&>code]:rounded [&>code]:bg-bg-elevated [&>code]:border [&>code]:border-border-subtle [&>code]:text-indigo-300"
    />
  );
}

export function Changelog() {
  return (
      <div className="w-full max-w-[720px] mx-auto px-6 pt-24 pb-32 flex-1">
        {/* Header */}
        <div className="mb-16">
          <div className="flex items-center gap-3 mb-4">
            <div className="flex items-center justify-center w-9 h-9 rounded-lg bg-indigo-500/10 border border-indigo-500/20">
              <Package className="w-4.5 h-4.5 text-indigo-400" />
            </div>
            <h1 className="text-[32px] font-bold font-sans text-text-primary tracking-tight">
              Changelog
            </h1>
          </div>
          <p className="text-[15px] text-text-secondary leading-relaxed max-w-[520px]">
            New updates and improvements to the Loafer CLI core engine.
          </p>
        </div>

        {/* Timeline */}
        <div className="relative">
          {/* Timeline rail */}
          <div className="absolute left-[7px] top-2 bottom-0 w-px bg-gradient-to-b from-border-strong via-border-default to-transparent" />

          <div className="flex flex-col gap-0">
            {releases.map((release, i) => (
              <div key={i} className="relative pl-10 pb-16 last:pb-0 group">
                {/* Timeline dot */}
                <div className="absolute left-0 top-[6px] w-[15px] h-[15px] rounded-full border-2 border-border-strong bg-bg-base group-first:border-indigo-500 group-first:shadow-[0_0_8px_rgba(99,102,241,0.3)] transition-colors z-10" />

                {/* Version header */}
                <div className="flex items-center gap-3 mb-1">
                  <h2 className="text-[18px] font-semibold font-mono text-text-primary tracking-tight">
                    {release.version}
                  </h2>
                  {i === 0 && (
                    <span className="text-[10px] font-semibold uppercase tracking-wider px-2 py-0.5 rounded-full bg-indigo-500/15 text-indigo-400 border border-indigo-500/25">
                      Latest
                    </span>
                  )}
                </div>
                <p className="text-[13px] text-text-muted mb-6">{release.date}</p>

                {/* Categories */}
                <div className="flex flex-col gap-5">
                  {categories.map((cat) => {
                    const items = release[cat.key];
                    if (!items || items.length === 0) return null;

                    return (
                      <div key={cat.key} className={`border-l-2 ${cat.borderClass} pl-4`}>
                        {/* Badge */}
                        <div className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11px] font-semibold tracking-wide uppercase border ${cat.accentClass} mb-3 select-none`}>
                          {cat.icon}
                          {cat.label}
                          <span className="ml-1 opacity-60 font-normal text-[10px]">{items.length}</span>
                        </div>

                        {/* Items */}
                        <ul className="flex flex-col gap-2.5">
                          {items.map((item, j) => (
                            <li key={j} className="flex items-start gap-2.5 text-[13.5px] text-text-secondary leading-[1.7]">
                              <span className={`mt-[9px] w-1 h-1 rounded-full ${cat.dotClass} opacity-50 shrink-0`} />
                              <RichText html={item} />
                            </li>
                          ))}
                        </ul>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
  );
}
