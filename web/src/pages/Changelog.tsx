import React from 'react';
import { Helmet } from 'react-helmet-async';

const releases = [
  {
    version: 'v0.3.1',
    date: 'June 2026',
    new: [
      '`loafer --version` (`-V`) prints the installed version'
    ],
    improved: [
      'AI transforms that coerce a column’s type no longer trip the destructive-operation guard — the change you asked for runs without `--yes`, while row drops and column removals still require confirmation',
      'An invalid LLM API key now surfaces a clear authentication message and stops immediately instead of dumping raw provider JSON and burning through the retry/backoff loop',
      '`list-schedules` now reports each job’s last run time and last status'
    ],
    fixed: [
      'Clean `pip`/`pipx` installs and the Docker image crashed on every command with `ModuleNotFoundError: No module named ‘click’` — `click` is now a declared dependency instead of relying on an older `typer` to pull it in',
      'AI transforms silently wrote zero rows on any streaming source (Postgres always, plus any source above `streaming_threshold`) — the AI runner now consumes the stream instead of an empty in-memory list',
      'ELT mode looped forever on `Generating and executing SQL` — the graph retry counter is now tracked correctly and bounded, with a recursion limit as a backstop',
      'ELT `write_mode: replace` is now honored — the target table is dropped before recreation, so re-runs no longer fail on an existing table',
      'Scheduled jobs never executed and `start -d` exited without leaving a daemon — the jobstore now uses an absolute path, the daemon stays running, and execution and errors are written to `~/.loafer/scheduler.log`',
      'A false `Source returned 0 rows` warning appeared on every successful streaming extract — the warning now fires only after the stream is drained and the real count is known',
      '`loafer init` crashed with an `UnboundLocalError` when scaffolding any non-custom transform or non-CSV source',
      '`loafer validate` printed the `Config validation failed:` prefix twice'
    ]
  },
  {
    version: 'v0.3.0',
    date: 'June 2026',
    new: [
      'Incremental loading — cursor/watermark extraction for Postgres, MySQL, SQLite, and REST sources',
      '`--full-refresh` flag to bypass the saved cursor and re-extract everything',
      'Upsert write mode for Postgres and MongoDB targets — idempotent loads keyed on one or more columns',
      'Transform code now runs in a resource-limited sandbox subprocess (CPU/memory/timeout enforced on Linux & macOS)',
      'Multi-step pipeline transforms with per-step result tracking',
      'New source connectors: SQLite, Excel, PDF, and REST API (with pagination)',
      'JSON target connector with incremental array writing',
      'Cron and interval scheduling with a background daemon (start/stop/status/logs)',
      'Claude, OpenAI, and Qwen LLM providers alongside Gemini',
      'Auto-detection of source, target, and transform types from URLs and file extensions',
      '`loafer init` scaffolder for new pipeline projects',
      'Official Docker images on GitHub Container Registry'
    ],
    improved: [
      'Pipeline state persists between runs next to the config file',
      'Friendlier error messages for LLM, connection, and config failures'
    ],
    fixed: [
      'Postgres target now honors `replace` and `error` write modes (previously ignored)',
      'IndexError during LLM error parsing',
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
      'MongoDB ObjectId serialisation in streaming mode'
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

export function Changelog() {
  return (
    <>
      <Helmet>
        <title>Changelog | Loafer</title>
      </Helmet>
      <div className="w-full max-w-[680px] mx-auto px-6 pt-24 pb-32 flex-1">
        <h1 className="text-[32px] font-bold font-sans text-text-primary tracking-tight mb-4">Changelog</h1>
        <p className="text-[15px] text-text-secondary leading-[1.6] mb-16 pb-8 border-b border-border-subtle">
          New updates and improvements to the Loafer CLI core engine.
        </p>

        <div className="flex flex-col gap-16">
          {releases.map((release, i) => (
            <div key={i} className="flex flex-col">
              <div className="flex items-baseline gap-4 mb-6">
                <h2 className="text-[16px] font-mono font-semibold text-text-primary">{release.version}</h2>
                <span className="text-[13px] text-text-muted">—</span>
                <span className="text-[13px] text-text-muted">{release.date}</span>
              </div>

              <div className="h-px w-full bg-border-subtle mb-6" />

              <div className="flex flex-col gap-6">
                {release.new.length > 0 && (
                  <div>
                    <div className="inline-block px-2 py-0.5 bg-indigo-500/10 text-indigo-400 text-[10px] font-medium tracking-wide uppercase rounded-sm border border-indigo-500/20 mb-3 select-none">
                      New
                    </div>
                    <ul className="flex flex-col gap-2">
                      {release.new.map((item, j) => (
                        <li key={j} className="flex items-start text-[13px] text-text-secondary leading-[1.6]">
                          <span className="text-text-muted mr-3 select-none">•</span>
                          <span>{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                {release.improved.length > 0 && (
                  <div>
                    <div className="inline-block px-2 py-0.5 bg-green-500/10 text-green-500 text-[10px] font-medium tracking-wide uppercase rounded-sm border border-green-500/20 mb-3 select-none">
                      Improved
                    </div>
                    <ul className="flex flex-col gap-2">
                      {release.improved.map((item, j) => (
                        <li key={j} className="flex items-start text-[13px] text-text-secondary leading-[1.6]">
                          <span className="text-text-muted mr-3 select-none">•</span>
                          <span>{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                {release.fixed.length > 0 && (
                  <div>
                    <div className="inline-block px-2 py-0.5 bg-amber-500/10 text-amber-500 text-[10px] font-medium tracking-wide uppercase rounded-sm border border-amber-500/20 mb-3 select-none">
                      Fixed
                    </div>
                    <ul className="flex flex-col gap-2">
                      {release.fixed.map((item, j) => (
                        <li key={j} className="flex items-start text-[13px] text-text-secondary leading-[1.6]">
                          <span className="text-text-muted mr-3 select-none">•</span>
                          <span>{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
    </>
  );
}
