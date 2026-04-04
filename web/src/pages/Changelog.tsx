import React from 'react';
import { Helmet } from 'react-helmet-async';

const releases = [
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
