import React from 'react';
import { Database, FileJson, FileSpreadsheet, FileText, Server } from 'lucide-react';

export function Connectors() {
  const sources = [
    { name: 'Postgres', icon: <Database className="w-6 h-6 text-indigo-400" /> },
    { name: 'MySQL', icon: <Database className="w-6 h-6 text-blue-400" /> },
    { name: 'MongoDB', icon: <Server className="w-6 h-6 text-green-500" /> },
    { name: 'CSV', icon: <FileText className="w-6 h-6 text-text-secondary" /> },
    { name: 'Excel', icon: <FileSpreadsheet className="w-6 h-6 text-green-400" /> },
    { name: 'REST API', icon: <Server className="w-6 h-6 text-amber-400" /> },
  ];

  const targets = [
    { name: 'Postgres', icon: <Database className="w-6 h-6 text-indigo-400" /> },
    { name: 'MongoDB', icon: <Server className="w-6 h-6 text-green-500" /> },
    { name: 'CSV', icon: <FileText className="w-6 h-6 text-text-secondary" /> },
    { name: 'JSON', icon: <FileJson className="w-6 h-6 text-amber-400" /> },
  ];

  return (
    <section className="bg-bg-base py-24 px-6 border-t border-border-subtle">
      <div className="max-w-5xl mx-auto flex flex-col items-center">
        <h2 className="text-[28px] md:text-[32px] font-semibold text-text-primary text-center mb-16 tracking-tight">
          Works with your existing data.
        </h2>

        <div className="w-full flex flex-col gap-12">
          {/* Sources */}
          <div>
            <h3 className="text-[14px] font-medium text-text-muted uppercase tracking-wider mb-6">Sources</h3>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
              {sources.map((c, i) => (
                <div key={i} className="flex flex-col items-center justify-center p-6 bg-bg-surface border border-border-subtle rounded-md hover:bg-bg-overlay hover:border-border-default transition-colors">
                  <div className="mb-3">{c.icon}</div>
                  <span className="text-[13px] font-medium text-text-secondary">{c.name}</span>
                </div>
              ))}
            </div>
            <div className="mt-4 text-[12px] text-text-muted text-right">More coming soon...</div>
          </div>

          {/* Targets */}
          <div>
            <h3 className="text-[14px] font-medium text-text-muted uppercase tracking-wider mb-6">Targets</h3>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
              {targets.map((c, i) => (
                <div key={i} className="flex flex-col items-center justify-center p-6 bg-bg-surface border border-border-subtle rounded-md hover:bg-bg-overlay hover:border-border-default transition-colors">
                  <div className="mb-3">{c.icon}</div>
                  <span className="text-[13px] font-medium text-text-secondary">{c.name}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
