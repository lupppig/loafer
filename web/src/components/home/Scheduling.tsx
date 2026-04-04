import React from 'react';

export function Scheduling() {
  return (
    <section className="bg-bg-surface py-24 px-6 border-t border-border-subtle">
      <div className="max-w-5xl mx-auto flex flex-col items-center">
        <h2 className="text-[28px] md:text-[32px] font-semibold text-text-primary text-center mb-16 tracking-tight">
          Run it once. Or every night.
        </h2>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 w-full">
          <div className="flex flex-col h-full bg-bg-elevated border border-border-default rounded-md p-6 shadow-sm">
            <h3 className="text-[13px] font-medium text-text-muted mb-4 uppercase tracking-wider">Cron Syntax</h3>
            <pre className="font-mono text-[13px] text-text-primary leading-[1.6]">
<span className="text-indigo-400">schedule:</span><br/>
  <span className="text-text-secondary">cron:</span> <span className="text-amber-400">"0 2 * * *"</span><br/>
  <span className="text-text-secondary">name:</span> <span className="text-amber-400">nightly-orders</span><br/>
            </pre>
          </div>

          <div className="flex flex-col h-full bg-bg-elevated border border-border-default rounded-md p-6 shadow-sm overflow-x-auto">
            <h3 className="text-[13px] font-medium text-text-muted mb-4 uppercase tracking-wider">CLI Status</h3>
            <pre className="font-mono text-[12px] leading-[1.6] text-text-secondary">
<span className="text-text-code">$ loafer jobs</span><br/>
<br/>
  <span className="text-text-muted">JOB               NEXT RUN        LAST RUN     STATUS</span><br/>
  nightly-orders    in 4h 23m       2 hours ago  <span className="text-green-500">✓ Success</span><br/>
  weekly-report     in 3d 12h       6 days ago   <span className="text-green-500">✓ Success</span><br/>
            </pre>
          </div>
        </div>
      </div>
    </section>
  );
}
