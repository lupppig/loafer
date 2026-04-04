import React, { useState } from 'react';
import ConnectContent from '../../content/home/connect.mdx';
import DescribeAiContent from '../../content/home/describe-ai.mdx';
import DescribeSqlContent from '../../content/home/describe-sql.mdx';
import DescribePythonContent from '../../content/home/describe-python.mdx';
import LoadContent from '../../content/home/load.mdx';
import { cn } from '../../utils/cn';

export function HowItWorks() {
  const [tab, setTab] = useState<'ai' | 'sql' | 'python'>('ai');

  return (
    <section className="bg-bg-surface py-24 px-6 lg:px-12 border-t border-border-subtle w-full">
      <div className="max-w-[1400px] w-full mx-auto flex flex-col items-center">
        <h2 className="text-[28px] md:text-[32px] font-semibold text-text-primary text-center mb-16 tracking-tight">
          Three steps. No infrastructure.
        </h2>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-12 w-full">
          {/* Step 1 */}
          <div className="flex flex-col min-w-0">
            <div className="flex items-center gap-4 mb-6">
              <div className="w-8 h-8 rounded-full border border-border-strong flex items-center justify-center font-mono text-text-muted text-[13px]">
                1
              </div>
              <h3 className="text-[18px] font-semibold text-text-primary">Connect</h3>
            </div>
            <div className="w-full min-w-0 [&_figure]:max-w-full [&_figure]:overflow-x-auto [&_figure]:m-0">
              <ConnectContent />
            </div>
          </div>

          {/* Step 2 */}
          <div className="flex flex-col min-w-0">
            <div className="flex items-center gap-4 mb-6">
              <div className="w-8 h-8 rounded-full border border-border-strong flex items-center justify-center font-mono text-text-muted text-[13px]">
                2
              </div>
              <h3 className="text-[18px] font-semibold text-text-primary">Describe</h3>
            </div>
            <div className="w-full relative min-w-0 [&_figure]:max-w-full [&_figure]:overflow-x-auto [&_figure]:m-0">
              {tab === 'ai' && <DescribeAiContent />}
              {tab === 'sql' && <DescribeSqlContent />}
              {tab === 'python' && <DescribePythonContent />}
              
              <div className="flex items-center gap-1 mt-4 p-1 bg-bg-base border border-border-default rounded-sm w-fit shadow-sm">
                <button 
                  onClick={() => setTab('ai')}
                  className={cn("px-3 py-1 text-[12px] font-medium rounded-[2px] transition-colors focus:outline-none focus-visible:ring-1 focus-visible:ring-indigo-500", tab === 'ai' ? "bg-bg-surface text-text-primary shadow-[0_1px_2px_rgba(0,0,0,0.12)] border border-border-subtle" : "text-text-muted hover:text-text-secondary border border-transparent")}
                >
                  AI
                </button>
                <button 
                  onClick={() => setTab('sql')}
                  className={cn("px-3 py-1 text-[12px] font-medium rounded-[2px] transition-colors focus:outline-none focus-visible:ring-1 focus-visible:ring-indigo-500", tab === 'sql' ? "bg-bg-surface text-text-primary shadow-[0_1px_2px_rgba(0,0,0,0.12)] border border-border-subtle" : "text-text-muted hover:text-text-secondary border border-transparent")}
                >
                  SQL
                </button>
                <button 
                  onClick={() => setTab('python')}
                  className={cn("px-3 py-1 text-[12px] font-medium rounded-[2px] transition-colors focus:outline-none focus-visible:ring-1 focus-visible:ring-indigo-500", tab === 'python' ? "bg-bg-surface text-text-primary shadow-[0_1px_2px_rgba(0,0,0,0.12)] border border-border-subtle" : "text-text-muted hover:text-text-secondary border border-transparent")}
                >
                  Python
                </button>
              </div>
            </div>
          </div>

          {/* Step 3 */}
          <div className="flex flex-col min-w-0">
            <div className="flex items-center gap-4 mb-6">
              <div className="w-8 h-8 rounded-full border border-border-strong flex items-center justify-center font-mono text-text-muted text-[13px]">
                3
              </div>
              <h3 className="text-[18px] font-semibold text-text-primary">Load</h3>
            </div>
            <div className="w-full min-w-0 [&_figure]:max-w-full [&_figure]:overflow-x-auto [&_figure]:m-0">
              <LoadContent />
            </div>
            {/* Short run summary below */}
            <div className="mt-4 bg-bg-base border border-border-default rounded-md p-4 font-mono text-[11px] leading-[1.6] text-text-code whitespace-pre shadow-sm overflow-x-auto">
              <div className="text-indigo-400">╭─ Complete ──────────────╮</div>
              <div>│ 41,923 rows loaded      │</div>
              <div>│ 878 filtered            │</div>
              <div className="text-indigo-400">╰─────────────────────────╯</div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
