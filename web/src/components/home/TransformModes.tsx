import React from 'react';
import { ArrowDown } from 'lucide-react';
import DescribeSqlContent from '../../content/home/describe-sql.mdx';
import DescribePythonContent from '../../content/home/describe-python.mdx';

export function TransformModes() {
  return (
    <section className="bg-bg-base py-24 px-6 border-t border-border-subtle">
      <div className="max-w-6xl mx-auto flex flex-col items-center">
        <h2 className="text-[28px] md:text-[32px] font-semibold text-text-primary text-center mb-16 tracking-tight">
          Three ways to transform your data.
        </h2>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 w-full">
          {/* AI Mode */}
          <div className="flex flex-col bg-bg-surface border border-border-subtle rounded-md p-6 shadow-sm">
            <div className="inline-block px-3 py-1 bg-indigo-500/10 text-indigo-400 text-[11px] font-medium tracking-wide uppercase rounded-sm border border-indigo-500/20 mb-6 w-fit">
              Natural Language
            </div>
            <div className="flex-1 flex flex-col items-center select-none">
               <div className="border border-border-default bg-bg-elevated rounded-sm px-4 py-2 text-[12px] text-text-secondary w-full flex justify-between">
                 <span>amount <i>(int)</i></span>
                 <span>email <i>(str)</i></span>
               </div>
               <ArrowDown className="w-4 h-4 text-text-muted my-2" />
               <div className="bg-bg-base border border-border-default rounded-sm p-4 w-full text-[13px] text-text-primary shadow-sm">
                 Convert amount from kobo to naira, normalize phone to E.164, drop nulls.
               </div>
               <ArrowDown className="w-4 h-4 text-text-muted my-2" />
               <div className="border border-border-default bg-bg-elevated rounded-sm px-4 py-2 text-[12px] text-text-secondary w-full flex justify-between">
                 <span>amount_naira <i>(float)</i></span>
                 <span>phone <i>(str)</i></span>
               </div>
            </div>
            <div className="mt-8 text-[13px] text-text-secondary border-t border-border-subtle pt-4 leading-[1.6]">
              Gemini generates the transform function. You approve it.
            </div>
          </div>

          {/* SQL Mode */}
          <div className="flex flex-col bg-bg-surface border border-border-subtle rounded-md p-6 shadow-sm">
            <div className="inline-block px-3 py-1 bg-amber-500/10 text-amber-500 text-[11px] font-medium tracking-wide uppercase rounded-sm border border-amber-500/20 mb-6 w-fit">
              SQL SELECT
            </div>
            <div className="flex-1 w-full text-xs [&_figure]:m-0 [&_figure]:border-none [&_figure]:bg-bg-elevated [&_pre]:!p-4">
              <DescribeSqlContent />
            </div>
            <div className="mt-8 text-[13px] text-text-secondary border-t border-border-subtle pt-4 leading-[1.6]">
              Any SELECT. <code className="text-text-code">sqlglot</code> validates and transpiles to your target dialect.
            </div>
          </div>

          {/* Python Mode */}
          <div className="flex flex-col bg-bg-surface border border-border-subtle rounded-md p-6 shadow-sm">
            <div className="inline-block px-3 py-1 bg-green-500/10 text-green-500 text-[11px] font-medium tracking-wide uppercase rounded-sm border border-green-500/20 mb-6 w-fit">
              Custom Python
            </div>
            <div className="flex-1 w-full text-xs [&_figure]:m-0 [&_figure]:border-none [&_figure]:bg-bg-elevated [&_pre]:!p-4">
              <DescribePythonContent />
            </div>
            <div className="mt-8 text-[13px] text-text-secondary border-t border-border-subtle pt-4 leading-[1.6]">
              Upload your function. Loafer extracts, runs it, and loads the output.
            </div>
          </div>

        </div>
      </div>
    </section>
  );
}
