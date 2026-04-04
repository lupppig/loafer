import React from 'react';
import { Button } from '../common/Button';
import { Link } from 'react-router-dom';

export function OpenSource() {
  return (
    <section className="bg-bg-base py-32 px-6 border-t border-border-subtle">
      <div className="max-w-3xl mx-auto flex flex-col items-center text-center">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="w-12 h-12 text-text-primary mb-8">
          <path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4"/>
          <path d="M9 18c-4.51 2-5-2-7-2"/>
        </svg>
        <h2 className="text-[28px] md:text-[32px] font-semibold text-text-primary mb-6 tracking-tight">
          Loafer is open source.
        </h2>
        <p className="text-[16px] text-text-secondary leading-[1.6] max-w-lg mb-10">
          The CLI is MIT licensed. Inspect the code, contribute to the connectors, and self-host your pipelines anywhere.
        </p>

        <div className="flex flex-col sm:flex-row items-center gap-4 mb-8">
          <Button asChild size="md" className="h-9 px-8 w-full sm:w-auto">
            <a href="https://github.com/lupppig/loafer" target="_blank" rel="noopener noreferrer">View on GitHub &rarr;</a>
          </Button>
          <Button asChild variant="outline" size="md" className="h-9 px-8 w-full sm:w-auto">
            <Link to="/docs">Read the docs &rarr;</Link>
          </Button>
        </div>

        <div className="text-[13px] text-text-muted select-none flex items-center justify-center gap-1.5">
          ★ 1,241 stars
        </div>
      </div>
    </section>
  );
}
