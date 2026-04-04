import React, { useState } from 'react';
import { Copy, Check } from 'lucide-react';
import { Button } from '../common/Button';
import { TerminalAnimation } from './TerminalAnimation';
import { Link } from 'react-router-dom';
import { cn } from '../../utils/cn';

export function Hero() {
  const [pkgManager, setPkgManager] = useState<'pip' | 'docker'>('pip');
  const [copied, setCopied] = useState(false);

  const installCmd = pkgManager === 'pip' ? 'pip install loafer-etl' : 'docker pull ghcr.io/lupppig/loafer:latest';

  const handleCopy = () => {
    navigator.clipboard.writeText(installCmd);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <section className="relative min-h-[calc(100vh-52px)] flex flex-col items-center pt-24 pb-32 px-6 overflow-hidden">
      {/* Background gradients and patterns */}
      <div 
        className="absolute inset-0 pointer-events-none before:absolute before:inset-0" 
        style={{
          background: 'radial-gradient(ellipse 80% 50% at 50% -10%, rgba(99,102,241,0.12), transparent)'
        }}
      />
      <div 
        className="absolute inset-0 pointer-events-none opacity-20"
        style={{
          backgroundImage: 'radial-gradient(var(--border-subtle) 1px, transparent 1px)',
          backgroundSize: '24px 24px'
        }}
      />

      {/* Content */}
      <div className="relative z-10 flex flex-col items-center text-center max-w-[680px] w-full">
        <div className="inline-flex items-center justify-center bg-bg-elevated border border-border-default text-text-muted text-[11px] tracking-[0.06em] rounded-full px-4 py-[3px] font-medium mb-8 select-none">
          OPEN SOURCE · AI-POWERED · CLI-FIRST
        </div>

        <h1 className="text-[56px] font-bold font-sans text-text-primary leading-[1.1] tracking-tight mb-6">
          Your data pipeline<br />in plain English.
        </h1>

        <p className="text-[18px] text-text-secondary leading-[1.6] max-w-[520px] mb-10">
          Connect a source, describe your transformation, load clean data anywhere.
          No boilerplate. No infrastructure. One command.
        </p>

        <div className="flex flex-col sm:flex-row items-center justify-center gap-3 w-full mb-8">
          <Button asChild size="md" className="h-9 px-8 w-full sm:w-auto text-[14px]">
            <Link to="/docs/quickstart">Get started &rarr;</Link>
          </Button>
          <Button asChild variant="outline" size="md" className="h-9 px-8 w-full sm:w-auto text-[14px] bg-bg-surface">
            <a href="https://github.com/lupppig/loafer" target="_blank" rel="noopener noreferrer">View on GitHub</a>
          </Button>
        </div>

        <div className="flex flex-col items-center w-full max-w-[280px] mt-4">
          <div className="flex items-center gap-1 mb-3 p-1 bg-bg-base border border-border-default rounded-[4px] w-full shadow-sm">
            <button 
              onClick={() => setPkgManager('pip')}
              className={cn("flex-1 px-3 py-1.5 text-[12px] font-medium rounded-[2px] transition-all outline-none", pkgManager === 'pip' ? "bg-bg-surface text-text-primary shadow-[0_1px_2px_rgba(0,0,0,0.12)] border border-border-subtle" : "text-text-muted hover:text-text-secondary border border-transparent")}
            >
              pip
            </button>
            <button 
              onClick={() => setPkgManager('docker')}
              className={cn("flex-1 px-3 py-1.5 text-[12px] font-medium rounded-[2px] transition-all outline-none", pkgManager === 'docker' ? "bg-bg-surface text-text-primary shadow-[0_1px_2px_rgba(0,0,0,0.12)] border border-border-subtle" : "text-text-muted hover:text-text-secondary border border-transparent")}
            >
              Docker
            </button>
          </div>
          <div className="flex flex-row items-center justify-between bg-bg-elevated border border-border-default rounded-sm px-4 py-2.5 w-full group shadow-sm">
            <span className="font-mono text-[13px] text-text-code">{installCmd}</span>
            <button 
              className="text-text-muted hover:text-text-primary transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 rounded-sm"
              onClick={handleCopy}
              aria-label="Copy install command"
            >
              {copied ? <Check className="w-4 h-4 text-green-500" /> : <Copy className="w-4 h-4" />}
            </button>
          </div>
        </div>
      </div>

      <div className="relative z-10 w-full max-w-[720px] mt-16 lg:mt-24">
        <TerminalAnimation />
      </div>
    </section>
  );
}
