import React, { useEffect, useRef, useState } from 'react';
import { motion, useInView } from 'framer-motion';

const lines = [
  { text: '$ loafer run pipeline.yaml', type: 'cmd', ts: 0 },
  { text: '', type: 'empty', ts: 0.1 },
  { text: '  Loafer v0.1.0  ·  ETL mode', type: 'info', ts: 0.4 },
  { text: '', type: 'empty', ts: 0.5 },
  { text: '  ✓  Config valid', type: 'success', ts: 0.8 },
  { text: '  ✓  Connection established — orders-db (PostgreSQL)', type: 'success', ts: 1.2 },
  { text: '', type: 'empty', ts: 1.3 },
  { text: '  Extracting...', type: 'log', ts: 1.6 },
  { text: '  ████████████████████  42,801 rows  ·  1.2s', type: 'progress', ts: 1.8, dur: 1.2 },
  { text: '', type: 'empty', ts: 3.1 },
  { text: '  Validating...', type: 'log', ts: 3.2 },
  { text: '  ✓  Schema consistent  ·  null rate within threshold', type: 'success', ts: 3.6 },
  { text: '', type: 'empty', ts: 3.7 },
  { text: '  Transforming...  (Gemini 1.5 Flash)', type: 'log', ts: 3.9 },
  { text: '  ✓  Function generated  ·  412 tokens', type: 'success', ts: 4.8 },
  { text: '  ████████████████████  42,801 rows  ·  3.4s', type: 'progress', ts: 5.0, dur: 3.4 },
  { text: '', type: 'empty', ts: 8.5 },
  { text: '  Loading...', type: 'log', ts: 8.6 },
  { text: '  ████████████████████  41,923 rows written  ·  2.1s', type: 'progress', ts: 8.8, dur: 2.1 },
  { text: '', type: 'empty', ts: 11.0 },
  { text: '  ╭─ Complete ─────────────────────────────╮', type: 'border', ts: 11.2 },
  { text: '  │  41,923 rows loaded  ·  6.8s total     │', type: 'border-inner', ts: 11.3 },
  { text: '  │  878 filtered by transform             │', type: 'border-inner', ts: 11.4 },
  { text: '  │  599 tokens used                       │', type: 'border-inner', ts: 11.5 },
  { text: '  ╰────────────────────────────────────────╯', type: 'border', ts: 11.6 },
];

export function TerminalAnimation() {
  const containerRef = useRef<HTMLDivElement>(null);
  const isInView = useInView(containerRef, { once: true, margin: "-100px" });
  const [visibleLines, setVisibleLines] = useState<number[]>([]);
  const [typedCommand, setTypedCommand] = useState('');
  const [progressFills, setProgressFills] = useState<Record<number, number>>({});

  useEffect(() => {
    if (!isInView) return;

    let startTime = Date.now();
    let isCancelled = false;

    // Command typing effect for first line
    const cmdText = lines[0].text;
    let charsTyped = 0;
    const typeInterval = setInterval(() => {
      charsTyped++;
      setTypedCommand(cmdText.slice(0, charsTyped));
      if (charsTyped >= cmdText.length) clearInterval(typeInterval);
    }, 40);

    const updateFrame = () => {
      if (isCancelled) return;
      const elapsed = (Date.now() - startTime) / 1000;

      // Reveal lines based on their ts
      const visible = lines.map((l, i) => (elapsed >= l.ts ? i : -1)).filter(i => i !== -1);
      setVisibleLines(visible);

      // Handle progress bars dynamically
      const newFills: Record<number, number> = {};
      lines.forEach((l, i) => {
        if (l.type === 'progress' && elapsed >= l.ts) {
          const p = Math.min(1, (elapsed - l.ts) / (l.dur || 1));
          newFills[i] = p;
        }
      });
      setProgressFills(newFills);

      // Loop after 16 seconds
      if (elapsed > 16) {
        startTime = Date.now();
        charsTyped = 0;
        setTypedCommand('');
      }

      requestAnimationFrame(updateFrame);
    };

    const frameId = requestAnimationFrame(updateFrame);

    return () => {
      isCancelled = true;
      clearInterval(typeInterval);
      cancelAnimationFrame(frameId);
    };
  }, [isInView]);

  return (
    <div 
      ref={containerRef}
      className="w-full bg-bg-elevated rounded-md border border-border-default shadow-2xl overflow-hidden font-mono text-[12px] leading-[1.6]"
    >
      {/* Title Bar */}
      <div className="h-9 border-b border-border-default/50 bg-bg-surface/50 flex items-center px-4 relative">
        <div className="flex gap-2">
          <div className="w-[10px] h-[10px] rounded-full bg-red-500/80" />
          <div className="w-[10px] h-[10px] rounded-full bg-amber-500/80" />
          <div className="w-[10px] h-[10px] rounded-full bg-green-500/80" />
        </div>
        <div className="absolute inset-0 flex items-center justify-center text-text-muted text-[11px] pointer-events-none">
          loafer — zsh
        </div>
      </div>

      {/* Terminal Body */}
      <div className="p-5 min-h-[380px]">
        {lines.map((line, idx) => {
          if (!visibleLines.includes(idx) && idx !== 0) return null;

          if (idx === 0) {
            return (
              <div key={idx} className="text-text-code whitespace-pre">
                {typedCommand}
                <span className="inline-block w-2 bg-text-muted animate-pulse ml-0.5 h-[1em] align-middle" />
              </div>
            );
          }

          return (
            <motion.div
              key={idx}
              initial={{ opacity: 0, y: 4 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.15 }}
            >
              <LineRenderer line={line} fill={progressFills[idx] || 0} />
            </motion.div>
          );
        })}
      </div>
    </div>
  );
}

function LineRenderer({ line, fill }: { line: any, fill: number }) {
  if (line.type === 'empty') return <div className="h-[1.6em]" />;
  
  if (line.type === 'progress') {
    const bars = 20;
    const filledBars = Math.floor(fill * bars);
    const barString = '█'.repeat(filledBars) + '░'.repeat(bars - filledBars);
    const parts = line.text.split('████████████████████');
    return (
      <div className="whitespace-pre">
        {parts[0]}
        <span className={fill === 1 ? 'text-indigo-400' : 'text-text-muted'}>{barString}</span>
        <span className="text-text-code">{parts[1]}</span>
      </div>
    );
  }

  const renderPart = () => {
    let content = line.text;
    
    // Highlight checkmarks
    if (content.includes('✓')) {
      const parts = content.split('✓');
      return (
        <span>
          {parts[0]}<span className="text-green-500">✓</span>{parts[1]}
        </span>
      );
    }
    
    // Highlight borders
    if (line.type === 'border' || line.type === 'border-inner') {
      return (
        <span className="text-indigo-400">
          {content.split(/([^╭╰─│╮╯]+)/).map((p: string, i: number) => {
            if (/^[╭╰─│╮╯]+$/.test(p)) return <span key={i} className="text-indigo-400">{p}</span>;
            return <span key={i} className="text-text-code">{p}</span>;
          })}
        </span>
      );
    }
    
    if (line.type === 'cmd') return <span className="text-text-code">{content}</span>;
    if (line.type === 'info') return <span className="text-text-secondary">{content}</span>;
    
    return <span className="text-text-code">{content}</span>;
  };

  return <div className="whitespace-pre text-text-code">{renderPart()}</div>;
}
