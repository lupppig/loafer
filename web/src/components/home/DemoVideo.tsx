import React, { useEffect, useRef, useState, useCallback } from 'react';
import { motion, AnimatePresence, useInView } from 'framer-motion';
import { Play, Pause, RotateCcw } from 'lucide-react';

/* ─── Scene Data ─── */

const YAML_LINES = [
  { text: 'name: Daily Orders Pipeline', delay: 0 },
  { text: 'mode: etl', delay: 0.6 },
  { text: '', delay: 0.9 },
  { text: 'source:', delay: 1.4 },
  { text: '  url: ${DATABASE_URL}', delay: 2.0 },
  { text: '  query: "SELECT * FROM orders"', delay: 2.8 },
  { text: '', delay: 3.2 },
  { text: 'target:', delay: 3.8 },
  { text: '  path: ./output/clean_orders.csv', delay: 4.5 },
  { text: '  write_mode: overwrite', delay: 5.2 },
  { text: '', delay: 5.6 },
  { text: 'transform: >', delay: 6.2 },
  { text: '  Drop cancelled orders, normalize', delay: 7.0 },
  { text: '  currency to USD, combine names.', delay: 7.8 },
  { text: '', delay: 8.3 },
  { text: 'llm:', delay: 8.8 },
  { text: '  provider: gemini', delay: 9.4 },
  { text: '  model: gemini-2.5-flash', delay: 10.0 },
];

const CLI_CMD = '$ loafer run pipeline.yaml';

const CLI_OUTPUT = [
  { text: '', type: 'empty' as const, ts: 0.6 },
  { text: '  Loafer v0.2.0  ·  ETL mode', type: 'info' as const, ts: 1.0 },
  { text: '', type: 'empty' as const, ts: 1.2 },
  { text: '  ✓  Config valid', type: 'success' as const, ts: 1.8 },
  { text: '  ✓  Connection established — orders-db (PostgreSQL)', type: 'success' as const, ts: 2.6 },
  { text: '', type: 'empty' as const, ts: 2.8 },
  { text: '  Extracting...', type: 'log' as const, ts: 3.2 },
  { text: '  ████████████████████  42,801 rows  ·  1.2s', type: 'progress' as const, ts: 3.6, dur: 1.8 },
  { text: '', type: 'empty' as const, ts: 5.6 },
  { text: '  Validating...', type: 'log' as const, ts: 5.8 },
  { text: '  ✓  Schema consistent  ·  null rate 0.02%', type: 'success' as const, ts: 6.6 },
  { text: '', type: 'empty' as const, ts: 6.8 },
  { text: '  Transforming...  (Gemini 2.5 Flash)', type: 'log' as const, ts: 7.2 },
  { text: '  ✓  Function generated  ·  412 tokens', type: 'success' as const, ts: 8.2 },
  { text: '  ████████████████████  42,801 rows  ·  3.4s', type: 'progress' as const, ts: 8.6, dur: 3.0 },
  { text: '', type: 'empty' as const, ts: 11.8 },
  { text: '  Loading...', type: 'log' as const, ts: 12.0 },
  { text: '  ████████████████████  41,923 rows  ·  2.1s', type: 'progress' as const, ts: 12.4, dur: 2.4 },
  { text: '', type: 'empty' as const, ts: 15.0 },
  { text: '  ╭─ Complete ─────────────────────────────────╮', type: 'border' as const, ts: 15.6 },
  { text: '  │  41,923 rows loaded  ·  6.8s total         │', type: 'border-inner' as const, ts: 15.8 },
  { text: '  │  878 filtered by transform                 │', type: 'border-inner' as const, ts: 16.0 },
  { text: '  │  target: ./output/clean_orders.csv         │', type: 'border-inner' as const, ts: 16.2 },
  { text: '  ╰────────────────────────────────────────────╯', type: 'border' as const, ts: 16.4 },
];

// Scene durations (ms)
const S_YAML = 12000;
const S_TRANSITION = 2500;
const S_CLI_TYPE = 2200; // typing the command
const S_CLI_RUN = 18000;
const S_DATA = 5000;
const S_HOLD = 2000;
const TOTAL = S_YAML + S_TRANSITION + S_CLI_TYPE + S_CLI_RUN + S_DATA + S_HOLD;

type Scene = 'yaml' | 'transition' | 'cli-type' | 'cli-run' | 'data' | 'done';

/* ─── Data Preview Rows ─── */
const DATA_HEADERS = ['order_id', 'full_name', 'amount_usd', 'status', 'created_at'];
const DATA_ROWS = [
  ['10241', 'Sarah Chen', '$142.50', 'paid', '2025-07-01'],
  ['10242', 'James Miller', '$89.00', 'paid', '2025-07-01'],
  ['10243', 'Ana Gutierrez', '$215.75', 'paid', '2025-07-01'],
  ['10244', 'Raj Patel', '$67.20', 'paid', '2025-07-02'],
  ['10245', 'Emma Wilson', '$310.00', 'paid', '2025-07-02'],
  ['10246', 'Liam O\'Brien', '$178.90', 'paid', '2025-07-02'],
];

/* ─── Helpers ─── */

function highlightYaml(text: string): React.ReactNode {
  if (text === '') return '\u00A0';
  if (text.trimStart().startsWith('#'))
    return <span style={{ color: '#52525b' }}>{text}</span>;

  const m = text.match(/^(\s*)([\w-]+)(:)(.*)$/);
  if (m) {
    const [, indent, key, colon, value] = m;
    const v = value.trim();
    let vc = '#e2e8f0';
    if (v.startsWith('"') || v.startsWith("'")) vc = '#f59e0b';
    else if (v.startsWith('$')) vc = '#818cf8';
    else if (['etl', 'elt', 'overwrite', 'gemini'].includes(v)) vc = '#22c55e';
    else if (v === '>') vc = '#52525b';
    return (
      <span>
        {indent}<span style={{ color: '#818cf8' }}>{key}</span>
        <span style={{ color: '#52525b' }}>{colon}</span>
        <span style={{ color: vc }}>{value}</span>
      </span>
    );
  }
  if (text.startsWith('  ') && !text.includes(':'))
    return <span style={{ color: '#f59e0b' }}>{text}</span>;
  return <span style={{ color: '#e2e8f0' }}>{text}</span>;
}

function CliLine({ line, fill }: { line: (typeof CLI_OUTPUT)[0]; fill: number }) {
  if (line.type === 'empty') return <div style={{ height: '1.6em' }} />;
  if (line.type === 'progress') {
    const n = 20, f = Math.floor(fill * n);
    const bar = '█'.repeat(f) + '░'.repeat(n - f);
    const [a, b] = line.text.split('████████████████████');
    return (
      <div style={{ whiteSpace: 'pre' }}>
        {a}<span style={{ color: fill >= 1 ? '#818cf8' : '#52525b' }}>{bar}</span>
        <span style={{ color: '#e2e8f0' }}>{b}</span>
      </div>
    );
  }
  if (line.type === 'success') {
    const [a, b] = line.text.split('✓');
    return <div style={{ whiteSpace: 'pre', color: '#e2e8f0' }}>{a}<span style={{ color: '#22c55e' }}>✓</span>{b}</div>;
  }
  if (line.type === 'border' || line.type === 'border-inner') {
    return (
      <div style={{ whiteSpace: 'pre' }}>
        {line.text.split(/([╭╰─│╮╯]+)/).map((p, i) =>
          /[╭╰─│╮╯]/.test(p)
            ? <span key={i} style={{ color: '#818cf8' }}>{p}</span>
            : <span key={i} style={{ color: '#e2e8f0' }}>{p}</span>
        )}
      </div>
    );
  }
  const c: Record<string, string> = { cmd: '#e2e8f0', info: '#a1a1aa', log: '#e2e8f0' };
  return <div style={{ whiteSpace: 'pre', color: c[line.type] || '#e2e8f0' }}>{line.text}</div>;
}

/* ─── Main ─── */

export function DemoVideo() {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref, { once: true, margin: '-80px' });

  const [scene, setScene] = useState<Scene>('yaml');
  const [elapsed, setElapsed] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [started, setStarted] = useState(false);
  const t0 = useRef(0);
  const paused = useRef(0);
  const raf = useRef(0);

  useEffect(() => { if (inView && !started) { setStarted(true); setPlaying(true); } }, [inView, started]);

  const tick = useCallback(() => {
    const ms = Date.now() - t0.current;
    setElapsed(ms);
    const b1 = S_YAML, b2 = b1 + S_TRANSITION, b3 = b2 + S_CLI_TYPE, b4 = b3 + S_CLI_RUN, b5 = b4 + S_DATA;
    if (ms < b1) setScene('yaml');
    else if (ms < b2) setScene('transition');
    else if (ms < b3) setScene('cli-type');
    else if (ms < b4) setScene('cli-run');
    else if (ms < b5) setScene('data');
    else { setScene('done'); setPlaying(false); return; }
    raf.current = requestAnimationFrame(tick);
  }, []);

  useEffect(() => {
    if (playing) { t0.current = Date.now() - paused.current; raf.current = requestAnimationFrame(tick); }
    else { paused.current = elapsed; cancelAnimationFrame(raf.current); }
    return () => cancelAnimationFrame(raf.current);
  }, [playing, tick]);

  const restart = () => { paused.current = 0; setElapsed(0); setScene('yaml'); setPlaying(true); };
  const toggle = () => { scene === 'done' ? restart() : setPlaying(!playing); };

  const progress = Math.min(100, (elapsed / TOTAL) * 100);
  const yamlT = elapsed / 1000;
  const cliTypeT = Math.max(0, elapsed - S_YAML - S_TRANSITION) / 1000;
  const cliRunT = Math.max(0, elapsed - S_YAML - S_TRANSITION - S_CLI_TYPE) / 1000;
  const dataT = Math.max(0, elapsed - S_YAML - S_TRANSITION - S_CLI_TYPE - S_CLI_RUN) / 1000;

  // Typed command chars
  const typedChars = Math.min(CLI_CMD.length, Math.floor(cliTypeT * 14));
  const typedCmd = CLI_CMD.slice(0, typedChars);

  const sceneLabel = { yaml: 'CONFIG', transition: 'RUNNING', 'cli-type': 'TERMINAL', 'cli-run': 'CLI OUTPUT', data: 'RESULT', done: 'COMPLETE' }[scene];
  const titleLabel = (scene === 'yaml' || scene === 'transition') ? 'pipeline.yaml — editor' : (scene === 'data' || scene === 'done') ? 'clean_orders.csv — preview' : 'loafer — zsh';

  return (
    <section className="py-24 px-6 border-t border-border-subtle w-full" id="demo">
      <div className="max-w-4xl mx-auto flex flex-col items-center" ref={ref}>
        <div className="inline-flex items-center justify-center bg-bg-elevated border border-border-default text-text-muted text-[11px] tracking-[0.06em] rounded-full px-4 py-[3px] font-medium mb-6 select-none">
          PRODUCT DEMO
        </div>
        <h2 className="text-[28px] md:text-[32px] font-semibold text-text-primary text-center mb-4 tracking-tight">
          See it in action.
        </h2>
        <p className="text-[16px] md:text-[18px] text-text-secondary text-center mb-12 max-w-[480px]">
          Write a YAML config. Run one command. Your data pipeline is live.
        </p>

        {/* Video player */}
        <div className="w-full rounded-lg overflow-hidden border border-border-default shadow-2xl shadow-black/40">
          {/* Title bar */}
          <div className="h-10 flex items-center px-4 relative border-b" style={{ background: 'var(--bg-surface)', borderColor: 'var(--border-default)' }}>
            <div className="flex gap-2">
              <div className="w-[10px] h-[10px] rounded-full" style={{ background: 'rgba(239,68,68,0.8)' }} />
              <div className="w-[10px] h-[10px] rounded-full" style={{ background: 'rgba(245,158,11,0.8)' }} />
              <div className="w-[10px] h-[10px] rounded-full" style={{ background: 'rgba(34,197,94,0.8)' }} />
            </div>
            <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
              <AnimatePresence mode="wait">
                <motion.span key={titleLabel} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -6 }} transition={{ duration: 0.4 }} className="font-mono text-[11px]" style={{ color: 'var(--text-muted)' }}>
                  {titleLabel}
                </motion.span>
              </AnimatePresence>
            </div>
          </div>

          {/* Content */}
          <div className="relative font-mono text-[12px] sm:text-[13px] leading-[1.7]" style={{ background: 'var(--bg-elevated)', minHeight: '440px', maxHeight: '440px', overflow: 'hidden' }}>
            <AnimatePresence mode="wait">

              {/* ── YAML Editor ── */}
              {scene === 'yaml' && (
                <motion.div key="yaml" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0, scale: 0.97, filter: 'blur(6px)' }} transition={{ duration: 0.6 }} className="absolute inset-0 p-5 overflow-hidden">
                  <div className="flex">
                    <div className="select-none pr-4 text-right shrink-0" style={{ color: 'var(--text-muted)', minWidth: '2rem' }}>
                      {YAML_LINES.map((l, i) => (
                        <div key={i} style={{ opacity: yamlT >= l.delay ? 1 : 0, transition: 'opacity 0.3s ease' }}>{i + 1}</div>
                      ))}
                    </div>
                    <div className="flex-1 min-w-0">
                      {YAML_LINES.map((l, i) => (
                        <motion.div key={i} initial={{ opacity: 0, x: -8 }} animate={{ opacity: yamlT >= l.delay ? 1 : 0, x: yamlT >= l.delay ? 0 : -8 }} transition={{ duration: 0.35, ease: 'easeOut' }} style={{ whiteSpace: 'pre' }}>
                          {highlightYaml(l.text)}
                        </motion.div>
                      ))}
                      <span className="inline-block w-[7px] bg-indigo-400 animate-pulse" style={{ height: '1.1em', verticalAlign: 'middle', borderRadius: 1 }} />
                    </div>
                  </div>
                </motion.div>
              )}

              {/* ── Transition ── */}
              {scene === 'transition' && (
                <motion.div key="trans" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.6 }} className="absolute inset-0 flex flex-col items-center justify-center gap-5" style={{ background: 'var(--bg-base)' }}>
                  <motion.div initial={{ scale: 0.7, opacity: 0 }} animate={{ scale: 1, opacity: 1 }} transition={{ duration: 0.6, ease: 'easeOut' }} className="w-14 h-14 rounded-full flex items-center justify-center" style={{ background: 'linear-gradient(135deg, rgba(99,102,241,0.2), rgba(99,102,241,0.05))', border: '1px solid rgba(99,102,241,0.3)' }}>
                    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#818cf8" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="4 17 10 11 4 5" /><line x1="12" y1="19" x2="20" y2="19" /></svg>
                  </motion.div>
                  <motion.span initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.5, delay: 0.3 }} className="font-mono text-[14px]" style={{ color: 'var(--text-secondary)' }}>
                    Opening terminal...
                  </motion.span>
                </motion.div>
              )}

              {/* ── CLI Typing ── */}
              {scene === 'cli-type' && (
                <motion.div key="cli-type" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.4 }} className="absolute inset-0 p-5 overflow-hidden">
                  <div style={{ whiteSpace: 'pre', color: '#e2e8f0' }}>
                    {typedCmd}
                    <span className="inline-block w-[7px] bg-text-muted animate-pulse ml-px" style={{ height: '1.1em', verticalAlign: 'middle', borderRadius: 1 }} />
                  </div>
                </motion.div>
              )}

              {/* ── CLI Running ── */}
              {(scene === 'cli-run') && (
                <motion.div key="cli-run" initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.35 }} className="absolute inset-0 p-5 overflow-hidden">
                  <div style={{ whiteSpace: 'pre', color: '#e2e8f0' }}>{CLI_CMD}</div>
                  {CLI_OUTPUT.map((line, i) => {
                    if (cliRunT < line.ts) return null;
                    let fill = 0;
                    if (line.type === 'progress' && 'dur' in line) fill = Math.min(1, (cliRunT - line.ts) / ((line as any).dur || 1));
                    return (
                      <motion.div key={i} initial={{ opacity: 0, y: 4 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.2 }}>
                        <CliLine line={line} fill={fill} />
                      </motion.div>
                    );
                  })}
                  <span className="inline-block w-[7px] bg-text-muted animate-pulse mt-1" style={{ height: '1.1em', verticalAlign: 'middle', borderRadius: 1 }} />
                </motion.div>
              )}

              {/* ── Data Preview ── */}
              {(scene === 'data' || scene === 'done') && (
                <motion.div key="data" initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.6, ease: 'easeOut' }} className="absolute inset-0 p-5 overflow-hidden flex flex-col gap-4">
                  <div className="flex items-center gap-3">
                    <span style={{ color: '#22c55e' }}>✓</span>
                    <span style={{ color: '#e2e8f0' }} className="text-[13px]">Pipeline complete — previewing <span style={{ color: '#818cf8' }}>clean_orders.csv</span></span>
                  </div>
                  <div className="border rounded-md overflow-hidden" style={{ borderColor: 'var(--border-default)' }}>
                    <table className="w-full text-[11px] sm:text-[12px]">
                      <thead>
                        <tr style={{ background: 'var(--bg-surface)' }}>
                          {DATA_HEADERS.map((h, i) => (
                            <motion.th key={h} initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: i * 0.08, duration: 0.3 }} className="px-3 py-2 text-left font-medium" style={{ color: '#818cf8', borderBottom: '1px solid var(--border-default)' }}>
                              {h}
                            </motion.th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {DATA_ROWS.map((row, ri) => {
                          const rowDelay = 0.3 + ri * 0.15;
                          const visible = dataT >= rowDelay;
                          return (
                            <motion.tr key={ri} initial={{ opacity: 0, x: -6 }} animate={{ opacity: visible ? 1 : 0, x: visible ? 0 : -6 }} transition={{ duration: 0.3 }} style={{ borderBottom: ri < DATA_ROWS.length - 1 ? '1px solid var(--border-subtle)' : 'none' }}>
                              {row.map((cell, ci) => (
                                <td key={ci} className="px-3 py-1.5" style={{ color: ci === 2 ? '#22c55e' : ci === 3 ? '#f59e0b' : '#e2e8f0' }}>
                                  {cell}
                                </td>
                              ))}
                            </motion.tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                  <div className="text-[11px] mt-1" style={{ color: 'var(--text-muted)' }}>
                    Showing 6 of 41,923 rows  ·  5 columns  ·  878 filtered by transform
                  </div>
                </motion.div>
              )}

            </AnimatePresence>
          </div>

          {/* Controls */}
          <div className="h-11 flex items-center gap-3 px-4 border-t" style={{ background: 'var(--bg-surface)', borderColor: 'var(--border-default)' }}>
            <button onClick={toggle} className="p-1 rounded transition-colors hover:bg-white/5 focus:outline-none focus-visible:ring-1 focus-visible:ring-indigo-500" aria-label={playing ? 'Pause' : 'Play'}>
              {playing ? <Pause className="w-3.5 h-3.5" style={{ color: 'var(--text-secondary)' }} /> : <Play className="w-3.5 h-3.5" style={{ color: 'var(--text-secondary)' }} />}
            </button>
            <button onClick={restart} className="p-1 rounded transition-colors hover:bg-white/5 focus:outline-none focus-visible:ring-1 focus-visible:ring-indigo-500" aria-label="Restart">
              <RotateCcw className="w-3.5 h-3.5" style={{ color: 'var(--text-muted)' }} />
            </button>
            <div className="flex-1 h-[3px] rounded-full overflow-hidden" style={{ background: 'var(--border-default)' }}>
              <motion.div className="h-full rounded-full" style={{ background: 'linear-gradient(90deg, #6366f1, #818cf8)', width: `${progress}%` }} transition={{ duration: 0.1 }} />
            </div>
            <span className="font-mono text-[10px] tracking-wider select-none" style={{ color: 'var(--text-muted)' }}>{sceneLabel}</span>
          </div>
        </div>

        {/* Legend */}
        <div className="mt-8 flex flex-wrap items-center justify-center gap-x-6 gap-y-2 text-[13px]" style={{ color: 'var(--text-muted)' }}>
          <span className="flex items-center gap-2"><span className="w-2 h-2 rounded-full" style={{ background: '#818cf8' }} />Write YAML config</span>
          <span className="flex items-center gap-2"><span className="w-2 h-2 rounded-full" style={{ background: '#22c55e' }} />Run one command</span>
          <span className="flex items-center gap-2"><span className="w-2 h-2 rounded-full" style={{ background: '#f59e0b' }} />Pipeline delivered</span>
        </div>
      </div>
    </section>
  );
}
