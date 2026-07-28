'use client'

import React, { useRef, useState } from 'react';
import {
  AlertOctagon,
  AlertTriangle,
  ArrowUpRight,
  Check,
  Copy,
  Info,
  Lightbulb,
} from 'lucide-react';
import { cn } from '../../utils/cn';

type ElementProps<Tag extends keyof React.JSX.IntrinsicElements> =
  React.ComponentPropsWithoutRef<Tag>;

type PreProps = ElementProps<'pre'> & {
  'data-language'?: string;
  title?: string;
};

type CalloutType = 'info' | 'warning' | 'tip' | 'danger';

export function CodeBlock(props: PreProps) {
  const [copied, setCopied] = useState(false);
  const preRef = useRef<HTMLPreElement>(null);
  const language = props['data-language'];
  const isOutput = language === 'text' || language === 'output' || props.title === 'Output';

  const onCopy = () => {
    if (!preRef.current) return;

    let text = preRef.current.innerText;
    if (language === 'bash' || language === 'sh' || language === 'shell' || language === 'zsh') {
      text = text
        .split('\n')
        .map(line => line.replace(/^(\$|>)\s*/, ''))
        .join('\n');
    }
    void navigator.clipboard.writeText(text.trim());
    setCopied(true);
    window.setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative group my-4 first:mt-0 last:mb-0">
      <pre
        ref={preRef}
        {...props}
        className={cn(
          props.className,
          'm-0 border-none bg-transparent overflow-x-auto',
          isOutput && 'opacity-80',
        )}
      />
      {!isOutput && (
        <button
          type="button"
          onClick={onCopy}
          className="absolute top-2 right-2 p-1.5 rounded-md bg-bg-surface/80 backdrop-blur-sm border border-border-default opacity-0 group-hover:opacity-100 transition-all hover:bg-bg-elevated hover:border-border-strong focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 shadow-sm z-10"
          title="Copy to clipboard"
        >
          {copied ? (
            <Check className="w-3.5 h-3.5 text-green-500" />
          ) : (
            <Copy className="w-3.5 h-3.5 text-text-muted" />
          )}
        </button>
      )}
      {isOutput && (
        <div className="absolute top-2 right-3 text-[10px] font-bold text-indigo-400 opacity-60 uppercase tracking-widest select-none pointer-events-none">
          Output
        </div>
      )}
    </div>
  );
}

export function MdxLink(props: ElementProps<'a'>) {
  const isExternal = props.href?.startsWith('http');
  return (
    <a
      className="text-indigo-400 no-underline hover:underline decoration-indigo-400/50 underline-offset-4 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 rounded-[2px]"
      target={isExternal ? '_blank' : undefined}
      rel={isExternal ? 'noopener noreferrer' : undefined}
      {...props}
    >
      {props.children}
      {isExternal && (
        <ArrowUpRight className="inline-block w-3 h-3 ml-0.5 relative -top-[1px] opacity-70" />
      )}
    </a>
  );
}

function textContent(node: React.ReactNode): string {
  if (typeof node === 'string' || typeof node === 'number') return String(node);
  if (Array.isArray(node)) return node.map(textContent).join('');
  if (React.isValidElement<{ children?: React.ReactNode }>(node)) {
    return textContent(node.props.children);
  }
  return '';
}

function stripCalloutPrefix(node: React.ReactNode): React.ReactNode {
  const pattern = /^(Warning:|Tip:|Danger:|Note:|\[!(WARNING|TIP|DANGER|NOTE)\])\s*/i;
  if (typeof node === 'string') return node.replace(pattern, '');
  if (!Array.isArray(node)) return node;

  const next = [...node];
  const firstText = next.findIndex(item => typeof item === 'string');
  if (firstText >= 0) next[firstText] = (next[firstText] as string).replace(pattern, '');
  return next;
}

export function Callout(props: ElementProps<'blockquote'>) {
  const text = textContent(props.children);
  let type: CalloutType = 'info';
  let title = 'Note';
  let Icon = Info;

  if (text.startsWith('Warning:') || text.startsWith('[!WARNING]')) {
    type = 'warning';
    title = 'Warning';
    Icon = AlertTriangle;
  } else if (text.startsWith('Tip:') || text.startsWith('[!TIP]')) {
    type = 'tip';
    title = 'Tip';
    Icon = Lightbulb;
  } else if (text.startsWith('Danger:') || text.startsWith('[!DANGER]')) {
    type = 'danger';
    title = 'Danger';
    Icon = AlertOctagon;
  }

  const borders: Record<CalloutType, string> = {
    info: 'border-l-blue-500/80',
    warning: 'border-l-amber-500/80',
    tip: 'border-l-green-500/80',
    danger: 'border-l-red-500/80',
  };
  const backgrounds: Record<CalloutType, string> = {
    info: 'bg-blue-900/10',
    warning: 'bg-amber-900/10',
    tip: 'bg-green-900/10',
    danger: 'bg-red-900/10',
  };
  const textColors: Record<CalloutType, string> = {
    info: 'text-blue-400',
    warning: 'text-amber-400',
    tip: 'text-green-400',
    danger: 'text-red-400',
  };

  const cleanChildren = React.Children.map(props.children, child => {
    if (!React.isValidElement<{ children?: React.ReactNode }>(child)) return child;
    return React.cloneElement(child, {
      children: stripCalloutPrefix(child.props.children),
    });
  });

  return (
    <div
      className={cn(
        'my-6 p-4 rounded-sm border-l-[3px] border border-border-subtle',
        borders[type],
        backgrounds[type],
      )}
    >
      <div
        className={cn(
          'flex items-center gap-2 mb-2 font-medium text-[13px] uppercase tracking-wide',
          textColors[type],
        )}
      >
        <Icon className="w-4 h-4" />
        {title}
      </div>
      <div className="text-[14px] text-text-secondary leading-[1.6]">
        {cleanChildren}
      </div>
    </div>
  );
}
