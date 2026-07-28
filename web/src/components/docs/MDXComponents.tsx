import type React from 'react';
import { Callout, CodeBlock, MdxLink } from './MDXPrimitives';

type ElementProps<Tag extends keyof React.JSX.IntrinsicElements> =
  React.ComponentPropsWithoutRef<Tag>;

export const MDXComponents = {
  h1: (props: ElementProps<'h1'>) => (
    <h1 className="text-[30px] font-sans font-bold text-text-primary mt-0 mb-8 tracking-tight" {...props} />
  ),
  h2: (props: ElementProps<'h2'>) => (
    <h2 className="text-[20px] font-sans font-semibold text-text-primary mt-12 mb-4 pt-6 border-t border-border-subtle scroll-mt-[70px]" {...props} />
  ),
  h3: (props: ElementProps<'h3'>) => (
    <h3 className="text-[16px] font-sans font-semibold text-text-primary mt-8 mb-3 scroll-mt-[70px]" {...props} />
  ),
  h4: (props: ElementProps<'h4'>) => (
    <h4 className="text-[13px] font-sans font-semibold text-text-primary uppercase tracking-[0.06em] mt-6 mb-2 scroll-mt-[70px]" {...props} />
  ),
  p: (props: ElementProps<'p'>) => (
    <p className="text-[14px] text-text-secondary leading-[1.75] mb-4" {...props} />
  ),
  pre: (props: ElementProps<'pre'>) => <CodeBlock {...props} />,
  a: (props: ElementProps<'a'>) => <MdxLink {...props} />,
  ul: (props: ElementProps<'ul'>) => (
    <ul className="list-disc pl-5 mb-6 text-[14px] text-text-secondary leading-[1.75] space-y-1" {...props} />
  ),
  ol: (props: ElementProps<'ol'>) => (
    <ol className="list-decimal pl-5 mb-6 text-[14px] text-text-secondary leading-[1.75] space-y-1" {...props} />
  ),
  li: (props: ElementProps<'li'>) => <li {...props} />,
  table: (props: ElementProps<'table'>) => (
    <div className="overflow-x-auto mb-8 rounded-md border border-border-subtle shadow-sm my-6 bg-bg-surface">
      <table className="w-full text-left border-collapse text-[13px] text-text-secondary" {...props} />
    </div>
  ),
  th: (props: ElementProps<'th'>) => (
    <th className="bg-bg-elevated px-4 py-2.5 border-b border-border-subtle font-medium text-text-primary whitespace-nowrap" {...props} />
  ),
  td: (props: ElementProps<'td'>) => (
    <td className="px-4 py-2 border-b border-border-subtle/50" {...props} />
  ),
  code: (props: ElementProps<'code'>) => {
    if (props.className?.includes('shiki')) return <code {...props} />;
    return (
      <code
        className="font-mono text-[12px] bg-bg-elevated border border-border-subtle rounded-[3px] px-1.5 py-0.5 text-text-code"
        {...props}
      />
    );
  },
  blockquote: (props: ElementProps<'blockquote'>) => <Callout {...props} />,
};
