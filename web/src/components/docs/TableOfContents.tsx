import React, { useEffect, useState } from 'react';
import { cn } from '../../utils/cn';

interface TOCItem {
  id: string;
  title: string;
  level: number;
}

export function TableOfContents() {
  const [headings, setHeadings] = useState<TOCItem[]>([]);
  const [activeId, setActiveId] = useState<string>('');

  useEffect(() => {
    // Allow DOM to render MDX
    const timeout = setTimeout(() => {
      const article = document.querySelector('article');
      if (!article) return;

      const elements = Array.from(article.querySelectorAll('h2, h3')) as HTMLHeadingElement[];
      
      // Auto-assign IDs if remark-slug wasn't used
      elements.forEach(el => {
        if (!el.id) {
          el.id = el.innerText.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
        }
      });

      const items = elements.map(el => ({
        id: el.id,
        title: el.innerText,
        level: Number(el.tagName.replace('H', ''))
      }));
      
      setHeadings(items);

      // Setup intersection observer
      const observer = new IntersectionObserver((entries) => {
        // Iterate through entries and set active if intersecting
        entries.forEach(entry => {
          if (entry.isIntersecting) {
            setActiveId(entry.target.id);
          }
        });
      }, { rootMargin: '0px 0px -80% 0px' });

      elements.forEach(el => observer.observe(el));

      return () => observer.disconnect();
    }, 100);

    return () => clearTimeout(timeout);
  }, []);

  if (headings.length === 0) return null;

  return (
    <nav className="w-[200px] shrink-0 sticky top-[76px] hidden xl:block select-none h-fit max-h-[calc(100vh-100px)] overflow-y-auto scrollbar-hide">
      <h4 className="text-[12px] font-medium text-text-primary mb-4 pb-2 border-b border-border-subtle">
        On this page
      </h4>
      <div className="flex flex-col gap-1.5 border-l border-border-default">
        {headings.map((h, i) => (
          <a
            key={i}
            href={`#${h.id}`}
            className={cn(
              "text-[12px] hover:text-text-primary transition-colors py-1 block pr-2 -ml-[1px] border-l outline-none focus-visible:ring-1 focus-visible:ring-indigo-500 rounded-r-sm",
              h.level === 3 ? "pl-5" : "pl-3",
              activeId === h.id 
                ? "text-text-primary border-indigo-500 font-medium bg-indigo-500/5 hover:bg-indigo-500/10" 
                : "text-text-muted border-transparent hover:border-border-strong"
            )}
            onClick={(e) => {
              e.preventDefault();
              const el = document.getElementById(h.id);
              if (el) {
                const y = el.getBoundingClientRect().top + window.scrollY - 70;
                window.scrollTo({ top: y, behavior: 'smooth' });
                window.history.pushState(null, '', `#${h.id}`);
              }
            }}
          >
            {h.title}
          </a>
        ))}
      </div>
    </nav>
  );
}
