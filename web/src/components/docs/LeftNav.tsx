import React from 'react';
import { NavLink } from 'react-router-dom';
import { Search, ArrowUpRight, X } from 'lucide-react';
import { cn } from '../../utils/cn';

const NAV_GROUPS = [
  {
    title: 'Getting Started',
    items: [
      { name: 'Introduction', path: '/docs/introduction' },
      { name: 'Installation', path: '/docs/installation' },
      { name: 'Quickstart', path: '/docs/quickstart' },
    ]
  },
  {
    title: 'Pipelines',
    items: [
      { name: 'Pipeline Configuration', path: '/docs/pipelines' },
    ]
  },
  {
    title: 'Connectors',
    items: [
      { name: 'Sources', path: '/docs/sources' },
      { name: 'Targets', path: '/docs/targets' },
    ]
  },
  {
    title: 'Transform',
    items: [
      { name: 'Transform Modes', path: '/docs/transform' },
    ]
  },
  {
    title: 'Reference',
    items: [
      { name: 'Scheduling', path: '/docs/scheduling' },
      { name: 'CLI Reference', path: '/docs/cli' },
      { name: 'Docker', path: '/docs/docker' },
    ]
  }
];

export function LeftNav({ onSearchClick, isMobileOpen, onClose }: { onSearchClick: () => void, isMobileOpen?: boolean, onClose?: () => void }) {
  return (
    <nav className={cn(
      "w-[240px] shrink-0 bg-bg-base border-r border-border-subtle h-[calc(100vh-52px)] sticky top-[52px] overflow-y-auto select-none scrollbar-hide z-50 transition-transform duration-300",
      "lg:block lg:translate-x-0",
      isMobileOpen ? "fixed left-0 translate-x-0" : "fixed -translate-x-full lg:static",
    )}>
      <div className="p-4 pt-6 flex items-center gap-2">
        <button
          onClick={onSearchClick}
          className="flex-1 flex items-center justify-between h-8 px-3 bg-bg-surface border border-border-default rounded-md text-[13px] text-text-muted hover:text-text-secondary hover:border-border-strong transition-colors outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 shadow-sm group"
        >
          <span className="flex items-center gap-2">
            <Search className="w-3.5 h-3.5 group-hover:text-text-primary transition-colors" />
            Search
          </span>
          <kbd className="hidden sm:inline-flex font-sans text-[10px] uppercase font-semibold bg-bg-elevated px-1.5 py-0.5 rounded-[3px] border border-border-subtle">⌘K</kbd>
        </button>

        {onClose && (
          <button 
            onClick={onClose}
            className="lg:hidden p-1.5 text-text-muted hover:text-text-primary transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        )}
      </div>

      <div className="px-3 pb-8">
        {NAV_GROUPS.map((group, i) => (
          <div key={i} className="mb-6 last:mb-0">
            <h4 className="text-[10px] font-semibold text-text-muted uppercase tracking-[0.08em] mb-2 px-2">
              {group.title}
            </h4>
            <div className="flex flex-col gap-0.5">
              {group.items.map((item, j) => (
                <NavLink
                  key={j}
                  to={item.path}
                  onClick={() => onClose?.()}
                  className={({ isActive }) => cn(
                    "h-7 flex items-center px-2 text-[13px] rounded-sm transition-colors outline-none focus-visible:ring-2 focus-visible:ring-indigo-500",
                    isActive 
                      ? "text-text-primary bg-indigo-950 font-medium border-l-2 border-indigo-500 pl-1.5" 
                      : "text-text-secondary hover:bg-bg-overlay hover:text-text-primary border-l-2 border-transparent pl-2"
                  )}
                >
                  {item.name}
                </NavLink>
              ))}
            </div>
          </div>
        ))}

        <div className="mt-8 border-t border-border-subtle pt-6">
          <h4 className="text-[10px] font-semibold text-text-muted uppercase tracking-[0.08em] mb-2 px-2">
            Community
          </h4>
          <div className="flex flex-col gap-0.5">
            <a 
              href="https://github.com/lupppig/loafer" 
              target="_blank" 
              rel="noopener noreferrer"
              className="h-7 flex items-center justify-between px-2 text-[13px] text-text-secondary rounded-sm hover:bg-bg-overlay hover:text-text-primary transition-colors group outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 border-l-2 border-transparent"
            >
              GitHub
              <ArrowUpRight className="w-3.5 h-3.5 opacity-0 group-hover:opacity-100 transition-opacity text-text-muted" />
            </a>
            <NavLink 
              to="/changelog" 
              onClick={() => onClose?.()}
              className="h-7 flex items-center justify-between px-2 text-[13px] text-text-secondary rounded-sm hover:bg-bg-overlay hover:text-text-primary transition-colors group outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 border-l-2 border-transparent"
            >
              Changelog
              <ArrowUpRight className="w-3.5 h-3.5 opacity-0 group-hover:opacity-100 transition-opacity text-text-muted" />
            </NavLink>
          </div>
        </div>
      </div>
    </nav>
  );
}
