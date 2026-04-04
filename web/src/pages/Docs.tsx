import React, { useState, useEffect } from 'react';
import { Helmet } from 'react-helmet-async';
import { Outlet, Navigate, useLocation } from 'react-router-dom';
import { LeftNav } from '../components/docs/LeftNav';
import { TableOfContents } from '../components/docs/TableOfContents';
import { SearchModal } from '../components/docs/SearchModal';
import { MDXProvider } from '@mdx-js/react';
import { MDXComponents } from '../components/docs/MDXComponents';
import { Menu, X, ChevronRight } from 'lucide-react';
import { cn } from '../utils/cn';

export function Docs() {
  const [searchOpen, setSearchOpen] = useState(false);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const location = useLocation();

  // Handle specific CMD+K logic locally across the docs layout
  useEffect(() => {
    const handleDown = (e: KeyboardEvent) => {
      if (e.key === 'k' && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setSearchOpen(true);
      }
    };
    document.addEventListener('keydown', handleDown);
    return () => document.removeEventListener('keydown', handleDown);
  }, []);

  // Close mobile nav on route change
  useEffect(() => {
    setMobileNavOpen(false);
  }, [location.pathname]);

  if (location.pathname === '/docs' || location.pathname === '/docs/') {
    return <Navigate to="/docs/introduction" replace />;
  }

  // Extract current page title for mobile header
  const currentPath = location.pathname;
  const pageTitle = currentPath.split('/').pop()?.replace(/-/g, ' ') || 'Docs';

  return (
    <>
      <Helmet>
        <title>Documentation | Loafer</title>
      </Helmet>
      
      {/* Mobile Docs Header */}
      <div className="lg:hidden sticky top-[52px] z-30 flex items-center justify-between w-full h-10 px-4 bg-bg-surface border-b border-border-subtle backdrop-blur-md bg-bg-surface/90">
        <button 
          onClick={() => setMobileNavOpen(true)}
          className="flex items-center gap-2 text-[13px] font-medium text-text-secondary hover:text-text-primary transition-colors outline-none"
        >
          <Menu className="w-4 h-4" />
          <span>Menu</span>
        </button>
        <div className="flex items-center gap-1.5 text-[12px] text-text-muted font-medium capitalize">
          Docs <ChevronRight className="w-3 h-3 opacity-50" /> {pageTitle}
        </div>
      </div>

      <div className="flex w-full max-w-[1440px] mx-auto items-start relative">
        {/* Sidebar Overlay for Mobile */}
        <div 
          className={cn(
            "fixed inset-0 z-50 bg-bg-base/60 backdrop-blur-sm lg:hidden transition-opacity duration-300",
            mobileNavOpen ? "opacity-100 pointer-events-auto" : "opacity-0 pointer-events-none"
          )}
          onClick={() => setMobileNavOpen(false)}
        />

        <LeftNav 
          onSearchClick={() => setSearchOpen(true)} 
          isMobileOpen={mobileNavOpen}
          onClose={() => setMobileNavOpen(false)}
        />
        
        <main className="flex-1 min-w-0 flex justify-center px-6 md:px-12 pt-8 lg:pt-12 pb-24 relative">
          <article className="w-full max-w-[680px]">
            <MDXProvider components={MDXComponents}>
              <Outlet />
            </MDXProvider>
          </article>
        </main>

        <TableOfContents />
      </div>

      <SearchModal isOpen={searchOpen} onClose={() => setSearchOpen(false)} />
    </>
  );
}
