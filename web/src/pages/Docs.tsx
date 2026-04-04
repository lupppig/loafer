import React, { useState, useEffect } from 'react';
import { Helmet } from 'react-helmet-async';
import { Outlet, Navigate, useLocation } from 'react-router-dom';
import { LeftNav } from '../components/docs/LeftNav';
import { TableOfContents } from '../components/docs/TableOfContents';
import { SearchModal } from '../components/docs/SearchModal';
import { MDXProvider } from '@mdx-js/react';
import { MDXComponents } from '../components/docs/MDXComponents';

export function Docs() {
  const [searchOpen, setSearchOpen] = useState(false);
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

  if (location.pathname === '/docs' || location.pathname === '/docs/') {
    return <Navigate to="/docs/introduction" replace />;
  }

  // Prev / Next dynamic values could be calculated here based on path

  return (
    <>
      <Helmet>
        <title>Documentation | Loafer</title>
      </Helmet>
      
      <div className="flex w-full max-w-[1440px] mx-auto items-start">
        <LeftNav onSearchClick={() => setSearchOpen(true)} />
        
        <main className="flex-1 min-w-0 flex justify-center px-6 md:px-12 pt-12 pb-24 relative">
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
