import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { ArrowUpRight, Star } from 'lucide-react';
import { NavLink } from './NavLink';
import { Button } from '../common/Button';
import { cn } from '../../utils/cn';

export function Topbar() {
  const [scrolled, setScrolled] = useState(false);

  useEffect(() => {
    const handleScroll = () => {
      setScrolled(window.scrollY > 20);
    };
    window.addEventListener('scroll', handleScroll, { passive: true });
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  return (
    <header
      className={cn(
        'fixed top-0 inset-x-0 z-50 h-[52px] flex items-center justify-center transition-all duration-200 border-b',
        scrolled 
          ? 'bg-bg-base/80 backdrop-blur-md border-border-subtle' 
          : 'bg-bg-base border-transparent'
      )}
    >
      <div className="w-full max-w-7xl px-6 flex items-center justify-between">
        <Link 
          to="/" 
          className="font-sans font-semibold text-base text-text-primary flex items-center outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 rounded-sm transition-opacity hover:opacity-80"
        >
          loafer
        </Link>

        <nav className="hidden md:flex items-center gap-6">
          <NavLink to="/docs">Docs</NavLink>
          <a href="https://github.com/lupppig/loafer" target="_blank" rel="noopener noreferrer" className="flex items-center text-[13px] font-medium text-text-muted hover:text-text-primary transition-colors h-full px-2 mt-[2px] group">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="w-4 h-4 mr-1.5 opacity-70 group-hover:opacity-100 transition-opacity">
              <path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4"/>
              <path d="M9 18c-4.51 2-5-2-7-2"/>
            </svg>
            GitHub
            <ArrowUpRight className="w-3.5 h-3.5 opacity-50 group-hover:opacity-100 transition-opacity ml-0.5" />
          </a>
          <NavLink to="/changelog">Changelog</NavLink>
          
          <div className="w-px h-4 bg-border-default mx-2" />

          <a 
            href="https://github.com/lupppig/loafer"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 rounded-sm"
          >
            <Button variant="outline" size="sm" className="h-7 px-2.5 text-xs gap-1.5 border-border-default" tabIndex={-1}>
              <Star className="w-3.5 h-3.5" />
              <span>Star on GitHub</span>
              <span className="text-text-muted ml-0.5">1.2k</span>
            </Button>
          </a>
        </nav>
        
        {/* Mobile Nav toggle for MVP: just wordmark + icon */}
        <div className="flex md:hidden items-center">
          <a
            href="https://github.com/lupppig/loafer"
            target="_blank"
            rel="noopener noreferrer"
            className="text-text-secondary hover:text-text-primary p-2 outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 rounded-sm"
            aria-label="GitHub"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="w-4 h-4 mr-2 opacity-80">
              <path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4"/>
              <path d="M9 18c-4.51 2-5-2-7-2"/>
            </svg>
          </a>
        </div>
      </div>
    </header>
  );
}
