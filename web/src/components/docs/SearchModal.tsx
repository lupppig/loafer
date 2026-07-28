'use client'

import { useCallback, useEffect, useMemo, useState, useRef } from 'react';
import Fuse from 'fuse.js';
import { Search } from 'lucide-react';
import { useRouter } from 'next/navigation';

interface SearchItem {
  title: string;
  url: string;
  excerpt: string;
  section: string;
}

export function SearchModal({ isOpen, onClose }: { isOpen: boolean, onClose: () => void }) {
  const [query, setQuery] = useState('');
  const [index, setIndex] = useState<SearchItem[]>([]);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const router = useRouter();
  const closeModal = useCallback(() => {
    setQuery('');
    setSelectedIndex(0);
    onClose();
  }, [onClose]);

  // Load index on mount
  useEffect(() => {
    fetch('/search-index.json')
      .then(r => r.json())
      .then(data => setIndex(data))
      .catch(e => console.error('Failed to load search index', e));
  }, []);

  const results = useMemo(() => {
    if (!query) return [];
    const fuse = new Fuse(index, {
      keys: ['title', 'excerpt', 'section'],
      threshold: 0.3,
      includeMatches: true
    });
    return fuse.search(query).slice(0, 8).map(r => r.item);
  }, [query, index]);

  useEffect(() => {
    if (!isOpen) return;

    const focusTimer = window.setTimeout(() => inputRef.current?.focus(), 50);

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closeModal();
      if (e.key === 'ArrowDown' && results.length > 0) {
        e.preventDefault();
        setSelectedIndex(s => Math.min(s + 1, results.length - 1));
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        setSelectedIndex(s => Math.max(s - 1, 0));
      }
      if (e.key === 'Enter' && results[selectedIndex]) {
        e.preventDefault();
        router.push(results[selectedIndex].url);
        closeModal();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => {
      window.clearTimeout(focusTimer);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [isOpen, results, selectedIndex, router, closeModal]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-[100] flex items-start justify-center pt-[15vh] sm:pt-[20vh] px-4">
      {/* Backdrop */}
      <div 
        className="fixed inset-0 bg-bg-base/80 backdrop-blur-sm"
        onClick={closeModal}
      />
      
      {/* Modal */}
      <div className="relative w-full max-w-[560px] bg-bg-elevated border border-border-strong text-text-primary rounded-md shadow-2xl overflow-hidden shadow-black/50">
        <div className="flex items-center px-4 h-12 border-b border-border-subtle">
          <Search className="w-4 h-4 text-text-muted mr-3" />
          <input
            ref={inputRef}
            className="flex-1 bg-transparent outline-none placeholder:text-text-muted text-[14px]"
            placeholder="Search documentation..."
            value={query}
            onChange={e => {
              setQuery(e.target.value);
              setSelectedIndex(0);
            }}
          />
          <kbd className="hidden sm:inline-block font-sans text-[10px] uppercase font-semibold text-text-muted border border-border-subtle rounded-sm px-1.5 py-0.5 ml-2">ESC</kbd>
        </div>

        {results.length > 0 && (
          <div className="max-h-[300px] overflow-y-auto p-2">
            {results.map((res, i) => (
              <div 
                key={i} 
                className={`p-3 rounded-sm cursor-pointer transition-colors ${i === selectedIndex ? 'bg-bg-overlay' : ''}`}
                onMouseEnter={() => setSelectedIndex(i)}
                onClick={() => { router.push(res.url); closeModal(); }}
              >
                <div className="text-[12px] font-semibold text-text-primary mb-1">
                  {res.title} <span className="text-text-muted font-normal ml-2">{res.section}</span>
                </div>
                <div className="text-[12px] text-text-secondary line-clamp-1">
                  {res.excerpt}
                </div>
              </div>
            ))}
          </div>
        )}
        
        {query && results.length === 0 && (
          <div className="p-8 text-center text-[13px] text-text-muted">
            No results found for "{query}".
          </div>
        )}
      </div>
    </div>
  );
}
