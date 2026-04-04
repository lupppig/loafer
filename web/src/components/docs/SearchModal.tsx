import React, { useEffect, useState, useRef } from 'react';
import Fuse from 'fuse.js';
import { Search } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

interface SearchItem {
  title: string;
  url: string;
  excerpt: string;
  section: string;
}

export function SearchModal({ isOpen, onClose }: { isOpen: boolean, onClose: () => void }) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchItem[]>([]);
  const [index, setIndex] = useState<SearchItem[]>([]);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const navigate = useNavigate();

  // Load index on mount
  useEffect(() => {
    fetch('/search-index.json')
      .then(r => r.json())
      .then(data => setIndex(data))
      .catch(e => console.error('Failed to load search index', e));
  }, []);

  // Cmd+K shortcut
  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === 'k' && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        if (isOpen) onClose();
        else onClose(); // parent handles open
      }
    };
    document.addEventListener('keydown', down);
    return () => document.removeEventListener('keydown', down);
  }, [isOpen, onClose]);

  // Handle fuse seach
  useEffect(() => {
    if (!query) {
      setResults([]);
      return;
    }
    const fuse = new Fuse(index, {
      keys: ['title', 'excerpt', 'section'],
      threshold: 0.3,
      includeMatches: true
    });
    const res = fuse.search(query).slice(0, 8).map(r => r.item);
    setResults(res);
    setSelectedIndex(0);
  }, [query, index]);

  // Handle navigation
  useEffect(() => {
    if (!isOpen) {
      setQuery('');
      return;
    }
    setTimeout(() => inputRef.current?.focus(), 50);

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setSelectedIndex(s => Math.min(s + 1, results.length - 1));
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        setSelectedIndex(s => Math.max(s - 1, 0));
      }
      if (e.key === 'Enter' && results[selectedIndex]) {
        e.preventDefault();
        navigate(results[selectedIndex].url);
        onClose();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, results, selectedIndex, navigate, onClose]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-[100] flex items-start justify-center pt-[15vh] sm:pt-[20vh] px-4">
      {/* Backdrop */}
      <div 
        className="fixed inset-0 bg-bg-base/80 backdrop-blur-sm"
        onClick={onClose}
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
            onChange={e => setQuery(e.target.value)}
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
                onClick={() => { navigate(res.url); onClose(); }}
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
