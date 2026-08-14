import Link from 'next/link';

export function Footer() {
  return (
    <footer className="bg-bg-surface border-t border-border-subtle py-12">
      <div className="w-full max-w-7xl mx-auto px-6">
        <div className="flex flex-col md:flex-row items-center justify-between mb-8 gap-4">
          <Link href="/" className="font-sans font-semibold text-base text-text-primary outline-none focus-visible:ring-2 focus-visible:ring-signal rounded-sm transition-opacity hover:opacity-80">
            loafer
          </Link>
          <div className="flex items-center gap-6 text-[13px] text-text-secondary">
            <Link href="/docs" className="hover:text-text-primary hover:underline underline-offset-4 decoration-border-strong outline-none focus-visible:ring-2 focus-visible:ring-signal rounded-sm transition-colors">Docs</Link>
            <a href="https://github.com/lupppig/loafer" target="_blank" rel="noopener noreferrer" className="hover:text-text-primary hover:underline underline-offset-4 decoration-border-strong outline-none focus-visible:ring-2 focus-visible:ring-signal rounded-sm transition-colors">GitHub</a>
            <Link href="/changelog" className="hover:text-text-primary hover:underline underline-offset-4 decoration-border-strong outline-none focus-visible:ring-2 focus-visible:ring-signal rounded-sm transition-colors">Changelog</Link>
            <a href="#" className="hover:text-text-primary hover:underline underline-offset-4 decoration-border-strong outline-none focus-visible:ring-2 focus-visible:ring-signal rounded-sm transition-colors">Status</a>
          </div>
          <div className="text-[13px] text-text-muted select-none">
            MIT License
          </div>
        </div>
        <div className="text-[11px] text-text-muted text-center pt-8 border-t border-border-subtle/50 select-none">
          Built by Darasimi Kelani. Open source.
        </div>
      </div>
    </footer>
  );
}
