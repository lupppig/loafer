import {
  Activity,
  ArrowRight,
  ChevronDown,
  CircleDot,
  Database,
  FileText,
  FlaskConical,
  GitBranch,
  LayoutDashboard,
  Plus,
  Search,
  Settings,
  ShieldCheck,
  Zap,
} from 'lucide-react';
import Link from 'next/link';

const navItems = [
  { label: 'Overview', icon: LayoutDashboard },
  { label: 'Pipelines', icon: GitBranch, active: true, count: '12' },
  { label: 'Runs', icon: Activity },
  { label: 'Connections', icon: Database },
  { label: 'Data explorer', icon: Search },
  { label: 'Quality', icon: ShieldCheck },
];

export function StudioTopbar() {
  return (
    <header className="studio-topbar">
      <div className="studio-brand">
        <Link href="/" className="studio-logo" aria-label="Back to Loafer home">
          <span className="studio-logo-mark"><Zap size={14} strokeWidth={2.5} /></span>
          <span>loafer</span>
        </Link>
        <span className="studio-brand-divider" />
        <button className="studio-workspace-button" type="button">
          Acme analytics <ChevronDown size={14} />
        </button>
      </div>

      <div className="studio-preview-notice" role="status">
        <FlaskConical size={14} />
        Control-plane preview · sample data
      </div>

      <div className="studio-top-actions">
        <button className="studio-icon-button" type="button" aria-label="Open activity">
          <CircleDot size={17} />
        </button>
        <button className="studio-icon-button" type="button" aria-label="Open settings">
          <Settings size={17} />
        </button>
        <button className="studio-avatar" type="button" aria-label="Open account menu">AK</button>
      </div>
    </header>
  );
}

export function StudioSidebar() {
  return (
    <aside className="studio-sidebar">
      <nav aria-label="Studio navigation">
        <p className="studio-nav-label">Workspace</p>
        {navItems.map(({ label, icon: Icon, active, count }) => (
          <button
            key={label}
            className={`studio-nav-item ${active ? 'is-active' : ''}`}
            type="button"
          >
            <Icon size={16} />
            <span>{label}</span>
            {count && <span className="studio-nav-count">{count}</span>}
          </button>
        ))}
      </nav>

      <div className="studio-sidebar-section">
        <div className="studio-section-heading">
          <span>Pinned</span>
          <button type="button" aria-label="Add pinned pipeline"><Plus size={14} /></button>
        </div>
        <button className="studio-pipeline-link is-current" type="button">
          <span className="studio-pipeline-dot is-green" />
          revenue_normalization
        </button>
        <button className="studio-pipeline-link" type="button">
          <span className="studio-pipeline-dot is-blue" />
          customer_360
        </button>
        <button className="studio-pipeline-link" type="button">
          <span className="studio-pipeline-dot is-amber" />
          event_archive
        </button>
      </div>

      <div className="studio-sidebar-footer">
        <Link href="/docs" className="studio-sidebar-help">
          <FileText size={15} />
          Documentation
          <ArrowRight size={13} />
        </Link>
        <div className="studio-runtime">
          <span className="studio-runtime-dot" />
          Runtime API not connected
        </div>
      </div>
    </aside>
  );
}
