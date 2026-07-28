import {
  ArrowRight,
  Braces,
  Check,
  Clock3,
  Database,
  GitBranch,
  MoreHorizontal,
  Plus,
  Sparkles,
  Table2,
  WandSparkles,
} from 'lucide-react';
import { pipelineYaml } from './data';
import type { EditorMode } from './types';

interface PipelineBuilderProps {
  mode: EditorMode;
  onModeChange: (mode: EditorMode) => void;
}

export function PipelineBuilder({ mode, onModeChange }: PipelineBuilderProps) {
  return (
    <section className="studio-builder">
      <div className="studio-builder-toolbar">
        <div className="studio-segmented" role="group" aria-label="Editor mode">
          <button
            className={mode === 'guided' ? 'is-active' : ''}
            type="button"
            onClick={() => onModeChange('guided')}
          >
            <WandSparkles size={14} /> Guided
          </button>
          <button
            className={mode === 'yaml' ? 'is-active' : ''}
            type="button"
            onClick={() => onModeChange('yaml')}
          >
            <Braces size={14} /> YAML
          </button>
        </div>
        <div className="studio-builder-meta">
          <span><GitBranch size={13} /> v18 draft</span>
          <span><Clock3 size={13} /> edited 4m ago</span>
        </div>
      </div>

      {mode === 'guided' ? <GuidedPipeline /> : <YamlEditor />}
    </section>
  );
}

function GuidedPipeline() {
  return (
    <div className="studio-canvas">
      <div className="studio-flow" aria-label="Pipeline flow">
        <PipelineNode
          className="source-node"
          icon={<Database size={18} />}
          eyebrow="Source"
          title="Production orders"
          description="PostgreSQL · public.orders"
          status={<><Check size={12} /> Connected</>}
          menuLabel="Source options"
        />

        <PipelineConnector label="incremental" />

        <PipelineNode
          className="transform-node"
          icon={<Sparkles size={18} />}
          eyebrow="Transform · 2 steps"
          title="Clean and normalize"
          description="SQL filter → generated transform artifact"
          status="AI assisted"
          statusClassName="is-purple"
          menuLabel="Transform options"
        />

        <PipelineConnector label="upsert" />

        <PipelineNode
          className="target-node"
          icon={<Table2 size={18} />}
          eyebrow="Target"
          title="Clean orders"
          description="Warehouse · analytics.orders_clean"
          status={<><Check size={12} /> Ready</>}
          menuLabel="Target options"
        />
      </div>

      <div className="studio-add-step">
        <button type="button"><Plus size={15} /> Add transformation step</button>
        <span>SQL, Python, or plain English</span>
      </div>
    </div>
  );
}

interface PipelineNodeProps {
  className: string;
  icon: React.ReactNode;
  eyebrow: string;
  title: string;
  description: string;
  status: React.ReactNode;
  statusClassName?: string;
  menuLabel: string;
}

function PipelineNode({
  className,
  icon,
  eyebrow,
  title,
  description,
  status,
  statusClassName = '',
  menuLabel,
}: PipelineNodeProps) {
  return (
    <article className={`studio-node ${className}`}>
      <div className="studio-node-icon">{icon}</div>
      <div className="studio-node-copy">
        <div className="studio-node-eyebrow">{eyebrow}</div>
        <h2>{title}</h2>
        <p>{description}</p>
      </div>
      <span className={`studio-node-status ${statusClassName}`}>{status}</span>
      <button className="studio-node-menu" type="button" aria-label={menuLabel}>
        <MoreHorizontal size={17} />
      </button>
    </article>
  );
}

function PipelineConnector({ label }: { label: string }) {
  return (
    <div className="studio-connector-line">
      <span>{label}</span>
      <ArrowRight size={16} />
    </div>
  );
}

function YamlEditor() {
  return (
    <div className="studio-code-editor">
      <div className="studio-code-gutter" aria-hidden="true">
        {pipelineYaml.split('\n').map((_, index) => <span key={index}>{index + 1}</span>)}
      </div>
      <pre><code>{pipelineYaml}</code></pre>
    </div>
  );
}
