import {
  ArrowLeft,
  Check,
  CheckCircle2,
  CircleDot,
  Code2,
  Play,
  ServerCog,
  ShieldCheck,
  Sparkles,
  TerminalSquare,
  X,
} from 'lucide-react';
import { formatPreviewMessage } from './data';
import type { PreviewState } from './types';

interface TransformInspectorProps {
  open: boolean;
  transform: string;
  previewState: PreviewState;
  progress: number;
  onTransformChange: (value: string) => void;
  onClose: () => void;
  onOpen: () => void;
  onValidate: () => void;
  onRunPreview: () => void;
}

export function TransformInspector({
  open,
  transform,
  previewState,
  progress,
  onTransformChange,
  onClose,
  onOpen,
  onValidate,
  onRunPreview,
}: TransformInspectorProps) {
  return (
    <>
      <aside className={`studio-inspector ${open ? 'is-open' : ''}`}>
        <div className="studio-inspector-header">
          <div>
            <span>Transform step 2</span>
            <h2>Normalize revenue</h2>
          </div>
          <button type="button" aria-label="Close inspector" onClick={onClose}>
            <X size={17} />
          </button>
        </div>

        <div className="studio-inspector-content">
          <div className="studio-field">
            <label htmlFor="transform-instruction">
              <Sparkles size={14} />
              Transformation instruction
            </label>
            <textarea
              id="transform-instruction"
              value={transform}
              onChange={(event) => onTransformChange(event.target.value)}
              rows={5}
            />
            <p>The model receives schema metadata and five sample rows, never the full dataset.</p>
          </div>

          <div className="studio-contract-card">
            <div className="studio-contract-heading">
              <ShieldCheck size={16} />
              Execution contract
            </div>
            <div className="studio-contract-row">
              <span>Class</span><strong>Row-local</strong>
            </div>
            <div className="studio-contract-row">
              <span>Compute</span><strong>Target warehouse</strong>
            </div>
            <div className="studio-contract-row">
              <span>Memory</span><strong>Bounded preview</strong>
            </div>
          </div>

          <div className="studio-artifact-card">
            <div className="studio-artifact-header">
              <Code2 size={15} />
              <span>Generated SQL artifact</span>
              <button type="button">Review</button>
            </div>
            <code>sha256:7c2a…91e4</code>
            <p>Versioned with this pipeline revision.</p>
          </div>

          <PreviewCard
            previewState={previewState}
            progress={progress}
            onValidate={onValidate}
            onRunPreview={onRunPreview}
          />
        </div>

        <div className="studio-inspector-footer">
          <button className="studio-button studio-button-secondary" type="button">
            <TerminalSquare size={14} /> Open YAML
          </button>
          <button className="studio-button studio-button-primary" type="button">
            Apply changes <Check size={14} />
          </button>
        </div>
      </aside>

      {!open && (
        <button className="studio-reopen-panel" type="button" onClick={onOpen}>
          <ServerCog size={16} />
          Inspector
          <ArrowLeft size={14} />
        </button>
      )}
    </>
  );
}

interface PreviewCardProps {
  previewState: PreviewState;
  progress: number;
  onValidate: () => void;
  onRunPreview: () => void;
}

function PreviewCard({
  previewState,
  progress,
  onValidate,
  onRunPreview,
}: PreviewCardProps) {
  return (
    <div className="studio-preview-card" aria-live="polite">
      <div className="studio-preview-heading">
        <span>Bounded preview</span>
        <span className={`studio-preview-state state-${previewState}`}>
          {previewState === 'complete' ? <CheckCircle2 size={13} /> : <CircleDot size={13} />}
          {previewState}
        </span>
      </div>
      <p>{formatPreviewMessage(previewState)}</p>
      <div className="studio-progress-track">
        <span style={{ width: `${progress}%` }} />
      </div>
      {previewState === 'ready' ? (
        <button
          className="studio-button studio-button-primary studio-wide-button"
          type="button"
          onClick={onRunPreview}
        >
          <Play size={14} fill="currentColor" /> Run 10k-row preview
        </button>
      ) : (
        <button
          className="studio-button studio-button-secondary studio-wide-button"
          type="button"
          onClick={onValidate}
          disabled={previewState === 'validating' || previewState === 'running'}
        >
          <ShieldCheck size={14} /> Validate pipeline
        </button>
      )}
    </div>
  );
}
