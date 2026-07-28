'use client'

import { useEffect, useMemo, useState } from 'react'
import {
  Check,
  History,
  MoreHorizontal,
  Play,
  Users,
} from 'lucide-react';
import { personas } from '../features/studio/data';
import { PipelineBuilder } from '../features/studio/PipelineBuilder';
import { RunActivity } from '../features/studio/RunActivity';
import { StudioSidebar, StudioTopbar } from '../features/studio/StudioChrome';
import { TransformInspector } from '../features/studio/TransformInspector';
import type {
  EditorMode,
  Persona,
  PreviewState,
} from '../features/studio/types';
import './Studio.css';

const initialTransform =
  'Convert totals to USD, remove refunded orders, and calculate net revenue after discounts.';

export function Studio() {
  const [persona, setPersona] = useState<Persona>('engineer');
  const [editorMode, setEditorMode] = useState<EditorMode>('guided');
  const [previewState, setPreviewState] = useState<PreviewState>('idle');
  const [panelOpen, setPanelOpen] = useState(true);
  const [transform, setTransform] = useState(initialTransform);

  useEffect(() => {
    if (previewState !== 'validating' && previewState !== 'running') return;

    const nextState = previewState === 'validating' ? 'ready' : 'complete';
    const timer = window.setTimeout(() => setPreviewState(nextState), 900);
    return () => window.clearTimeout(timer);
  }, [previewState]);

  const progress = useMemo(() => {
    if (previewState === 'complete') return 100;
    if (previewState === 'running') return 64;
    if (previewState === 'ready') return 18;
    return 0;
  }, [previewState]);

  return (
    <div className="studio-shell">
      <StudioTopbar />
      <StudioSidebar />

      <main className="studio-main">
        <div className="studio-breadcrumbs">
          <button type="button">Pipelines</button>
          <span>/</span>
          <span>revenue_normalization</span>
        </div>

        <section className="studio-page-header">
          <div>
            <div className="studio-title-row">
              <h1>Revenue normalization</h1>
              <span className="studio-env-badge">Development</span>
              <span className="studio-saved"><Check size={13} /> Saved</span>
            </div>
            <p>PostgreSQL orders into a modeled warehouse table, every 30 minutes.</p>
          </div>
          <div className="studio-header-actions">
            <button className="studio-button studio-button-secondary" type="button">
              <History size={15} />
              Versions
            </button>
            <button
              className="studio-button studio-button-primary"
              type="button"
              onClick={() => setPreviewState('running')}
            >
              <Play size={14} fill="currentColor" />
              Run preview
            </button>
            <button className="studio-icon-button studio-more" type="button" aria-label="More actions">
              <MoreHorizontal size={18} />
            </button>
          </div>
        </section>

        <div className="studio-viewbar">
          <div className="studio-view-tabs" role="tablist" aria-label="Pipeline view">
            <button className="is-active" type="button" role="tab" aria-selected="true">Build</button>
            <button type="button" role="tab" aria-selected="false">Runs</button>
            <button type="button" role="tab" aria-selected="false">Quality</button>
            <button type="button" role="tab" aria-selected="false">Settings</button>
          </div>
          <div className="studio-persona-switcher">
            <Users size={14} />
            <label htmlFor="persona">View for</label>
            <select
              id="persona"
              value={persona}
              onChange={(event) => setPersona(event.target.value as Persona)}
            >
              {personas.map(({ id, label }) => <option key={id} value={id}>{label}</option>)}
            </select>
          </div>
        </div>

        <PipelineBuilder mode={editorMode} onModeChange={setEditorMode} />
        <RunActivity />
      </main>

      <TransformInspector
        open={panelOpen}
        transform={transform}
        previewState={previewState}
        progress={progress}
        onTransformChange={setTransform}
        onClose={() => setPanelOpen(false)}
        onOpen={() => setPanelOpen(true)}
        onValidate={() => setPreviewState('validating')}
        onRunPreview={() => setPreviewState('running')}
      />
    </div>
  );
}
