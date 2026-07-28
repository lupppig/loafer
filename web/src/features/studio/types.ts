export type PreviewState = 'idle' | 'validating' | 'ready' | 'running' | 'complete';
export type Persona = 'engineer' | 'analyst' | 'scientist' | 'backend';
export type EditorMode = 'guided' | 'yaml';

export interface RecentRun {
  id: string;
  pipeline: string;
  status: 'Succeeded' | 'Running' | 'Warning';
  rows: string;
  duration: string;
  started: string;
}
