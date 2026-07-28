import type { Persona, PreviewState, RecentRun } from './types';

export const pipelineYaml = `name: revenue_normalization
mode: elt

source:
  url: \${ANALYTICS_DB}
  query: SELECT * FROM public.orders

transform:
  - name: valid_orders
    query: |
      SELECT * FROM {{source}}
      WHERE status = 'paid'
  - name: normalize_revenue
    instruction: >
      Convert totals to USD and add
      net_revenue after discounts.

target:
  url: \${WAREHOUSE_URL}
  table: analytics.orders_clean
  write_mode: upsert
  key: order_id

incremental:
  column: updated_at`;

export const personas: { id: Persona; label: string }[] = [
  { id: 'engineer', label: 'Data engineer' },
  { id: 'analyst', label: 'Analyst' },
  { id: 'scientist', label: 'Data scientist' },
  { id: 'backend', label: 'Backend' },
];

export const recentRuns: RecentRun[] = [
  {
    id: 'run_8f31',
    pipeline: 'revenue_normalization',
    status: 'Succeeded',
    rows: '42.8M',
    duration: '18m 42s',
    started: '12 minutes ago',
  },
  {
    id: 'run_8f30',
    pipeline: 'customer_360',
    status: 'Running',
    rows: '9.7M',
    duration: '6m 14s',
    started: '6 minutes ago',
  },
  {
    id: 'run_8f2d',
    pipeline: 'event_archive',
    status: 'Warning',
    rows: '84.1M',
    duration: '51m 08s',
    started: '3 hours ago',
  },
];

export function formatPreviewMessage(state: PreviewState) {
  if (state === 'validating') return 'Validating configuration and transform contract…';
  if (state === 'ready') return 'Validated. A bounded 10,000-row preview is ready.';
  if (state === 'running') return 'Running preview against an isolated sample…';
  if (state === 'complete') return 'Preview complete: 9,842 rows passed, 158 rejected.';
  return 'Validate before previewing. No production data will be written.';
}
