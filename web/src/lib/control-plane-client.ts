/** Typed browser client for the same loaferd resources used by the Python CLI. */

import type {
  PipelineResponse,
  RunResponse,
  WorkspaceResponse,
} from './control-plane-types.generated'

export type Workspace = WorkspaceResponse
export type PipelineVersion = PipelineResponse
export type Run = RunResponse

type Problem = { detail?: string; request_id?: string }

export class ControlPlaneError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly requestId?: string,
  ) {
    super(message)
  }
}

export async function listWorkspaces(): Promise<Workspace[]> {
  return request('/api/control/workspaces')
}

export async function listPipelines(workspaceId: string): Promise<PipelineVersion[]> {
  return request(`/api/control/workspaces/${encodeURIComponent(workspaceId)}/pipelines`)
}

export async function listRuns(workspaceId: string): Promise<Run[]> {
  return request(`/api/control/workspaces/${encodeURIComponent(workspaceId)}/runs`)
}

export async function createRun(
  workspaceId: string,
  pipelineVersionId: string,
  idempotencyKey = crypto.randomUUID(),
): Promise<Run> {
  return request(`/api/control/workspaces/${encodeURIComponent(workspaceId)}/runs`, {
    method: 'POST',
    headers: { 'Idempotency-Key': idempotencyKey },
    body: JSON.stringify({ pipeline_version_id: pipelineVersionId }),
  })
}

export async function cancelRun(workspaceId: string, runId: string): Promise<Run> {
  return request(
    `/api/control/workspaces/${encodeURIComponent(workspaceId)}/runs/${encodeURIComponent(runId)}/cancel`,
    { method: 'POST' },
  )
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  if (init.body) headers.set('Content-Type', 'application/json')
  const response = await fetch(path, { ...init, headers, cache: 'no-store' })
  if (!response.ok) {
    const problem = (await response.json().catch(() => ({}))) as Problem
    throw new ControlPlaneError(
      problem.detail ?? `Loafer API returned ${response.status}`,
      response.status,
      problem.request_id,
    )
  }
  return (await response.json()) as T
}
